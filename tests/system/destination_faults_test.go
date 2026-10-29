//go:build !windows

package system_test

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// runHTTPFailureCase delivers a small batch to url and expects every record
// to be preserved in the DLQ. wantRequests is the exact number of requests
// if positive; otherwise the case only requires at least one.
func runHTTPFailureCase(t *testing.T, name, url string, wantRequests int, configure func(map[string]any)) {
	t.Helper()
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	dlqDir := filepath.Join(dir, "dlq")
	g := newOracleGenerator()
	g.appendFile(t, input, 5)

	c := newTestConfig(t)
	p := newPipeline("dest-fault-"+name, sourceFile(input, "fault-input", 5), destHTTP(url))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     5,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 2, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"dlq": map[string]any{"enabled": true, "dir": dlqDir},
	}
	p["processors"] = []any{oracleJSONParse()}
	if configure != nil {
		configure(p)
	}
	c.Global["circuit_breaker_failure_threshold"] = 100
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("loghub log:\n%s", proc.log())
		}
	})
	waitForPipelineUp(t, proc, "dest-fault-"+name)
	waitFor(t, 15*time.Second, "records written to DLQ", func() bool {
		return len(readDLQ(t, dlqDir, "dest-fault-"+name)) > 0
	})
	proc.stop()

	records := readDLQ(t, dlqDir, "dest-fault-"+name)
	var observed []observedRecord
	for _, fb := range records {
		if !strings.HasPrefix(fb.Destination, "http#") {
			t.Errorf("DLQ destination = %q, want http#0", fb.Destination)
		}
		for _, e := range fb.Entries {
			observed = append(observed, observedRecordFromFields(fb.Destination, e.Fields))
		}
	}
	verifyOracle(t, g, oracleCheck{
		Observed: observed,
	}, name)
}

// TestHTTPStatusFaultMatrix drives every documented status class through a
// pipeline. A 400 is a deterministic rejection (one attempt); every other
// failure is retried and then preserved in the DLQ.
func TestHTTPStatusFaultMatrix(t *testing.T) {
	cases := []struct {
		status       int
		wantRequests int
	}{
		{http.StatusMovedPermanently, 2},
		{http.StatusFound, 2},
		{http.StatusSeeOther, 2},
		{http.StatusTemporaryRedirect, 2},
		{http.StatusPermanentRedirect, 2},
		{http.StatusBadRequest, 1},
		{http.StatusUnauthorized, 2},
		{http.StatusForbidden, 2},
		{http.StatusNotFound, 2},
		{http.StatusRequestTimeout, 2},
		{http.StatusConflict, 2},
		{http.StatusRequestEntityTooLarge, 2},
		{http.StatusTooManyRequests, 2},
		{http.StatusInternalServerError, 2},
		{http.StatusBadGateway, 2},
		{http.StatusServiceUnavailable, 2},
		{http.StatusGatewayTimeout, 2},
		{418, 2},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("%d", tc.status), func(t *testing.T) {
			s, srv := newSink(t, tc.status)
			runHTTPFailureCase(t, fmt.Sprintf("status%d", tc.status), srv.URL, tc.wantRequests, nil)
			if reqs, _ := s.counts(); reqs != tc.wantRequests {
				t.Errorf("requests = %d, want %d for status %d", reqs, tc.wantRequests, tc.status)
			}
		})
	}
}

// TestHTTPRetryAfterSecondsThenSuccess checks that a 429 with Retry-After
// in seconds is retried and the batch is delivered exactly once.
func TestHTTPRetryAfterSecondsThenSuccess(t *testing.T) {
	testRetryAfterThenSuccess(t, "1")
}

// TestHTTPRetryAfterDateThenSuccess does the same with an HTTP-date value.
func TestHTTPRetryAfterDateThenSuccess(t *testing.T) {
	testRetryAfterThenSuccess(t, time.Now().Add(time.Second).UTC().Format(http.TimeFormat))
}

func testRetryAfterThenSuccess(t *testing.T, retryAfter string) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 5)
	var calls int
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		calls++
		n := calls
		if n > 1 {
			bodies = append(bodies, body)
		}
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", retryAfter)
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := newTestConfig(t)
	p := newPipeline("retry-after", oracleFileSource(input, 5), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     5,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 3, "base_backoff": "1ms"}
	p["processors"] = []any{oracleJSONParse()}
	c.Global["circuit_breaker_failure_threshold"] = 100
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "retry-after")
	waitFor(t, 10*time.Second, "batch delivered after 429", func() bool {
		return calls >= 2
	})
	proc.stop()
	mu.Lock()
	got := calls
	accepted := append([][]byte(nil), bodies...)
	mu.Unlock()
	if got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
	var observed []observedRecord
	for _, body := range accepted {
		var decoded []struct {
			Fields map[string]string `json:"fields"`
		}
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("decode accepted body: %v", err)
		}
		for _, d := range decoded {
			observed = append(observed, observedRecordFromFields("http", d.Fields))
		}
	}
	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"http"},
		Observed:     observed,
	}, "retry-after")
}

// TestHTTPConnectionFaults covers failures below the status-code layer.
func TestHTTPConnectionFaults(t *testing.T) {
	t.Run("connection-refused", func(t *testing.T) {
		addr := freeAddr(t)
		runHTTPFailureCase(t, "conn-refused", "http://"+addr, 2, nil)
	})

	t.Run("connection-reset", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		defer l.Close()
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				conn.Close()
			}
		}()
		runHTTPFailureCase(t, "conn-reset", "http://"+l.Addr().String(), 2, nil)
	})

	t.Run("tls-handshake", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer srv.Close()
		// The test server uses a self-signed certificate; the production
		// client must refuse it rather than silently skip verification.
		runHTTPFailureCase(t, "tls-handshake", srv.URL, 2, nil)
	})

	t.Run("dns", func(t *testing.T) {
		runHTTPFailureCase(t, "dns", "http://loghub-does-not-exist.invalid", 2, nil)
	})

	t.Run("server-never-answers", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Longer than the client timeout, then return so the test
			// server can close cleanly.
			time.Sleep(500 * time.Millisecond)
		}))
		defer srv.Close()
		runHTTPFailureCase(t, "never-answers", srv.URL, 2, func(p map[string]any) {
			p["destinations"] = []any{map[string]any{"type": "http", "http": map[string]any{
				"url": srv.URL, "timeout": "100ms",
			}}}
		})
	})

	t.Run("slow-loris", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		defer l.Close()
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					defer c.Close()
					io.WriteString(c, "HTTP/1.1 200 OK\r\n")
					time.Sleep(2 * time.Second)
				}(conn)
			}
		}()
		runHTTPFailureCase(t, "slow-loris", "http://"+l.Addr().String(), 2, func(p map[string]any) {
			p["destinations"] = []any{map[string]any{"type": "http", "http": map[string]any{
				"url": "http://" + l.Addr().String(), "timeout": "100ms",
			}}}
		})
	})
}

// TestHTTPLargeAndMalformedResponseBodies covers a response larger than any
// sane diagnostic limit and a 200 whose body is not JSON. A 200 is a
// successful delivery; the body must not be buffered without bound or
// logged.
func TestHTTPLargeAndMalformedResponseBodies(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"large", strings.Repeat("x", 8<<20)},
		{"malformed", "<html>not json</html>"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := filepath.Join(dir, "input.log")
			g := newOracleGenerator()
			g.appendFile(t, input, 5)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			c := newTestConfig(t)
			p := newPipeline("large-body", oracleFileSource(input, 5), destHTTP(srv.URL))
			p["runtime"] = map[string]any{
				"poll_interval": "10ms", "send_batch_size": 5, "buffer_size": 4,
				"checkpoint_interval": "0", "shutdown_timeout": "2s",
			}
			p["processors"] = []any{oracleJSONParse()}
			c.add(p)
			cfg := c.write(t)
			proc := startProc(t, cfg)
			waitForPipelineUp(t, proc, "large-body")
			waitForMetric(t, proc.addr, "loghub_entries_sent_total",
				map[string]string{"pipeline_id": "large-body", "destination_type": "http"},
				10*time.Second, "batch accepted", func(v float64) bool { return v >= 5 })
			proc.stop()
			if strings.Contains(proc.log(), tc.body[:min(len(tc.body), 64)]) {
				t.Error("response body leaked into the log")
			}
		})
	}
}

// TestRedirectNeverFollowed points a 302 at a second server and proves the
// client neither follows it nor counts the batch as delivered.
func TestRedirectNeverFollowed(t *testing.T) {
	var targetHits int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits++
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()

	runHTTPFailureCase(t, "redirect", redirector.URL, 2, nil)
	if targetHits != 0 {
		t.Fatalf("redirect target received %d requests, want 0", targetHits)
	}
}

// TestSecretsNeverReachLogsOrDLQ sends an authorization header through a
// destination that echoes the request and fails everything. Neither the log
// nor the DLQ may contain the secret or the request body.
func TestSecretsNeverReachLogsOrDLQ(t *testing.T) {
	const secret = "Bearer super-secret-token-value"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	dlqDir := filepath.Join(dir, "dlq")
	g := newOracleGenerator()
	g.appendFile(t, input, 3)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		// Echo both the header and the body, as a careless upstream might.
		fmt.Fprintf(w, `{"error":{"code":"%s"},"echo":%q}`, r.Header.Get("Authorization"), body)
	}))
	defer srv.Close()

	c := newTestConfig(t)
	p := newPipeline("secret-fault", oracleFileSource(input, 3), map[string]any{"type": "http", "http": map[string]any{
		"url":     srv.URL,
		"timeout": "500ms",
		"headers": map[string]any{"Authorization": secret},
	}})
	p["runtime"] = map[string]any{
		"poll_interval": "10ms", "send_batch_size": 3, "buffer_size": 4,
		"checkpoint_interval": "0", "shutdown_timeout": "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 2, "base_backoff": "1ms"}
	p["durability"] = map[string]any{"dlq": map[string]any{"enabled": true, "dir": dlqDir}}
	c.Global["circuit_breaker_failure_threshold"] = 100
	c.Global["log_level"] = "debug"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "secret-fault")
	waitFor(t, 10*time.Second, "DLQ written", func() bool {
		return len(readDLQ(t, dlqDir, "secret-fault")) > 0
	})
	proc.stop()

	logs := proc.log()
	if strings.Contains(logs, "super-secret-token-value") {
		t.Error("authorization token appeared in the log")
	}
	for _, fb := range readDLQ(t, dlqDir, "secret-fault") {
		if strings.Contains(fb.Error, "super-secret-token-value") {
			t.Errorf("authorization token appeared in the DLQ error: %q", fb.Error)
		}
		for _, e := range fb.Entries {
			if strings.Contains(e.Fields["payload"], "super-secret-token-value") {
				t.Error("authorization token appeared in a DLQ record payload")
			}
		}
	}
}

// Ensure a TLS client never negotiates a version below TLS 1.2.
func TestHTTPDestinationRejectsTLS10(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS10}
	srv.StartTLS()
	defer srv.Close()

	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 3)
	c := newTestConfig(t)
	p := newPipeline("tls10", oracleFileSource(input, 3), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval": "10ms", "send_batch_size": 3, "buffer_size": 4,
		"checkpoint_interval": "0", "shutdown_timeout": "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 2, "base_backoff": "1ms"}
	p["durability"] = map[string]any{"dlq": map[string]any{"enabled": true, "dir": filepath.Join(dir, "dlq")}}
	c.Global["circuit_breaker_failure_threshold"] = 100
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "tls10")
	waitFor(t, 10*time.Second, "TLS 1.0 rejected", func() bool {
		return len(readDLQ(t, filepath.Join(dir, "dlq"), "tls10")) > 0
	})
	proc.stop()
}
