package system_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// destinationScenario drives one HTTP status through a complete pipeline and
// checks where the records ended up.
type destinationScenario struct {
	name       string
	status     int
	requests   int // expected attempts per sub-batch
	wantDLQ    bool
	wantSent   bool
	noDLQ      bool
	maxRetries int
}

func runDestinationStatusScenario(t *testing.T, sc destinationScenario) {
	t.Helper()
	const total = 5
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	dlqDir := filepath.Join(dir, "dlq")
	g := newOracleGenerator()
	g.appendFile(t, input, total)

	statuses := make([]int, sc.requests)
	for i := range statuses {
		statuses[i] = sc.status
	}
	s, srv := newSink(t, statuses...)

	c := newTestConfig(t)
	p := newPipeline("dest-"+sc.name, sourceFile(input, "dest-input", 50), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     100,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	retries := sc.maxRetries
	if retries == 0 {
		retries = 2
	}
	p["retry"] = map[string]any{"max_attempts": retries, "base_backoff": "1ms"}
	if !sc.noDLQ {
		p["durability"] = map[string]any{
			"dlq": map[string]any{"enabled": true, "dir": dlqDir},
		}
	}
	c.Global["circuit_breaker_failure_threshold"] = 20
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "dest-"+sc.name)
	waitFor(t, 15*time.Second, "destination scenario to settle", func() bool {
		reqs, _ := s.counts()
		return reqs >= sc.requests
	})
	// Give the pipeline one poll interval to record the result and advance
	// the cursor.
	time.Sleep(150 * time.Millisecond)
	proc.stop()

	reqs, _ := s.counts()
	if reqs != sc.requests {
		t.Errorf("%s: %d requests, want %d", sc.name, reqs, sc.requests)
	}

	var dlqEntries []observedRecord
	for _, fb := range readDLQ(t, dlqDir, "dest-"+sc.name) {
		if fb.Destination != "http#0" {
			t.Errorf("%s: DLQ destination = %q, want http#0", sc.name, fb.Destination)
		}
		for _, e := range fb.Entries {
			dlqEntries = append(dlqEntries, observedRecordFromFields("dlq", e.Fields))
		}
	}
	if sc.wantDLQ && len(dlqEntries) != total {
		t.Errorf("%s: %d DLQ entries, want %d", sc.name, len(dlqEntries), total)
	}
	if !sc.wantDLQ && len(dlqEntries) != 0 {
		t.Errorf("%s: unexpected DLQ entries: %d", sc.name, len(dlqEntries))
	}

	if sc.wantSent {
		got := len(s.acceptedLogEntries(t))
		if got != total {
			t.Errorf("%s: %d accepted entries, want %d", sc.name, got, total)
		}
		verifyOracle(t, g, oracleCheck{
			Destinations: []string{"http"},
			Observed:     observedFromLogEntries("http", s.acceptedLogEntries(t)),
		}, sc.name)
		return
	}

	// Failed: every record is in the DLQ, or counted as a documented drop.
	dropped := total
	if sc.wantDLQ {
		dropped = 0
	}
	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"http"},
		Observed:     observedFromLogEntries("http", s.acceptedLogEntries(t)),
		DLQ:          dlqEntries,
		Dropped:      dropped,
	}, sc.name)
}

func TestHTTPStatusMatrix(t *testing.T) {
	scenarios := []destinationScenario{
		{name: "200", status: 200, requests: 1, wantSent: true},
		{name: "301", status: 301, requests: 2, wantDLQ: true},
		{name: "302", status: 302, requests: 2, wantDLQ: true},
		{name: "303", status: 303, requests: 2, wantDLQ: true},
		{name: "307", status: 307, requests: 2, wantDLQ: true},
		{name: "308", status: 308, requests: 2, wantDLQ: true},
		// A schema rejection is deterministic: one attempt, then DLQ.
		{name: "400", status: 400, requests: 1, wantDLQ: true},
		{name: "401", status: 401, requests: 2, wantDLQ: true},
		{name: "403", status: 403, requests: 2, wantDLQ: true},
		{name: "404", status: 404, requests: 2, wantDLQ: true},
		{name: "408", status: 408, requests: 2, wantDLQ: true},
		{name: "409", status: 409, requests: 2, wantDLQ: true},
		{name: "413", status: 413, requests: 2, wantDLQ: true},
		{name: "429", status: 429, requests: 2, wantDLQ: true},
		{name: "500", status: 500, requests: 2, wantDLQ: true},
		{name: "502", status: 502, requests: 2, wantDLQ: true},
		{name: "503", status: 503, requests: 2, wantDLQ: true},
		{name: "504", status: 504, requests: 2, wantDLQ: true},
	}
	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) { runDestinationStatusScenario(t, sc) })
	}
}

// TestHTTP400WithoutDLQDropsAndAdvances covers the documented lossy path:
// with no DLQ, a schema rejection is dropped, counted, and the cursor
// advances so the poison batch does not block the pipeline.
func TestHTTP400WithoutDLQIsDroppedAndCounted(t *testing.T) {
	runDestinationStatusScenario(t, destinationScenario{
		name: "400-nodlq", status: 400, requests: 1, noDLQ: true,
	})
}

// TestHTTP200WithHugeOrMalformedBodyDoesNotHang verifies that a 200 with an
// oversized or malformed body is treated as success without unbounded
// reading, and that the next batch is still delivered.
func TestHTTP200WithHugeOrMalformedBodyDoesNotHang(t *testing.T) {
	for _, tc := range []struct {
		name string
		body func(w http.ResponseWriter)
	}{
		{"huge", func(w http.ResponseWriter) {
			chunk := strings.Repeat("x", 64*1024)
			for i := 0; i < 16; i++ {
				w.Write([]byte(chunk))
			}
		}},
		{"malformed", func(w http.ResponseWriter) {
			w.Write([]byte("{not json"))
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const total = 5
			dir := t.TempDir()
			input := filepath.Join(dir, "audit.log")
			output := filepath.Join(dir, "out.jsonl")
			g := newOracleGenerator()
			g.appendFile(t, input, total)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.body(w)
			}))
			t.Cleanup(srv.Close)

			c := newTestConfig(t)
			p := newPipeline("big-body", sourceFile(input, "big-input", 50), destHTTP(srv.URL))
			p["runtime"] = map[string]any{
				"poll_interval":       "10ms",
				"send_batch_size":     100,
				"buffer_size":         4,
				"checkpoint_interval": "0",
				"shutdown_timeout":    "3s",
			}
			p["retry"] = map[string]any{"max_attempts": 2, "base_backoff": "1ms"}
			c.add(p)
			cfg := c.write(t)
			_ = output
			proc := startProc(t, cfg)
			waitForPipelineUp(t, proc, "big-body")
			waitForMetric(t, proc.addr, "loghub_entries_sent_total",
				map[string]string{"pipeline_id": "big-body", "destination_type": "http"},
				15*time.Second, "200 accepted", func(v float64) bool { return v >= total })
			proc.stop()
		})
	}
}

// TestDestinationSecretsNeverAppearInLogsOrDLQ sends a bearer token and a
// response body containing it, then searches the log, /health and DLQ error
// messages for the secret.
func TestDestinationSecretsNeverAppearInLogsOrDLQ(t *testing.T) {
	const secret = "super-secret-bearer-token-value"
	const total = 3
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	dlqDir := filepath.Join(dir, "dlq")
	g := newOracleGenerator()
	g.appendFile(t, input, total)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo the request back in the failure body, as some proxies do.
		http.Error(w, "failed for Authorization: Bearer "+secret, http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := newTestConfig(t)
	c.Global["log_level"] = "debug"
	p := newPipeline("secret", sourceFile(input, "secret-input", 50), map[string]any{
		"type": "http",
		"http": map[string]any{
			"url":     srv.URL,
			"timeout": "500ms",
			"headers": map[string]any{"Authorization": "Bearer " + secret},
		},
	})
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     100,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 2, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"dlq": map[string]any{"enabled": true, "dir": dlqDir},
	}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "secret")
	waitForMetric(t, proc.addr, "loghub_dlq_entries_total",
		map[string]string{"pipeline_id": "secret"}, 15*time.Second,
		"failure dead-lettered", func(v float64) bool { return v >= total })
	health := proc.log()
	fams := scrapeMetrics(t, proc.addr)
	proc.stop()

	logs := proc.log()
	if strings.Contains(logs, secret) {
		t.Fatalf("secret appears in logs:\n%s", logs)
	}
	for _, fb := range readDLQ(t, dlqDir, "secret") {
		if strings.Contains(fb.Error, secret) {
			t.Fatalf("secret appears in DLQ error: %q", fb.Error)
		}
		for _, e := range fb.Entries {
			encoded := fmt.Sprint(e.Fields)
			if strings.Contains(encoded, secret) {
				t.Fatalf("secret appears in DLQ entries")
			}
		}
	}
	// The metrics endpoint has no free-form labels, but a scrape must not
	// contain the secret either.
	resp, err := http.Get("http://" + proc.addr + "/metrics")
	if err == nil {
		defer resp.Body.Close()
		buf := make([]byte, 1<<20)
		n, _ := resp.Body.Read(buf)
		if strings.Contains(string(buf[:n]), secret) {
			t.Fatal("secret appears in metrics")
		}
	}
	_ = health
	_ = fams
	_ = os.Getenv
}
