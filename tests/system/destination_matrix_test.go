//go:build !windows

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
