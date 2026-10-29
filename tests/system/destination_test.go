//go:build !windows

package system_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHTTPRetryThenSuccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 20, 10)
	s, srv := newSink(t, http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK)

	c := newTestConfig(t)
	p := newPipeline("retry-success",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 4, "base_backoff": "1ms"}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitFor(t, 15*time.Second, "accepted after retries", func() bool {
		return s.acceptedEntries() >= 20
	})
	fams := scrapeMetrics(t, proc.addr)
	dest := map[string]string{"pipeline_id": "retry-success", "destination_type": "http"}
	if got := mustValue(t, fams, "loghub_entries_sent_total", dest); got != 20 {
		t.Errorf("entries_sent_total = %v, want 20", got)
	}
	if got := counterValue(fams, "loghub_retries_total", dest); got < 2 {
		t.Errorf("retries_total = %v, want >= 2", got)
	}
	proc.stop()
}

func TestHTTP429ThenSuccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 10, 10)
	s, srv := newSink(t, http.StatusTooManyRequests, http.StatusOK)

	c := newTestConfig(t)
	p := newPipeline("retry-429",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 3, "base_backoff": "1ms"}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitFor(t, 15*time.Second, "accepted after 429", func() bool { return s.acceptedEntries() >= 10 })
	fams := scrapeMetrics(t, proc.addr)
	if got := counterValue(fams, "loghub_retries_total", map[string]string{"pipeline_id": "retry-429", "destination_type": "http"}); got < 1 {
		t.Errorf("retries_total = %v, want >= 1", got)
	}
	proc.stop()
}

func TestHTTP400WithDLQ(t *testing.T) {
	const total = 20
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	dlqDir := filepath.Join(dir, "dlq")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 10)
	_, srv := newSink(t, http.StatusBadRequest)

	c := newTestConfig(t)
	p := newPipeline("http-400-dlq",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 5, "base_backoff": "1ms"}
	p["durability"] = map[string]any{"dlq": map[string]any{"enabled": true, "dir": dlqDir}}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_dlq_entries_total",
		map[string]string{"pipeline_id": "http-400-dlq"}, 10*time.Second,
		"DLQ entries", func(v float64) bool { return v >= total })
	waitForHealth(t, proc.addr, 5*time.Second, "degraded after rejection", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "degraded"
	})
	fams := scrapeMetrics(t, proc.addr)
	dest := map[string]string{"pipeline_id": "http-400-dlq", "destination_type": "http"}
	if got := counterValue(fams, "loghub_retries_total", dest); got != 0 {
		t.Errorf("retries_total = %v, want 0 for HTTP 400", got)
	}
	if got := counterValue(fams, "loghub_entries_sent_total", dest); got != 0 {
		t.Errorf("entries_sent_total = %v, want 0", got)
	}
	proc.stop()

	records := readDLQ(t, dlqDir, "http-400-dlq")
	if len(records) != 2 {
		t.Fatalf("DLQ records = %d, want 2 (two 10-entry batches)", len(records))
	}
	for _, fb := range records {
		if fb.Destination != "http#0" {
			t.Errorf("DLQ destination = %q, want http#0", fb.Destination)
		}
		if !strings.Contains(fb.Error, "bad request") {
			t.Errorf("DLQ error = %q, want a bad-request error", fb.Error)
		}
	}
}

func TestHTTP400WithoutDLQDropsAndAdvances(t *testing.T) {
	const total = 20
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 10)
	s, srv := newSink(t, http.StatusBadRequest)

	c := newTestConfig(t)
	p := newPipeline("http-400-drop",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 5, "base_backoff": "1ms"}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	reason := map[string]string{"pipeline_id": "http-400-drop", "reason": "destination_rejected"}
	waitForMetric(t, proc.addr, "loghub_entries_dropped_total", reason, 10*time.Second,
		"dropped entries", func(v float64) bool { return v >= total })
	fams := scrapeMetrics(t, proc.addr)
	if got := mustValue(t, fams, "loghub_entries_dropped_total", reason); got < total {
		t.Errorf("entries_dropped_total = %v, want >= %d", got, total)
	}
	if got := s.acceptedEntries(); got != 0 {
		t.Errorf("sink accepted %d entries from 400 responses", got)
	}
	proc.stop()
	if got := readCursor(t, filepath.Join(c.dir, "checkpoints"), "http-400-drop"); got == "" {
		t.Fatal("cursor did not advance after dropping rejected entries")
	}
}

func TestHTTPErrorStatusMapping(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusRequestEntityTooLarge} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "audit.db")
			dlqDir := filepath.Join(dir, "dlq")
			fixture := newSQLiteFixture(t, dbPath)
			fixture.insert(t, 10, 10)
			_, srv := newSink(t, status)

			c := newTestConfig(t)
			p := newPipeline("http-status",
				sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
				destHTTP(srv.URL))
			p["runtime"] = map[string]any{
				"poll_interval":       "10ms",
				"send_batch_size":     10,
				"buffer_size":         4,
				"checkpoint_interval": "0",
				"shutdown_timeout":    "3s",
			}
			p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
			p["durability"] = map[string]any{"dlq": map[string]any{"enabled": true, "dir": dlqDir}}
			c.Global["stalled_threshold_multiplier"] = 1000
			c.add(p)
			cfg := c.write(t)

			proc := startProc(t, cfg)
			waitForMetric(t, proc.addr, "loghub_dlq_entries_total",
				map[string]string{"pipeline_id": "http-status"}, 10*time.Second,
				"DLQ after status", func(v float64) bool { return v >= 10 })
			fams := scrapeMetrics(t, proc.addr)
			dest := map[string]string{"pipeline_id": "http-status", "destination_type": "http"}
			if got := counterValue(fams, "loghub_retries_total", dest); got < 1 {
				t.Errorf("retries_total = %v, want >= 1 (status is treated as retryable)", got)
			}
			proc.stop()
		})
	}
}

func TestHTTPRedirectNotFollowed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 10, 10)

	var mu sync.Mutex
	var finalMethod string
	var finalPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/ok", http.StatusFound)
			return
		}
		mu.Lock()
		finalMethod = r.Method
		finalPath = r.URL.Path
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := newTestConfig(t)
	p := newPipeline("http-redirect",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destHTTP(srv.URL+"/redirect"))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	// The 302 is a failed send, retried like any other.
	waitForMetric(t, proc.addr, "loghub_retries_total",
		map[string]string{"pipeline_id": "http-redirect", "destination_type": "http"},
		10*time.Second, "redirect retried", func(v float64) bool { return v >= 1 })
	fams := scrapeMetrics(t, proc.addr)
	sent := counterValue(fams, "loghub_entries_sent_total", map[string]string{"pipeline_id": "http-redirect", "destination_type": "http"})
	proc.stop()

	mu.Lock()
	method, path := finalMethod, finalPath
	mu.Unlock()
	if path != "" {
		t.Fatalf("redirect was followed: %s %s reached; the POST body would have been dropped", method, path)
	}
	if sent != 0 {
		t.Fatalf("entries_sent_total = %v for a destination that only redirects, want 0", sent)
	}
}

func TestDummyDropRate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 20, 10)

	c := newTestConfig(t)
	p := newPipeline("dummy-drop",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destDummy(map[string]any{"drop_rate": 1.0}))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	// drop_rate 1.0 rejects every entry individually.
	waitForMetric(t, proc.addr, "loghub_entries_failed_total",
		map[string]string{"pipeline_id": "dummy-drop", "destination_type": "dummy"},
		10*time.Second, "dummy drops", func(v float64) bool { return v >= 20 })
	proc.stop()
}
