package system_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"loghub/internal/dlq"
)

func waitForReloadedPipeline(t *testing.T, addr, id string, wantPipelines int) {
	t.Helper()
	waitForHealth(t, addr, 15*time.Second, "reload to "+id, func(code int, h healthResponse) bool {
		if code != http.StatusOK || len(h.Pipelines) != wantPipelines {
			return false
		}
		for _, ph := range h.Pipelines {
			if ph.ID == id && ph.Status != "running" && ph.Status != "degraded" {
				return false
			}
		}
		return true
	})
}

func TestSIGHUPReloadAddsPipeline(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	outA := filepath.Join(dir, "a.jsonl")
	outB := filepath.Join(dir, "b.jsonl")
	inputB := filepath.Join(dir, "b.log")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 100, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("reload-a", dbPath, outA, 25, 25))
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "reload-a")
	waitForJSONLCount(t, outA, 50, 15*time.Second)

	writeLogFile(t, inputB, "[INFO] b-1", "[INFO] b-2", "[INFO] b-3")
	pB := newPipeline("reload-b", sourceFile(inputB, "b-input", 10), destLocalJSONL(outB, ""))
	pB["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(pB)
	c.write(t)

	proc.signal(syscall.SIGHUP)
	waitForReloadedPipeline(t, proc.addr, "reload-b", 2)
	waitForJSONLCount(t, outB, 3, 10*time.Second)
	proc.stop()
}

func TestSIGHUPReloadChangesDestination(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	out1 := filepath.Join(dir, "one.jsonl")
	out2 := filepath.Join(dir, "two.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 50, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("reload-dest", dbPath, out1, 25, 25))
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "reload-dest")
	waitForJSONLCount(t, out1, 50, 15*time.Second)

	c.Pipelines[0]["destinations"] = []map[string]any{destLocalJSONL(out2, "sentinel")}
	c.write(t)
	proc.signal(syscall.SIGHUP)
	waitForReloadedPipeline(t, proc.addr, "reload-dest", 1)

	fixture.insert(t, 20, 10)
	waitForJSONLCount(t, out2, 20, 15*time.Second)
	proc.stop()

	if got := len(readSentinelRecords(t, out1)); got != 50 {
		t.Fatalf("old destination records = %d, want 50", got)
	}
	newRecords := readSentinelRecords(t, out2)
	if len(newRecords) != 20 {
		t.Fatalf("new destination records = %d, want 20", len(newRecords))
	}
	assertUnique(t, idsFromSentinel(newRecords))

	cursor := readCursor(t, filepath.Join(c.dir, "checkpoints"), "reload-dest")
	want := `v1:{"timestamp":"2026-05-06T12:00:06Z","tie":"000070"}`
	if cursor != want {
		t.Fatalf("cursor = %q, want %q", cursor, want)
	}
}

func TestSIGHUPInvalidConfigKeepsRunning(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 20, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("reload-invalid", dbPath, output, 10, 10))
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "reload-invalid")
	waitForJSONLCount(t, output, 20, 10*time.Second)

	if err := os.WriteFile(cfg, []byte("pipelines: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	proc.signal(syscall.SIGHUP)
	waitForLog(t, proc, "Config reload failed", 5*time.Second)
	waitForRunning(t, proc, "reload-invalid")

	// The old pipeline must still deliver after the failed reload.
	fixture.insert(t, 5, 10)
	waitForJSONLCount(t, output, 25, 10*time.Second)
	proc.stop()
}

func TestSIGHUPDuringTrafficNoLoss(t *testing.T) {
	const total = 500
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("reload-traffic", dbPath, output, 25, 10))
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "reload-traffic")
	waitForJSONLCount(t, output, 50, 15*time.Second)

	for i := 0; i < 3; i++ {
		c.write(t)
		proc.signal(syscall.SIGHUP)
		time.Sleep(50 * time.Millisecond)
	}
	waitForJSONLCount(t, output, total, 30*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	ids := idsFromSentinel(records)
	assertSetContains(t, ids, fixtureIDs(1, total))
	// A reload can replay one in-flight batch, but must not duplicate
	// arbitrarily or lose rows.
	if len(records) > total+100 {
		t.Fatalf("records after reloads = %d, want bounded duplicates (<= %d)", len(records), total+100)
	}
}

func TestCheckpointerLockSecondInstanceBlocked(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 10, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("lock-a", dbPath, output, 10, 10))
	cfg := c.write(t)

	a := startProc(t, cfg)
	waitForRunning(t, a, "lock-a")

	b := startProc(t, cfg)
	code, logs := b.waitExit(10 * time.Second)
	if code == 0 {
		t.Fatalf("second instance exited 0, want lock failure\n%s", logs)
	}
	if !strings.Contains(logs, "lock") {
		t.Fatalf("second instance log does not mention the lock:\n%s", logs)
	}
	a.stop()
}

func TestCheckpointerLockBlocksThirdInstance(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 10, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("lock-third", dbPath, output, 10, 10))
	cfg := c.write(t)

	a := startProc(t, cfg)
	waitForRunning(t, a, "lock-third")

	b := startProc(t, cfg)
	if code, logs := b.waitExit(10 * time.Second); code == 0 {
		t.Fatalf("second instance exited 0, want lock failure\n%s", logs)
	}

	_, stderr, code, timedOut := runBinaryFor(t, 3*time.Second, nil,
		"--config", cfg, "--metrics", freeAddr(t))
	if timedOut {
		t.Fatalf("third instance acquired a new lock while the first still runs\n%s", stderr)
	}
	if code == 0 {
		t.Fatalf("third instance exit = 0, want lock failure\n%s", stderr)
	}
	a.stop()
}

func TestDLQCommandListAndExport(t *testing.T) {
	const total = 20
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	dlqDir := filepath.Join(dir, "dlq")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 10)
	_, srv := newSink(t, http.StatusBadRequest)

	c := newTestConfig(t)
	p := newPipeline("dlq-cli",
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
		map[string]string{"pipeline_id": "dlq-cli"}, 10*time.Second,
		"DLQ entries", func(v float64) bool { return v >= total })
	proc.stop()

	stdout, stderr, code := runBinary(t, "dlq", "list", "--config", cfg)
	if code != 0 {
		t.Fatalf("dlq list exit %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "dlq-cli") || !strings.Contains(stdout, "http#0") {
		t.Fatalf("dlq list output missing pipeline/destination:\n%s", stdout)
	}

	stdout, stderr, code = runBinary(t, "dlq", "export", "--config", cfg, "--pipeline", "dlq-cli")
	if code != 0 {
		t.Fatalf("dlq export exit %d: %s", code, stderr)
	}
	var records []dlq.FailedBatch
	dec := json.NewDecoder(strings.NewReader(stdout))
	for dec.More() {
		var fb dlq.FailedBatch
		if err := dec.Decode(&fb); err != nil {
			t.Fatalf("decode export: %v\n%s", err, stdout)
		}
		records = append(records, fb)
	}
	if len(records) != 2 {
		t.Fatalf("exported records = %d, want 2", len(records))
	}
	entries := 0
	for _, fb := range records {
		entries += len(fb.Entries)
	}
	if entries != total {
		t.Fatalf("exported entries = %d, want %d", entries, total)
	}
}

func TestSIGINTGracefulShutdown(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	output := filepath.Join(dir, "out.jsonl")
	writeLogFile(t, input, "[INFO] signal-1", "[INFO] signal-2")

	c := newTestConfig(t)
	c.Global["log_level"] = "info"
	p := newPipeline("sigint",
		sourceFile(input, "in", 10),
		destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "sigint")
	proc.signal(syscall.SIGINT)
	code, logs := proc.waitExit(10 * time.Second)
	if code != 0 {
		t.Fatalf("SIGINT exit = %d\n%s", code, logs)
	}
	if !strings.Contains(logs, "All pipelines stopped") {
		t.Fatalf("SIGINT did not report a clean shutdown:\n%s", logs)
	}
}

func TestSecretInNestedHTTPHeader(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	writeLogFile(t, input, "[INFO] secret-header")
	s, srv := newSink(t, http.StatusOK)

	c := newTestConfig(t)
	c.Secrets = map[string]any{"env": map[string]any{"type": "env"}}
	p := newPipeline("secret-header",
		sourceFile(input, "in", 10),
		map[string]any{"type": "http", "http": map[string]any{
			"url":     srv.URL,
			"timeout": "500ms",
			"headers": map[string]any{"Authorization": "${env:LOGHUB_TEST_TOKEN}"},
		}})
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg, "LOGHUB_TEST_TOKEN=Bearer test-token")
	waitFor(t, 10*time.Second, "header delivered", func() bool { return s.acceptedEntries() >= 1 })
	proc.stop()
	if got := s.lastHeader().Get("Authorization"); got != "Bearer test-token" {
		t.Fatalf("Authorization header = %q, want the resolved secret", got)
	}
}

func TestHTTPErrorResponseBodyIsNotLogged(t *testing.T) {
	const secret = "SECRET-TOKEN-123"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	writeLogFile(t, input, "[INFO] one", "[INFO] two", "[INFO] three", "[INFO] four", "[INFO] five")

	var mu sync.Mutex
	var sawAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sawAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "upstream echoed %s", secret)
	}))
	t.Cleanup(srv.Close)

	c := newTestConfig(t)
	c.Secrets = map[string]any{"env": map[string]any{"type": "env"}}
	p := newPipeline("redaction",
		sourceFile(input, "in", 10),
		map[string]any{"type": "http", "http": map[string]any{
			"url":     srv.URL,
			"timeout": "500ms",
			"headers": map[string]any{"Authorization": "${env:LOGHUB_TEST_TOKEN}"},
		}})
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg, "LOGHUB_TEST_TOKEN="+secret)
	waitForMetric(t, proc.addr, "loghub_entries_failed_total",
		map[string]string{"pipeline_id": "redaction", "destination_type": "http"},
		10*time.Second, "failed send", func(v float64) bool { return v >= 1 })
	proc.stop()

	mu.Lock()
	auth := sawAuth
	mu.Unlock()
	if auth != secret {
		t.Fatalf("server saw Authorization %q, want the resolved secret", auth)
	}
	logs := proc.log()
	if strings.Contains(logs, secret) {
		t.Fatalf("secret appeared in logs:\n%s", logs)
	}
}
