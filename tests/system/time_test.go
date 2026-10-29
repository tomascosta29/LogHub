package system_test

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// faketimeEnv returns the environment for a process whose wall clock can be
// moved by rewriting the timestamp file. libfaketime reads the offset from
// the file on each call, so a test can simulate a clock jump while the
// process runs.
func faketimeEnv(t *testing.T, offsetFile string) []string {
	t.Helper()
	lib := "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1"
	if _, err := os.Stat(lib); err != nil {
		t.Skip("libfaketime is not installed")
	}
	if err := os.WriteFile(offsetFile, []byte("+0"), 0o600); err != nil {
		t.Fatalf("seed faketime offset: %v", err)
	}
	return []string{"LD_PRELOAD=" + lib, "FAKETIME_TIMESTAMP_FILE=" + offsetFile, "FAKETIME_NO_CACHE=1"}
}

func setFaketimeOffset(t *testing.T, path, offset string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(offset), 0o600); err != nil {
		t.Fatalf("write faketime offset: %v", err)
	}
}

// TestBackwardClockJumpDoesNotHideAStall moves the wall clock back an hour
// while a pipeline cannot read its source. Staleness is measured against
// wall-clock timestamps, so timestamps recorded before the jump are now in
// the future; a naive comparison concludes that no time has passed and
// leaves /health healthy while no data is being read.
func TestBackwardClockJumpDoesNotHideAStall(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "missing.log") // never created
	output := filepath.Join(dir, "out.jsonl")
	offsetFile := filepath.Join(dir, "faketime")

	c := newTestConfig(t)
	p := newPipeline("clock-back", sourceFile(input, "clock-input", 10), destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "100ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "2s",
	}
	c.Global["stalled_threshold_multiplier"] = 5
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg, faketimeEnv(t, offsetFile)...)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("loghub log:\n%s", proc.log())
		}
	})
	// The source is missing, so the pipeline must become stalled and
	// report 503.
	waitForHealth(t, proc.addr, 10*time.Second, "stalled without a source", func(code int, h healthResponse) bool {
		return code == http.StatusServiceUnavailable
	})

	setFaketimeOffset(t, offsetFile, "-1h")
	time.Sleep(1500 * time.Millisecond)

	code, h, err := fetchHealth(proc.addr)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if code != http.StatusServiceUnavailable {
		status := ""
		if len(h.Pipelines) > 0 {
			status = h.Pipelines[0].Status
		}
		t.Fatalf("a one-hour backward clock jump hid a stall: /health=%d status=%q after the source had been unreachable for more than the stall window",
			code, status)
	}
	proc.stop()
}

// TestForwardClockJumpKeepsDelivering moves the clock forward while a
// healthy pipeline is running. Records must keep arriving and /health must
// return to healthy; a false permanent stall is a failure.
func TestForwardClockJumpKeepsDelivering(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	offsetFile := filepath.Join(dir, "faketime")
	g := newOracleGenerator()
	g.appendFile(t, input, 20)

	c := newTestConfig(t)
	p := oracleFilePipeline("clock-forward", input, output, 10)
	p["runtime"].(map[string]any)["poll_interval"] = "50ms"
	c.Global["stalled_threshold_multiplier"] = 5
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg, faketimeEnv(t, offsetFile)...)
	waitForPipelineUp(t, proc, "clock-forward")
	waitForJSONLCount(t, output, 20, 15*time.Second)

	setFaketimeOffset(t, offsetFile, "+24h")
	time.Sleep(300 * time.Millisecond)

	g.appendFile(t, input, 20)
	waitForJSONLCount(t, output, 40, 20*time.Second)
	waitForHealth(t, proc.addr, 5*time.Second, "healthy after a forward jump", func(code int, h healthResponse) bool {
		return code == http.StatusOK
	})
	proc.stop()

	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"local_jsonl"},
		Observed:     observedFromLogEntries("local_jsonl", readLogEntriesTolerant(t, output)),
	}, "forward clock jump")
}

// TestLeapSecondTimestampIsNotLost reads rows whose timestamp uses the
// leap-second spelling 23:59:60. Go's time.Parse rejects it and SQLite's
// julianday returns NULL, so the row must still be delivered rather than
// silently skipped or stalled on.
func TestLeapSecondTimestampIsNotLost(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	db := openOracleDB(t, dbPath)
	if _, err := db.Exec(`CREATE TABLE leap_events (id TEXT PRIMARY KEY, event_time TEXT, payload TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	values := []struct{ id, ts, payload string }{
		{"0001", "2026-12-31T23:59:58Z", "a"},
		{"0002", "2026-12-31T23:59:60Z", "b"},
		{"0003", "2027-01-01T00:00:00Z", "c"},
	}
	for _, v := range values {
		if _, err := db.Exec(`INSERT INTO leap_events (id, event_time, payload) VALUES (?, ?, ?)`, v.id, v.ts, v.payload); err != nil {
			t.Fatalf("insert %s: %v", v.id, err)
		}
	}

	c := newTestConfig(t)
	p := newPipeline("leap", sourceSQLite(dbPath, "leap_events", "event_time", "id", 10, nil), destLocalJSONL(output, ""))
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "leap")
	waitFor(t, 10*time.Second, "leap-second rows delivered", func() bool {
		return len(readLogEntriesTolerant(t, output)) >= len(values)
	})
	proc.stop()

	entries := readLogEntriesTolerant(t, output)
	have := map[string]bool{}
	for _, e := range entries {
		have[e.Fields["id"]] = true
	}
	for _, v := range values {
		if !have[v.id] {
			t.Errorf("record %s at leap-second boundary was not delivered", v.id)
		}
	}
	_ = fmt.Sprint
}
