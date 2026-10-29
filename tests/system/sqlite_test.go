//go:build !windows

package system_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"loghub/internal/checkpointer"
)

func sentinelTimestampProcessor() map[string]any {
	return timestampProcessor("event_time", "2006-01-02T15:04:05Z07:00", "timestamp")
}

func TestSQLiteToSentinelBaseline(t *testing.T) {
	const total = 1000
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 25)

	c := newTestConfig(t)
	p := newPipeline("sqlite-baseline",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 125, nil),
		destLocalJSONL(output, "sentinel"))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     25,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "5s",
	}
	p["processors"] = []any{sentinelTimestampProcessor()}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "sqlite-baseline")
	waitForJSONLCount(t, output, total, 30*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != total {
		t.Fatalf("records = %d, want %d", len(records), total)
	}
	ids := idsFromSentinel(records)
	assertUnique(t, ids)
	for i, id := range ids {
		want := fmt.Sprintf("%06d", i+1)
		if id != want {
			t.Fatalf("record %d id = %q, want %q (source order)", i, id, want)
		}
	}
	for _, r := range records {
		if r["TimeGenerated"] == "" || r["event_time"] == "" {
			t.Fatalf("record missing timestamps: %+v", r)
		}
		if r["TimeGenerated"] != r["event_time"] {
			t.Fatalf("TimeGenerated %q != event_time %q", r["TimeGenerated"], r["event_time"])
		}
		if _, nested := r["fields"]; nested {
			t.Fatalf("sentinel record should be flat: %+v", r)
		}
	}
	cursor := readCursor(t, filepath.Join(c.dir, "checkpoints"), "sqlite-baseline")
	wantCursor := `v1:{"timestamp":"2026-05-06T12:00:39Z","tie":"001000"}`
	if cursor != wantCursor {
		t.Fatalf("cursor = %q, want %q", cursor, wantCursor)
	}
}

func TestSQLiteAppendWhileRunning(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 100, 10)

	c := newTestConfig(t)
	p := newPipeline("sqlite-append",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 25, nil),
		destLocalJSONL(output, "sentinel"))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     25,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "5s",
	}
	p["processors"] = []any{sentinelTimestampProcessor()}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "sqlite-append")
	waitForJSONLCount(t, output, 100, 15*time.Second)

	fixture.insert(t, 50, 10)
	waitForJSONLCount(t, output, 150, 15*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != 150 {
		t.Fatalf("records = %d, want 150", len(records))
	}
	ids := idsFromSentinel(records)
	assertUnique(t, ids)
	assertSetContains(t, ids, fixtureIDs(1, 150))
}

func TestSQLiteColumnsAllowlist(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 20, 5)

	c := newTestConfig(t)
	p := newPipeline("sqlite-columns",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10,
			map[string]any{"columns": []string{"id", "event_time", "user_name"}}),
		destLocalJSONL(output, "sentinel"))
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
	waitForRunning(t, proc, "sqlite-columns")
	waitForJSONLCount(t, output, 20, 10*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != 20 {
		t.Fatalf("records = %d, want 20", len(records))
	}
	for _, r := range records {
		if _, ok := r["action"]; ok {
			t.Fatalf("allowlist leaked action: %+v", r)
		}
		if _, ok := r["payload"]; ok {
			t.Fatalf("allowlist leaked payload: %+v", r)
		}
		if r["id"] == "" || r["user_name"] == "" || r["event_time"] == "" {
			t.Fatalf("allowlisted columns missing: %+v", r)
		}
	}
}

func TestSQLiteCorruptCursorStopsCollection(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 10, 5)

	checkpointDir := filepath.Join(dir, "checkpoints")
	cp, err := checkpointer.NewFileCheckpointer(checkpointDir)
	if err != nil {
		t.Fatalf("seed checkpointer: %v", err)
	}
	if err := cp.Set("sqlite-corrupt", `v99:{"timestamp":"2026-05-06T12:00:00Z"}`); err != nil {
		t.Fatalf("write corrupt cursor: %v", err)
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("close checkpointer: %v", err)
	}

	c := newTestConfig(t)
	c.Global["checkpoints_dir"] = checkpointDir
	p := newPipeline("sqlite-corrupt",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
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
	waitForMetric(t, proc.addr, "loghub_collect_errors_total",
		map[string]string{"pipeline_id": "sqlite-corrupt", "source_name": ""},
		10*time.Second, "collect error for corrupt cursor", func(v float64) bool { return v >= 1 })

	_, h, err := fetchHealth(proc.addr)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	ph := h.Pipelines[0]
	if !containsAny(ph.ErrorMsg, "invalid cursor", "unsupported version", "cursor") {
		t.Fatalf("health ErrorMsg = %q, want the cursor error", ph.ErrorMsg)
	}
	if got := tryCountJSONL(output); got != 0 {
		t.Fatalf("destination received %d records despite a corrupt cursor", got)
	}
	proc.stop()
}

func TestSourceDeadFromStartupStalls(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "out.jsonl")
	missingDB := filepath.Join(dir, "missing-dir", "audit.db")

	c := newTestConfig(t)
	p := newPipeline("never-start",
		sourceSQLite(missingDB, "audit_events", "event_time", "id", 10, nil),
		destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.Global["stalled_threshold_multiplier"] = 3
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_collect_errors_total",
		map[string]string{"pipeline_id": "never-start", "source_name": ""},
		10*time.Second, "collect error", func(v float64) bool { return v >= 1 })

	// After 3 x 20ms with no successful collect or send since start, the
	// pipeline is stalled and /health returns 503.
	waitForHealth(t, proc.addr, 5*time.Second, "never-successful source stalls", func(code int, h healthResponse) bool {
		return code == 503 && containsAny(h.Reason, "stalled")
	})
	proc.stop()
}

func TestSQLiteNoNewRowsNoDuplicates(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 10, 5)

	c := newTestConfig(t)
	p := newPipeline("sqlite-quiet",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destLocalJSONL(output, "sentinel"))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["processors"] = []any{sentinelTimestampProcessor()}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "sqlite-quiet")
	waitForJSONLCount(t, output, 10, 10*time.Second)
	time.Sleep(250 * time.Millisecond)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != 10 {
		t.Fatalf("records = %d, want 10 (no re-reads)", len(records))
	}
	assertUnique(t, idsFromSentinel(records))
}

func fixtureIDs(start, end int) []string {
	out := make([]string, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, fmt.Sprintf("%06d", i))
	}
	return out
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
