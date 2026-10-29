package system_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

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
