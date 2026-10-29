//go:build !windows

package system_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestSQLiteTableDroppedWhileRunningIsObservable drops the source table
// while the pipeline is polling. Collect must report an error instead of
// silently treating the empty result as progress; after the table and its
// rows return, the pipeline catches up from its checkpoint.
func TestSQLiteTableDroppedWhileRunningIsObservable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	db := openOracleDB(t, dbPath)
	g := newOracleGenerator()
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, 20)

	c := newTestConfig(t)
	p := oracleSQLitePipeline("table-drop", dbPath, output, 10)
	c.Global["stalled_threshold_multiplier"] = 20
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "table-drop")
	waitForJSONLCount(t, output, 20, 15*time.Second)

	if _, err := db.Exec(`DROP TABLE oracle_events`); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	waitFor(t, 10*time.Second, "collect error after the table disappeared", func() bool {
		fams, err := tryScrapeMetrics(proc.addr)
		if err != nil {
			return false
		}
		v, ok := sampleValue(fams, "loghub_collect_errors_total",
			map[string]string{"pipeline_id": "table-drop", "source_name": "oracle-input"})
		return ok && v > 0
	})
	waitForHealth(t, proc.addr, 10*time.Second, "pipeline degraded by the collect error", func(code int, h healthResponse) bool {
		return len(h.Pipelines) == 1 && (h.Pipelines[0].Status == "degraded" || h.Pipelines[0].Status == "stalled")
	})

	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, 20)
	waitForJSONLCount(t, output, 40, 20*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	if len(entries) != 40 {
		t.Fatalf("delivered %d rows after the table came back, want 40", len(entries))
	}
}

// TestSQLiteNullCursorColumnDoesNotSkipRows inserts a row whose tie-breaker
// column is NULL. The generated query compares the tie column with ">",
// which is false for NULL, so a NULL row can be skipped after the cursor
// passes it.
func TestSQLiteNullCursorColumnDoesNotSkipRows(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	db := openOracleDB(t, dbPath)
	g := newOracleGenerator()
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, 5)
	if _, err := db.Exec(`INSERT INTO oracle_events (id, seq, event_time, generated_at, checksum, payload) VALUES (NULL, 99, '2026-05-06T12:10:00Z', '2026-05-06T12:10:00Z', 'x', 'null-id')`); err != nil {
		t.Fatalf("insert NULL tie row: %v", err)
	}
	g.insertSQLite(t, db, 5)

	c := newTestConfig(t)
	p := oracleSQLitePipeline("null-tie", dbPath, output, 5)
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "null-tie")
	time.Sleep(2 * time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	haveNull := false
	for _, e := range entries {
		if e.Fields["payload"] == "null-id" {
			haveNull = true
		}
	}
	if !haveNull {
		t.Fatalf("row with a NULL cursor column was skipped (%d rows delivered)", len(entries))
	}
}

// TestSQLiteLateWriterOlderThanCursor documents the loss of rows inserted
// with a timestamp older than the acknowledged cursor. That is inherent to
// a forward-only cursor and must be stated in the operator documentation.
func TestSQLiteLateWriterOlderThanCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	db := openOracleDB(t, dbPath)
	g := newOracleGenerator()
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, 10)

	c := newTestConfig(t)
	p := oracleSQLitePipeline("late-writer", dbPath, output, 5)
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "late-writer")
	waitForJSONLCount(t, output, 10, 15*time.Second)

	// A late writer commits a row at an earlier timestamp.
	if _, err := db.Exec(`INSERT INTO oracle_events (id, seq, event_time, generated_at, checksum, payload) VALUES ('999999', 99, '2026-05-06T11:00:00Z', '2026-05-06T11:00:00Z', 'x', 'late')`); err != nil {
		t.Fatalf("insert late row: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	proc.stop()

	entries := readLogEntries(t, output)
	seen := false
	for _, e := range entries {
		if e.Fields["id"] == "999999" {
			seen = true
		}
	}
	if seen {
		t.Fatalf("late writer row was delivered; the test's premise (older rows are skipped) is wrong")
	}
	t.Log("late writer row older than the cursor was not collected; the docs do not state this loss explicitly")
}

// TestSQLiteMixedTimestampFormats keeps one row per supported spelling and
// checks that ordering is by time, not by text.
func TestSQLiteMixedTimestampFormats(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	db := openOracleDB(t, dbPath)
	if _, err := db.Exec(`CREATE TABLE mixed (id TEXT PRIMARY KEY, ts TEXT, payload TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	values := []struct{ id, ts string }{
		{"1", "2026-05-06T12:00:00Z"},
		{"2", "2026-05-06 12:00:01"},
		{"3", "2026-05-06T14:00:02+02:00"},
		{"4", "2026-05-06T12:00:03.123456Z"},
	}
	for _, v := range values {
		if _, err := db.Exec(`INSERT INTO mixed (id, ts, payload) VALUES (?, ?, ?)`, v.id, v.ts, "p"+v.id); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	c := newTestConfig(t)
	p := newPipeline("mixed", sourceSQLite(dbPath, "mixed", "ts", "id", 10, nil), destLocalJSONL(output, ""))
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "mixed")
	waitForJSONLCount(t, output, 4, 15*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	var ids []string
	for _, e := range entries {
		ids = append(ids, e.Fields["id"])
	}
	want := []string{"1", "2", "3", "4"}
	for i := range want {
		if i >= len(ids) || ids[i] != want[i] {
			t.Fatalf("order = %v, want %v", ids, want)
		}
	}
	_ = fmt.Sprint
}
