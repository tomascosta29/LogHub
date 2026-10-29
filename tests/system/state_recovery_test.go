//go:build !windows

package system_test

import (
	"bytes"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// openOracleDB opens a SQLite database for tests that create their own
// tables.
func openOracleDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		t.Fatalf("wal: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// corruptFile overwrites path with bytes that cannot be a valid journal.
func corruptFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x00, 0xff, 'n', 'o', 't', '\n'}, 64), 0o600); err != nil {
		t.Fatalf("corrupt %s: %v", path, err)
	}
}

func journalPath(c *testConfig, pipelineID string) string {
	return filepath.Join(c.dir, "checkpoints", pipelineID+".journal")
}

// TestCorruptCheckpointDoesNotSilentlyRestartAtZero starts a pipeline on a
// checkpoint journal filled with garbage. The durable cursor is gone, so
// LogHub must either recover it or refuse to start with a clear error.
// Silently restarting from the beginning re-reads the whole source, which
// is a duplicate storm and, for a source with retention, unbounded replay.
func TestCorruptCheckpointDoesNotSilentlyRestartAtZero(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 100)

	c := newTestConfig(t)
	c.add(oracleFilePipeline("corrupt-cursor", input, output, 25))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "corrupt-cursor")
	waitForJSONLCount(t, output, 100, 20*time.Second)
	proc.stop()
	before := len(readLogEntriesTolerant(t, output))

	corruptFile(t, journalPath(c, "corrupt-cursor"))

	proc = startProc(t, cfg)
	code, exited, logs := proc.exitedWithin(2 * time.Second)
	if exited && code != 0 {
		// Refusing to start with a clear error is the other acceptable
		// outcome. The message must name the checkpoint, not be a panic.
		if !bytes.Contains([]byte(logs), []byte("checkpoint")) && !bytes.Contains([]byte(logs), []byte("journal")) {
			t.Fatalf("process refused to start without a clear checkpoint error:\n%s", logs)
		}
		return
	}

	// It started. Wait for it to make progress, then require that it did
	// not replay the source from offset zero.
	time.Sleep(500 * time.Millisecond)
	proc.stop()
	after := len(readLogEntriesTolerant(t, output))
	if after > before {
		t.Fatalf("corrupt checkpoint silently restarted at offset zero: %d records re-delivered (%d -> %d)\n%s",
			after-before, before, after, proc.log())
	}
}

// TestTruncatedCheckpointDoesNotSilentlyRestartAtZero is the same failure
// with the most common power-loss artifact: a zero-length state file.
func TestTruncatedCheckpointDoesNotSilentlyRestartAtZero(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 60)

	c := newTestConfig(t)
	c.add(oracleFilePipeline("truncated-cursor", input, output, 25))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "truncated-cursor")
	waitForJSONLCount(t, output, 60, 20*time.Second)
	proc.stop()
	before := len(readLogEntriesTolerant(t, output))

	if err := os.Truncate(journalPath(c, "truncated-cursor"), 0); err != nil {
		t.Fatalf("truncate checkpoint: %v", err)
	}

	proc = startProc(t, cfg)
	code, exited, _ := proc.exitedWithin(2 * time.Second)
	if exited && code != 0 {
		return
	}
	time.Sleep(500 * time.Millisecond)
	proc.stop()
	after := len(readLogEntriesTolerant(t, output))
	if after > before {
		t.Fatalf("empty checkpoint silently restarted at offset zero: %d records re-delivered (%d -> %d)",
			after-before, before, after)
	}
}

// TestCorruptDiskBufferDoesNotSilentlyDropBacklog starts on a disk-buffer
// journal full of garbage. The pending batches are the only copy of records
// the destination never accepted, so LogHub must recover them or refuse to
// start; starting with an empty queue loses them.
func TestCorruptDiskBufferDoesNotSilentlyDropBacklog(t *testing.T) {
	const total = 100
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	bufferDir := filepath.Join(dir, "buffer")
	db := openOracleDB(t, dbPath)
	g := newOracleGenerator()
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, total)
	s, srv := newSink(t, http.StatusInternalServerError)

	c := newTestConfig(t)
	p := newPipeline("corrupt-buffer",
		oracleSQLiteSource(dbPath, 25),
		destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "5ms",
		"send_batch_size":     25,
		"buffer_size":         10,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"disk_buffer": map[string]any{"enabled": true, "dir": bufferDir},
	}
	c.Global["circuit_breaker_failure_threshold"] = 1
	c.Global["circuit_breaker_recovery_timeout"] = "50ms"
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			if fams, err := tryScrapeMetrics(proc.addr); err == nil {
				for name, fam := range fams {
					t.Logf("metric %s has %d samples", name, len(fam.GetMetric()))
				}
			}
			t.Logf("loghub log:\n%s", proc.log())
		}
	})
	waitForMetric(t, proc.addr, "loghub_disk_buffer_batches",
		map[string]string{"pipeline_id": "corrupt-buffer"}, 15*time.Second,
		"backlog spilled to disk", func(v float64) bool { return v >= 1 })
	proc.stop()
	if got := s.acceptedEntries(); got != 0 {
		t.Fatalf("sink accepted %d entries during the outage, want 0", got)
	}

	// The audit table is pruned by retention while the backlog waits, so
	// the disk buffer is now the only copy.
	if _, err := db.Exec(`DELETE FROM oracle_events`); err != nil {
		t.Fatalf("prune source: %v", err)
	}
	corruptFile(t, filepath.Join(bufferDir, "corrupt-buffer.buffer.journal"))
	s.setStatuses(http.StatusOK)

	proc = startProc(t, cfg)
	code, exited, logs := proc.exitedWithin(2 * time.Second)
	if exited {
		if code == 0 {
			t.Fatalf("process exited with status 0 without starting")
		}
		if !bytes.Contains([]byte(logs), []byte("buffer")) && !bytes.Contains([]byte(logs), []byte("journal")) {
			t.Fatalf("process refused to start without naming the corrupted buffer:\n%s", logs)
		}
		return
	}
	time.Sleep(time.Second)
	proc.stop()

	observed := observedFromLogEntries("http", s.deliveredLogEntries(t))
	res := g.verify(oracleCheck{Destinations: []string{"http"}, Observed: observed})
	if len(res.Missing) > 0 {
		t.Fatalf("corrupt disk buffer silently dropped %d of %d records that existed only in the backlog: %s",
			len(res.Missing), total, sampleIDs(res.Missing, 10))
	}
}

// TestCorruptDLQDoesNotSilentlyDiscardRecords fills the DLQ, corrupts its
// journal and checks that the records are still readable or that startup
// refuses. A DLQ that quietly reads as empty makes an operator think the
// failed records are gone when they are the only copy.
func TestCorruptDLQDoesNotSilentlyDiscardRecords(t *testing.T) {
	const total = 40
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	dlqDir := filepath.Join(dir, "dlq")
	g := newOracleGenerator()
	g.appendFile(t, input, total)
	_, srv := newSink(t, http.StatusBadRequest) // poison pill, no retries

	c := newTestConfig(t)
	p := newPipeline("corrupt-dlq", sourceFile(input, "dlq-input", 20), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "5ms",
		"send_batch_size":     20,
		"buffer_size":         10,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"dlq": map[string]any{"enabled": true, "dir": dlqDir},
	}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_dlq_entries_total",
		map[string]string{"pipeline_id": "corrupt-dlq"}, 15*time.Second,
		"records dead-lettered", func(v float64) bool { return v >= total })
	proc.stop()

	before := readDLQ(t, dlqDir, "corrupt-dlq")
	if len(before) == 0 {
		t.Fatal("no DLQ records before corruption")
	}
	corruptFile(t, filepath.Join(dlqDir, "corrupt-dlq.dlq.journal"))

	proc = startProc(t, cfg)
	code, exited, logs := proc.exitedWithin(2 * time.Second)
	if exited {
		if code == 0 {
			t.Fatalf("process exited with status 0 without starting")
		}
		if !bytes.Contains([]byte(logs), []byte("DLQ")) && !bytes.Contains([]byte(logs), []byte("journal")) {
			t.Fatalf("process refused to start without naming the corrupted DLQ:\n%s", logs)
		}
		return
	}
	proc.stop()

	after := readDLQ(t, dlqDir, "corrupt-dlq")
	if len(after) == 0 {
		t.Fatalf("corrupt DLQ silently read as empty; %d records were discarded", len(before))
	}
}

// checkpoint journal to another pipeline with a different source. If the
// cursor is accepted blindly, the second pipeline resumes somewhere in the
// middle (or past the end) of its own data and loses records.
func TestCheckpointFromAnotherPipelineIsNotTrusted(t *testing.T) {
	const total = 100
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	outputA := filepath.Join(dir, "a.jsonl")
	outputB := filepath.Join(dir, "b.jsonl")

	db := openOracleDB(t, dbPath)
	ga := newOracleGenerator()
	ga.createSQLiteTable(t, db)
	ga.insertSQLite(t, db, total)
	// Pipeline b reads a table whose rows are all older than the cursor
	// pipeline a leaves behind.
	gb := newOracleGenerator()
	gb.base = ga.base.Add(-48 * time.Hour)
	gb.createSQLiteTableOn(t, db, "old_events")
	gb.insertSQLiteOn(t, db, "old_events", total)

	c := newTestConfig(t)
	c.add(oracleSQLitePipeline("source-a", dbPath, outputA, 10))
	c.add(oracleSQLitePipelineForTable("source-b", dbPath, outputB, 10, "old_events"))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "source-a")
	waitForPipelineUp(t, proc, "source-b")
	waitForJSONLCount(t, outputA, total, 20*time.Second)
	waitForJSONLCount(t, outputB, total, 20*time.Second)
	proc.stop()

	// New rows arrive for b, still older than a's cursor. They must be
	// delivered on the next start; a foreign cursor makes them invisible.
	gb.insertSQLiteOn(t, db, "old_events", 20)

	// Replace b's journal with a's. Both are plain cursor journals, so
	// the file format alone cannot tell them apart.
	aJournal := journalPath(c, "source-a")
	bJournal := journalPath(c, "source-b")
	data, err := os.ReadFile(aJournal)
	if err != nil {
		t.Fatalf("read a journal: %v", err)
	}
	if err := os.WriteFile(bJournal, data, 0o600); err != nil {
		t.Fatalf("copy journal: %v", err)
	}

	proc = startProc(t, cfg)
	code, exited, _ := proc.exitedWithin(2 * time.Second)
	if exited && code != 0 {
		return
	}
	time.Sleep(time.Second)
	proc.stop()

	// The swapped cursor points at the end of a table whose timestamps
	// are newer than every b row, so a trusted cursor hides b's new rows.
	have := make(map[string]bool)
	for _, e := range readLogEntriesTolerant(t, outputB) {
		have[e.Fields["id"]] = true
	}
	var missing []string
	for _, rec := range gb.all() {
		if !have[rec.ID] {
			missing = append(missing, rec.ID)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("pipeline b accepted a foreign checkpoint without an error and lost %d newly inserted rows (%s)",
			len(missing), sampleIDs(missing, 10))
	}
}

// TestCorruptDiskBufferJournalDoesNotDiscardPendingBatches fills the disk
// buffer with undelivered pages, corrupts the journal, and restarts. The
// buffered records are the only copy left, so the process must recover
// them or refuse to start rather than start with an empty queue.
func TestCorruptDiskBufferJournalDoesNotDiscardPendingBatches(t *testing.T) {
	const total = 60
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	bufDir := filepath.Join(dir, "buffer")
	g := newOracleGenerator()
	db := openOracleDB(t, dbPath)
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, total)
	s, srv := newSink(t, http.StatusInternalServerError)

	c := newTestConfig(t)
	p := oracleSQLitePipeline("corrupt-buffer", dbPath, filepath.Join(dir, "out.jsonl"), 20)
	p["destinations"] = []any{destHTTP(srv.URL)}
	runtime := p["runtime"].(map[string]any)
	runtime["send_batch_size"] = 10
	runtime["poll_interval"] = "10ms"
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"disk_buffer": map[string]any{"enabled": true, "dir": bufDir},
	}
	c.Global["circuit_breaker_failure_threshold"] = 1
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_disk_buffer_batches",
		map[string]string{"pipeline_id": "corrupt-buffer"}, 15*time.Second,
		"pages spilled to disk", func(v float64) bool { return v >= 2 })
	proc.stop()

	// The audit table is pruned by retention while the backlog waits, so
	// the buffer is the only remaining copy.
	if _, err := db.Exec(`DELETE FROM oracle_events`); err != nil {
		t.Fatalf("prune source: %v", err)
	}
	corruptFile(t, filepath.Join(bufDir, "corrupt-buffer.buffer.journal"))
	s.setStatuses(http.StatusOK)

	proc = startProc(t, cfg)
	code, exited, _ := proc.exitedWithin(1500 * time.Millisecond)
	if exited {
		if code != 0 {
			return
		}
		t.Fatal("process exited with status 0")
	}
	waitFor(t, 15*time.Second, "all buffered records delivered", func() bool {
		return s.acceptedEntries() >= total
	})
	proc.stop()

	if got := s.acceptedEntries(); got != total {
		t.Fatalf("corrupt disk buffer silently discarded %d of %d buffered records (only %d delivered)",
			total-got, total, got)
	}
}

// TestCorruptDLQJournalDoesNotSilentlyDiscardRecords writes a dead-letter
// record, corrupts the DLQ journal and restarts. Recovery data must not
// disappear without an error.
func TestCorruptDLQJournalDoesNotSilentlyDiscardRecords(t *testing.T) {
	const total = 10
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	dlqDir := filepath.Join(dir, "dlq")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	db := openOracleDB(t, dbPath)
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, total)

	c := newTestConfig(t)
	// A json_parse processor set to poison on error rejects every entry
	// (the payload is not JSON) and sends it to the processor DLQ, the
	// simplest deterministic way to create records.
	p := oracleSQLitePipeline("corrupt-dlq", dbPath, output, 10)
	p["processors"] = []any{map[string]any{"type": "json_parse", "json_parse": map[string]any{
		"source_field": "payload",
		"on_error":     "poison",
	}}}
	p["durability"] = map[string]any{
		"dlq": map[string]any{"enabled": true, "dir": dlqDir},
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_dlq_entries_total",
		map[string]string{"pipeline_id": "corrupt-dlq"}, 15*time.Second,
		"DLQ records written", func(v float64) bool { return v >= total })
	proc.stop()
	dlqPath := filepath.Join(dlqDir, "corrupt-dlq.dlq.journal")
	records := readDLQ(t, dlqDir, "corrupt-dlq")
	if len(records) == 0 {
		t.Fatal("no DLQ records before corruption")
	}
	corruptFile(t, dlqPath)

	proc = startProc(t, cfg)
	code, exited, _ := proc.exitedWithin(1500 * time.Millisecond)
	if exited {
		if code != 0 {
			return
		}
		t.Fatal("process exited with status 0")
	}
	time.Sleep(300 * time.Millisecond)
	proc.stop()

	after := readDLQ(t, dlqDir, "corrupt-dlq")
	if len(after) == 0 {
		t.Fatalf("corrupt DLQ journal was silently accepted: %d dead-lettered records disappeared without an error", len(records))
	}
}
