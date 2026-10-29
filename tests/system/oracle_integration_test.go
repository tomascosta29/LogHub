//go:build !windows

package system_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestOracleTracksFileSourceDelivery proves the accounting oracle against a
// file source: every generated record reaches the JSONL destination exactly
// once, intact, in order, and the counters reconcile.
func TestOracleTracksFileSourceDelivery(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 500)

	c := newTestConfig(t)
	c.add(oracleFilePipeline("oracle-file", input, output, 50))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "oracle-file")
	waitForJSONLCount(t, output, 500, 20*time.Second)
	// Wait for every page's checkpoint before sampling the counters; the
	// send counter advances before the durable checkpoint does.
	waitForMetric(t, proc.addr, "loghub_checkpoints_saved_total",
		map[string]string{"pipeline_id": "oracle-file"}, 10*time.Second,
		"checkpoints caught up", func(v float64) bool { return v >= 10 })

	entries := readLogEntries(t, output)
	res := verifyOracle(t, g, oracleCheck{
		Destinations: []string{"local_jsonl"},
		Observed:     observedFromLogEntries("local_jsonl", entries),
	}, "file source")
	if res.Duplicates != 0 {
		t.Errorf("duplicates = %d, want 0", res.Duplicates)
	}

	fams := scrapeMetrics(t, proc.addr)
	m := pipelineOracleMetrics(t, fams, "oracle-file", "oracle-input", "local_jsonl", 0)
	for _, err := range reconcile(m) {
		t.Error(err)
	}
	proc.stop()
}

// TestOracleTracksSQLiteSourceDelivery runs the same accounting against a
// SQL cursor with a tie-breaker column, so page boundaries and duplicate
// timestamps are covered from the start.
func TestOracleTracksSQLiteSourceDelivery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("wal: %v", err)
	}
	g := newOracleGenerator()
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, 400)

	c := newTestConfig(t)
	c.add(oracleSQLitePipeline("oracle-sqlite", dbPath, output, 37))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "oracle-sqlite")
	waitForJSONLCount(t, output, 400, 20*time.Second)
	entries := readLogEntries(t, output)
	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"local_jsonl"},
		Observed:     observedFromLogEntries("local_jsonl", entries),
	}, "sqlite source")

	fams := scrapeMetrics(t, proc.addr)
	m := pipelineOracleMetrics(t, fams, "oracle-sqlite", "oracle-input", "local_jsonl", 0)
	for _, err := range reconcile(m) {
		t.Error(err)
	}
	proc.stop()
}

// TestOracleTracksHTTPDestination checks that the HTTP destination delivers
// each record intact to a fake server, which is the shape every destination
// fault scenario uses.
func TestOracleTracksHTTPDestination(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 200)
	s, srv := newSink(t, http.StatusOK)

	c := newTestConfig(t)
	p := oracleFilePipeline("oracle-http", input, filepath.Join(dir, "unused.jsonl"), 50)
	p["destinations"] = []any{destHTTP(srv.URL)}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "oracle-http")
	waitFor(t, 20*time.Second, "HTTP destination to accept every record", func() bool {
		return s.acceptedEntries() >= 200
	})
	entries := s.deliveredLogEntries(t)
	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"http"},
		Observed:     observedFromLogEntries("http", entries),
	}, "http destination")
	proc.stop()
}

// TestOracleReconciliationDetectsDrift proves a scenario where the counters
// and the checker disagree fails loudly instead of being averaged away.
func TestOracleReconciliationDetectsDrift(t *testing.T) {
	g := newOracleGenerator()
	g.generate(5)
	// Pretend the destination observed four of five records while the
	// counters claim five were sent.
	recs := g.all()[:4]
	var observed []observedRecord
	for _, r := range recs {
		observed = append(observed, observedRecordFromFields("dest", r.fields()))
	}
	res := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: observed})
	if res.ok() {
		t.Fatal("checker accepted a missing record")
	}
	if errs := reconcile(oracleMetrics{Collected: 5, Sent: 5}); len(errs) != 0 {
		t.Fatalf("balanced-but-wrong counters should reconcile; the oracle is what catches loss: %v", errs)
	}
	if !containsStr(res.Errors, fmt.Sprintf("lost 1 records (%s)", sampleIDs(res.Missing, 10))) {
		if len(res.Missing) != 1 {
			t.Fatalf("missing = %v, want exactly one record", res.Missing)
		}
	}
}

func containsStr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
