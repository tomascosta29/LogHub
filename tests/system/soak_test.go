package system_test

import (
	"path/filepath"
	"testing"
	"time"
)

// TestSoak10kRows is a short throughput/soak smoke test: ten thousand rows
// through SQLite, the timestamp processor and the sentinel JSONL writer.
// The full 30-minute soak belongs in a nightly job; this catches gross
// regressions in the PR suite.
func TestSoak10kRows(t *testing.T) {
	const total = 10000
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 25)

	c := newTestConfig(t)
	p := sqliteSentinelPipeline("soak", dbPath, output, 500, 100)
	p["runtime"] = map[string]any{
		"poll_interval":       "5ms",
		"send_batch_size":     100,
		"buffer_size":         50,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "10s",
	}
	c.add(p)
	cfg := c.write(t)

	start := time.Now()
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "soak")
	waitForJSONLCount(t, output, total, 60*time.Second)
	elapsed := time.Since(start)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != total {
		t.Fatalf("records = %d, want %d", len(records), total)
	}
	ids := idsFromSentinel(records)
	assertUnique(t, ids)
	assertSetContains(t, ids, fixtureIDs(1, total))
	t.Logf("delivered %d rows in %s (%.0f rows/s)", total, elapsed, float64(total)/elapsed.Seconds())
	if elapsed > 60*time.Second {
		t.Fatalf("10k-row soak took %s, want < 60s", elapsed)
	}
}
