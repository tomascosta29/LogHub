package system_test

import (
	"net/http"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestReloadWithPendingDiskBufferLosesNoRecords reloads while pages are
// waiting in the disk buffer and a destination is down. The replacement
// pipeline must inherit exactly the pending set: nothing may be stranded in
// the old journal and nothing delivered twice.
func TestReloadWithPendingDiskBufferLosesNoRecords(t *testing.T) {
	const total = 300
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	bufDir := filepath.Join(dir, "buffer")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	db := openOracleDB(t, dbPath)
	g.createSQLiteTable(t, db)
	g.insertSQLite(t, db, total)
	s, srv := newSink(t, http.StatusInternalServerError)

	c := newTestConfig(t)
	p := oracleSQLitePipeline("reload-buffer", dbPath, output, 20)
	p["destinations"] = []any{destHTTP(srv.URL)}
	runtime := p["runtime"].(map[string]any)
	runtime["send_batch_size"] = 10
	runtime["poll_interval"] = "10ms"
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"disk_buffer": map[string]any{"enabled": true, "dir": bufDir},
	}
	c.Global["circuit_breaker_failure_threshold"] = 1
	c.Global["circuit_breaker_recovery_timeout"] = "200ms"
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_disk_buffer_batches",
		map[string]string{"pipeline_id": "reload-buffer"}, 15*time.Second,
		"pages spilled to disk", func(v float64) bool { return v >= 2 })

	// Reload with the same configuration while the backlog is on disk.
	proc.signal(syscall.SIGHUP)
	waitForHealth(t, proc.addr, 15*time.Second, "reloaded pipeline running", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 &&
			(h.Pipelines[0].Status == "running" || h.Pipelines[0].Status == "degraded")
	})

	s.setStatuses(http.StatusOK)
	waitFor(t, 40*time.Second, "all records delivered after reload", func() bool {
		return s.acceptedEntries() >= total
	})
	proc.stop()

	observed := observedFromLogEntries("http", s.deliveredLogEntries(t))
	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"http"},
		Observed:     observed,
	}, "reload with pending disk buffer")
}
