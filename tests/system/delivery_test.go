//go:build !windows

package system_test

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"loghub/internal/checkpointer"
)

func sqliteSentinelPipeline(id, dbPath, output string, fetchSize, sendBatch int) map[string]any {
	p := newPipeline(id,
		sourceSQLite(dbPath, "audit_events", "event_time", "id", fetchSize, nil),
		destLocalJSONL(output, "sentinel"))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     sendBatch,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "5s",
	}
	p["processors"] = []any{sentinelTimestampProcessor()}
	return p
}

func TestGracefulRestartResume(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 1000, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("graceful", dbPath, output, 100, 10))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "graceful")
	waitForJSONLCount(t, output, 100, 15*time.Second)
	proc.stop()

	fixture.insert(t, 50, 10)
	proc = startProc(t, cfg)
	waitForRunning(t, proc, "graceful")
	waitForJSONLCount(t, output, 1050, 20*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != 1050 {
		t.Fatalf("records = %d, want 1050", len(records))
	}
	ids := idsFromSentinel(records)
	assertUnique(t, ids)
	assertSetContains(t, ids, fixtureIDs(1, 1050))

	cursor := readCursor(t, filepath.Join(c.dir, "checkpoints"), "graceful")
	want := `v1:{"timestamp":"2026-05-06T12:01:44Z","tie":"001050"}`
	if cursor != want {
		t.Fatalf("cursor = %q, want %q", cursor, want)
	}
}

func TestHardCrashResume(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 5000, 10)

	c := newTestConfig(t)
	p := sqliteSentinelPipeline("hard-crash", dbPath, output, 50, 10)
	p["runtime"] = map[string]any{
		"poll_interval":       "5ms",
		"send_batch_size":     10,
		"buffer_size":         8,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "5s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "hard-crash")
	waitForJSONLCount(t, output, 50, 10*time.Second)
	code, logs := proc.kill()
	if code == 0 {
		t.Fatalf("SIGKILL produced exit code 0\n%s", logs)
	}

	proc = startProc(t, cfg)
	waitForRunning(t, proc, "hard-crash")
	waitForJSONLCount(t, output, 5000, 60*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	ids := idsFromSentinel(records)
	assertSetContains(t, ids, fixtureIDs(1, 5000))
	if len(records) > 5000+200 {
		t.Fatalf("duplicate replay = %d records for 5000 rows, want bounded by in-flight pages", len(records))
	}
	cursor := readCursor(t, filepath.Join(c.dir, "checkpoints"), "hard-crash")
	want := `v1:{"timestamp":"2026-05-06T12:08:19Z","tie":"005000"}`
	if cursor != want {
		t.Fatalf("cursor = %q, want %q", cursor, want)
	}
}

func TestSendBatchSizeSplits(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 200, 10)

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("split", dbPath, output, 100, 10))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "split")
	waitForJSONLCount(t, output, 200, 15*time.Second)

	fams := scrapeMetrics(t, proc.addr)
	pid := map[string]string{"pipeline_id": "split"}
	dest := map[string]string{"pipeline_id": "split", "destination_type": "local_jsonl"}
	if got := mustValue(t, fams, "loghub_entries_sent_total", dest); got != 200 {
		t.Errorf("entries_sent_total = %v, want 200", got)
	}
	if got := mustValue(t, fams, "loghub_batches_sent_total", pid); got < 20 {
		t.Errorf("batches_sent_total = %v, want >= 20 (200 entries / 10)", got)
	}
	proc.stop()

	cursor := readCursor(t, filepath.Join(c.dir, "checkpoints"), "split")
	want := `v1:{"timestamp":"2026-05-06T12:00:19Z","tie":"000200"}`
	if cursor != want {
		t.Fatalf("cursor = %q, want %q", cursor, want)
	}
}

func TestCheckpointIntervalFlushesOnStop(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, 300, 10)

	c := newTestConfig(t)
	p := sqliteSentinelPipeline("checkpoint-interval", dbPath, output, 25, 25)
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     25,
		"buffer_size":         4,
		"checkpoint_interval": "100ms",
		"shutdown_timeout":    "5s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "checkpoint-interval")
	waitForJSONLCount(t, output, 300, 15*time.Second)
	fams := scrapeMetrics(t, proc.addr)
	if got := counterValue(fams, "loghub_checkpoints_saved_total", map[string]string{"pipeline_id": "checkpoint-interval"}); got < 1 {
		t.Errorf("checkpoints_saved_total = %v, want >= 1", got)
	}
	proc.stop()

	cursor := readCursor(t, filepath.Join(c.dir, "checkpoints"), "checkpoint-interval")
	want := `v1:{"timestamp":"2026-05-06T12:00:29Z","tie":"000300"}`
	if cursor != want {
		t.Fatalf("cursor = %q, want %q", cursor, want)
	}
}

func TestDiskBufferSurvivesRestart(t *testing.T) {
	const total = 100
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 10)
	s, srv := newSink(t, http.StatusInternalServerError)

	c := newTestConfig(t)
	p := newPipeline("disk-restart",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         20,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"disk_buffer": map[string]any{"enabled": true, "dir": filepath.Join(dir, "disk-buffer")},
	}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.Global["circuit_breaker_failure_threshold"] = 1
	c.Global["circuit_breaker_recovery_timeout"] = "30s"
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_disk_buffer_batches",
		map[string]string{"pipeline_id": "disk-restart"}, 10*time.Second,
		"disk buffer spill", func(v float64) bool { return v >= 1 })
	proc.stop()
	if got := s.acceptedEntries(); got != 0 {
		t.Fatalf("sink accepted %d entries during a 500 outage", got)
	}

	s.setStatuses(http.StatusOK)
	proc = startProc(t, cfg)
	waitFor(t, 20*time.Second, "disk buffer drained after restart", func() bool {
		return s.acceptedEntries() >= total
	})
	proc.stop()
	if got := s.acceptedEntries(); got != total {
		t.Fatalf("accepted entries after restart = %d, want %d", got, total)
	}
}

func TestMultiDestinationIsolationAndDLQ(t *testing.T) {
	const total = 200
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "healthy.jsonl")
	dlqDir := filepath.Join(dir, "dlq")
	bufDir := filepath.Join(dir, "buffer")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 10)
	s, srv := newSink(t, http.StatusInternalServerError)

	c := newTestConfig(t)
	p := newPipeline("fanout",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		destLocalJSONL(output, "sentinel"),
		destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         20,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["processors"] = []any{sentinelTimestampProcessor()}
	p["durability"] = map[string]any{
		"dlq":         map[string]any{"enabled": true, "dir": dlqDir},
		"disk_buffer": map[string]any{"enabled": true, "dir": bufDir},
	}
	c.Global["circuit_breaker_failure_threshold"] = 2
	c.Global["circuit_breaker_recovery_timeout"] = "150ms"
	c.Global["stalled_threshold_multiplier"] = 50
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "fanout")
	waitForJSONLCount(t, output, total, 20*time.Second)
	waitForMetric(t, proc.addr, "loghub_dlq_entries_total",
		map[string]string{"pipeline_id": "fanout"}, 10*time.Second,
		"DLQ write", func(v float64) bool { return v >= 1 })
	waitForMetric(t, proc.addr, "loghub_circuit_breaker_state",
		map[string]string{"pipeline_id": "fanout", "destination_type": "http"},
		10*time.Second, "breaker open", func(v float64) bool { return v == 1 })

	waitForHealth(t, proc.addr, 5*time.Second, "fanout degraded", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "degraded"
	})

	s.setStatuses(http.StatusOK)
	waitForMetric(t, proc.addr, "loghub_circuit_breaker_state",
		map[string]string{"pipeline_id": "fanout", "destination_type": "http"},
		10*time.Second, "breaker closed", func(v float64) bool { return v == 0 })
	waitForMetric(t, proc.addr, "loghub_disk_buffer_batches",
		map[string]string{"pipeline_id": "fanout"}, 10*time.Second,
		"disk buffer drained", func(v float64) bool { return v == 0 })
	waitForHealth(t, proc.addr, 10*time.Second, "fanout running again", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "running"
	})
	proc.stop()

	healthy := readSentinelRecords(t, output)
	if len(healthy) != total {
		t.Fatalf("healthy destination records = %d, want %d", len(healthy), total)
	}
	ids := idsFromSentinel(healthy)
	assertUnique(t, ids)
	assertSetContains(t, ids, fixtureIDs(1, total))

	records := readDLQ(t, dlqDir, "fanout")
	if len(records) == 0 {
		t.Fatal("no DLQ records after the failing destination")
	}
	dlqEntries := 0
	for _, fb := range records {
		if fb.Destination != "http#1" {
			t.Errorf("DLQ destination = %q, want http#1", fb.Destination)
		}
		dlqEntries += len(fb.Entries)
	}
	if got := s.acceptedEntries() + dlqEntries; got != total {
		t.Fatalf("accepted (%d) + DLQ (%d) = %d, want %d", s.acceptedEntries(), dlqEntries, got, total)
	}
}

func TestDuplicateTimestampGroupsNoGaps(t *testing.T) {
	const total = 500
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 50) // 10 rows per page share a timestamp

	c := newTestConfig(t)
	c.add(sqliteSentinelPipeline("tie-groups", dbPath, output, 37, 37))
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "tie-groups")
	waitForJSONLCount(t, output, total, 20*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != total {
		t.Fatalf("records = %d, want %d", len(records), total)
	}
	ids := idsFromSentinel(records)
	assertUnique(t, ids)
	if !containsAll(ids, fixtureIDs(1, total)) {
		t.Fatal("duplicate timestamp groups caused skipped ids")
	}
}

func TestDiskBufferSizeLimitRetainsData(t *testing.T) {
	const total = 300
	const payloadSize = 8000
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	big := strings.Repeat("x", payloadSize)
	lines := make([]string, total)
	for i := range lines {
		lines[i] = fmt.Sprintf("[INFO] %04d-%s", i, big)
	}
	writeLogFile(t, input, lines...)
	s, srv := newSink(t, http.StatusInternalServerError)

	c := newTestConfig(t)
	p := newPipeline("disk-limit", sourceFile(input, "file-input", 300), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     20,
		"buffer_size":         20,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "5s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["durability"] = map[string]any{
		"disk_buffer": map[string]any{
			"enabled":     true,
			"dir":         filepath.Join(dir, "disk-buffer"),
			"max_size_mb": 1,
		},
	}
	c.Global["circuit_breaker_failure_threshold"] = 1
	c.Global["circuit_breaker_recovery_timeout"] = "100ms"
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("loghub log:\n%s", proc.log())
		}
	})
	waitForLog(t, proc, "disk buffer size limit exceeded", 20*time.Second)
	s.setStatuses(http.StatusOK)
	waitFor(t, 40*time.Second, "all entries delivered after disk-buffer pressure", func() bool {
		return s.acceptedEntries() >= total
	})
	proc.stop()

	if got := s.acceptedEntries(); got != total {
		t.Fatalf("accepted = %d, want %d", got, total)
	}
}

func containsAll(have, want []string) bool {
	set := make(map[string]bool, len(have))
	for _, id := range have {
		set[id] = true
	}
	for _, id := range want {
		if !set[id] {
			return false
		}
	}
	return true
}

func TestFanOutFiveDestinations(t *testing.T) {
	const total = 100
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 10)

	c := newTestConfig(t)
	var outputs []string
	var dests []map[string]any
	for i := 0; i < 5; i++ {
		out := filepath.Join(dir, fmt.Sprintf("out-%d.jsonl", i))
		outputs = append(outputs, out)
		dests = append(dests, destLocalJSONL(out, "sentinel"))
	}
	p := newPipeline("fanout-five",
		sourceSQLite(dbPath, "audit_events", "event_time", "id", 10, nil),
		dests...)
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         10,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["processors"] = []any{sentinelTimestampProcessor()}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "fanout-five")
	for _, out := range outputs {
		waitForJSONLCount(t, out, total, 20*time.Second)
	}
	proc.stop()
	for _, out := range outputs {
		records := readSentinelRecords(t, out)
		if len(records) != total {
			t.Fatalf("%s records = %d, want %d", out, len(records), total)
		}
		ids := idsFromSentinel(records)
		assertUnique(t, ids)
		assertSetContains(t, ids, fixtureIDs(1, total))
	}
}

func TestMultiplePipelinesIsolation(t *testing.T) {
	dir := t.TempDir()
	c := newTestConfig(t)
	for i := 0; i < 3; i++ {
		input := filepath.Join(dir, fmt.Sprintf("in-%d.log", i))
		output := filepath.Join(dir, fmt.Sprintf("out-%d.jsonl", i))
		writeLogFile(t, input, fmt.Sprintf("[INFO] pipeline-%d-a", i), fmt.Sprintf("[INFO] pipeline-%d-b", i))
		p := newPipeline(fmt.Sprintf("multi-%d", i), sourceFile(input, fmt.Sprintf("source-%d", i), 10), destLocalJSONL(output, ""))
		p["runtime"] = map[string]any{
			"poll_interval":       "10ms",
			"send_batch_size":     10,
			"buffer_size":         4,
			"checkpoint_interval": "0",
			"shutdown_timeout":    "3s",
		}
		c.add(p)
	}
	cfg := c.write(t)
	proc := startProc(t, cfg)
	for i := 0; i < 3; i++ {
		waitForRunning(t, proc, fmt.Sprintf("multi-%d", i))
	}
	for i := 0; i < 3; i++ {
		output := filepath.Join(dir, fmt.Sprintf("out-%d.jsonl", i))
		waitForJSONLCount(t, output, 2, 10*time.Second)
	}
	proc.stop()
	for i := 0; i < 3; i++ {
		output := filepath.Join(dir, fmt.Sprintf("out-%d.jsonl", i))
		entries := readLogEntries(t, output)
		if len(entries) != 2 {
			t.Fatalf("pipeline %d records = %d, want 2", i, len(entries))
		}
		for _, e := range entries {
			if !strings.HasPrefix(e.Message, fmt.Sprintf("pipeline-%d-", i)) {
				t.Fatalf("pipeline %d received foreign message %q", i, e.Message)
			}
		}
		waitFor(t, 5*time.Second, "cursor for pipeline", func() bool {
			cp, err := checkpointer.NewFileCheckpointer(filepath.Join(c.dir, "checkpoints"))
			if err != nil {
				return false
			}
			defer cp.Close()
			_, err = cp.Get(fmt.Sprintf("multi-%d", i))
			return err == nil
		})
	}
}
