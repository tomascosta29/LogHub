//go:build !windows

package system_test

import (
	"path/filepath"
	"testing"
	"time"
)

func TestProcessorChainFilterJSONTimestamp(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	writeLogFile(t, input,
		`{"user":"alice","event_time":"2026-05-06T12:00:00Z","action":"login"}`,
		`[ERROR] {"user":"eve","event_time":"2026-05-06T12:01:00Z","action":"denied"}`,
		`not-json`)

	c := newTestConfig(t)
	p := newPipeline("processor-chain", sourceFile(input, "file-input", 10), destLocalJSONL(output, "sentinel"))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["processors"] = []any{
		map[string]any{"type": "filter", "filter": map[string]any{
			"include_level": []string{"INFO"},
		}},
		map[string]any{"type": "json_parse", "json_parse": map[string]any{
			"source_field": "raw",
			"on_error":     "drop",
		}},
		timestampProcessor("event_time", "2006-01-02T15:04:05Z07:00", "timestamp"),
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "processor-chain")
	waitForJSONLCount(t, output, 1, 10*time.Second)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1 (ERROR and invalid JSON filtered): %+v", len(records), records)
	}
	r := records[0]
	if r["user"] != "alice" || r["action"] != "login" {
		t.Fatalf("unexpected record: %+v", r)
	}
	if _, ok := r["raw"]; ok {
		t.Fatalf("keep_source=false left raw in record: %+v", r)
	}
	if r["TimeGenerated"] != "2026-05-06T12:00:00Z" {
		t.Fatalf("TimeGenerated = %q, want the parsed event_time", r["TimeGenerated"])
	}
}

func TestProcessorPoisonPillDLQsOnlyBadEntry(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	dlqDir := filepath.Join(dir, "dlq")
	writeLogFile(t, input, `not-json`, `{"user":"bob","event_time":"2026-05-06T12:00:00Z"}`)

	c := newTestConfig(t)
	p := newPipeline("processor-poison", sourceFile(input, "file-input", 10), destLocalJSONL(output, "sentinel"))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["processors"] = []any{
		map[string]any{"type": "json_parse", "json_parse": map[string]any{
			"source_field": "raw",
			"on_error":     "poison",
		}},
		timestampProcessor("event_time", "2006-01-02T15:04:05Z07:00", "timestamp"),
	}
	p["durability"] = map[string]any{"dlq": map[string]any{"enabled": true, "dir": dlqDir}}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("loghub log:\n%s", proc.log())
		}
	})
	waitForPipelineUp(t, proc, "processor-poison")
	waitForJSONLCount(t, output, 1, 10*time.Second)
	waitForMetric(t, proc.addr, "loghub_dlq_entries_total",
		map[string]string{"pipeline_id": "processor-poison"}, 10*time.Second,
		"poison entry in DLQ", func(v float64) bool { return v >= 1 })
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != 1 || records[0]["user"] != "bob" {
		t.Fatalf("good entry not delivered: %+v", records)
	}
	dlqRecords := readDLQ(t, dlqDir, "processor-poison")
	if len(dlqRecords) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(dlqRecords))
	}
	if dlqRecords[0].Destination != "processor" {
		t.Fatalf("DLQ destination = %q, want processor", dlqRecords[0].Destination)
	}
	if len(dlqRecords[0].Entries) != 1 || dlqRecords[0].Entries[0].Message != "not-json" {
		t.Fatalf("DLQ entry = %+v, want the invalid raw line", dlqRecords[0].Entries)
	}
}
