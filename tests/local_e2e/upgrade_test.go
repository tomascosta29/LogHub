package locale2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"loghub/internal/buffer"
	"loghub/internal/checkpointer"
	"loghub/internal/config"
	"loghub/internal/core"
	"loghub/internal/dlq"
	"loghub/internal/pipeline"
	"loghub/internal/plugins/destination"
	"loghub/internal/plugins/processor"
	"loghub/internal/plugins/source"
)

// TestUpgradeFromV010State starts a pipeline on a data directory left by
// v0.1.0: a plain .cursor file, a JSONL disk buffer holding one
// undelivered batch, and a JSONL DLQ. The pipeline must resume from the
// old cursor, deliver the buffered batch, keep the DLQ record, and not
// re-send rows the old cursor had already passed.
func TestUpgradeFromV010State(t *testing.T) {
	const totalRows = 750
	const pipelineID = "upgrade-e2e"

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	outputPath := filepath.Join(dir, "sentinel.jsonl")
	checkpointDir := filepath.Join(dir, "checkpoints")
	bufferDir := filepath.Join(dir, "buffer")
	dlqDir := filepath.Join(dir, "dlq")
	for _, d := range []string{checkpointDir, bufferDir, dlqDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
	}
	seedAuditDB(t, dbPath, totalRows, 25)

	// v0.1.0 state: rows 1-500 delivered, one batch waiting on disk.
	writeFile(t, filepath.Join(checkpointDir, pipelineID+".cursor"),
		`v1:{"timestamp":"2026-05-06T12:00:19Z","tie":"000500"}`+"\n")
	writeFile(t, filepath.Join(bufferDir, pipelineID+".buffer"),
		`{"entries":[{"timestamp":"2026-05-06T11:00:00Z","source":"sqlite","level":"info","message":"","fields":{"id":"buffered-1","event_time":"2026-05-06T11:00:00Z","user_name":"user-b","action":"login"}}],"next_cursor":"v1:{\"timestamp\":\"2026-05-06T12:00:19Z\",\"tie\":\"000500\"}"}`+"\n")
	writeFile(t, filepath.Join(dlqDir, pipelineID+".dlq"),
		`{"timestamp":"2026-05-01T00:00:00Z","pipeline_id":"upgrade-e2e","entries":[{"message":"old"}],"error":"bad request","destination":"sentinel"}`+"\n")

	cfgPath := filepath.Join(dir, "loghub.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(`
global:
  checkpoints_dir: %q
pipelines:
  - id: %q
    source:
      type: "sqlite"
      sqlite:
        database: %q
        table: "audit_events"
        timestamp_column: "event_time"
        cursor_column: "id"
        fetch_size: 125
    processors:
      - type: "timestamp"
        timestamp:
          source_field: "event_time"
          source_layout: "2006-01-02T15:04:05Z07:00"
          target_field: "timestamp"
    destinations:
      - type: "local_jsonl"
        local_jsonl:
          path: %q
          format: "sentinel"
`, checkpointDir, pipelineID, dbPath, outputPath))
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	pc := cfg.Pipelines[0]

	src, err := source.Create(pc.Source.Type, context.Background(), pc.Source.Properties, nil)
	if err != nil {
		t.Fatalf("source create failed: %v", err)
	}
	proc, err := processor.Create(pc.Processors[0].Type, pc.Processors[0].Properties)
	if err != nil {
		t.Fatalf("processor create failed: %v", err)
	}
	dest, err := destination.Create(pc.Destinations[0].Type, pc.Destinations[0].Properties)
	if err != nil {
		t.Fatalf("destination create failed: %v", err)
	}
	cp, err := checkpointer.NewFileCheckpointer(checkpointDir)
	if err != nil {
		t.Fatalf("checkpointer create failed: %v", err)
	}
	defer cp.Close()
	diskBuf, err := buffer.NewDiskBuffer(bufferDir, pipelineID, 0)
	if err != nil {
		t.Fatalf("disk buffer create failed: %v", err)
	}
	dlqWriter, err := dlq.New(dlqDir, pipelineID, 0)
	if err != nil {
		t.Fatalf("dlq create failed: %v", err)
	}
	recovered, err := dlqWriter.Recover()
	if err != nil || len(recovered) != 1 || recovered[0].Error != "bad request" {
		t.Fatalf("DLQ after upgrade = %+v, %v; want the v0.1.0 record", recovered, err)
	}

	p := pipeline.NewPipeline(pc.ID, src, []core.Processor{proc},
		[]pipeline.DestinationInfo{{Destination: dest}}, cp,
		&pipeline.PipelineSettings{
			PollInterval:     time.Millisecond,
			SendBatchSize:    25,
			BufferSize:       4,
			MaxRetries:       3,
			RetryBaseBackoff: time.Millisecond,
			ShutdownTimeout:  5 * time.Second,
		}, diskBuf, dlqWriter)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("pipeline start failed: %v", err)
	}
	const want = totalRows - 500 + 1 // rows 501-750 plus the buffered batch
	waitFor(t, 5*time.Second, func() bool {
		return len(readSentinelJSONL(t, outputPath)) >= want
	}, "destination did not receive the rows after the old cursor")
	p.Stop()

	records := readSentinelJSONL(t, outputPath)
	if len(records) != want {
		t.Fatalf("got %d records, want %d", len(records), want)
	}
	seen := make(map[string]bool, len(records))
	for _, r := range records {
		id := r["id"]
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
		if id != "buffered-1" && id <= "000500" {
			t.Fatalf("row %s was re-sent although the v0.1.0 cursor had passed it", id)
		}
	}
	if !seen["buffered-1"] {
		t.Fatal("the batch left in the v0.1.0 disk buffer was not delivered")
	}

	cursor, err := cp.Get(pc.ID)
	if err != nil || cursor != `v1:{"timestamp":"2026-05-06T12:00:29Z","tie":"000750"}` {
		t.Fatalf("cursor = %q, %v; want the last row", cursor, err)
	}
	for _, f := range []string{
		filepath.Join(checkpointDir, pipelineID+".cursor"),
		filepath.Join(bufferDir, pipelineID+".buffer"),
		filepath.Join(dlqDir, pipelineID+".dlq"),
	} {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("%s still exists after the upgrade", f)
		}
	}
}
