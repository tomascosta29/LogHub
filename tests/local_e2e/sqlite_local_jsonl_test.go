package locale2e_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"loghub/internal/checkpointer"
	"loghub/internal/config"
	"loghub/internal/core"
	"loghub/internal/pipeline"
	"loghub/internal/plugins/destination"
	"loghub/internal/plugins/processor"
	"loghub/internal/plugins/source"
)

func TestSQLiteToLocalJSONLSentinelE2E(t *testing.T) {
	const totalRows = 750
	const batchSize = 125

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	outputPath := filepath.Join(dir, "sentinel.jsonl")
	checkpointDir := filepath.Join(dir, "checkpoints")

	seedAuditDB(t, dbPath, totalRows, 25)

	cfgPath := filepath.Join(dir, "loghub.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(`
global:
  checkpoints_dir: %q
pipelines:
  - id: "sqlite-local-e2e"
    runtime:
      poll_interval: "1ms"
      send_batch_size: %d
      buffer_size: 4
      checkpoint_interval: "0"
      shutdown_timeout: "5s"
    retry:
      max_attempts: 3
      base_backoff: "1ms"
    source:
      type: "sqlite"
      sqlite:
        database: %q
        table: "audit_events"
        timestamp_column: "event_time"
        cursor_column: "id"
        fetch_size: %d
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
`, checkpointDir, 25, dbPath, batchSize, outputPath))

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	pc := cfg.Pipelines[0]

	src, err := source.Create(pc.Source.Type, context.Background(), pc.Source.Properties, nil)
	if err != nil {
		t.Fatalf("source create failed: %v", err)
	}
	var processors []core.Processor
	for _, procConfig := range pc.Processors {
		proc, err := processor.Create(procConfig.Type, procConfig.Properties)
		if err != nil {
			t.Fatalf("processor create failed: %v", err)
		}
		processors = append(processors, proc)
	}
	dest, err := destination.Create(pc.Destinations[0].Type, pc.Destinations[0].Properties)
	if err != nil {
		t.Fatalf("destination create failed: %v", err)
	}
	cp, err := checkpointer.NewFileCheckpointer(cfg.Global.CheckpointsDir)
	if err != nil {
		t.Fatalf("checkpointer create failed: %v", err)
	}

	p := pipeline.NewPipeline(
		pc.ID,
		src,
		processors,
		[]pipeline.DestinationInfo{{Destination: dest}},
		cp,
		&pipeline.PipelineSettings{
			PollInterval:       time.Millisecond,
			BufferSize:         4,
			MaxRetries:         3,
			RetryBaseBackoff:   time.Millisecond,
			CheckpointInterval: 0,
			ShutdownTimeout:    5 * time.Second,
		},
		nil,
		nil,
	)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("pipeline start failed: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(readSentinelJSONL(t, outputPath)) == totalRows
	}, "local JSONL destination did not receive all rows")
	p.Stop()

	records := readSentinelJSONL(t, outputPath)
	if len(records) != totalRows {
		t.Fatalf("expected %d records, got %d", totalRows, len(records))
	}

	seen := make(map[string]bool, totalRows)
	for i, record := range records {
		id := record["id"]
		if id == "" {
			t.Fatalf("record %d missing id: %+v", i, record)
		}
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
		if record["TimeGenerated"] == "" || record["event_time"] == "" || record["user_name"] == "" || record["action"] == "" {
			t.Fatalf("record %s missing required Sentinel fields: %+v", id, record)
		}
		if record["TimeGenerated"] != record["event_time"] {
			t.Fatalf("record %s TimeGenerated %q does not match event_time %q", id, record["TimeGenerated"], record["event_time"])
		}
		if _, ok := record["fields"]; ok {
			t.Fatalf("record %s should be flattened, got nested fields key", id)
		}
	}
	for i := 1; i <= totalRows; i++ {
		id := fmt.Sprintf("%06d", i)
		if !seen[id] {
			t.Fatalf("missing id %s", id)
		}
	}

	cursor, err := cp.Get(pc.ID)
	if err != nil {
		t.Fatalf("checkpoint read failed: %v", err)
	}
	if cursor != `v1:{"timestamp":"2026-05-06T12:00:29Z","tie":"000750"}` {
		t.Fatalf("unexpected checkpoint cursor: %s", cursor)
	}
}

func seedAuditDB(t *testing.T, dbPath string, totalRows, duplicateTimestampGroupSize int) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE audit_events (
			id TEXT PRIMARY KEY,
			event_time TEXT NOT NULL,
			user_name TEXT NOT NULL,
			action TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("failed to create audit_events table: %v", err)
	}

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO audit_events (id, event_time, user_name, action) VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("failed to prepare insert: %v", err)
	}
	defer stmt.Close()

	for i := 1; i <= totalRows; i++ {
		group := (i - 1) / duplicateTimestampGroupSize
		eventTime := base.Add(time.Duration(group) * time.Second).Format(time.RFC3339)
		id := fmt.Sprintf("%06d", i)
		action := "login"
		if i%7 == 0 {
			action = "denied"
		}
		if _, err := stmt.Exec(id, eventTime, "user-"+id, action); err != nil {
			t.Fatalf("failed to insert row %s: %v", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit seed data: %v", err)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
}

func readSentinelJSONL(t *testing.T, path string) []map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("failed to open %s: %v", path, err)
	}
	defer file.Close()

	var records []map[string]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("failed to decode JSONL record: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan JSONL records: %v", err)
	}
	return records
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if condition() {
			return
		}
		select {
		case <-deadline:
			t.Fatal(message)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
