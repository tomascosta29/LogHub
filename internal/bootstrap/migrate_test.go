package bootstrap

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"loghub/internal/buffer"
	"loghub/internal/checkpointer"
	"loghub/internal/dlq"
	"loghub/internal/journal"
)

// writeUnifiedJournal writes records in the format unreleased builds used
// for <id>.pipeline.journal: one tagged entry per committed transaction.
func writeUnifiedJournal(t *testing.T, path string, records ...string) {
	t.Helper()
	j, err := journal.Open(path)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	defer j.Close()
	for _, r := range records {
		if _, err := j.AppendBarrier([]byte(r)); err != nil {
			t.Fatalf("AppendBarrier: %v", err)
		}
	}
}

func TestImportUnifiedJournal(t *testing.T) {
	cpDir, bufDir, dlqDir := t.TempDir(), t.TempDir(), t.TempDir()
	batch, _ := json.Marshal(buffer.DiskBatch{NextCursor: "c-new"})
	failed, _ := json.Marshal(dlq.FailedBatch{PipelineID: "p", Error: "boom", Destination: "sentinel"})
	// The cursor is followed by other writes: the case where the
	// unreleased build lost the cursor on restart.
	writeUnifiedJournal(t, unifiedJournalPath(cpDir, "p"),
		`{"type":"disk_batch","data":{"entries":[],"next_cursor":"drained"}}`,
		`{"type":"drain_marker"}`,
		`{"type":"cursor","data":"cursor-7"}`,
		`{"type":"disk_batch","data":`+string(batch)+`}`,
		`{"type":"dlq","data":`+string(failed)+`}`,
	)

	cp, err := checkpointer.NewFileCheckpointer(cpDir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer: %v", err)
	}
	defer cp.Close()
	diskBuf, err := buffer.NewDiskBuffer(bufDir, "p", 0)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	defer diskBuf.Close()
	dlqWriter, err := dlq.New(dlqDir, "p", 0)
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	defer dlqWriter.Close()

	if err := importUnifiedJournal(cpDir, "p", cp, diskBuf, dlqWriter); err != nil {
		t.Fatalf("importUnifiedJournal: %v", err)
	}

	if got, err := cp.Get("p"); err != nil || got != "cursor-7" {
		t.Errorf("cursor = %q, %v; want cursor-7", got, err)
	}
	var pending []string
	diskBuf.Drain(func(_ int64, b buffer.DiskBatch) error {
		pending = append(pending, b.NextCursor)
		return nil
	})
	if strings.Join(pending, ",") != "c-new" {
		t.Errorf("pending disk batches = %v, want [c-new] (the drained one is not imported)", pending)
	}
	if recs, _ := dlqWriter.Recover(); len(recs) != 1 || recs[0].Error != "boom" {
		t.Errorf("DLQ records = %+v, want the imported record", recs)
	}
	if _, err := os.Stat(unifiedJournalPath(cpDir, "p") + ".migrated"); err != nil {
		t.Errorf("journal not renamed to .migrated: %v", err)
	}

	// A second run finds nothing to import.
	if err := importUnifiedJournal(cpDir, "p", cp, diskBuf, dlqWriter); err != nil {
		t.Fatalf("second importUnifiedJournal: %v", err)
	}
	if got := diskBuf.Count(); got != 1 {
		t.Errorf("disk buffer count after second run = %d, want 1", got)
	}
}

func TestImportUnifiedJournalRefusesToDropBatches(t *testing.T) {
	cpDir := t.TempDir()
	writeUnifiedJournal(t, unifiedJournalPath(cpDir, "p"),
		`{"type":"disk_batch","data":{"entries":[],"next_cursor":"c"}}`)
	cp, err := checkpointer.NewFileCheckpointer(cpDir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer: %v", err)
	}
	defer cp.Close()

	err = importUnifiedJournal(cpDir, "p", cp, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "disk_buffer is disabled") {
		t.Fatalf("importUnifiedJournal = %v, want an error about the disabled disk buffer", err)
	}
	if _, err := os.Stat(unifiedJournalPath(cpDir, "p")); err != nil {
		t.Errorf("journal must be left in place when it cannot be imported: %v", err)
	}
}

func TestDecodeUnifiedCursor(t *testing.T) {
	for raw, want := range map[string]string{
		`"plain"`:          "plain",
		`{"v":1,"ts":"x"}`: `{"v":1,"ts":"x"}`,
		`12345`:            "12345",
		`null`:             "",
	} {
		if got := decodeUnifiedCursor(json.RawMessage(raw)); got != want {
			t.Errorf("decodeUnifiedCursor(%s) = %q, want %q", raw, got, want)
		}
	}
}
