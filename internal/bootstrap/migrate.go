package bootstrap

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"loghub/internal/buffer"
	"loghub/internal/core"
	"loghub/internal/dlq"
	"loghub/internal/journal"
)

// unifiedJournalRecord is one entry of the <pipelineID>.pipeline.journal
// written by unreleased builds that shared one journal between the
// checkpointer, disk buffer and DLQ.
type unifiedJournalRecord struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// unifiedJournalPath returns where unreleased builds kept the shared
// journal for pipelineID.
func unifiedJournalPath(checkpointsDir, pipelineID string) string {
	return filepath.Join(checkpointsDir, pipelineID+".pipeline.journal")
}

// importUnifiedJournal moves the state held in a shared pipeline journal
// into the per-subsystem stores, then renames the file with a
// ".migrated" suffix. It does nothing if there is no such file.
//
// The shared journal was the source of truth whenever it existed, so its
// latest cursor replaces the checkpointer's. Disk-buffer batches written
// after its last drain marker, and every DLQ record, are appended to the
// disk buffer and DLQ. If the import is interrupted, it runs again on the
// next start; batches imported twice are delivered twice (at-least-once).
func importUnifiedJournal(checkpointsDir, pipelineID string, cp core.Checkpointer, diskBuf *buffer.DiskBuffer, dlqWriter *dlq.DeadLetterQueue) error {
	path := unifiedJournalPath(checkpointsDir, pipelineID)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var cursor string
	var batches []buffer.DiskBatch
	var failed []dlq.FailedBatch
	err := journal.ReadCommittedFile(path, func(id int64, data []byte) error {
		var r unifiedJournalRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return fmt.Errorf("entry %d: %w", id, err)
		}
		switch r.Type {
		case "cursor":
			cursor = decodeUnifiedCursor(r.Data)
		case "drain_marker":
			batches = batches[:0]
		case "disk_batch":
			var b buffer.DiskBatch
			if err := json.Unmarshal(r.Data, &b); err != nil {
				return fmt.Errorf("disk batch entry %d: %w", id, err)
			}
			batches = append(batches, b)
		case "dlq":
			var fb dlq.FailedBatch
			if err := json.Unmarshal(r.Data, &fb); err != nil {
				return fmt.Errorf("DLQ entry %d: %w", id, err)
			}
			failed = append(failed, fb)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	if len(batches) > 0 && diskBuf == nil {
		return fmt.Errorf("%s holds %d undelivered disk-buffer batches but durability.disk_buffer is disabled; enable it for one start to import them, or delete the file to discard them", path, len(batches))
	}
	if len(failed) > 0 && dlqWriter == nil {
		return fmt.Errorf("%s holds %d DLQ records but durability.dlq is disabled; enable it for one start to import them, or delete the file to discard them", path, len(failed))
	}

	if cursor != "" {
		if err := cp.Set(pipelineID, cursor); err != nil {
			return fmt.Errorf("import cursor: %w", err)
		}
	}
	for _, b := range batches {
		if err := diskBuf.Write(b); err != nil {
			return fmt.Errorf("import disk-buffer batch: %w", err)
		}
	}
	for _, fb := range failed {
		if err := dlqWriter.Import(fb); err != nil {
			return fmt.Errorf("import DLQ record: %w", err)
		}
	}
	if err := os.Rename(path, path+".migrated"); err != nil {
		return err
	}
	slog.Info("Imported shared pipeline journal from an unreleased build", "pipeline_id", pipelineID, "file", path, "cursor", cursor, "disk_batches", len(batches), "dlq_records", len(failed))
	return nil
}

// decodeUnifiedCursor returns the cursor stored in a shared-journal
// entry. Cursors that were valid JSON were stored inline; any other
// cursor was stored as a JSON string.
func decodeUnifiedCursor(data json.RawMessage) string {
	if len(data) == 0 || string(data) == "null" {
		return ""
	}
	var s string
	if data[0] == '"' && json.Unmarshal(data, &s) == nil {
		return s
	}
	return string(data)
}
