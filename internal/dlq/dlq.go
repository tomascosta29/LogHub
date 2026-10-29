// Package dlq implements a per-pipeline dead-letter queue backed by a
// journal. The journal provides a single durable commit point for DLQ
// writes.
//
// On-disk layout per pipeline:
//
//	<dir>/<pipelineID>.dlq.journal                 current queue
//	<dir>/<pipelineID>.dlq.journal.<ts>.archive    rotated queues
//	<dir>/<pipelineID>.dlq                         v0.1.0 JSONL file, imported on open
//
// Use ReadFile (or `loghub dlq export`) to read any of the journal files;
// each record's payload is a JSON-encoded FailedBatch.
package dlq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"loghub/internal/core"
	"loghub/internal/journal"
	"loghub/internal/metrics"
	"loghub/internal/util"
)

type DeadLetterQueue struct {
	journal    *journal.Journal
	pipelineID string
	mu         sync.Mutex
	count      int
	maxSizeMB  int64
}

type FailedBatch struct {
	Timestamp   time.Time        `json:"timestamp"`
	PipelineID  string           `json:"pipeline_id"`
	Entries     []*core.LogEntry `json:"entries"`
	Error       string           `json:"error"`
	Destination string           `json:"destination"`
}

// JournalPath returns the path of the current DLQ journal for pipelineID.
func JournalPath(dir, pipelineID string) string {
	return filepath.Join(dir, fmt.Sprintf("%s.dlq.journal", pipelineID))
}

// ArchivePaths returns the rotated DLQ journals for pipelineID, oldest
// first.
func ArchivePaths(dir, pipelineID string) ([]string, error) {
	paths, err := filepath.Glob(JournalPath(dir, pipelineID) + ".*.archive")
	if err != nil {
		return nil, err
	}
	sort.Strings(paths) // the timestamp suffix sorts chronologically
	return paths, nil
}

// New creates or opens a journal-backed dead-letter queue rooted at dir.
// maxSizeMB is the size threshold for automatic rotation; 0 disables
// rotation. Records left in a v0.1.0 <pipelineID>.dlq file are imported
// and the old file is renamed with a ".migrated" suffix.
func New(dir, pipelineID string, maxSizeMB int64) (*DeadLetterQueue, error) {
	j, err := journal.Open(JournalPath(dir, pipelineID))
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ journal: %w", err)
	}

	d := &DeadLetterQueue{
		journal:    j,
		pipelineID: pipelineID,
		maxSizeMB:  maxSizeMB,
	}
	if err := d.recoverCount(); err != nil {
		j.Close()
		return nil, fmt.Errorf("failed to recover DLQ count: %w", err)
	}
	if err := d.importV010(filepath.Join(dir, pipelineID+".dlq")); err != nil {
		j.Close()
		return nil, fmt.Errorf("failed to import v0.1.0 DLQ: %w", err)
	}
	return d, nil
}

func (d *DeadLetterQueue) importV010(path string) error {
	imported, skipped, err := util.ImportJSONL(path, func(line []byte) error {
		var fb FailedBatch
		if err := json.Unmarshal(line, &fb); err != nil {
			return fmt.Errorf("%w: %v", util.ErrBadLine, err)
		}
		return d.Import(fb)
	})
	if err != nil {
		return err
	}
	if imported+skipped > 0 {
		slog.Info("Imported v0.1.0 DLQ file", "pipeline_id", d.pipelineID, "file", path, "records", imported, "unreadable_lines", skipped)
	}
	return nil
}

// maybeRotate checks the journal size and rotates if it exceeds maxSizeMB.
func (d *DeadLetterQueue) maybeRotate() {
	if d.maxSizeMB <= 0 {
		return
	}
	size, err := d.journal.Size()
	if err != nil {
		slog.Warn("Failed to check DLQ journal size", "pipeline_id", d.pipelineID, "error", err)
		return
	}
	if size > d.maxSizeMB*1024*1024 {
		if err := d.Rotate(); err != nil {
			slog.Warn("Failed to rotate DLQ journal", "pipeline_id", d.pipelineID, "error", err)
		} else {
			slog.Info("Rotated DLQ journal", "pipeline_id", d.pipelineID, "size_before", size)
		}
	}
}

// Rotate archives the current DLQ journal and starts a new one.
func (d *DeadLetterQueue) Rotate() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.journal.Close(); err != nil {
		return fmt.Errorf("dlq: close before rotate: %w", err)
	}

	archivePath, err := freeArchivePath(d.journal.Path(), time.Now())
	if err != nil {
		return fmt.Errorf("dlq: choose archive name: %w", err)
	}
	if err := os.Rename(d.journal.Path(), archivePath); err != nil {
		renameErr := fmt.Errorf("dlq: rotate rename failed: %w", err)
		// Keep writing to the current journal rather than leaving the
		// queue with a closed one.
		j, openErr := journal.Open(d.journal.Path())
		if openErr != nil {
			return errors.Join(renameErr, fmt.Errorf("dlq: reopen after failed rotate: %w", openErr))
		}
		d.journal = j
		return renameErr
	}

	j, err := journal.Open(d.journal.Path())
	if err != nil {
		return fmt.Errorf("dlq: rotate reopen failed: %w", err)
	}
	d.journal = j
	d.count = 0
	return nil
}

// freeArchivePath returns an archive name for journalPath that does not
// exist yet. os.Rename replaces an existing target, so two rotations that
// produced the same name would silently discard the older archive. Names
// sort chronologically.
func freeArchivePath(journalPath string, now time.Time) (string, error) {
	base := journalPath + "." + now.UTC().Format("20060102T150405.000000000")
	for n := 0; n < 1000; n++ {
		path := base + ".archive"
		if n > 0 {
			path = fmt.Sprintf("%s-%03d.archive", base, n)
		}
		if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
			return path, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("no free archive name for %s", base)
}

// recoverCount counts the committed records in the journal. Each record
// is a full FailedBatch; the count is the number of batches (not the
// number of log entries inside them).
func (d *DeadLetterQueue) recoverCount() error {
	count := 0
	err := d.journal.ReadCommitted(func(_ int64, data []byte) error {
		if len(data) > 0 {
			count++
		}
		return nil
	})
	if err != nil {
		return err
	}
	d.count = count
	// The DLQEntries counter is not seeded on recovery: it is a
	// process-lifetime counter and resets to 0 on restart regardless of
	// what the journal holds.
	return nil
}

// Write persists a failed batch to the journal. The write is committed
// atomically (entry + barrier + fsync).
func (d *DeadLetterQueue) Write(entries []*core.LogEntry, destination string, errMsg string) error {
	return d.Import(FailedBatch{
		Timestamp:   time.Now().UTC(),
		PipelineID:  d.pipelineID,
		Entries:     entries,
		Error:       errMsg,
		Destination: destination,
	})
}

// Import persists an already-built FailedBatch, keeping its timestamp.
// It is used to carry records over from older on-disk formats.
func (d *DeadLetterQueue) Import(fb FailedBatch) error {
	data, err := json.Marshal(fb)
	if err != nil {
		return fmt.Errorf("dlq: marshal failed: %w", err)
	}

	d.mu.Lock()
	if _, err := d.journal.AppendBarrier(data); err != nil {
		d.mu.Unlock()
		return fmt.Errorf("dlq: append failed: %w", err)
	}
	d.count++
	d.mu.Unlock()
	metrics.DLQEntries.WithLabelValues(d.pipelineID).Add(float64(len(fb.Entries)))

	d.maybeRotate()
	return nil
}

// Recover returns the records in the current DLQ journal (not the
// archives).
func (d *DeadLetterQueue) Recover() ([]FailedBatch, error) {
	d.mu.Lock()
	j := d.journal
	d.mu.Unlock()
	var out []FailedBatch
	err := readJournal(j.ReadCommitted, func(fb FailedBatch) error {
		out = append(out, fb)
		return nil
	})
	return out, err
}

// ReadFile calls fn for each record in the DLQ journal (current or
// archived) at path, without opening it for writing. It is safe to use
// while LogHub is running.
func ReadFile(path string, fn func(FailedBatch) error) error {
	return readJournal(func(cb func(int64, []byte) error) error {
		return journal.ReadCommittedFile(path, cb)
	}, fn)
}

func readJournal(read func(func(int64, []byte) error) error, fn func(FailedBatch) error) error {
	return read(func(id int64, data []byte) error {
		if len(data) == 0 {
			return nil
		}
		var fb FailedBatch
		if err := json.Unmarshal(data, &fb); err != nil {
			return fmt.Errorf("dlq: unmarshal record %d: %w", id, err)
		}
		return fn(fb)
	})
}

// Count returns the number of batches in the current journal.
func (d *DeadLetterQueue) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

// Close closes the underlying journal.
func (d *DeadLetterQueue) Close() error {
	if d.journal == nil {
		return nil
	}
	return d.journal.Close()
}
