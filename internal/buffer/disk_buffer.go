package buffer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"sort"
	"sync"

	"loghub/internal/core"
	"loghub/internal/journal"
	"loghub/internal/metrics"
	"loghub/internal/util"
)

// legacyDrainMarker is the drain-boundary payload written by unreleased
// builds: every batch before it had been delivered. It is still honoured
// when reading old journals but is no longer written.
var legacyDrainMarker = []byte("__LOGHUB_DISK_BUFFER_DRAIN__")

// ErrDrainStop is a sentinel error returned by a Drain callback to signal
// that iteration should stop without it being a real error. The Drain
// function returns ErrDrainStop to the caller; callers can check via
// errors.Is to distinguish "partial failure, please retry" from a real
// error.
var ErrDrainStop = errors.New("disk buffer: drain stopped by callback")

// DiskBuffer persists batches that did not fit in the in-memory pipeline
// channel, or that still need delivery after a partial failure.
//
// It is backed by one journal per pipeline. Each batch is a committed
// journal entry; once delivered, a marker naming the batch's entry ID is
// committed after it. The pending set is therefore always recovered from
// the same file that Drain reads, and a drain that stops partway only
// re-delivers the batches it did not finish.
//
// On-disk layout per pipeline:
//
//	<dir>/<pipelineID>.buffer.journal   batches and delivered markers
//	<dir>/<pipelineID>.buffer[.drain]   v0.1.0 files, imported on open
type DiskBuffer struct {
	journal    *journal.Journal
	pipelineID string
	maxSizeMB  int64

	mu           sync.Mutex
	pending      map[int64]pendingBatch // entry ID → undelivered batch
	pendingBytes int64
	draining     bool
}

// pendingBatch describes an undelivered batch. seq is its position in
// delivery order: a batch's own entry ID, or for a replacement written
// by Replace, the seq of the batch it replaced, so a partly delivered
// batch keeps its place ahead of batches written after it.
type pendingBatch struct {
	size int64
	seq  int64
}

type DiskBatch struct {
	Entries      []*core.LogEntry `json:"entries"`
	EntryCursors []string         `json:"entry_cursors,omitempty"`
	NextCursor   string           `json:"next_cursor"`
	// Destinations lists the keys of the destinations that still need
	// this batch. Empty means every destination; batches written before
	// this field existed decode that way.
	Destinations []string `json:"destinations,omitempty"`
}

// deliveredMarker is the journal payload recording that the batch at
// entry ID Delivered no longer needs delivery.
type deliveredMarker struct {
	Delivered int64 `json:"delivered"`
}

// replacementRecord is the journal payload of a batch written by
// Replace. ReplacesSeq is the delivery position it inherits.
type replacementRecord struct {
	ReplacesSeq int64     `json:"replaces_seq"`
	Batch       DiskBatch `json:"batch"`
}

var replacementPrefix = []byte(`{"replaces_seq":`)

// NewDiskBuffer creates or opens a journal-backed disk buffer rooted at
// dir. Batches left in v0.1.0 buffer files are imported into the journal
// and the old files are renamed with a ".migrated" suffix.
func NewDiskBuffer(dir, pipelineID string, maxSizeMB int64) (*DiskBuffer, error) {
	path := filepath.Join(dir, fmt.Sprintf("%s.buffer.journal", pipelineID))
	j, err := journal.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open disk buffer journal: %w", err)
	}

	db := &DiskBuffer{
		journal:    j,
		pipelineID: pipelineID,
		maxSizeMB:  maxSizeMB,
		pending:    make(map[int64]pendingBatch),
	}
	if err := db.recoverPending(); err != nil {
		j.Close()
		return nil, fmt.Errorf("failed to recover disk buffer: %w", err)
	}
	if err := db.importV010(dir); err != nil {
		j.Close()
		return nil, fmt.Errorf("failed to import v0.1.0 disk buffer: %w", err)
	}
	db.updateGauge()
	return db, nil
}

// recoverPending rebuilds the pending set from the journal.
func (db *DiskBuffer) recoverPending() error {
	return db.journal.ReadCommitted(func(id int64, data []byte) error {
		if bytes.Equal(data, legacyDrainMarker) {
			clear(db.pending)
			db.pendingBytes = 0
			return nil
		}
		if delivered, ok := parseDeliveredMarker(data); ok {
			db.removePendingLocked(delivered)
			return nil
		}
		seq := id
		if bytes.HasPrefix(data, replacementPrefix) {
			var r replacementRecord
			if err := json.Unmarshal(data, &r); err != nil {
				return fmt.Errorf("disk buffer: unmarshal replacement %d: %w", id, err)
			}
			seq = r.ReplacesSeq
		}
		db.addPendingLocked(id, int64(len(data)), seq)
		return nil
	})
}

func (db *DiskBuffer) addPendingLocked(id, size, seq int64) {
	db.pending[id] = pendingBatch{size: size, seq: seq}
	db.pendingBytes += size
}

func (db *DiskBuffer) removePendingLocked(id int64) (pendingBatch, bool) {
	pb, ok := db.pending[id]
	if ok {
		db.pendingBytes -= pb.size
		delete(db.pending, id)
	}
	return pb, ok
}

// importV010 appends the batches from v0.1.0's JSONL buffer files to
// the journal. The in-progress drain file holds the older batches, so
// it is imported first.
func (db *DiskBuffer) importV010(dir string) error {
	for _, name := range []string{db.pipelineID + ".buffer.drain", db.pipelineID + ".buffer"} {
		path := filepath.Join(dir, name)
		imported, skipped, err := util.ImportJSONL(path, func(line []byte) error {
			var b DiskBatch
			if err := json.Unmarshal(line, &b); err != nil {
				return fmt.Errorf("%w: %v", util.ErrBadLine, err)
			}
			return db.write(b, false)
		})
		if err != nil {
			return err
		}
		if imported+skipped > 0 {
			slog.Info("Imported v0.1.0 disk buffer file", "pipeline_id", db.pipelineID, "file", path, "batches", imported, "unreadable_lines", skipped)
		}
	}
	return nil
}

func parseDeliveredMarker(data []byte) (int64, bool) {
	if !bytes.HasPrefix(data, []byte(`{"delivered":`)) {
		return 0, false
	}
	var m deliveredMarker
	if err := json.Unmarshal(data, &m); err != nil || m.Delivered <= 0 {
		return 0, false
	}
	return m.Delivered, true
}

// Write persists a batch to the journal. The batch is committed atomically
// (entry + barrier + fsync). Returns an error if the configured size limit
// would be exceeded.
func (db *DiskBuffer) Write(batch DiskBatch) error {
	return db.write(batch, true)
}

func (db *DiskBuffer) write(batch DiskBatch, enforceLimit bool) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("disk buffer: marshal failed: %w", err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if enforceLimit && db.maxSizeMB > 0 && db.pendingBytes+int64(len(data)) > db.maxSizeMB*1024*1024 {
		return fmt.Errorf("disk buffer size limit exceeded")
	}
	id, err := db.journal.AppendBarrier(data)
	if err != nil {
		return fmt.Errorf("disk buffer: append failed: %w", err)
	}
	db.addPendingLocked(id, int64(len(data)), id)
	db.updateGaugeLocked()
	return nil
}

// Drain streams the pending batches to the callback in delivery order,
// one batch at a time. Batches written after Drain starts are not
// included. Memory stays bounded to a single batch, plus the batches
// written by Replace that are still pending: those are delivered in
// place of the batch they replaced, ahead of later batches, so they are
// read before the main pass.
//
// The callback reports what happened to each batch by calling
// MarkDelivered or Replace with the batch's id; a batch it does neither
// for stays pending. It may return nil to continue, or ErrDrainStop to
// stop without it being a real error. Any other non-nil error stops
// iteration and is returned by Drain.
func (db *DiskBuffer) Drain(callback func(id int64, b DiskBatch) error) error {
	db.mu.Lock()
	if db.draining {
		db.mu.Unlock()
		return fmt.Errorf("disk buffer: drain already in progress")
	}
	db.draining = true
	snapshot := make(map[int64]pendingBatch, len(db.pending))
	replacements := 0
	for id, pb := range db.pending {
		snapshot[id] = pb
		if pb.seq != id {
			replacements++
		}
	}
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		db.draining = false
		db.mu.Unlock()
	}()

	if len(snapshot) == 0 {
		return nil
	}
	var queued []queuedBatch
	if replacements > 0 {
		var err error
		if queued, err = db.readReplacements(snapshot); err != nil {
			return err
		}
	}
	// flushBefore delivers the queued replacements whose seq comes
	// before seq.
	flushBefore := func(seq int64) error {
		for len(queued) > 0 && queued[0].seq < seq {
			q := queued[0]
			queued = queued[1:]
			if err := callback(q.id, q.batch); err != nil {
				return err
			}
		}
		return nil
	}

	err := db.journal.ReadCommitted(func(id int64, data []byte) error {
		pb, ok := snapshot[id]
		if !ok || pb.seq != id {
			return nil
		}
		if err := flushBefore(id); err != nil {
			return err
		}
		var b DiskBatch
		if err := json.Unmarshal(data, &b); err != nil {
			return fmt.Errorf("disk buffer: unmarshal batch %d: %w", id, err)
		}
		return callback(id, b)
	})
	if err == nil {
		err = flushBefore(math.MaxInt64)
	}
	if err != nil {
		if errors.Is(err, ErrDrainStop) {
			return ErrDrainStop
		}
		return fmt.Errorf("disk buffer: drain failed: %w", err)
	}
	return nil
}

type queuedBatch struct {
	id, seq int64
	batch   DiskBatch
}

// readReplacements loads the pending replacement batches in snapshot,
// ordered by delivery position.
func (db *DiskBuffer) readReplacements(snapshot map[int64]pendingBatch) ([]queuedBatch, error) {
	var out []queuedBatch
	err := db.journal.ReadCommitted(func(id int64, data []byte) error {
		pb, ok := snapshot[id]
		if !ok || pb.seq == id {
			return nil
		}
		var r replacementRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return fmt.Errorf("disk buffer: unmarshal replacement %d: %w", id, err)
		}
		out = append(out, queuedBatch{id: id, seq: pb.seq, batch: r.Batch})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("disk buffer: read replacements: %w", err)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out, nil
}

// MarkDelivered records that the batch with the given id no longer needs
// delivery.
func (db *DiskBuffer) MarkDelivered(id int64) error {
	return db.Replace(id, nil)
}

// Replace atomically marks the batch with the given id delivered and
// writes remaining in its place, in one journal transaction. It is used
// when only part of a batch was delivered: a crash leaves either the
// original batch or its replacement pending, never both or neither.
// The replacement keeps the original batch's place in delivery order.
func (db *DiskBuffer) Replace(id int64, remaining []DiskBatch) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	orig, ok := db.pending[id]
	if !ok {
		return fmt.Errorf("disk buffer: batch %d is not pending", id)
	}

	payloads := make([][]byte, 0, len(remaining)+1)
	for _, b := range remaining {
		data, err := json.Marshal(replacementRecord{ReplacesSeq: orig.seq, Batch: b})
		if err != nil {
			return fmt.Errorf("disk buffer: marshal failed: %w", err)
		}
		payloads = append(payloads, data)
	}
	marker, err := json.Marshal(deliveredMarker{Delivered: id})
	if err != nil {
		return fmt.Errorf("disk buffer: marshal marker failed: %w", err)
	}
	payloads = append(payloads, marker)

	ids, err := db.journal.CommitTransaction(payloads...)
	if err != nil {
		return fmt.Errorf("disk buffer: commit failed: %w", err)
	}
	db.removePendingLocked(id)
	for i, newID := range ids[:len(remaining)] {
		db.addPendingLocked(newID, int64(len(payloads[i])), orig.seq)
	}
	db.updateGaugeLocked()
	return nil
}

// Compact empties the journal file when no batch is pending, so delivered
// batches do not accumulate on disk. It does nothing while a drain is in
// progress or while batches are pending.
func (db *DiskBuffer) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.draining || len(db.pending) > 0 {
		return nil
	}
	size, err := db.journal.Size()
	if err != nil {
		return fmt.Errorf("disk buffer: size: %w", err)
	}
	if size == 0 {
		return nil
	}
	if err := db.journal.RewriteWith(nil); err != nil {
		return fmt.Errorf("disk buffer: compact: %w", err)
	}
	return nil
}

// Count returns the current number of un-drained batches.
func (db *DiskBuffer) Count() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.pending)
}

// Close closes the underlying journal.
func (db *DiskBuffer) Close() error {
	if db.journal == nil {
		return nil
	}
	return db.journal.Close()
}

func (db *DiskBuffer) updateGauge() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.updateGaugeLocked()
}

func (db *DiskBuffer) updateGaugeLocked() {
	metrics.DiskBufferBatches.WithLabelValues(db.pipelineID).Set(float64(len(db.pending)))
}
