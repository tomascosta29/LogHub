// Package journal provides a single-writer, append-only, durable log with
// explicit barrier markers. It is the foundational building block for
// transactional commit of pipeline state.
//
// A transaction is the sequence of Append calls since the previous Barrier.
// A Barrier fsyncs the journal and marks the preceding entries as committed.
// Recover returns only the entries inside the most recent fully-closed
// transaction; entries after the last Barrier are not yet visible.
//
// On-disk format is NDJSON: one JSON object per line. Regular entries are
// {"id":N,"data":"<base64>"}; barriers are {"barrier":true}.
package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Entry is a regular journal record. ID is assigned by the journal and is
// monotonically increasing across the lifetime of the journal file.
type Entry struct {
	ID   int64  `json:"id"`
	Data []byte `json:"data,omitempty"`
}

type barrierMarker struct {
	Barrier bool `json:"barrier"`
}

// ErrCorrupt is returned when a journal has unreadable records inside its
// committed part. Such a file was damaged after it was written, so its
// state cannot be trusted; treating it as empty would silently restart a
// cursor from the beginning or discard a backlog.
var ErrCorrupt = errors.New("journal is corrupt")

// lineKind classifies one journal line.
type lineKind int

const (
	lineEmpty lineKind = iota
	lineEntry
	lineBarrier
	lineInvalid
)

func classify(line []byte) (lineKind, Entry) {
	if len(line) == 0 {
		return lineEmpty, Entry{}
	}
	var e Entry
	if err := json.Unmarshal(line, &e); err == nil && e.ID > 0 {
		return lineEntry, e
	}
	var b barrierMarker
	if err := json.Unmarshal(line, &b); err == nil && b.Barrier {
		return lineBarrier, Entry{}
	}
	return lineInvalid, Entry{}
}

// isZeroFill reports whether line holds only NUL bytes, which is what a
// power loss can leave where an unsynced write had extended the file.
func isZeroFill(line []byte) bool {
	for _, c := range line {
		if c != 0 {
			return false
		}
	}
	return true
}

// inspect validates the journal read from r. It returns the highest entry
// ID and the byte offset just past the last barrier; everything after that
// offset is an uncommitted tail. An unreadable line before the last
// barrier is corruption. In the tail only what a crash can leave is
// accepted: a final line cut off before its newline, or zero fill.
func inspect(r io.Reader) (maxID, committedEnd int64, err error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var offset int64
	var lineNo int
	firstBadLine := 0
	for {
		raw, readErr := br.ReadBytes('\n')
		if len(raw) > 0 {
			lineNo++
			offset += int64(len(raw))
			terminated := raw[len(raw)-1] == '\n'
			line := raw
			if terminated {
				line = raw[:len(raw)-1]
			}
			switch kind, e := classify(line); kind {
			case lineEntry:
				if firstBadLine > 0 {
					return 0, 0, fmt.Errorf("unreadable record at line %d: %w", firstBadLine, ErrCorrupt)
				}
				maxID = max(maxID, e.ID)
			case lineBarrier:
				if firstBadLine > 0 {
					return 0, 0, fmt.Errorf("unreadable record at line %d: %w", firstBadLine, ErrCorrupt)
				}
				committedEnd = offset
			case lineInvalid:
				if terminated && !isZeroFill(line) {
					return 0, 0, fmt.Errorf("unreadable record at line %d: %w", lineNo, ErrCorrupt)
				}
				if firstBadLine == 0 {
					firstBadLine = lineNo
				}
			}
		}
		if readErr == io.EOF {
			return maxID, committedEnd, nil
		}
		if readErr != nil {
			return 0, 0, readErr
		}
	}
}

// Journal is a single-writer append-only log with explicit barriers. It is
// safe for concurrent callers of Append and Barrier, but Recover holds the
// write lock and should not be called concurrently with writes from the same
// process.
type Journal struct {
	path string
	dir  string
	mu   sync.Mutex
	f    *os.File
	enc  *json.Encoder
	next int64
}

// Open creates or opens a journal at the given path. If the file already
// contains committed entries from a prior run, the next ID is set past the
// last seen ID.
func Open(path string) (*Journal, error) {
	if path == "" {
		return nil, fmt.Errorf("journal: path is required")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("journal: failed to create directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY|openNoFollow, 0o600)
	if err != nil {
		return nil, fmt.Errorf("journal: failed to open: %w", err)
	}
	j := &Journal{
		path: path,
		dir:  dir,
		f:    f,
		enc:  json.NewEncoder(f),
	}
	if err := j.recoverNextID(); err != nil {
		f.Close()
		return nil, fmt.Errorf("journal: failed to scan existing file: %w", err)
	}
	return j, nil
}

// recoverNextID scans the file and finds the maximum ID seen across regular
// entries. Barrier markers and trailing partial lines are ignored.
func (j *Journal) recoverNextID() error {
	f, err := os.Open(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	maxID, committedEnd, err := inspect(f)
	if err != nil {
		return fmt.Errorf("%s: %w", j.path, err)
	}
	j.next = maxID
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > committedEnd {
		// Drop the uncommitted tail a crash left behind, so the next
		// append starts on a fresh line and is not merged with a torn
		// record or committed together with stale ones.
		if err := j.f.Truncate(committedEnd); err != nil {
			return fmt.Errorf("journal: trim uncommitted tail: %w", err)
		}
		if err := j.f.Sync(); err != nil {
			return fmt.Errorf("journal: sync after trim: %w", err)
		}
	}
	return nil
}

// Append encodes and writes a single entry. The entry is not durable until
// the next Barrier. Returns the assigned entry ID. If the encode fails,
// the ID is not consumed and the next Append reuses it.
func (j *Journal) Append(data []byte) (int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.next++
	e := Entry{ID: j.next, Data: data}
	if err := j.enc.Encode(&e); err != nil {
		j.next--
		return 0, fmt.Errorf("journal: append failed: %w", err)
	}
	return j.next, nil
}

// Barrier writes a barrier marker and fsyncs the journal file and the
// containing directory. All entries appended since the previous Barrier
// (or since Open) are now durable and will be visible to a future Recover.
func (j *Journal) Barrier() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.enc.Encode(&barrierMarker{Barrier: true}); err != nil {
		return fmt.Errorf("journal: barrier encode failed: %w", err)
	}
	if err := j.f.Sync(); err != nil {
		return fmt.Errorf("journal: barrier sync failed: %w", err)
	}
	if err := syncDir(j.dir); err != nil {
		return fmt.Errorf("journal: barrier dir sync failed: %w", err)
	}
	return nil
}

// AppendBarrier is a convenience for Append+Barrier in a single fsync.
// Writes the entry, the barrier marker, and fsyncs file+directory. The
// on-disk format is identical to a separate Append and Barrier call; this
// just folds the two writes and the fsync into one operation.
//
// Returns the assigned entry ID. On failure the write is undone (see
// undoWrite) and the ID is released.
func (j *Journal) AppendBarrier(data []byte) (int64, error) {
	ids, err := j.CommitTransaction(data)
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// CommitTransaction appends N entries to the journal and barrier-fsyncs
// once at the end. All entries are durably committed in a single
// fsync; on crash, either all are visible (last transaction contains
// all N) or none are (the in-progress write is discarded by the
// partial-line tolerance).
//
// Callers that need to atomically commit multiple state changes
// (e.g. "drain disk buffer marker" + "advance cursor") write them in
// one CommitTransaction call.
//
// If any write or the fsync fails, the transaction is undone (see
// undoWrite) and its IDs are released, so a caller that retries does not
// leave a second copy behind.
//
// It returns the IDs assigned to the payloads, in order.
func (j *Journal) CommitTransaction(payloads ...[]byte) ([]int64, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	start, err := j.f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("journal: transaction start: %w", err)
	}
	firstID := j.next
	fail := func(err error) ([]int64, error) {
		return nil, j.undoWrite(start, firstID, err)
	}
	ids := make([]int64, 0, len(payloads))
	for _, data := range payloads {
		j.next++
		e := Entry{ID: j.next, Data: data}
		if err := j.enc.Encode(&e); err != nil {
			return fail(fmt.Errorf("journal: transaction append failed: %w", err))
		}
		ids = append(ids, j.next)
	}
	if err := j.enc.Encode(&barrierMarker{Barrier: true}); err != nil {
		return fail(fmt.Errorf("journal: transaction barrier encode failed: %w", err))
	}
	if err := j.f.Sync(); err != nil {
		return fail(fmt.Errorf("journal: transaction sync failed: %w", err))
	}
	if err := syncDir(j.dir); err != nil {
		return fail(fmt.Errorf("journal: transaction dir sync failed: %w", err))
	}
	return ids, nil
}

// undoWrite cuts the file back to size and restores the ID counter after a
// failed transaction. Without it, a commit whose fsync failed stays in the
// page cache with its barrier and becomes durable on a later fsync, next
// to the copy the caller writes when it retries. The caller must hold
// j.mu.
func (j *Journal) undoWrite(size, nextID int64, cause error) error {
	j.next = nextID
	if err := j.f.Truncate(size); err != nil {
		return errors.Join(cause, fmt.Errorf("journal: undo failed write: %w", err))
	}
	return cause
}

// ReadCommitted invokes callback once per committed entry, in order,
// with the entry's ID and data. An entry is committed when a barrier
// follows it; entries after the last barrier (an interrupted write)
// are not returned.
//
// It sees the journal as it was when the call started: entries appended
// later are not returned. The journal lock is not held while callbacks
// run, so a callback may append to the same journal.
func (j *Journal) ReadCommitted(callback func(id int64, data []byte) error) error {
	j.mu.Lock()
	f, err := os.Open(j.path)
	if err != nil {
		j.mu.Unlock()
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	fi, err := f.Stat()
	j.mu.Unlock()
	if err != nil {
		f.Close()
		return err
	}
	defer f.Close()
	return scanCommitted(io.LimitReader(f, fi.Size()), callback)
}

// ReadCommittedFile reads the committed entries of the journal file at
// path without opening it for writing, so it is safe to use on a
// journal another process is appending to, or on an archived journal.
// A missing file has no entries.
func ReadCommittedFile(path string, callback func(id int64, data []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	return scanCommitted(f, callback)
}

func scanCommitted(r io.Reader, callback func(id int64, data []byte) error) error {
	var pending []Entry
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	lineNo, badLine := 0, 0
	for scanner.Scan() {
		lineNo++
		kind, e := classify(scanner.Bytes())
		if kind == lineInvalid {
			if badLine == 0 {
				badLine = lineNo
			}
			continue
		}
		if kind != lineEmpty && badLine > 0 {
			return fmt.Errorf("unreadable record at line %d: %w", badLine, ErrCorrupt)
		}
		switch kind {
		case lineEntry:
			pending = append(pending, e)
		case lineBarrier:
			for _, pe := range pending {
				if err := callback(pe.ID, pe.Data); err != nil {
					return err
				}
			}
			pending = pending[:0]
		}
	}
	return scanner.Err()
}

// RewriteWith replaces the journal file contents atomically with the
// given raw entries. Each entry is written with a fresh ID (starting
// from 1) followed by its own barrier, so every entry is an independent
// committed transaction. After the rewrite, the in-memory next-counter
// is set to len(entries) and the file is reopened in append mode.
//
// The caller is responsible for deciding which entries to retain; the
// journal does not interpret the entry payloads.
//
// If the rewrite fails, the original file is left in place (or, if the
// rename already happened, the new file is), the next-counter matches
// whichever file is on disk, and the journal is reopened in append
// mode. If that reopen also fails, both errors are returned and every
// later write fails until the process reopens the journal.
func (j *Journal) RewriteWith(entries [][]byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rewriteLocked(entries, true)
}

// rewriteLocked writes entries to a temp file, renames it over j.path
// and reopens the journal for appending. When barrierEach is true every
// entry is followed by a barrier; otherwise all entries form a single
// transaction closed by one trailing barrier. The caller must hold j.mu.
func (j *Journal) rewriteLocked(entries [][]byte, barrierEach bool) error {
	if j.f != nil {
		err := j.f.Close()
		j.f = nil
		if err != nil {
			return j.reopenAfterFailure(fmt.Errorf("journal: close before rewrite: %w", err))
		}
	}

	if err := os.MkdirAll(j.dir, 0o700); err != nil {
		return j.reopenAfterFailure(fmt.Errorf("journal: mkdir before rewrite: %w", err))
	}
	tmpPath := j.path + ".rewrite.tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|openNoFollow, 0o600)
	if err != nil {
		return j.reopenAfterFailure(fmt.Errorf("journal: open rewrite temp: %w", err))
	}
	abort := func(err error) error {
		f.Close()
		os.Remove(tmpPath)
		return j.reopenAfterFailure(err)
	}

	enc := json.NewEncoder(f)
	var next int64
	for _, data := range entries {
		next++
		if err := enc.Encode(&Entry{ID: next, Data: data}); err != nil {
			return abort(fmt.Errorf("journal: rewrite encode entry: %w", err))
		}
		if barrierEach {
			if err := enc.Encode(&barrierMarker{Barrier: true}); err != nil {
				return abort(fmt.Errorf("journal: rewrite encode barrier: %w", err))
			}
		}
	}
	if !barrierEach && len(entries) > 0 {
		if err := enc.Encode(&barrierMarker{Barrier: true}); err != nil {
			return abort(fmt.Errorf("journal: rewrite encode barrier: %w", err))
		}
	}

	if err := f.Sync(); err != nil {
		return abort(fmt.Errorf("journal: rewrite sync: %w", err))
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return j.reopenAfterFailure(fmt.Errorf("journal: rewrite close: %w", err))
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		os.Remove(tmpPath)
		return j.reopenAfterFailure(fmt.Errorf("journal: rewrite rename: %w", err))
	}
	// The new file is now the journal; IDs continue from its contents.
	j.next = next

	if err := syncDir(j.dir); err != nil {
		return j.reopenAfterFailure(fmt.Errorf("journal: rewrite dir sync: %w", err))
	}
	if err := j.reopenLocked(); err != nil {
		return fmt.Errorf("journal: reopen after rewrite: %w", err)
	}
	return nil
}

// reopenAfterFailure reopens the journal file after a failed rewrite and
// returns cause, joined with the reopen error if the reopen also failed.
func (j *Journal) reopenAfterFailure(cause error) error {
	if err := j.reopenLocked(); err != nil {
		return errors.Join(cause, fmt.Errorf("journal: reopen after failed rewrite: %w", err))
	}
	return cause
}

// reopenLocked re-opens the journal file in append mode and restores
// j.f / j.enc. The caller must hold j.mu.
func (j *Journal) reopenLocked() error {
	if j.f != nil {
		j.f.Close()
		j.f = nil
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY|openNoFollow, 0o600)
	if err != nil {
		return err
	}
	j.f = f
	j.enc = json.NewEncoder(f)
	return nil
}

// Recover returns the entries of the most recent committed transaction.
// A transaction is the set of entries after the previous barrier (or the
// start of the file) up to and including the next barrier. The most recent
// committed transaction is the one whose closing barrier is the latest
// barrier in the file. Entries after the most recent barrier (i.e., the
// in-progress transaction) are NOT returned — they have not been
// barrier-fsync'd yet.
//
// A file with zero barriers returns nil. A file whose only barrier has no
// entries before it (a barrier at file start) returns nil. A file with a
// partial trailing line from a crash returns the last committed transaction
// as if the partial line were absent.
func (j *Journal) Recover() ([]Entry, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.recoverLocked()
}

// recoverLocked implements Recover. The caller must hold j.mu.
func (j *Journal) recoverLocked() ([]Entry, error) {
	f, err := os.Open(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	allEntries := make([]Entry, 0, 16)
	barrierEntryIdx := []int{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	lineNo, badLine := 0, 0
	for scanner.Scan() {
		lineNo++
		kind, e := classify(scanner.Bytes())
		if kind == lineInvalid {
			if badLine == 0 {
				badLine = lineNo
			}
			continue
		}
		if kind != lineEmpty && badLine > 0 {
			return nil, fmt.Errorf("journal: %s: unreadable record at line %d: %w", j.path, badLine, ErrCorrupt)
		}
		switch kind {
		case lineEntry:
			allEntries = append(allEntries, e)
		case lineBarrier:
			barrierEntryIdx = append(barrierEntryIdx, len(allEntries))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("journal: recover scan failed: %w", err)
	}

	if len(barrierEntryIdx) == 0 {
		return nil, nil
	}
	prevBarrierEntry := 0
	if len(barrierEntryIdx) >= 2 {
		prevBarrierEntry = barrierEntryIdx[len(barrierEntryIdx)-2]
	}
	to := barrierEntryIdx[len(barrierEntryIdx)-1]
	if to <= prevBarrierEntry {
		return nil, nil
	}
	out := make([]Entry, to-prevBarrierEntry)
	copy(out, allEntries[prevBarrierEntry:to])
	return out, nil
}

// Compact rewrites the journal to contain only the most recent committed
// transaction. This reduces file size and speeds up future Recover calls.
// The retained entries are rewritten as a single transaction, so a
// multi-entry CommitTransaction survives compaction intact. The journal
// lock is held throughout, so no commit can land between reading the
// last transaction and rewriting the file. If the rewrite fails, the
// original journal is preserved.
func (j *Journal) Compact() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	entries, err := j.recoverLocked()
	if err != nil {
		return fmt.Errorf("journal: compact recover failed: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	payloads := make([][]byte, len(entries))
	for i, e := range entries {
		payloads[i] = e.Data
	}
	return j.rewriteLocked(payloads, false)
}

// Size returns the current on-disk size of the journal in bytes.
func (j *Journal) Size() (int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	fi, err := j.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Close closes the journal file. The caller must ensure no Append or Barrier
// is in flight.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f != nil {
		err := j.f.Close()
		j.f = nil
		return err
	}
	return nil
}

// Path returns the file path of the journal.
func (j *Journal) Path() string {
	return j.path
}
