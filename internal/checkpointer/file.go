package checkpointer

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"loghub/internal/core"
	"loghub/internal/journal"
	"loghub/internal/lock"
)

// ownerPrefix marks the record, written in the same transaction as each
// cursor, that names the pipeline the journal belongs to. The cursor stays
// the last record of the transaction, so older builds that read only the
// last record still find it.
const ownerPrefix = "loghub.pipeline="

// compactEvery is how many cursor saves a pipeline's journal accumulates
// before it is compacted down to the latest cursor.
const compactEvery = 1000

// FileCheckpointer stores each pipeline's cursor in a per-pipeline journal.
// The journal provides a single durable commit point for cursor writes.
//
// The checkpointer acquires an advisory file lock (<dir>/.lock) at
// construction. The lock prevents two LogHub processes
// from racing on the same cursor state. The lock is auto-released on
// process crash (via flock's fd-close semantics on Unix).
//
// On-disk layout per pipeline:
//
//	<dir>/.lock                   advisory file lock (single LogHub process)
//	<dir>/<pipelineID>.journal    append-only NDJSON, barriers delimit commits
//	<dir>/<pipelineID>.cursor     legacy file, migrated on first use
type FileCheckpointer struct {
	dir  string
	lock *lock.FileLock

	// lifecycle excludes Close while any pipeline operation is active.
	lifecycle sync.RWMutex
	closed    bool
	mu        sync.Mutex
	state     map[string]*pipelineState
}

type pipelineState struct {
	mu           sync.Mutex // serializes only this pipeline
	journal      *journal.Journal
	journalPath  string
	cached       string // last committed cursor; "" means "no cursor"
	sinceCompact int    // cursor saves since the journal was last compacted
}

// NewFileCheckpointer creates a checkpointer rooted at dirPath. The directory
// is created if it does not exist. An advisory file lock is acquired on
// <dirPath>/.lock; if another process holds the lock, this function returns
// an error and the caller should fail-fast.
func NewFileCheckpointer(dirPath string) (*FileCheckpointer, error) {
	if err := os.MkdirAll(dirPath, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create checkpoints directory: %w", err)
	}
	l, err := lock.Acquire(filepath.Join(dirPath, ".lock"))
	if err != nil {
		return nil, fmt.Errorf("failed to acquire checkpointer lock: %w", err)
	}
	return &FileCheckpointer{
		dir:   dirPath,
		lock:  l,
		state: make(map[string]*pipelineState),
	}, nil
}

// Get returns the last committed cursor for pipelineID. Returns
// core.ErrCursorNotFound if no cursor has ever been set and no legacy file
// exists. Get never creates files on disk; it only opens an existing journal.
func (fc *FileCheckpointer) Get(pipelineID string) (string, error) {
	fc.lifecycle.RLock()
	defer fc.lifecycle.RUnlock()
	if fc.closed {
		return "", os.ErrClosed
	}
	s := fc.pipelineState(pipelineID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s, err := fc.openReadOnly(pipelineID, s)
	if err != nil {
		return "", err
	}
	if s == nil {
		return "", core.ErrCursorNotFound
	}
	if s.cached == "" {
		return "", core.ErrCursorNotFound
	}
	return s.cached, nil
}

// Set writes the cursor through the pipeline's journal and barrier-fsyncs.
// The in-memory cache is updated only after a successful barrier; if the
// barrier fails the previous cursor remains the source of truth.
//
// Every compactEvery saves, the journal is compacted to the latest
// cursor. A failed compaction is not an error for Set: the cursor is
// already durable, and the next save retries.
func (fc *FileCheckpointer) Set(pipelineID, cursor string) error {
	fc.lifecycle.RLock()
	defer fc.lifecycle.RUnlock()
	if fc.closed {
		return os.ErrClosed
	}
	s := fc.pipelineState(pipelineID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s, err := fc.openWritable(pipelineID, s)
	if err != nil {
		return err
	}
	if _, err := s.journal.CommitTransaction([]byte(ownerPrefix+pipelineID), []byte(cursor)); err != nil {
		return fmt.Errorf("checkpointer: commit failed for %s: %w", pipelineID, err)
	}
	s.cached = cursor
	s.sinceCompact++
	if s.sinceCompact >= compactEvery {
		if err := s.journal.Compact(); err != nil {
			slog.Warn("Failed to compact checkpoint journal", "pipeline_id", pipelineID, "error", err)
		} else {
			s.sinceCompact = 0
		}
	}
	return nil
}

// Close closes all open journals and releases the file lock. Errors from
// individual closes are joined.
func (fc *FileCheckpointer) Close() error {
	fc.lifecycle.Lock()
	defer fc.lifecycle.Unlock()
	if fc.closed {
		return nil
	}
	fc.closed = true
	var firstErr error
	for _, s := range fc.state {
		if s.journal == nil {
			continue
		}
		if err := s.journal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	fc.state = make(map[string]*pipelineState)
	if fc.lock != nil {
		if err := fc.lock.Release(); err != nil && firstErr == nil {
			firstErr = err
		}
		fc.lock = nil
	}
	return firstErr
}

// pipelineState looks up a pipeline without holding the map lock during I/O.
// The caller holds lifecycle.RLock until it finishes using the state.
func (fc *FileCheckpointer) pipelineState(id string) *pipelineState {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	s := fc.state[id]
	if s == nil {
		s = &pipelineState{}
		fc.state[id] = s
	}
	return s
}

// openReadOnly returns the pipeline's state if a journal or a legacy cursor
// file exists. Returns nil if neither exists, in which case Get returns
// core.ErrCursorNotFound. If only a legacy file exists, openPipelineLocked
// will migrate it (creating the journal) — this is the only path that
// creates a journal during Get.
func (fc *FileCheckpointer) openReadOnly(pipelineID string, s *pipelineState) (*pipelineState, error) {
	if s.journal != nil {
		return s, nil
	}
	journalPath := filepath.Join(fc.dir, pipelineID+".journal")
	legacyPath := filepath.Join(fc.dir, pipelineID+".cursor")
	_, jerr := os.Stat(journalPath)
	_, lerr := os.Stat(legacyPath)
	journalExists := jerr == nil
	legacyExists := lerr == nil
	if !journalExists && !legacyExists {
		if jerr != nil && !os.IsNotExist(jerr) {
			return nil, jerr
		}
		if lerr != nil && !os.IsNotExist(lerr) {
			return nil, lerr
		}
		return nil, nil
	}
	return fc.openPipelineLocked(pipelineID, s)
}

// openWritable opens (or creates) the pipeline's journal. Triggers legacy
// migration on first use.
func (fc *FileCheckpointer) openWritable(pipelineID string, s *pipelineState) (*pipelineState, error) {
	if s.journal != nil {
		return s, nil
	}
	return fc.openPipelineLocked(pipelineID, s)
}

// openPipelineLocked opens the journal, recovers the last committed cursor,
// and on first use migrates any legacy .cursor file into the journal.
func (fc *FileCheckpointer) openPipelineLocked(pipelineID string, s *pipelineState) (*pipelineState, error) {
	journalPath := filepath.Join(fc.dir, pipelineID+".journal")
	j, err := journal.Open(journalPath)
	if err != nil {
		return nil, fmt.Errorf("checkpointer: open journal for %s: %w", pipelineID, err)
	}
	s.journal, s.journalPath = j, journalPath

	entries, err := j.Recover()
	if err != nil {
		j.Close()
		s.journal = nil
		return nil, fmt.Errorf("checkpointer: recover journal for %s: %w", pipelineID, err)
	}
	if len(entries) > 0 {
		if owner, ok := strings.CutPrefix(string(entries[0].Data), ownerPrefix); ok && len(entries) > 1 && owner != pipelineID {
			j.Close()
			s.journal = nil
			return nil, fmt.Errorf("checkpointer: %s holds the cursor of pipeline %q, not %q; it was copied or renamed. Move it aside to start %q without a cursor", journalPath, owner, pipelineID, pipelineID)
		}
		s.cached = string(entries[len(entries)-1].Data)
	} else {
		// Empty journal: try migration from legacy .cursor file.
		legacyPath := filepath.Join(fc.dir, pipelineID+".cursor")
		data, err := os.ReadFile(legacyPath)
		if err == nil {
			cursor := strings.TrimRight(string(data), "\r\n\t ")
			if cursor != "" {
				if _, err := j.Append([]byte(cursor)); err == nil {
					if err := j.Barrier(); err == nil {
						s.cached = cursor
						// Keep the old file, renamed, as the disk buffer
						// and DLQ migrations do: it is the state to restore
						// when rolling back to v0.1.0.
						if err := os.Rename(legacyPath, legacyPath+".migrated"); err != nil {
							slog.Warn("Imported legacy cursor file but could not rename it", "pipeline_id", pipelineID, "file", legacyPath, "error", err)
						}
					}
				}
			}
		} else if !os.IsNotExist(err) {
			slog.Warn("Failed to read legacy cursor file; starting without it", "pipeline_id", pipelineID, "file", legacyPath, "error", err)
		}
	}
	return s, nil
}
