package core

import (
	"context"
	"errors"
)

var ErrCursorNotFound = errors.New("cursor not found for pipeline")

type Source interface {
	Collect(ctx context.Context, cursor string) ([]*LogEntry, string, error)
	Close() error
	Name() string
}

type Processor interface {
	Process(ctx context.Context, logs []*LogEntry) ([]*LogEntry, error)
}

type Destination interface {
	// Send delivers the entries to the destination and returns a per-entry
	// status slice. The returned slice MUST have the same length as the
	// input; each EntryStatus describes the outcome of the corresponding
	// input entry (matched by Index).
	//
	// A non-nil function error indicates a batch-level failure (e.g.
	// network unreachable, 5xx from the server): no entries were delivered
	// and the caller should retry or DLQ the entire batch.
	//
	// When the function error is nil, the caller should examine the
	// per-entry statuses: entries with nil Error were accepted; entries
	// with non-nil Error were rejected (typically a 4xx) and should be
	// DLQ'd. The caller may advance the cursor past accepted entries but
	// must preserve rejected entries in the DLQ for manual replay.
	Send(ctx context.Context, logs []*LogEntry) ([]EntryStatus, error)
	Close() error
	Type() string
}

// EntryStatus is the per-entry outcome of a Destination.Send call.
type EntryStatus struct {
	// Index is the 0-based position of the entry in the input slice.
	Index int
	// Error is nil if the entry was accepted by the destination, or a
	// non-nil error describing why the entry was rejected. Rejection
	// reasons typically include 4xx responses, schema mismatches, or
	// payload size limits.
	Error error
}

type Checkpointer interface {
	Get(pipelineID string) (string, error)
	Set(pipelineID string, cursor string) error
	Close() error
}

type SecretResolver interface {
	Resolve(ctx context.Context, value string) (string, error)
}
