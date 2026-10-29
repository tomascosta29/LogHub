package processor

import (
	"context"
	"fmt"

	"loghub/internal/core"
)

// ErrorMode defines how processors handle per-entry errors.
type ErrorMode string

const (
	ErrorModeKeep   ErrorMode = "keep"
	ErrorModeDrop   ErrorMode = "drop"
	ErrorModePoison ErrorMode = "poison"
)

// processEntryFn transforms a single LogEntry. It may return an error.
type processEntryFn func(*core.LogEntry) (*core.LogEntry, error)

// processBatch applies fn to each entry in logs, handling errors according to mode.
// It returns the transformed slice and any fatal (poison) error.
// procName is the name of the processor, used for poison pill error messages.
func processBatch(_ context.Context, logs []*core.LogEntry, mode ErrorMode, procName string, fn processEntryFn) ([]*core.LogEntry, error) {
	result := make([]*core.LogEntry, 0, len(logs))
	for _, entry := range logs {
		processed, err := fn(entry)
		if err != nil {
			switch mode {
			case ErrorModeDrop:
				continue
			case ErrorModePoison:
				return nil, newPoisonError(procName, err)
			default: // keep
				result = append(result, entry)
			}
			continue
		}
		result = append(result, processed)
	}
	return result, nil
}

// newPoisonError wraps an error with a processor-specific prefix for poison mode.
func newPoisonError(proc string, err error) error {
	return fmt.Errorf("%s poison pill: %w", proc, err)
}
