package processor

import (
	"context"
	"fmt"
	"time"

	"loghub/internal/core"
	"loghub/internal/util"
)

func init() {
	Register("timestamp", func(props map[string]interface{}) (core.Processor, error) {
		var cfg TimestampConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		return NewTimestamp(cfg)
	})

}

// TimestampConfig configures the timestamp normalization processor.
type TimestampConfig struct {
	// SourceField is the field containing the timestamp string. If empty, uses entry.Timestamp.
	SourceField string `json:"source_field"`

	// SourceLayout is the current layout of the timestamp. Uses Go reference time layout.
	// Common layouts: "2006-01-02T15:04:05Z07:00" (RFC3339), "2006-01-02 15:04:05"
	SourceLayout string `json:"source_layout"`

	// TargetLayout is the desired output layout. If empty, uses RFC3339.
	TargetLayout string `json:"target_layout"`

	// TargetField is where to store the formatted timestamp. If empty, overwrites SourceField.
	// Use "timestamp" to set entry.Timestamp.
	TargetField string `json:"target_field"`

	// OnError: "drop", "keep", or "poison" (default: keep).
	OnError string `json:"on_error"`
}

// Timestamp normalizes timestamp formats in log entries.
type Timestamp struct {
	config TimestampConfig
}

func NewTimestamp(cfg TimestampConfig) (*Timestamp, error) {
	if cfg.SourceLayout == "" {
		return nil, fmt.Errorf("source_layout is required for timestamp processor")
	}
	if cfg.TargetLayout == "" {
		cfg.TargetLayout = time.RFC3339
	}
	if cfg.OnError == "" {
		cfg.OnError = string(ErrorModeKeep)
	}
	return &Timestamp{config: cfg}, nil
}

func (t *Timestamp) Process(ctx context.Context, logs []*core.LogEntry) ([]*core.LogEntry, error) {
	return processBatch(ctx, logs, ErrorMode(t.config.OnError), "timestamp", t.processEntry)
}

func (t *Timestamp) processEntry(entry *core.LogEntry) (*core.LogEntry, error) {
	var tsStr string
	if t.config.SourceField != "" {
		var ok bool
		tsStr, ok = entry.Fields[t.config.SourceField]
		if !ok {
			return entry, nil // field not found, nothing to do
		}
	} else {
		if entry.Timestamp.IsZero() {
			return entry, nil
		}
		tsStr = entry.Timestamp.Format(t.config.SourceLayout)
	}

	parsed, err := time.Parse(t.config.SourceLayout, tsStr)
	if err != nil {
		return nil, err
	}

	newEntry := core.CopyLogEntry(entry)
	formatted := parsed.Format(t.config.TargetLayout)

	switch t.config.TargetField {
	case "timestamp":
		newEntry.Timestamp = parsed
	default:
		if t.config.TargetField != "" {
			newEntry.Fields[t.config.TargetField] = formatted
		} else if t.config.SourceField != "" {
			newEntry.Fields[t.config.SourceField] = formatted
		} else {
			newEntry.Timestamp = parsed
		}
	}

	return newEntry, nil
}
