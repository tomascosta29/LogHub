package destination

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"loghub/internal/core"
	"loghub/internal/util"
)

type localJSONLConfig struct {
	Path   string `mapstructure:"path"`
	Format string `mapstructure:"format"`
}

func init() {
	Register("local_jsonl", func(props map[string]interface{}) (core.Destination, error) {
		var cfg localJSONLConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		return NewLocalJSONL(cfg.Path, cfg.Format)
	})

}

type LocalJSONL struct {
	path   string
	format string
	mu     sync.Mutex
	file   *os.File
	enc    *json.Encoder
	closed bool
}

func NewLocalJSONL(path, format string) (*LocalJSONL, error) {
	if path == "" {
		return nil, fmt.Errorf("local_jsonl destination: path is required")
	}
	if format == "" {
		format = "log_entry"
	}
	switch format {
	case "log_entry", "sentinel":
	default:
		return nil, fmt.Errorf("local_jsonl destination: unsupported format %q", format)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("local_jsonl destination: failed to create directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("local_jsonl destination: failed to open file: %w", err)
	}
	return &LocalJSONL{
		path:   path,
		format: format,
		file:   file,
		enc:    json.NewEncoder(file),
	}, nil
}

func (l *LocalJSONL) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	if len(logs) == 0 {
		return nil, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, fmt.Errorf("local_jsonl destination: send after close")
	}

	switch l.format {
	case "sentinel":
		records, err := BuildSentinelRecords(logs)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if err := l.enc.Encode(record); err != nil {
				return nil, fmt.Errorf("local_jsonl destination: failed to write sentinel record: %w", err)
			}
		}
	default:
		for _, entry := range logs {
			if err := l.enc.Encode(entry); err != nil {
				return nil, fmt.Errorf("local_jsonl destination: failed to write log entry: %w", err)
			}
		}
	}

	if err := l.file.Sync(); err != nil {
		return nil, fmt.Errorf("local_jsonl destination: failed to sync: %w", err)
	}
	return allSuccess(len(logs)), nil
}

func (l *LocalJSONL) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	l.enc = nil
	return err
}

func (l *LocalJSONL) Type() string {
	return "local_jsonl"
}
