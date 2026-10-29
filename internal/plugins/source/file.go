package source

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"loghub/internal/core"
	"loghub/internal/util"
)

type fileConfig struct {
	Path      string `mapstructure:"path"`
	Name      string `mapstructure:"name"`
	FetchSize int    `mapstructure:"fetch_size"`
	BatchSize int    `mapstructure:"batch_size"`
}

func init() {
	Register("file", func(ctx context.Context, props map[string]interface{}, _ SecretResolver) (core.Source, error) {
		var cfg fileConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		return NewFileSource(cfg.Path, cfg.Name, resolveFetchSize(cfg.FetchSize, cfg.BatchSize))
	})

}

var logFormat = regexp.MustCompile(`(?i)^\[([A-Z]+)\]\s+(.*)$`)

type FileSource struct {
	filePath   string
	sourceName string
	file       *os.File
	reader     *bufio.Reader
	lastOffset int64
	batchSize  int
	mu         sync.Mutex
}

func NewFileSource(filePath, sourceName string, batchSize int) (*FileSource, error) {
	if filePath == "" {
		return nil, fmt.Errorf("file source: path is required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("fetch_size must be positive, got %d", batchSize)
	}
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute path: %w", err)
	}
	return &FileSource{
		filePath:   absPath,
		sourceName: sourceName,
		batchSize:  batchSize,
	}, nil
}

func (fs *FileSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if cursor != "" {
		offset, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid cursor: %w", err)
		}
		if offset != fs.lastOffset {
			fs.lastOffset = offset
			if fs.file != nil {
				if _, err := fs.file.Seek(fs.lastOffset, os.SEEK_SET); err != nil {
					return nil, "", err
				}
				fs.reader = bufio.NewReader(fs.file)
			}
		}
	}

	if stat, err := os.Stat(fs.filePath); err != nil {
		return nil, "", fmt.Errorf("failed to stat file: %w", err)
	} else if stat.Size() < fs.lastOffset {
		if fs.file != nil {
			fs.file.Close()
			fs.file = nil
			fs.reader = nil
		}
		fs.lastOffset = 0
	}

	if fs.file == nil {
		file, err := os.Open(fs.filePath)
		if err != nil {
			return nil, "", err
		}
		fs.file = file

		if _, err := fs.file.Seek(fs.lastOffset, os.SEEK_SET); err != nil {
			fs.file.Close()
			fs.file = nil
			fs.reader = nil
			return nil, "", err
		}

		fs.reader = bufio.NewReader(fs.file)
	}

	var entries []*core.LogEntry
	for i := 0; i < fs.batchSize; i++ {
		// Honor ctx.Done() between line reads. A long read
		// loop on a slow file would otherwise block shutdown.
		select {
		case <-ctx.Done():
			return entries, fmt.Sprintf("%d", fs.lastOffset), ctx.Err()
		default:
		}
		lineBytes, err := fs.reader.ReadBytes('\n')
		if len(lineBytes) > 0 {
			fs.lastOffset += int64(len(lineBytes))
			line := strings.TrimRight(string(lineBytes), "\r\n")
			entry := fs.parseLine(line)
			entry.Cursor = fmt.Sprintf("%d", fs.lastOffset)
			entries = append(entries, entry)
		}
		if err != nil {
			break
		}
	}

	nextCursor := fmt.Sprintf("%d", fs.lastOffset)

	return entries, nextCursor, nil
}

func (fs *FileSource) parseLine(line string) *core.LogEntry {
	fields := make(map[string]string)
	level := "INFO"
	message := line

	matches := logFormat.FindStringSubmatch(line)
	if len(matches) == 3 {
		level = strings.ToUpper(matches[1])
		message = matches[2]
	}

	fields["raw"] = line

	return &core.LogEntry{
		Timestamp: time.Now(),
		Source:    fs.sourceName,
		Level:     level,
		Message:   message,
		Fields:    fields,
	}
}
func (fs *FileSource) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.file != nil {
		return fs.file.Close()
	}
	return nil
}

func (fs *FileSource) Name() string {
	return fs.sourceName
}
