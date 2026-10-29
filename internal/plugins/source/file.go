package source

import (
	"bufio"
	"context"
	"fmt"
	"io"
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
	fileID     string
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

// A file cursor is "<offset>@<file id>", where the file id is the
// device and inode of the file the offset belongs to (see fileID). A
// cursor with no file id, as written by earlier releases and on Windows,
// applies to whatever file is at the path.
func formatFileCursor(offset int64, id string) string {
	if id == "" {
		return strconv.FormatInt(offset, 10)
	}
	return strconv.FormatInt(offset, 10) + "@" + id
}

func parseFileCursor(cursor string) (int64, string, error) {
	offsetText, id, _ := strings.Cut(cursor, "@")
	offset, err := strconv.ParseInt(offsetText, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid cursor: %w", err)
	}
	if offset < 0 {
		return 0, "", fmt.Errorf("invalid cursor: negative offset %d", offset)
	}
	return offset, id, nil
}

// Collect reads up to batchSize complete lines. A last line without a
// newline is left unread until the writer finishes it, so it is not
// split into two entries; once its file has been rotated away it can no
// longer grow, and it is delivered as is.
//
// Rotation by rename (logrotate's default) is detected by comparing the
// open file with the file now at the path. The rest of the old file is
// read first, then the new file from its start. Truncation in place
// (copytruncate) is detected by the file shrinking below the offset.
func (fs *FileSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if cursor != "" {
		offset, id, err := parseFileCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		switch {
		case fs.file == nil:
			fs.lastOffset = offset
			fs.fileID = id
		case id != "" && id != fs.fileID:
			// The cursor belongs to a file this source has rotated
			// away from. The open file's position is the newer one.
		case offset != fs.lastOffset:
			fs.lastOffset = offset
			if err := fs.seek(); err != nil {
				return nil, "", err
			}
		}
	}

	pathStat, err := os.Stat(fs.filePath)
	if err != nil {
		if fs.file == nil {
			return nil, "", fmt.Errorf("failed to stat file: %w", err)
		}
		// The path is gone: deleted, or renamed and not yet recreated.
		// Finish reading the open file first, then report the missing
		// path until a file appears there again.
		entries, readErr := fs.readLines(ctx, true)
		if len(entries) > 0 || readErr != nil {
			return entries, fs.cursor(), readErr
		}
		fs.closeFile()
		return nil, "", fmt.Errorf("failed to stat file: %w", err)
	}

	if fs.file != nil {
		openStat, statErr := fs.file.Stat()
		switch {
		case statErr == nil && !os.SameFile(openStat, pathStat):
			entries, err := fs.readLines(ctx, true)
			if len(entries) > 0 || err != nil {
				return entries, fs.cursor(), err
			}
			fs.closeFile()
			fs.lastOffset = 0
		case pathStat.Size() < fs.lastOffset:
			fs.closeFile()
			fs.lastOffset = 0
		}
	}

	if fs.file == nil {
		if err := fs.open(); err != nil {
			return nil, "", err
		}
	}

	entries, err := fs.readLines(ctx, false)
	return entries, fs.cursor(), err
}

// open opens the file at the path and positions it at lastOffset. A
// cursor taken from a different file (the path was rotated while LogHub
// was stopped) or past the end of the file (truncated) starts from the
// beginning instead.
func (fs *FileSource) open() error {
	file, err := os.Open(fs.filePath)
	if err != nil {
		return err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	id := fileID(stat)
	if (fs.fileID != "" && id != "" && fs.fileID != id) || stat.Size() < fs.lastOffset {
		fs.lastOffset = 0
	}
	fs.file = file
	fs.fileID = id
	return fs.seek()
}

func (fs *FileSource) seek() error {
	if _, err := fs.file.Seek(fs.lastOffset, io.SeekStart); err != nil {
		fs.closeFile()
		return err
	}
	if fs.reader == nil {
		fs.reader = bufio.NewReader(fs.file)
	} else {
		fs.reader.Reset(fs.file)
	}
	return nil
}

func (fs *FileSource) closeFile() {
	if fs.file != nil {
		fs.file.Close()
	}
	fs.file = nil
	fs.reader = nil
}

func (fs *FileSource) cursor() string {
	return formatFileCursor(fs.lastOffset, fs.fileID)
}

// readLines reads up to batchSize lines from the open file. A trailing
// partial line is returned only when final is set; otherwise the reader
// is moved back to the start of it so the next call reads it whole.
func (fs *FileSource) readLines(ctx context.Context, final bool) ([]*core.LogEntry, error) {
	var entries []*core.LogEntry
	for i := 0; i < fs.batchSize; i++ {
		// Honor ctx.Done() between line reads. A long read
		// loop on a slow file would otherwise block shutdown.
		select {
		case <-ctx.Done():
			return entries, ctx.Err()
		default:
		}
		lineBytes, err := fs.reader.ReadBytes('\n')
		if err != nil && len(lineBytes) > 0 && !final {
			if seekErr := fs.seek(); seekErr != nil {
				return entries, seekErr
			}
			break
		}
		if len(lineBytes) > 0 {
			fs.lastOffset += int64(len(lineBytes))
			line := strings.TrimRight(string(lineBytes), "\r\n")
			entry := fs.parseLine(line)
			entry.Cursor = fs.cursor()
			entries = append(entries, entry)
		}
		if err != nil {
			break
		}
	}
	return entries, nil
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
