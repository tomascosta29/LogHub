package source

import (
	"bufio"
	"bytes"
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
	// prefix holds the first bytes already read from the open file. If
	// they change, the file was truncated and rewritten in place.
	prefix    []byte
	batchSize int
	mu        sync.Mutex
}

// prefixLen is how much of the start of a file is kept to recognize it.
const prefixLen = 256

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
		case pathStat.Size() < fs.lastOffset, fs.rewritten():
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
	fs.rememberPrefix()
	return entries, fs.cursor(), err
}

// rewritten reports whether the open file was truncated and written again
// since the last read, which a size check misses once the new content is
// longer than the old offset. The offset always follows a newline, and the
// file's first bytes were recorded as they were read; either changing
// means the content at the offset is not what was read up to it.
func (fs *FileSource) rewritten() bool {
	if fs.lastOffset == 0 {
		return false
	}
	var b [1]byte
	if _, err := fs.file.ReadAt(b[:], fs.lastOffset-1); err != nil || b[0] != '\n' {
		return true
	}
	if len(fs.prefix) == 0 {
		return false
	}
	cur := make([]byte, len(fs.prefix))
	if _, err := fs.file.ReadAt(cur, 0); err != nil {
		return true
	}
	return !bytes.Equal(cur, fs.prefix)
}

// rememberPrefix records up to prefixLen bytes from the start of the open
// file, limited to what has already been read.
func (fs *FileSource) rememberPrefix() {
	n := min(int64(prefixLen), fs.lastOffset)
	if fs.file == nil || int64(len(fs.prefix)) >= n {
		return
	}
	buf := make([]byte, n)
	if _, err := fs.file.ReadAt(buf, 0); err == nil {
		fs.prefix = buf
	}
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
	fs.prefix = nil
	if fs.rewritten() {
		fs.lastOffset = 0
	}
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
	fs.prefix = nil
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
