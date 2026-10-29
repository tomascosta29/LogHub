package source

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"loghub/internal/core"
)

func TestFileSourceCollect(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-source-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.WriteString("[INFO] First log message\n")
	tmpfile.WriteString("[ERROR] Second log message\n")
	tmpfile.WriteString("[DEBUG] Third log message\n")
	tmpfile.Close()

	src, err := NewFileSource(tmpfile.Name(), "test-source", 10)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}

	entries, cursor, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	if entries[0].Level != "INFO" {
		t.Errorf("Expected level INFO, got %s", entries[0].Level)
	}

	if entries[1].Level != "ERROR" {
		t.Errorf("Expected level ERROR, got %s", entries[1].Level)
	}
	if entries[2].Cursor == "" {
		t.Error("Expected entry cursor to be populated")
	}

	if cursor == "" {
		t.Error("Expected non-empty cursor")
	}

	src.Close()
}

func TestFileSourceParseLine(t *testing.T) {
	src, err := NewFileSource("/fake/path", "test", 10)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}

	entry := src.parseLine("[WARN] Something went wrong")

	if entry.Level != "WARN" {
		t.Errorf("Expected level WARN, got %s", entry.Level)
	}

	if entry.Message != "Something went wrong" {
		t.Errorf("Unexpected message: %s", entry.Message)
	}

	if entry.Source != "test" {
		t.Errorf("Expected source 'test', got %s", entry.Source)
	}
}

func TestFileSourceParseLineNoLevel(t *testing.T) {
	src, err := NewFileSource("/fake/path", "test", 10)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}

	entry := src.parseLine("Just a plain message")

	if entry.Level != "INFO" {
		t.Errorf("Expected level INFO, got %s", entry.Level)
	}

	if entry.Message != "Just a plain message" {
		t.Errorf("Unexpected message: %s", entry.Message)
	}
}

func TestFileSourceCollectWithCursor(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-source-cursor-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.WriteString("Line 1\n")
	tmpfile.WriteString("Line 2\n")
	tmpfile.WriteString("Line 3\n")
	tmpfile.Close()

	src, err := NewFileSource(tmpfile.Name(), "test", 10)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}

	entries1, cursor1, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("First collect failed: %v", err)
	}
	if len(entries1) != 3 {
		t.Errorf("Expected 3 entries first collect, got %d", len(entries1))
	}
	if cursor1 == "" {
		t.Error("Expected non-empty cursor")
	}
	src.Close()

	src2, err := NewFileSource(tmpfile.Name(), "test", 10)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}
	entries2, _, err := src2.Collect(context.Background(), cursor1)
	if err != nil {
		t.Fatalf("Second collect failed: %v", err)
	}
	if len(entries2) != 0 {
		t.Errorf("Expected 0 entries with cursor (file exhausted), got %d", len(entries2))
	}
}

func TestFileSourceSeeksWhenCursorChangesWhileOpen(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-source-rewind-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.WriteString("Line 1\n")
	tmpfile.WriteString("Line 2\n")
	tmpfile.Close()

	src, err := NewFileSource(tmpfile.Name(), "test", 1)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}
	defer src.Close()

	entries1, _, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("first collect failed: %v", err)
	}
	if len(entries1) != 1 || entries1[0].Message != "Line 1" {
		t.Fatalf("expected first line, got %+v", entries1)
	}

	entries2, _, err := src.Collect(context.Background(), "0")
	if err != nil {
		t.Fatalf("rewind collect failed: %v", err)
	}
	if len(entries2) != 1 || entries2[0].Message != "Line 1" {
		t.Fatalf("expected rewind to reread first line, got %+v", entries2)
	}
}

func TestFileSourceConstructorValidation(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		sourceName string
		batchSize  int
		wantErr    string
	}{
		{
			name:       "empty path",
			path:       "",
			sourceName: "test",
			batchSize:  10,
			wantErr:    "file source: path is required",
		},
		{
			name:       "zero batch size",
			path:       "/some/path",
			sourceName: "test",
			batchSize:  0,
			wantErr:    "fetch_size must be positive, got 0",
		},
		{
			name:       "negative batch size",
			path:       "/some/path",
			sourceName: "test",
			batchSize:  -1,
			wantErr:    "fetch_size must be positive, got -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFileSource(tt.path, tt.sourceName, tt.batchSize)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestFileSourceCollectHonorsCtxDone(t *testing.T) {
	// Regression test: ctx cancellation must interrupt a long
	// read loop. Use a large batch_size and a pre-cancelled context.
	tmpfile, err := os.CreateTemp("", "test-source-ctx-*.log")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	for i := 0; i < 1000; i++ {
		tmpfile.WriteString("line\n")
	}
	tmpfile.Close()

	fs, err := NewFileSource(tmpfile.Name(), "test", 1000000)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer fs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	_, _, err = fs.Collect(ctx, "")
	if err == nil {
		t.Fatal("expected ctx.Err() from Collect, got nil")
	}
}

func TestFileSourceTruncationResetsAndReopens(t *testing.T) {
	// Regression test: when the file is truncated below
	// lastOffset, fs.file and fs.reader must be set to nil so the
	// reopen branch runs.
	tmpfile, err := os.CreateTemp("", "test-source-trunc-*.log")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.WriteString("first line\nsecond line\nthird line\n")
	tmpfile.Close()

	fs, err := NewFileSource(tmpfile.Name(), "test", 10)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer fs.Close()

	// First Collect — reads all three lines.
	entries, _, err := fs.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect 1: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Collect 1 returned %d entries, want 3", len(entries))
	}

	// Truncate the file to 0 bytes.
	if err := os.WriteFile(tmpfile.Name(), []byte(""), 0644); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Second Collect — must detect truncation, close+nil the cached
	// file, reset lastOffset, and re-open.
	_, _, err = fs.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect 2: %v", err)
	}

	// Third Collect after writing new content.
	if err := os.WriteFile(tmpfile.Name(), []byte("new line\n"), 0644); err != nil {
		t.Fatalf("write new content: %v", err)
	}
	entries, _, err = fs.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect 3: %v", err)
	}
	if len(entries) != 1 || entries[0].Message != "new line" {
		t.Errorf("Collect 3 entries = %+v, want one entry with message 'new line'", entries)
	}
}

func TestFileSourceHoldsPartialLastLine(t *testing.T) {
	// A last line without a newline is still being written. It must not
	// be emitted (and the cursor must not pass it) until the rest arrives,
	// or it becomes two entries.
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path, []byte("line one\npartial"), 0o600); err != nil {
		t.Fatal(err)
	}

	fs, err := NewFileSource(path, "test", 100)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer fs.Close()

	entries, cursor, err := fs.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect 1: %v", err)
	}
	if len(entries) != 1 || entries[0].Message != "line one" {
		t.Fatalf("Collect 1 = %v, want only \"line one\"", messages(entries))
	}

	appendFile(t, path, " rest\nnext\n")
	entries, _, err = fs.Collect(context.Background(), cursor)
	if err != nil {
		t.Fatalf("Collect 2: %v", err)
	}
	if got := messages(entries); len(got) != 2 || got[0] != "partial rest" || got[1] != "next" {
		t.Fatalf("Collect 2 = %q, want [\"partial rest\" \"next\"]", got)
	}

	// A restarted source resumes from the cursor, before the partial line.
	fs2, err := NewFileSource(path, "test", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()
	entries, _, err = fs2.Collect(context.Background(), cursor)
	if err != nil {
		t.Fatalf("Collect after restart: %v", err)
	}
	if got := messages(entries); len(got) != 2 || got[0] != "partial rest" {
		t.Fatalf("Collect after restart = %q, want to start at \"partial rest\"", got)
	}
}

func TestFileSourceFollowsRenameRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("old 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileSource(path, "test", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	entries, cursor, err := fs.Collect(context.Background(), "")
	if err != nil || len(entries) != 1 {
		t.Fatalf("Collect 1 = %v, %v; want one entry", messages(entries), err)
	}

	// Lines written to the old file before rotation, including an
	// unfinished last line, must still be read.
	appendFile(t, path, "old 2\nold tail")
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	// The new file is longer than the old offset, so a size check alone
	// would not notice the rotation.
	if err := os.WriteFile(path, []byte("new 1 with a longer line\nnew 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var got []string
	for i := 0; i < 3; i++ {
		entries, cursor, err = fs.Collect(context.Background(), cursor)
		if err != nil {
			t.Fatalf("Collect after rotation: %v", err)
		}
		got = append(got, messages(entries)...)
	}
	want := []string{"old 2", "old tail", "new 1 with a longer line", "new 2"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("after rotation got %q, want %q", got, want)
	}

	// The committed cursor can still name the old file when the new file
	// had nothing to read yet; it must not move the new file's position.
	appendFile(t, path, "new 3\n")
	entries, _, err = fs.Collect(context.Background(), entriesCursorOld(t, path+".1"))
	if err != nil {
		t.Fatal(err)
	}
	if got := messages(entries); len(got) != 1 || got[0] != "new 3" {
		t.Fatalf("Collect with a stale old-file cursor = %q, want [\"new 3\"]", got)
	}
}

func TestFileSourceDeletedFileReadsRestThenErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileSource(path, "test", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	_, cursor, err := fs.Collect(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}

	appendFile(t, path, "two\n")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	entries, cursor, err := fs.Collect(context.Background(), cursor)
	if err != nil || len(entries) != 1 || entries[0].Message != "two" {
		t.Fatalf("Collect after delete = %q, %v; want the unread line \"two\"", messages(entries), err)
	}
	if _, _, err := fs.Collect(context.Background(), cursor); err == nil {
		t.Fatal("Collect with the file gone and fully read returned no error")
	}

	if err := os.WriteFile(path, []byte("three\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, _, err = fs.Collect(context.Background(), cursor)
	if err != nil || len(entries) != 1 || entries[0].Message != "three" {
		t.Fatalf("Collect after the file returns = %q, %v; want [\"three\"]", messages(entries), err)
	}
}

func TestFileSourceRestartAfterRotationReadsNewFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("cursors carry no file identity on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("old 1\nold 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileSource(path, "test", 100)
	if err != nil {
		t.Fatal(err)
	}
	_, cursor, err := fs.Collect(context.Background(), "")
	fs.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Rotated while stopped; the new file is longer than the old offset.
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("new 1 is a fairly long line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs2, err := NewFileSource(path, "test", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()
	entries, _, err := fs2.Collect(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	if got := messages(entries); len(got) != 1 || got[0] != "new 1 is a fairly long line" {
		t.Fatalf("Collect after restart = %q, want the new file from its start", got)
	}
}

func TestFileSourceReadsLegacyOffsetCursor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path, []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileSource(path, "test", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	entries, _, err := fs.Collect(context.Background(), "4")
	if err != nil {
		t.Fatal(err)
	}
	if got := messages(entries); len(got) != 1 || got[0] != "two" {
		t.Fatalf("Collect from legacy cursor \"4\" = %q, want [\"two\"]", got)
	}
}

// entriesCursorOld returns a cursor at the end of the rotated file,
// as the pipeline would hold after delivering its last line.
func entriesCursorOld(t *testing.T, oldPath string) string {
	t.Helper()
	fi, err := os.Stat(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	return formatFileCursor(fi.Size(), fileID(fi))
}

func messages(entries []*core.LogEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Message
	}
	return out
}

func appendFile(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(text); err != nil {
		t.Fatal(err)
	}
}
