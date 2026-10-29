package source

import (
	"context"
	"os"
	"testing"
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

func TestFileSourceNoDataLossOnPartialRead(t *testing.T) {
	// Regression test: a partial-line read (no trailing newline)
	// at EOF must still be returned to the caller. The next Collect
	// must NOT advance the cursor past the partial line and lose it.
	tmpfile, err := os.CreateTemp("", "test-source-partial-*.log")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.WriteString("line one\nline two") // no trailing newline
	tmpfile.Close()

	fs, err := NewFileSource(tmpfile.Name(), "test", 100)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer fs.Close()

	entries, _, err := fs.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2 (line one and line two)", len(entries))
	}
	if entries[0].Message != "line one" {
		t.Errorf("entries[0] = %q, want 'line one'", entries[0].Message)
	}
	if entries[1].Message != "line two" {
		t.Errorf("entries[1] = %q, want 'line two'", entries[1].Message)
	}
}
