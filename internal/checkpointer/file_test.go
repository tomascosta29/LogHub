package checkpointer

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"loghub/internal/journal"
)

func TestFileCheckpointerSetAndGet(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("failed to create checkpointer: %v", err)
	}

	cursor := "test-cursor-value"
	if err := cp.Set("pipeline1", cursor); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := cp.Get("pipeline1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != cursor {
		t.Errorf("got %q, want %q", got, cursor)
	}
}

func TestFileCheckpointerGetNonExistent(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("failed to create checkpointer: %v", err)
	}

	_, err = cp.Get("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent cursor")
	}
}

func TestFileCheckpointerOverwrite(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("failed to create checkpointer: %v", err)
	}

	if err := cp.Set("pipeline1", "cursor1"); err != nil {
		t.Fatalf("first Set failed: %v", err)
	}

	if err := cp.Set("pipeline1", "cursor2"); err != nil {
		t.Fatalf("second Set failed: %v", err)
	}

	got, err := cp.Get("pipeline1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != "cursor2" {
		t.Errorf("got %q, want %q", got, "cursor2")
	}
}

func TestFileCheckpointerTempFileCleanup(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("failed to create checkpointer: %v", err)
	}

	if err := cp.Set("pipeline1", "cursor1"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".tmp") {
			t.Errorf("temp file %q should not exist after Set", name)
		}
	}
}

func TestFileCheckpointerSyncDirSucceeds(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("failed to create checkpointer: %v", err)
	}

	if err := cp.Set("pipeline1", "cursor1"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	journalPath := filepath.Join(dir, "pipeline1.journal")
	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		t.Error("cursor journal should exist after Set")
	}
}

func TestFileCheckpointerMultiplePipelines(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("failed to create checkpointer: %v", err)
	}

	pipelines := map[string]string{
		"pipeline1": "cursor1",
		"pipeline2": "cursor2",
		"pipeline3": "cursor3",
	}

	for id, cursor := range pipelines {
		if err := cp.Set(id, cursor); err != nil {
			t.Fatalf("Set for %s failed: %v", id, err)
		}
	}

	for id, expected := range pipelines {
		got, err := cp.Get(id)
		if err != nil {
			t.Fatalf("Get for %s failed: %v", id, err)
		}
		if got != expected {
			t.Errorf("pipeline %s: got %q, want %q", id, got, expected)
		}
	}
}

func TestNewFileCheckpointerCreatesDir(t *testing.T) {
	dir := t.TempDir()
	newDir := filepath.Join(dir, "subdir", "checkpoints")

	cp, err := NewFileCheckpointer(newDir)
	if err != nil {
		t.Fatalf("failed to create checkpointer: %v", err)
	}

	if err := cp.Set("pipeline1", "cursor1"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := cp.Get("pipeline1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != "cursor1" {
		t.Errorf("got %q, want %q", got, "cursor1")
	}
}

func TestNewFileCheckpointerInvalidDir(t *testing.T) {
	if os.PathSeparator == '/' {
		_, err := NewFileCheckpointer("/proc/invalid")
		if err == nil {
			t.Error("expected error for invalid directory path")
		}
	}
}

func TestFileCheckpointerMigratesLegacyCursor(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "pipeline1.cursor")
	if err := os.WriteFile(legacy, []byte("legacy-cursor-value\n"), 0644); err != nil {
		t.Fatalf("WriteFile legacy: %v", err)
	}

	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer: %v", err)
	}
	got, err := cp.Get("pipeline1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "legacy-cursor-value" {
		t.Errorf("after migration, got %q, want %q", got, "legacy-cursor-value")
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Errorf("legacy file should be removed after migration, stat err: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pipeline1.journal")); err != nil {
		t.Errorf("journal should exist after migration: %v", err)
	}
}

func TestFileCheckpointerPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cp1, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer 1: %v", err)
	}
	if err := cp1.Set("pipeline1", "checkpoint-A"); err != nil {
		t.Fatalf("Set A: %v", err)
	}
	if err := cp1.Close(); err != nil {
		t.Fatalf("Close cp1: %v", err)
	}

	cp2, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer 2: %v", err)
	}
	defer cp2.Close()
	got, err := cp2.Get("pipeline1")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got != "checkpoint-A" {
		t.Errorf("after reopen, got %q, want %q", got, "checkpoint-A")
	}
}

func TestFileCheckpointerGetDoesNotCreateFile(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer: %v", err)
	}
	defer cp.Close()

	if _, err := cp.Get("nonexistent-pipeline"); err == nil {
		t.Error("expected error for Get on never-set pipeline")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		// .lock is expected; it's the advisory file lock from NewFileCheckpointer.
		if e.Name() == ".lock" {
			continue
		}
		t.Errorf("Get created an unexpected file: %s", e.Name())
	}
}

func TestFileCheckpointerLockPreventsSecondInstance(t *testing.T) {
	dir := t.TempDir()
	cp1, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("first NewFileCheckpointer: %v", err)
	}
	defer cp1.Close()

	cp2, err := NewFileCheckpointer(dir)
	if err == nil {
		cp2.Close()
		t.Fatal("expected second NewFileCheckpointer to fail while first is held")
	}
	if cp2 != nil {
		t.Errorf("expected nil checkpointer on failure, got %+v", cp2)
	}
}

func TestFileCheckpointerConcurrentSets(t *testing.T) {
	dir := t.TempDir()
	cp, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer: %v", err)
	}
	defer cp.Close()

	const N = 50
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cursor := "cursor-" + strconv.Itoa(i)
			if err := cp.Set("pipeline1", cursor); err != nil {
				t.Errorf("Set %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	got, err := cp.Get("pipeline1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !strings.HasPrefix(got, "cursor-") {
		t.Errorf("got %q, want a cursor- prefix", got)
	}
}

func TestFileCheckpointerCompactsJournal(t *testing.T) {
	dir := t.TempDir()
	fc, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer: %v", err)
	}
	for i := 0; i < compactEvery+5; i++ {
		if err := fc.Set("p", fmt.Sprintf("cursor-%d", i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	lines := 0
	if err := journal.ReadCommittedFile(filepath.Join(dir, "p.journal"), func(int64, []byte) error {
		lines++
		return nil
	}); err != nil {
		t.Fatalf("ReadCommittedFile: %v", err)
	}
	if lines > 10 {
		t.Errorf("journal holds %d cursors after %d saves, want it compacted", lines, compactEvery+5)
	}
	fc.Close()

	reopened, err := NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	want := fmt.Sprintf("cursor-%d", compactEvery+4)
	if got, err := reopened.Get("p"); err != nil || got != want {
		t.Errorf("Get after compaction = %q, %v; want %q", got, err, want)
	}
}
