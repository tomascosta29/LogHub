package dlq

import (
	"os"
	"path/filepath"
	"testing"

	"loghub/internal/core"
)

func makeEntries(msgs ...string) []*core.LogEntry {
	out := make([]*core.LogEntry, len(msgs))
	for i, m := range msgs {
		out[i] = &core.LogEntry{Message: m}
	}
	return out
}

func TestDLQWriteAndRecover(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, "p1", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Close()

	if err := d.Write(makeEntries("a"), "sentinel", "boom-1"); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if err := d.Write(makeEntries("b", "c"), "http", "boom-2"); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if got := d.Count(); got != 2 {
		t.Errorf("Count = %d, want 2", got)
	}

	got, err := d.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Recover returned %d batches, want 2", len(got))
	}
	if got[0].Destination != "sentinel" || got[0].Entries[0].Message != "a" {
		t.Errorf("batch 0: destination=%q msg=%q", got[0].Destination, got[0].Entries[0].Message)
	}
	if got[1].Destination != "http" || got[1].Entries[0].Message != "b" || got[1].Entries[1].Message != "c" {
		t.Errorf("batch 1: destination=%q msgs=%q,%q",
			got[1].Destination, got[1].Entries[0].Message, got[1].Entries[1].Message)
	}
}

func TestDLQPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, "p1", 0)
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	if err := d.Write(makeEntries("a", "b"), "sentinel", "boom"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2, err := New(dir, "p1", 0)
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer d2.Close()
	if got := d2.Count(); got != 1 {
		t.Errorf("after reopen, Count = %d, want 1", got)
	}
	got, err := d2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 || got[0].Entries[0].Message != "a" {
		t.Errorf("after reopen, Recover = %+v, want [a]", got)
	}
}

func TestDLQJournalFileName(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, "mypipe", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Close()
	if err := d.Write(makeEntries("x"), "sentinel", "boom"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	expected := filepath.Join(dir, "mypipe.dlq.journal")
	if _, err := os.Stat(expected); err != nil {
		t.Errorf("expected journal at %q, stat err: %v", expected, err)
	}
}

func TestDLQRotateRenameFailureKeepsQueueWritable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	q, err := New(dir, "p1", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer q.Close()

	// A read-only directory makes the archive rename fail while the
	// existing journal file stays writable.
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0755) })

	if err := q.Rotate(); err == nil {
		t.Fatal("expected Rotate to fail")
	}
	if err := q.Write([]*core.LogEntry{{Message: "after"}}, "sentinel#0", "boom"); err != nil {
		t.Fatalf("Write after failed Rotate: %v", err)
	}
}

func TestDLQImportsV010File(t *testing.T) {
	dir := t.TempDir()
	old := `{"timestamp":"2026-05-07T10:30:00Z","pipeline_id":"p1","entries":[{"message":"m"}],"error":"bad request","destination":"sentinel"}` + "\n{\"timest"
	if err := os.WriteFile(filepath.Join(dir, "p1.dlq"), []byte(old), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	q, err := New(dir, "p1", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	recovered, err := q.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recovered) != 1 || recovered[0].Error != "bad request" || recovered[0].Timestamp.Year() != 2026 {
		t.Fatalf("Recover = %+v, want the imported v0.1.0 record with its timestamp", recovered)
	}
	q.Close()
	if _, err := os.Stat(filepath.Join(dir, "p1.dlq.migrated")); err != nil {
		t.Errorf("p1.dlq.migrated missing: %v", err)
	}

	q2, err := New(dir, "p1", 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	if got := q2.Count(); got != 1 {
		t.Errorf("Count after reopen = %d, want 1 (no second import)", got)
	}
}

func TestDLQReadFileIncludesArchives(t *testing.T) {
	dir := t.TempDir()
	q, err := New(dir, "p1", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer q.Close()
	if err := q.Write(makeEntries("first"), "sentinel#0", "boom"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := q.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if err := q.Write(makeEntries("second"), "sentinel#0", "boom"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	archives, err := ArchivePaths(dir, "p1")
	if err != nil || len(archives) != 1 {
		t.Fatalf("ArchivePaths = %v, %v; want one archive", archives, err)
	}
	var got []string
	for _, path := range append(archives, JournalPath(dir, "p1")) {
		if err := ReadFile(path, func(fb FailedBatch) error {
			got = append(got, fb.Entries[0].Message)
			return nil
		}); err != nil {
			t.Fatalf("ReadFile(%s): %v", path, err)
		}
	}
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("records = %v, want [first second]", got)
	}
}
