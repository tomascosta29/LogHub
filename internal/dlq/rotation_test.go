package dlq

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"loghub/internal/core"
)

// TestRotationKeepsEveryArchive checks that rapid rotations do not overwrite
// an archive. Archive names use a one-second timestamp, so two rotations in
// the same second would otherwise rename onto the same path and discard the
// first archive.
func TestRotationKeepsEveryArchive(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, "pipeline", 1)
	if err != nil {
		t.Fatalf("open DLQ: %v", err)
	}
	defer d.Close()

	payload := strings.Repeat("x", 1<<20+1024)
	for i := 0; i < 3; i++ {
		if err := d.Write([]*core.LogEntry{{Message: payload, Timestamp: time.Now()}}, "dest", "rotation test"); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	total := 0
	archives, err := ArchivePaths(dir, "pipeline")
	if err != nil {
		t.Fatalf("archive paths: %v", err)
	}
	for _, path := range archives {
		if err := ReadFile(path, func(FailedBatch) error { total++; return nil }); err != nil {
			t.Fatalf("read archive %s: %v", filepath.Base(path), err)
		}
	}
	if err := ReadFile(JournalPath(dir, "pipeline"), func(FailedBatch) error { total++; return nil }); err != nil {
		t.Fatalf("read current journal: %v", err)
	}
	if total != 3 {
		t.Fatalf("recovered %d of 3 dead-lettered records after %d rotations in the same second", total, len(archives))
	}
}
