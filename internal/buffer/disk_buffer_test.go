package buffer

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"loghub/internal/core"
	"loghub/internal/journal"
)

func makeBatch(msg, next string) DiskBatch {
	return DiskBatch{
		Entries:    []*core.LogEntry{{Message: msg}},
		NextCursor: next,
	}
}

func openBuffer(t *testing.T, dir string) *DiskBuffer {
	t.Helper()
	db, err := NewDiskBuffer(dir, "test", 100)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	return db
}

// drainAll drains db, marking every batch delivered, and returns the
// batches' first messages in order.
func drainAll(t *testing.T, db *DiskBuffer) []string {
	t.Helper()
	var got []string
	err := db.Drain(func(id int64, b DiskBatch) error {
		got = append(got, b.Entries[0].Message)
		return db.MarkDelivered(id)
	})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	return got
}

func TestDiskBufferRestoresCountOnRestart(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	if err := db.Write(makeBatch("persisted", "1")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened := openBuffer(t, dir)
	defer reopened.Close()
	if got := reopened.Count(); got != 1 {
		t.Fatalf("expected restored count 1, got %d", got)
	}
}

func TestDiskBufferDeliveredBatchesStayDeliveredAfterRestart(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	for _, m := range []string{"a", "b", "c"} {
		if err := db.Write(makeBatch(m, "c")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	// Deliver "a", then stop: a crash or a failing destination.
	err := db.Drain(func(id int64, b DiskBatch) error {
		if err := db.MarkDelivered(id); err != nil {
			return err
		}
		return ErrDrainStop
	})
	if !errors.Is(err, ErrDrainStop) {
		t.Fatalf("Drain = %v, want ErrDrainStop", err)
	}
	if got := db.Count(); got != 2 {
		t.Fatalf("Count after delivering one = %d, want 2", got)
	}
	db.Close()

	reopened := openBuffer(t, dir)
	defer reopened.Close()
	if got := reopened.Count(); got != 2 {
		t.Errorf("Count after restart = %d, want 2", got)
	}
	if got := drainAll(t, reopened); strings.Join(got, ",") != "b,c" {
		t.Errorf("drained after restart = %v, want [b c] (a was already delivered)", got)
	}
}

func TestDiskBufferReplaceSwapsBatchAtomically(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	if err := db.Write(makeBatch("whole", "c")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err := db.Drain(func(id int64, b DiskBatch) error {
		rest := makeBatch("rest", "c")
		rest.Destinations = []string{"http#1"}
		return db.Replace(id, []DiskBatch{rest})
	})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	db.Close()

	reopened := openBuffer(t, dir)
	defer reopened.Close()
	var got []DiskBatch
	if err := reopened.Drain(func(id int64, b DiskBatch) error {
		got = append(got, b)
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 1 || got[0].Entries[0].Message != "rest" || len(got[0].Destinations) != 1 {
		t.Errorf("pending after Replace = %+v, want only the replacement for http#1", got)
	}
}

func TestDiskBufferDrainSkipsBatchesWrittenDuringDrain(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	defer db.Close()
	if err := db.Write(makeBatch("before", "c")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var seen []string
	err := db.Drain(func(id int64, b DiskBatch) error {
		seen = append(seen, b.Entries[0].Message)
		if err := db.Write(makeBatch("during", "c")); err != nil {
			return err
		}
		return db.MarkDelivered(id)
	})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if strings.Join(seen, ",") != "before" {
		t.Errorf("first drain saw %v, want [before]", seen)
	}
	if got := db.Count(); got != 1 {
		t.Errorf("Count = %d, want 1 (the batch written during the drain stays pending)", got)
	}
}

func TestDiskBufferCompactEmptiesDrainedJournal(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	defer db.Close()
	for _, m := range []string{"a", "b"} {
		if err := db.Write(makeBatch(m, "c")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact with pending batches: %v", err)
	}
	if size, _ := db.journal.Size(); size == 0 {
		t.Fatal("Compact emptied a journal that still had pending batches")
	}

	drainAll(t, db)
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if size, _ := db.journal.Size(); size != 0 {
		t.Errorf("journal size after compacting a drained buffer = %d, want 0", size)
	}
	if err := db.Write(makeBatch("after", "c")); err != nil {
		t.Fatalf("Write after Compact: %v", err)
	}
	if got := drainAll(t, db); strings.Join(got, ",") != "after" {
		t.Errorf("drained after Compact = %v, want [after]", got)
	}
}

func TestDiskBufferHonoursLegacyDrainMarker(t *testing.T) {
	// Journals written by unreleased builds mark everything before a
	// drain marker as delivered.
	dir := t.TempDir()
	j, err := journal.Open(filepath.Join(dir, "test.buffer.journal"))
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	for _, payload := range []any{makeBatch("old", "c"), nil, makeBatch("new", "c")} {
		data := legacyDrainMarker
		if payload != nil {
			data, _ = json.Marshal(payload)
		}
		if _, err := j.AppendBarrier(data); err != nil {
			t.Fatalf("AppendBarrier: %v", err)
		}
	}
	j.Close()

	db := openBuffer(t, dir)
	defer db.Close()
	if got := drainAll(t, db); strings.Join(got, ",") != "new" {
		t.Errorf("drained = %v, want [new]", got)
	}
}

func TestDiskBufferImportsV010Files(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, lines ...string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	write("test.buffer.drain", `{"entries":[{"message":"oldest"}],"next_cursor":"c1"}`)
	write("test.buffer",
		`{"entries":[{"message":"newer"}],"next_cursor":"c2"}`,
		`{"entries":[{"message":"trunc`) // partial line from a crash

	db := openBuffer(t, dir)
	if got := drainAll(t, db); strings.Join(got, ",") != "oldest,newer" {
		t.Errorf("drained = %v, want [oldest newer]", got)
	}
	db.Close()

	for _, name := range []string{"test.buffer.drain", "test.buffer"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s still exists after import", name)
		}
		if _, err := os.Stat(filepath.Join(dir, name+".migrated")); err != nil {
			t.Errorf("%s.migrated missing: %v", name, err)
		}
	}

	// Reopening must not import again.
	reopened := openBuffer(t, dir)
	defer reopened.Close()
	if got := reopened.Count(); got != 0 {
		t.Errorf("Count after reopen = %d, want 0", got)
	}
}

func TestDiskBufferEnforcesSizeLimit(t *testing.T) {
	dir := t.TempDir()
	// 1 MB limit. Build a payload larger than 1 MB to guarantee the second
	// write overflows.
	limit := int64(1)
	db, err := NewDiskBuffer(dir, "test", limit)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	defer db.Close()

	big := strings.Repeat("x", 600*1024)
	batch := DiskBatch{
		Entries:    []*core.LogEntry{{Message: big}},
		NextCursor: "c",
	}
	if err := db.Write(batch); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := db.Write(batch); err == nil {
		t.Error("expected error for second write over limit, got nil")
	}

	// Delivered batches no longer count against the limit.
	drainAll(t, db)
	if err := db.Write(batch); err != nil {
		t.Errorf("Write after draining: %v", err)
	}
}

func TestDiskBufferJournalFileName(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDiskBuffer(dir, "mypipeline", 100)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	defer db.Close()
	if err := db.Write(makeBatch("x", "c")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	expected := filepath.Join(dir, "mypipeline.buffer.journal")
	if _, err := os.Stat(expected); err != nil {
		t.Errorf("expected journal at %q, stat err: %v", expected, err)
	}
}

func TestDiskBufferDrainStreamsBatchesInOrder(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	defer db.Close()

	const N = 50
	var want []string
	for i := 0; i < N; i++ {
		m := string(rune('a' + i%26))
		want = append(want, m)
		if err := db.Write(makeBatch(m, "c")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if got := drainAll(t, db); strings.Join(got, "") != strings.Join(want, "") {
		t.Errorf("drain order = %v, want %v", got, want)
	}
	if got := db.Count(); got != 0 {
		t.Errorf("Count after drain = %d, want 0", got)
	}
}

func TestDiskBufferDrainStopKeepsUndeliveredBatches(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	defer db.Close()
	for _, m := range []string{"a", "b", "c"} {
		if err := db.Write(makeBatch(m, "c")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	seen := 0
	err := db.Drain(func(id int64, b DiskBatch) error {
		seen++
		if seen == 2 {
			return ErrDrainStop
		}
		return db.MarkDelivered(id)
	})
	if !errors.Is(err, ErrDrainStop) {
		t.Errorf("Drain should return ErrDrainStop, got %v", err)
	}
	if got := drainAll(t, db); strings.Join(got, ",") != "b,c" {
		t.Errorf("second drain = %v, want [b c]", got)
	}
}

func TestDiskBufferRejectsConcurrentDrain(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	defer db.Close()
	if err := db.Write(makeBatch("a", "c")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err := db.Drain(func(id int64, b DiskBatch) error {
		if err := db.Drain(func(int64, DiskBatch) error { return nil }); err == nil {
			t.Error("nested Drain: want error, got nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

func TestDiskBufferReplacementKeepsDeliveryOrder(t *testing.T) {
	dir := t.TempDir()
	db := openBuffer(t, dir)
	for _, m := range []string{"a", "b"} {
		if err := db.Write(makeBatch(m, "c")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	// "a" is partly delivered: replace it with what is left and stop.
	err := db.Drain(func(id int64, b DiskBatch) error {
		if err := db.Replace(id, []DiskBatch{makeBatch("a-rest", "c")}); err != nil {
			return err
		}
		return ErrDrainStop
	})
	if !errors.Is(err, ErrDrainStop) {
		t.Fatalf("Drain = %v, want ErrDrainStop", err)
	}
	if got := drainAll(t, db); strings.Join(got, ",") != "a-rest,b" {
		t.Errorf("drain order = %v, want [a-rest b] (the replacement keeps a's place)", got)
	}
	db.Close()

	// The order is recovered from the journal after a restart too.
	dir2 := t.TempDir()
	db2 := openBuffer(t, dir2)
	for _, m := range []string{"a", "b"} {
		if err := db2.Write(makeBatch(m, "c")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	db2.Drain(func(id int64, b DiskBatch) error {
		db2.Replace(id, []DiskBatch{makeBatch("a-rest", "c")})
		return ErrDrainStop
	})
	db2.Close()
	reopened := openBuffer(t, dir2)
	defer reopened.Close()
	if got := drainAll(t, reopened); strings.Join(got, ",") != "a-rest,b" {
		t.Errorf("drain order after restart = %v, want [a-rest b]", got)
	}
}
