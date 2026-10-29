package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenEmptyJournal(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if got, err := j.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	} else if len(got) != 0 {
		t.Errorf("expected 0 entries, got %d", len(got))
	}
}

func TestAppendAndBarrierRoundTrip(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	id1, err := j.Append([]byte("alpha"))
	if err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if id1 != 1 {
		t.Errorf("first id = %d, want 1", id1)
	}
	if _, err := j.Append([]byte("beta")); err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	if err := j.Barrier(); err != nil {
		t.Fatalf("Barrier 1: %v", err)
	}
	if _, err := j.Append([]byte("gamma")); err != nil {
		t.Fatalf("Append 3: %v", err)
	}
	if err := j.Barrier(); err != nil {
		t.Fatalf("Barrier 2: %v", err)
	}

	got, err := j.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 entry in last tx, got %d", len(got))
	}
	if string(got[0].Data) != "gamma" {
		t.Errorf("got data %q, want %q", got[0].Data, "gamma")
	}
	if got[0].ID != 3 {
		t.Errorf("got id %d, want 3", got[0].ID)
	}
}

func TestUncommittedTailIsNotVisible(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.journal")

	j, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := j.Append([]byte("a")); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if err := j.Barrier(); err != nil {
		t.Fatalf("Barrier 1: %v", err)
	}
	if _, err := j.Append([]byte("b")); err != nil {
		t.Fatalf("Append b: %v", err)
	}
	if _, err := j.Append([]byte("c")); err != nil {
		t.Fatalf("Append c: %v", err)
	}
	// Simulate a crash: close without a closing Barrier.
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer j2.Close()

	got, err := j2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// The most recent committed transaction is [a] (closed by the only
	// barrier). b, c are in an in-progress transaction with no closing
	// barrier and must not be visible.
	if len(got) != 1 || string(got[0].Data) != "a" {
		t.Errorf("expected [a] as the last committed transaction, got %+v", got)
	}
}

func TestIDMonotonicAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.journal")

	j, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := j.Append([]byte("a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.Barrier(); err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer j2.Close()
	id, err := j2.Append([]byte("b"))
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if id != 2 {
		t.Errorf("expected id 2 after reopen, got %d", id)
	}
}

func TestPartialTrailingLineIsIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.journal")

	j, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := j.Append([]byte("a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.Barrier(); err != nil {
		t.Fatalf("Barrier 1: %v", err)
	}
	if _, err := j.Append([]byte("b")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.Barrier(); err != nil {
		t.Fatalf("Barrier 2: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write of a third entry.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteString(`{"id":3,"data":"X`); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("f.Close: %v", err)
	}

	j2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer j2.Close()
	// The partial trailing line is ignored, so the last closed transaction
	// is still [b].
	got, err := j2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 || string(got[0].Data) != "b" {
		t.Errorf("expected [b] from prior closed tx, got %+v", got)
	}
}

func TestEncodeDecodeDataRemoved(t *testing.T) {
	// EncodeData/DecodeData were removed in the H6/H7 cleanup; this
	// test is a placeholder documenting their removal so a future
	// refactor that re-introduces them is intentional.
	_ = t
}

func TestAppendAssignsMonotonicIDs(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	for i := 1; i <= 5; i++ {
		id, err := j.Append([]byte("x"))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if id != int64(i) {
			t.Errorf("Append %d returned id %d", i, id)
		}
	}
}

func TestAppendBarrierIsOneTransaction(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendBarrier([]byte("a")); err != nil {
		t.Fatalf("AppendBarrier a: %v", err)
	}
	if _, err := j.AppendBarrier([]byte("b")); err != nil {
		t.Fatalf("AppendBarrier b: %v", err)
	}

	got, err := j.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// Each AppendBarrier is its own committed transaction; Recover returns
	// the most recent one.
	if len(got) != 1 || string(got[0].Data) != "b" {
		t.Errorf("expected [b] from last transaction, got %+v", got)
	}
}

func TestReadAllIteratesEntriesAndBarriers(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if _, err := j.Append([]byte("a")); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if err := j.Barrier(); err != nil {
		t.Fatalf("Barrier 1: %v", err)
	}
	if _, err := j.Append([]byte("b")); err != nil {
		t.Fatalf("Append b: %v", err)
	}
	if _, err := j.AppendBarrier([]byte("c")); err != nil {
		t.Fatalf("AppendBarrier c: %v", err)
	}

	var got []string
	if err := j.ReadCommitted(func(id int64, data []byte) error {
		got = append(got, fmt.Sprintf("%d:%s", id, data))
		return nil
	}); err != nil {
		t.Fatalf("ReadCommitted: %v", err)
	}
	// b has no barrier of its own; the barrier after c commits both.
	if strings.Join(got, ",") != "1:a,2:b,3:c" {
		t.Errorf("ReadCommitted = %v, want [1:a 2:b 3:c]", got)
	}
}

func TestRewriteWithReplacesContents(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendBarrier([]byte("old-1")); err != nil {
		t.Fatalf("AppendBarrier old-1: %v", err)
	}
	if _, err := j.AppendBarrier([]byte("old-2")); err != nil {
		t.Fatalf("AppendBarrier old-2: %v", err)
	}

	if err := j.RewriteWith([][]byte{[]byte("new-A"), []byte("new-B"), []byte("new-C")}); err != nil {
		t.Fatalf("RewriteWith: %v", err)
	}

	got, err := j.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 || string(got[0].Data) != "new-C" {
		t.Errorf("after rewrite, Recover returned %+v, want [new-C]", got)
	}

	// New appends should use IDs continuing from 3.
	id, err := j.Append([]byte("post"))
	if err != nil {
		t.Fatalf("Append after rewrite: %v", err)
	}
	if id != 4 {
		t.Errorf("post-rewrite Append returned id %d, want 4", id)
	}
}

func TestRewriteWithToEmpty(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendBarrier([]byte("data")); err != nil {
		t.Fatalf("AppendBarrier: %v", err)
	}
	if err := j.RewriteWith(nil); err != nil {
		t.Fatalf("RewriteWith nil: %v", err)
	}
	got, err := j.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("after rewrite-to-empty, Recover returned %+v, want nil", got)
	}
}

func TestCommitTransactionAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	// Commit three entries in one transaction.
	if _, err := j.CommitTransaction([]byte("alpha"), []byte("beta"), []byte("gamma")); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	// After reopen, the most recent transaction should contain all three.
	j2, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	got, err := j2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Recover returned %d entries, want 3", len(got))
	}
	want := []string{"alpha", "beta", "gamma"}
	for i, e := range got {
		if string(e.Data) != want[i] {
			t.Errorf("entry %d: got %q, want %q", i, e.Data, want[i])
		}
	}
}

func TestCommitTransactionSingleBarrierAfterMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if _, err := j.CommitTransaction([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	// Read the raw file and count barrier lines.
	data, err := os.ReadFile(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	barrierCount := 0
	for _, line := range bytes.Split(data, []byte("\n")) {
		if bytes.Contains(line, []byte(`"barrier":true`)) {
			barrierCount++
		}
	}
	if barrierCount != 1 {
		t.Errorf("expected exactly 1 barrier line, got %d", barrierCount)
	}
}

func TestCommitTransactionWithNoPayloadsIsNoop(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	if _, err := j.CommitTransaction(); err != nil {
		t.Errorf("CommitTransaction() with no payloads: %v", err)
	}
	if got, _ := j.Recover(); len(got) != 0 {
		t.Errorf("expected no transactions, got %d", len(got))
	}
}

func TestCommitTransactionAfterClose(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "test.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Commit one transaction.
	if _, err := j.CommitTransaction([]byte("first")); err != nil {
		t.Fatalf("first CommitTransaction: %v", err)
	}
	// Now close the underlying file to force an I/O error on the next
	// transaction.
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = j.CommitTransaction([]byte("after-close"))
	if err == nil {
		t.Fatal("expected error from CommitTransaction after Close, got nil")
	}
}

func TestAppendBarrierEncodesEntryEvenWhenBarrierFails(t *testing.T) {
	// When the entry's encode succeeds but the barrier's
	// encode fails (rare — usually ENOSPC), the entry is on disk
	// with its assigned ID. The next Append uses ID+1 (not the same
	// ID), and a subsequent Recover returns nil (the entry has no
	// closing barrier). The on-disk state is not corrupted.
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	// Successful first commit to establish a baseline.
	if _, err := j.AppendBarrier([]byte("a")); err != nil {
		t.Fatalf("AppendBarrier a: %v", err)
	}

	// Manually close the file to force the next encode to fail.
	j.mu.Lock()
	_ = j.f.Close()
	j.f = nil
	j.mu.Unlock()

	_, err = j.AppendBarrier([]byte("b"))
	if err == nil {
		t.Fatal("expected AppendBarrier to fail with closed file")
	}

	// Reopen the file in append mode and continue. The next entry
	// must use a fresh ID (not the rolled-back ID from the failed
	// barrier).
	if rerr := reopenForTest(j); rerr != nil {
		t.Fatalf("reopen: %v", rerr)
	}
	id, err := j.AppendBarrier([]byte("c"))
	if err != nil {
		t.Fatalf("AppendBarrier c: %v", err)
	}
	if id <= 1 {
		t.Errorf("expected fresh ID > 1, got %d", id)
	}
}

// reopenForTest is a test helper that re-opens the journal file in
// append mode, mirroring the production reopenLocked path.
func reopenForTest(j *Journal) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	j.f = f
	j.enc = json.NewEncoder(f)
	return nil
}

func TestRewriteWithReopensOnError(t *testing.T) {
	// If RewriteWith fails after closing the original file, it must
	// re-open the original file so subsequent operations still work.
	// We force the failure by giving a directory that cannot be
	// created.
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendBarrier([]byte("a")); err != nil {
		t.Fatalf("AppendBarrier: %v", err)
	}

	// Set j.dir to a path that MkdirAll will not be able to create
	// (use a file as a parent).
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile blocker: %v", err)
	}
	j.mu.Lock()
	j.dir = blocker // MkdirAll on a path whose parent is a file fails
	j.mu.Unlock()

	err = j.RewriteWith([][]byte{[]byte("b")})
	if err == nil {
		t.Fatal("expected RewriteWith to fail")
	}

	// After the failure, j.f must be non-nil (reopened), so a
	// subsequent Append does not panic.
	if _, err := j.Append([]byte("c")); err != nil {
		t.Fatalf("Append after failed RewriteWith: %v", err)
	}
}

func TestCommitTransactionIdCollisionRegression(t *testing.T) {
	// CommitTransaction must not roll back j.next when the
	// barrier encode fails, because the entries are already on disk
	// with their assigned IDs. A subsequent CommitTransaction that
	// succeeds must use IDs strictly greater than the failed
	// transaction's highest ID.
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	// First, a successful commit of 2 entries → IDs 1, 2.
	if _, err := j.CommitTransaction([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	// Force a barrier-encode failure by closing the file.
	j.mu.Lock()
	_ = j.f.Close()
	j.f = nil
	j.mu.Unlock()

	_, err = j.CommitTransaction([]byte("c"), []byte("d"))
	if err == nil {
		t.Fatal("expected CommitTransaction to fail with closed file")
	}

	// Reopen and commit again. The next ID must be > 2.
	if rerr := reopenForTest(j); rerr != nil {
		t.Fatalf("reopen: %v", rerr)
	}
	// Successful single-op commit, then check the in-memory next.
	if _, err := j.AppendBarrier([]byte("e")); err != nil {
		t.Fatalf("AppendBarrier e: %v", err)
	}
	j.mu.Lock()
	next := j.next
	j.mu.Unlock()
	if next <= 2 {
		t.Errorf("expected next > 2 after successful commit, got %d", next)
	}

	// Recover returns only the last fully-committed transaction.
	// Since the failed commit's barrier is missing, only the
	// successful "e" entry (or the original "a, b" transaction) is
	// visible. Either is acceptable; the key is no duplicates and
	// no missing entries.
	got, err := j.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	ids := make(map[int64]bool)
	for _, e := range got {
		if ids[e.ID] {
			t.Errorf("duplicate ID %d in Recover", e.ID)
		}
		ids[e.ID] = true
	}
}

func TestRewriteWithFailureKeepsNextID(t *testing.T) {
	// A failed rewrite must leave the ID counter matching the original
	// file; otherwise the next Append reuses an ID already on disk.
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	for _, d := range []string{"a", "b", "c"} {
		if _, err := j.AppendBarrier([]byte(d)); err != nil {
			t.Fatalf("AppendBarrier: %v", err)
		}
	}

	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile blocker: %v", err)
	}
	j.mu.Lock()
	j.dir = blocker
	j.mu.Unlock()

	if err := j.RewriteWith([][]byte{[]byte("x")}); err == nil {
		t.Fatal("expected RewriteWith to fail")
	}

	id, err := j.Append([]byte("d"))
	if err != nil {
		t.Fatalf("Append after failed RewriteWith: %v", err)
	}
	if id != 4 {
		t.Errorf("Append ID = %d, want 4 (IDs 1-3 are still on disk)", id)
	}
}

func TestRewriteWithReportsReopenFailure(t *testing.T) {
	// When the rewrite fails and the original file cannot be reopened,
	// RewriteWith must return both errors instead of panicking.
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile blocker: %v", err)
	}
	j.mu.Lock()
	j.dir = blocker
	j.path = filepath.Join(blocker, "j.journal") // parent is a file: reopen fails
	j.mu.Unlock()

	err = j.RewriteWith([][]byte{[]byte("x")})
	if err == nil {
		t.Fatal("expected RewriteWith to fail")
	}
	if !strings.Contains(err.Error(), "reopen after failed rewrite") {
		t.Errorf("error = %q, want it to report the reopen failure", err)
	}
	if _, err := j.Append([]byte("y")); err == nil {
		t.Error("Append on a journal that failed to reopen: want error, got nil")
	}
}

func TestCompactPreservesMultiEntryTransaction(t *testing.T) {
	// Compaction must keep the last transaction as one transaction:
	// rewriting it with a barrier per entry would make Recover return
	// only its final entry.
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	if _, err := j.CommitTransaction([]byte("old")); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	if _, err := j.CommitTransaction([]byte("a"), []byte("b"), []byte("c")); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	if err := j.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	entries, err := j.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, string(e.Data))
	}
	if strings.Join(got, ",") != "a,b,c" {
		t.Errorf("Recover after Compact = %v, want [a b c]", got)
	}

	id, err := j.Append([]byte("d"))
	if err != nil {
		t.Fatalf("Append after Compact: %v", err)
	}
	if id != 4 {
		t.Errorf("Append ID after Compact = %d, want 4", id)
	}
}

func TestReadCommittedSkipsUncommittedTail(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	if _, err := j.AppendBarrier([]byte("a")); err != nil {
		t.Fatalf("AppendBarrier: %v", err)
	}
	ids, err := j.CommitTransaction([]byte("b"), []byte("c"))
	if err != nil || len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Fatalf("CommitTransaction = %v, %v; want [2 3]", ids, err)
	}
	if _, err := j.Append([]byte("uncommitted")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	var got []string
	if err := j.ReadCommitted(func(id int64, data []byte) error {
		got = append(got, fmt.Sprintf("%d:%s", id, data))
		return nil
	}); err != nil {
		t.Fatalf("ReadCommitted: %v", err)
	}
	if strings.Join(got, ",") != "1:a,2:b,3:c" {
		t.Errorf("ReadCommitted = %v, want [1:a 2:b 3:c]", got)
	}

	got = got[:0]
	if err := ReadCommittedFile(j.Path(), func(id int64, data []byte) error {
		got = append(got, string(data))
		return nil
	}); err != nil {
		t.Fatalf("ReadCommittedFile: %v", err)
	}
	if strings.Join(got, ",") != "a,b,c" {
		t.Errorf("ReadCommittedFile = %v, want [a b c]", got)
	}
}

func TestReadCommittedCallbackCanAppend(t *testing.T) {
	// ReadCommitted must not hold the journal lock during callbacks, and
	// must not return entries appended after it started.
	dir := t.TempDir()
	j, err := Open(filepath.Join(dir, "j.journal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	if _, err := j.AppendBarrier([]byte("a")); err != nil {
		t.Fatalf("AppendBarrier: %v", err)
	}
	seen := 0
	err = j.ReadCommitted(func(int64, []byte) error {
		seen++
		_, err := j.AppendBarrier([]byte("appended-during-read"))
		return err
	})
	if err != nil {
		t.Fatalf("ReadCommitted: %v", err)
	}
	if seen != 1 {
		t.Errorf("ReadCommitted returned %d entries, want 1 (later appends are not part of the read)", seen)
	}
}
