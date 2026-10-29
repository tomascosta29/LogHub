package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestRecoverAfterEveryTruncation simulates unsynced data being lost in a
// power cut by truncating the file at every byte boundary. Recovery must
// return either the last fully committed transaction or nothing: never a
// partial record, never an unclosed transaction, and never a record from
// after the last barrier.
func TestRecoverAfterEveryTruncation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal")
	j, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var payloads []string
	for i := 0; i < 8; i++ {
		payload := fmt.Sprintf("transaction-%d", i)
		payloads = append(payloads, payload)
		if _, err := j.AppendBarrier([]byte(payload)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	for cut := 0; cut <= len(data); cut++ {
		prefix := data[:cut]
		pathCut := filepath.Join(dir, "cut")
		if err := os.WriteFile(pathCut, prefix, 0o600); err != nil {
			t.Fatalf("write cut %d: %v", cut, err)
		}
		want := expectedTransaction(prefix)

		jc, err := Open(pathCut)
		if err != nil {
			t.Fatalf("cut %d: open: %v", cut, err)
		}
		entries, err := jc.Recover()
		if err != nil {
			jc.Close()
			t.Fatalf("cut %d: recover: %v", cut, err)
		}
		var got []string
		for _, e := range entries {
			got = append(got, string(e.Data))
		}
		if len(got) != len(want) {
			jc.Close()
			t.Fatalf("cut %d: recovered %d entries %v, want %v", cut, len(got), got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				jc.Close()
				t.Fatalf("cut %d: entry %d = %q, want %q", cut, i, got[i], want[i])
			}
		}
		// The journal must still be writable after recovery of a torn file.
		if _, err := jc.AppendBarrier([]byte("after")); err != nil {
			jc.Close()
			t.Fatalf("cut %d: append after recovery: %v", cut, err)
		}
		jc.Close()
	}
}

// expectedTransaction returns the entries of the last transaction whose
// closing barrier is present in prefix.
func expectedTransaction(prefix []byte) []string {
	var current, last []string
	for _, line := range bytes.Split(prefix, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var b barrierMarker
		if json.Unmarshal(line, &b) == nil && b.Barrier {
			last = append([]string(nil), current...)
			current = current[:0]
			continue
		}
		var e Entry
		if json.Unmarshal(line, &e) == nil && e.ID > 0 {
			current = append(current, string(e.Data))
		}
	}
	return last
}
