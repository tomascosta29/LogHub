package destination

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"loghub/internal/core"
)

func TestLocalJSONLWritesSentinelRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.jsonl")
	dest, err := NewLocalJSONL(path, "sentinel")
	if err != nil {
		t.Fatalf("NewLocalJSONL failed: %v", err)
	}
	defer dest.Close()

	_, err = dest.Send(context.Background(), []*core.LogEntry{{
		Timestamp: time.Date(2026, 5, 6, 12, 0, 1, 0, time.UTC),
		Fields: map[string]string{
			"id":         "000001",
			"event_time": "2026-05-06T12:00:01Z",
			"user_name":  "user-000001",
			"action":     "login",
		},
	}})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	records := readJSONLLines(t, path)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if got, want := records[0]["TimeGenerated"], "2026-05-06T12:00:01Z"; got != want {
		t.Fatalf("TimeGenerated got %q, want %q", got, want)
	}
	if _, ok := records[0]["fields"]; ok {
		t.Fatal("sentinel format should be flattened")
	}
}

func readJSONLLines(t *testing.T, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %s: %v", path, err)
	}
	defer f.Close()

	var records []map[string]string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var record map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("failed to unmarshal JSONL line: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan JSONL: %v", err)
	}
	return records
}

func TestLocalJSONLSendAfterCloseReturnsError(t *testing.T) {
	// Regression test: Send after Close must return an error
	// (not panic with a nil-pointer dereference on l.enc.Encode).
	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")
	l, err := NewLocalJSONL(path, "log_entry")
	if err != nil {
		t.Fatalf("NewLocalJSONL: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = l.Send(context.Background(), []*core.LogEntry{{Message: "x"}})
	if err == nil {
		t.Fatal("expected error from Send after Close")
	}
	// And the error should be specific (not a panic-recovered nil-deref).
	if !strings.Contains(err.Error(), "close") {
		t.Errorf("error = %v, want one mentioning 'close'", err)
	}
}

func TestLocalJSONLCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")
	l, err := NewLocalJSONL(path, "log_entry")
	if err != nil {
		t.Fatalf("NewLocalJSONL: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close returned %v, want nil (idempotent)", err)
	}
}
