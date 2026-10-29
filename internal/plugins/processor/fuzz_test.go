package processor

import (
	"context"
	"testing"
	"time"

	"loghub/internal/core"
)

// FuzzTimestampProcessor checks the timestamp processor never panics on
// arbitrary field values and layouts, and never mutates the input entry.
// Run with: go test ./internal/plugins/processor -fuzz=FuzzTimestampProcessor -fuzztime=30s
func FuzzTimestampProcessor(f *testing.F) {
	f.Add("2026-05-06T12:00:00Z", "2006-01-02T15:04:05Z07:00")
	f.Add("", "2006-01-02 15:04:05")
	f.Add("not-a-time", "2006-01-02")
	f.Add("2026-05-06 12:00:00", "2006-01-02 15:04:05")
	f.Add("23:59:60", "15:04:05")
	f.Fuzz(func(t *testing.T, value, layout string) {
		proc, err := NewTimestamp(TimestampConfig{SourceField: "ts", SourceLayout: layout, TargetField: "timestamp"})
		if err != nil {
			return
		}
		entry := &core.LogEntry{
			Timestamp: time.Unix(0, 0),
			Fields:    map[string]string{"ts": value, "keep": "x"},
		}
		_, _ = proc.Process(context.Background(), []*core.LogEntry{entry})
		if entry.Fields["ts"] != value {
			t.Fatalf("processor mutated its input: %q became %q", value, entry.Fields["ts"])
		}
	})
}

// FuzzJSONParseProcessor checks the JSON parser never panics on arbitrary
// content and leaves the input entry alone on error.
// Run with: go test ./internal/plugins/processor -fuzz=FuzzJSONParseProcessor -fuzztime=30s
func FuzzJSONParseProcessor(f *testing.F) {
	for _, seed := range []string{
		`{"a":1}`,
		`{"a":{"b":[1,2,{"c":"d"}]}}`,
		`{"a":"\ud800"}`,
		`{"a":1e999}`,
		`{"a":1,"a":2}`,
		`not json`,
		``,
		`[]`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		for _, target := range []string{"fields", "message"} {
			proc, err := NewJSONParse(JSONParseConfig{SourceField: "raw", TargetField: target})
			if err != nil {
				t.Fatal(err)
			}
			entry := &core.LogEntry{Fields: map[string]string{"raw": value}}
			_, _ = proc.Process(context.Background(), []*core.LogEntry{entry})
			if entry.Fields["raw"] != value {
				t.Fatalf("processor mutated its input")
			}
		}
	})
}
