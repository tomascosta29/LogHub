package processor

import (
	"context"
	"testing"
	"time"

	"loghub/internal/core"
)

func TestFilterIncludeLevel(t *testing.T) {
	f, err := NewFilter(FilterConfig{
		IncludeLevel: []string{"ERROR", "WARN"},
	})
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	logs := []*core.LogEntry{
		{Level: "ERROR", Message: "err1"},
		{Level: "INFO", Message: "info1"},
		{Level: "warn", Message: "warn1"},
	}

	result, err := f.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result[0].Message != "err1" || result[1].Message != "warn1" {
		t.Errorf("unexpected messages: %v", result)
	}
}

func TestFilterExcludeLevel(t *testing.T) {
	f, err := NewFilter(FilterConfig{
		ExcludeLevel: []string{"DEBUG"},
	})
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	logs := []*core.LogEntry{
		{Level: "DEBUG", Message: "dbg"},
		{Level: "INFO", Message: "info"},
	}

	result, err := f.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 1 || result[0].Message != "info" {
		t.Fatalf("expected 1 result with 'info', got %v", result)
	}
}

func TestFilterMessageRegex(t *testing.T) {
	f, err := NewFilter(FilterConfig{
		IncludeMessage: `error.*\d+`,
	})
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "error 123 occurred"},
		{Message: "success"},
		{Message: "error 456 here"},
	}

	result, err := f.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
}

func TestFilterFields(t *testing.T) {
	f, err := NewFilter(FilterConfig{
		IncludeFields: map[string]string{"env": "prod"},
		ExcludeFields: map[string]string{"skip": "true"},
	})
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "a", Fields: map[string]string{"env": "prod"}},
		{Message: "b", Fields: map[string]string{"env": "dev"}},
		{Message: "c", Fields: map[string]string{"env": "prod", "skip": "true"}},
	}

	result, err := f.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 1 || result[0].Message != "a" {
		t.Fatalf("expected 1 result, got %v", result)
	}
}

func TestJSONParseMessage(t *testing.T) {
	p, err := NewJSONParse(JSONParseConfig{
		SourceField: "",
		TargetField: "",
		KeepSource:  true,
	})
	if err != nil {
		t.Fatalf("NewJSONParse failed: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: `{"user":"alice","action":"login"}`, Fields: map[string]string{"app": "web"}},
		{Message: `not json`, Fields: map[string]string{"app": "web"}},
	}

	result, err := p.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result[0].Fields["user"] != "alice" || result[0].Fields["action"] != "login" {
		t.Errorf("unexpected fields: %v", result[0].Fields)
	}
	if result[1].Message != "not json" {
		t.Errorf("expected non-JSON to be kept, got %v", result[1])
	}
}

func TestJSONParseDropOnError(t *testing.T) {
	p, err := NewJSONParse(JSONParseConfig{
		SourceField: "",
		OnError:     "drop",
	})
	if err != nil {
		t.Fatalf("NewJSONParse failed: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: `{"valid":"json"}`},
		{Message: `not json`},
	}

	result, err := p.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].Fields["valid"] != "json" {
		t.Errorf("unexpected result: %v", result[0])
	}
}

func TestJSONParseSourceField(t *testing.T) {
	p, err := NewJSONParse(JSONParseConfig{
		SourceField: "payload",
		KeepSource:  false,
	})
	if err != nil {
		t.Fatalf("NewJSONParse failed: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "hello", Fields: map[string]string{"payload": `{"key":"val"}`}},
	}

	result, err := p.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].Fields["key"] != "val" {
		t.Errorf("expected key=val, got %v", result[0].Fields)
	}
	if _, ok := result[0].Fields["payload"]; ok {
		t.Error("expected payload field to be removed")
	}
}

func TestTimestampNormalize(t *testing.T) {
	p, err := NewTimestamp(TimestampConfig{
		SourceField:  "ts",
		SourceLayout: "2006-01-02 15:04:05",
		TargetLayout: time.RFC3339,
	})
	if err != nil {
		t.Fatalf("failed to create timestamp processor: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "test", Fields: map[string]string{"ts": "2024-03-15 14:30:00"}},
	}

	result, err := p.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].Fields["ts"] != "2024-03-15T14:30:00Z" {
		t.Errorf("unexpected timestamp: %s", result[0].Fields["ts"])
	}
}

func TestTimestampToTimestampField(t *testing.T) {
	p, err := NewTimestamp(TimestampConfig{
		SourceField:  "ts",
		SourceLayout: "2006-01-02 15:04:05",
		TargetField:  "timestamp",
	})
	if err != nil {
		t.Fatalf("failed to create timestamp processor: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "test", Fields: map[string]string{"ts": "2024-03-15 14:30:00"}},
	}

	result, err := p.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	expected := time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC)
	if !result[0].Timestamp.Equal(expected) {
		t.Errorf("unexpected timestamp: %v", result[0].Timestamp)
	}
}

func TestTimestampMissingField(t *testing.T) {
	p, err := NewTimestamp(TimestampConfig{
		SourceField:  "missing",
		SourceLayout: "2006-01-02 15:04:05",
	})
	if err != nil {
		t.Fatalf("failed to create timestamp processor: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "test", Fields: map[string]string{"other": "val"}},
	}

	result, err := p.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].Fields["other"] != "val" {
		t.Errorf("entry should remain unchanged")
	}
}

func TestTimestampInvalidDrop(t *testing.T) {
	p, err := NewTimestamp(TimestampConfig{
		SourceField:  "ts",
		SourceLayout: "2006-01-02 15:04:05",
		OnError:      "drop",
	})
	if err != nil {
		t.Fatalf("failed to create timestamp processor: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "test", Fields: map[string]string{"ts": "not-a-date"}},
	}

	result, err := p.Process(context.Background(), logs)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 results, got %d", len(result))
	}
}
