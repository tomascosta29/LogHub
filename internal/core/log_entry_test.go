package core

import (
	"testing"
	"time"
)

func TestLogEntryCreation(t *testing.T) {
	entry := NewLogEntry(
		time.Now(),
		"test-source",
		"INFO",
		"test message",
		map[string]string{"key": "value"},
	)

	if entry.Source != "test-source" {
		t.Errorf("Expected Source 'test-source', got '%s'", entry.Source)
	}
	if entry.Level != "INFO" {
		t.Errorf("Expected Level 'INFO', got '%s'", entry.Level)
	}
	if entry.Message != "test message" {
		t.Errorf("Expected Message 'test message', got '%s'", entry.Message)
	}
	if entry.Fields["key"] != "value" {
		t.Errorf("Expected Fields[key] 'value', got '%s'", entry.Fields["key"])
	}
}

func TestLogEntryDefaultFields(t *testing.T) {
	entry := NewLogEntry(time.Now(), "source", "ERROR", "msg", nil)

	if entry.Fields == nil {
		t.Error("Expected Fields to be initialized, got nil")
	}
}
