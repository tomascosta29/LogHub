package core

import (
	"time"
)

type LogEntry struct {
	Timestamp time.Time         `json:"timestamp"`
	Source    string            `json:"source"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields"`
	Cursor    string            `json:"-"`
}

func NewLogEntry(timestamp time.Time, source, level, message string, fields map[string]string) *LogEntry {
	if fields == nil {
		fields = make(map[string]string)
	}
	return &LogEntry{
		Timestamp: timestamp,
		Source:    source,
		Level:     level,
		Message:   message,
		Fields:    fields,
	}
}

// CopyLogEntry returns a deep copy of the given LogEntry.
func CopyLogEntry(entry *LogEntry) *LogEntry {
	if entry == nil {
		return nil
	}
	newFields := make(map[string]string, len(entry.Fields))
	for k, v := range entry.Fields {
		newFields[k] = v
	}
	return &LogEntry{
		Timestamp: entry.Timestamp,
		Source:    entry.Source,
		Level:     entry.Level,
		Message:   entry.Message,
		Fields:    newFields,
		Cursor:    entry.Cursor,
	}
}
