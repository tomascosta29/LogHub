package source

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"loghub/internal/core"
)

func TestSQLiteSourceCollect(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()

	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:00:00', 'First event');
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:01:00', 'Second event');
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:02:00', 'Third event');
	`)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	src, err := NewSQLiteSource(
		SQLiteAuth{
			Database: tmpfile.Name(),
		},
		"events",
		"",
		"event_time",
		"test",
		10,
	)

	entries, cursor, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	if cursor == "" {
		t.Error("Expected non-empty cursor")
	}

	if entries[0].Fields["message"] != "First event" {
		t.Errorf("Expected 'First event', got '%s'", entries[0].Fields["message"])
	}

	if entries[0].Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
	if entries[2].Cursor == "" {
		t.Error("Expected cursor metadata on collected entry")
	}

	src.Close()
}

func TestSQLiteSourceCustomQuery(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}

	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:00:00', 'First event');
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:01:00', 'Second event');
	`)
	sqlDB.Close()

	src, err := NewSQLiteSource(
		SQLiteAuth{
			Database: tmpfile.Name(),
		},
		"",
		"SELECT id, event_time, message FROM events WHERE event_time > ? ORDER BY event_time LIMIT 10",
		"event_time",
		"test",
		10,
	)

	entries, _, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	src.Close()
}

func TestSQLiteSourceMaxLookbackGeneratedQuery(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-lookback-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()

	now := time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)
	oldTS := now.Add(-10 * time.Minute).Format(time.RFC3339)
	recentTS := now.Add(-2 * time.Minute).Format(time.RFC3339)
	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES (?, 'old');
		INSERT INTO events (event_time, message) VALUES (?, 'recent');
	`, oldTS, recentTS)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	raw, err := Create("sqlite", context.Background(), map[string]interface{}{
		"database":         tmpfile.Name(),
		"table":            "events",
		"timestamp_column": "event_time",
		"name":             "test",
		"fetch_size":       10,
		"max_lookback":     "5m",
	}, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	src := raw.(*SQLiteSource)
	src.now = func() time.Time { return now }
	defer src.Close()

	entries, cursor, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 recent entry, got %d", len(entries))
	}
	if entries[0].Fields["message"] != "recent" {
		t.Fatalf("expected recent entry, got %+v", entries[0].Fields)
	}
	if cursor != "v1:"+recentTS {
		t.Fatalf("expected cursor %q, got %q", "v1:"+recentTS, cursor)
	}
}

func TestSQLiteSourceMaxLookbackCustomQuery(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-lookback-custom-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()

	now := time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)
	oldTS := now.Add(-10 * time.Minute).Format(time.RFC3339)
	recentTS := now.Add(-2 * time.Minute).Format(time.RFC3339)
	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES (?, 'old');
		INSERT INTO events (event_time, message) VALUES (?, 'recent');
	`, oldTS, recentTS)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	raw, err := Create("sqlite", context.Background(), map[string]interface{}{
		"database":         tmpfile.Name(),
		"query":            "SELECT id, event_time, message FROM events WHERE event_time > ? ORDER BY event_time ASC LIMIT 10",
		"timestamp_column": "event_time",
		"name":             "test",
		"fetch_size":       10,
		"max_lookback":     "5m",
	}, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	src := raw.(*SQLiteSource)
	src.now = func() time.Time { return now }
	defer src.Close()

	entries, cursor, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 recent entry, got %d", len(entries))
	}
	if entries[0].Fields["message"] != "recent" {
		t.Fatalf("expected recent entry, got %+v", entries[0].Fields)
	}
	if cursor != "v1:"+recentTS {
		t.Fatalf("expected cursor %q, got %q", "v1:"+recentTS, cursor)
	}
}

func TestSQLiteSourceTimestampParsing(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}

	now := time.Now()
	ts := now.Format("2006-01-02 15:04:05")
	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			event_time TEXT NOT NULL
		);
		INSERT INTO events (event_time) VALUES (?)`, ts)
	sqlDB.Close()

	src, err := NewSQLiteSource(
		SQLiteAuth{Database: tmpfile.Name()},
		"events",
		"",
		"event_time",
		"test",
		10,
	)

	entries, _, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}

	src.Close()
}

func TestSQLiteSourceCollectWithSourceTimezoneResumesFromUTCCheckpoint(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-timezone-cursor-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()

	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES ('2026-05-18 10:55:00', 'first');
		INSERT INTO events (event_time, message) VALUES ('2026-05-18 11:00:00', 'second');
	`)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	raw, err := Create("sqlite", context.Background(), map[string]interface{}{
		"database":         tmpfile.Name(),
		"table":            "events",
		"timestamp_column": "event_time",
		"name":             "test",
		"fetch_size":       10,
		"source_timezone":  "Europe/Berlin",
	}, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	src := raw.(*SQLiteSource)
	defer src.Close()

	entries, cursor, err := src.Collect(context.Background(), "2026-05-18T08:55:00Z")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 resumed entry, got %d", len(entries))
	}
	if got, want := entries[0].Fields["message"], "second"; got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
	if got, want := cursor, "v1:2026-05-18T09:00:00Z"; got != want {
		t.Fatalf("expected cursor %q, got %q", want, got)
	}
}

func TestSQLiteSourceCursorColumnHandlesDuplicateTimestamps(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-duplicates-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:00:00', 'First event');
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:00:00', 'Second event');
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:00:00', 'Third event');
	`)
	sqlDB.Close()
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	src, err := NewSQLiteSourceWithCursor(
		SQLiteAuth{Database: tmpfile.Name()},
		"events",
		"",
		"event_time",
		"id",
		"test",
		2,
		"",
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("NewSQLiteSource failed: %v", err)
	}
	defer src.Close()

	entries1, cursor1, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("first collect failed: %v", err)
	}
	if len(entries1) != 2 {
		t.Fatalf("expected first page of 2 entries, got %d", len(entries1))
	}

	entries2, _, err := src.Collect(context.Background(), cursor1)
	if err != nil {
		t.Fatalf("second collect failed: %v", err)
	}
	if len(entries2) != 1 || entries2[0].Fields["message"] != "Third event" {
		t.Fatalf("expected remaining duplicate-timestamp row, got %+v", entries2)
	}
	if entries2[0].Cursor == "" {
		t.Fatal("expected cursor metadata on duplicate-timestamp row")
	}
}

func TestSQLiteSourceConstructorValidation(t *testing.T) {
	tests := []struct {
		name         string
		auth         SQLiteAuth
		table        string
		query        string
		timestampCol string
		cursorCol    string
		sourceName   string
		batchSize    int
		wantErr      string
	}{
		{
			name:         "empty database",
			auth:         SQLiteAuth{Database: ""},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "sqlite source: database is required",
		},
		{
			name:         "empty query with empty table",
			auth:         SQLiteAuth{Database: "/tmp/test.db"},
			table:        "",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "sqlite source: table is required when query is empty",
		},
		{
			name:         "empty query with empty timestamp_column",
			auth:         SQLiteAuth{Database: "/tmp/test.db"},
			table:        "events",
			query:        "",
			timestampCol: "",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "sqlite source: timestamp_column is required when query is empty",
		},
		{
			name:         "empty query with empty table and timestamp_column",
			auth:         SQLiteAuth{Database: "/tmp/test.db"},
			table:        "",
			query:        "",
			timestampCol: "",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "sqlite source: table is required when query is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSQLiteSourceWithCursor(tt.auth, tt.table, tt.query, tt.timestampCol, tt.cursorCol, tt.sourceName, tt.batchSize, "", "", nil)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestSQLiteSourceSourceTimezoneNormalizesNaiveTimestamp(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-timezone-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()

	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES ('2026-05-18 10:55:00', 'berlin-local');
	`)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	raw, err := Create("sqlite", context.Background(), map[string]interface{}{
		"database":         tmpfile.Name(),
		"table":            "events",
		"timestamp_column": "event_time",
		"name":             "test",
		"fetch_size":       10,
		"source_timezone":  "Europe/Berlin",
	}, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	src := raw.(*SQLiteSource)
	defer src.Close()

	entries, cursor, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if got, want := entries[0].Timestamp.UTC().Format(time.RFC3339), "2026-05-18T08:55:00Z"; got != want {
		t.Fatalf("expected normalized timestamp %q, got %q", want, got)
	}
	if got, want := entries[0].Fields["event_time"], "2026-05-18T08:55:00Z"; got != want {
		t.Fatalf("expected normalized field timestamp %q, got %q", want, got)
	}
	if cursor != "v1:2026-05-18T08:55:00Z" {
		t.Fatalf("expected cursor to use normalized UTC timestamp, got %q", cursor)
	}
}

func TestSQLiteSourceColumnsAllowlist(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test-sqlite-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	sqlDB, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()

	_, err = sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL,
			sensitive_field TEXT NOT NULL
		);
		INSERT INTO events (event_time, message, sensitive_field) VALUES ('2024-01-01 10:00:00', 'First event', 'secret123');
	`)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// 1. Invalid columns are rejected
	_, err = NewSQLiteSourceWithCursor(
		SQLiteAuth{Database: tmpfile.Name()},
		"events",
		"",
		"event_time",
		"",
		"test",
		10,
		"",
		"",
		[]string{"message", "id; DROP TABLE events"},
	)
	if err == nil {
		t.Error("expected error with invalid column identifier, got nil")
	}

	// 2. Valid columns projection works
	src, err := NewSQLiteSourceWithCursor(
		SQLiteAuth{Database: tmpfile.Name()},
		"events",
		"",
		"event_time",
		"",
		"test",
		10,
		"",
		"",
		[]string{"event_time", "message"},
	)
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	entries, _, err := src.Collect(context.Background(), "")
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Should have projected event_time and message
	if entries[0].Fields["message"] != "First event" {
		t.Errorf("expected 'message' to be 'First event', got %q", entries[0].Fields["message"])
	}
	// Should NOT have sensitive_field in Fields
	if val, ok := entries[0].Fields["sensitive_field"]; ok {
		t.Errorf("expected 'sensitive_field' to be omitted, but got %q", val)
	}
}

func newEventsSource(t *testing.T) core.Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	sqlDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()
	if _, err := sqlDB.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_time TEXT NOT NULL,
			message TEXT NOT NULL
		);
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:00:00', 'First event');
		INSERT INTO events (event_time, message) VALUES ('2024-01-01 10:01:00', 'Second event');
	`); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	// No cursor column: the cursor is the bare timestamp.
	src, err := NewSQLiteSource(SQLiteAuth{Database: path}, "events", "", "event_time", "test", 10)
	if err != nil {
		t.Fatalf("NewSQLiteSource: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	return src
}

// A timestamp-only cursor must resume where it left off. It used to be
// parsed as an empty cursor, so every poll re-read the whole table.
func TestSQLiteSourceResumesFromTimestampOnlyCursor(t *testing.T) {
	src := newEventsSource(t)
	entries, cursor, err := src.Collect(context.Background(), "")
	if err != nil || len(entries) != 2 {
		t.Fatalf("first Collect = %d entries, %v; want 2", len(entries), err)
	}
	if !strings.HasPrefix(cursor, "v1:") || strings.Contains(cursor, "{") {
		t.Fatalf("cursor = %q, want a v1 timestamp-only cursor", cursor)
	}

	again, next, err := src.Collect(context.Background(), cursor)
	if err != nil {
		t.Fatalf("second Collect: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second Collect returned %d entries, want 0 (nothing new since the cursor)", len(again))
	}
	if next != cursor {
		t.Errorf("cursor after an empty poll = %q, want it unchanged (%q)", next, cursor)
	}
}

func TestSQLiteSourceRejectsCorruptCursor(t *testing.T) {
	src := newEventsSource(t)
	for _, bad := range []string{`v1:{"timestamp":`, `v9:2024-01-01 10:00:00`} {
		entries, next, err := src.Collect(context.Background(), bad)
		if !errors.Is(err, errInvalidCursor) {
			t.Errorf("Collect(%q) = %d entries, %q, %v; want errInvalidCursor", bad, len(entries), next, err)
			continue
		}
		if !strings.Contains(err.Error(), "delete it to start from the beginning") {
			t.Errorf("error %q does not tell the operator how to recover", err)
		}
	}
}

// newSQLiteTable creates a database with the given schema and rows, and a
// source over the events table with event_time as the timestamp column.
func newSQLiteTable(t *testing.T, schema string, cursorColumn string, fetchSize int, rows ...[]interface{}) *SQLiteSource {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	sqlDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()
	if _, err := sqlDB.Exec(schema); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	for _, row := range rows {
		if _, err := sqlDB.Exec(`INSERT INTO events (event_time, message) VALUES (?, ?)`, row...); err != nil {
			t.Fatalf("Failed to insert %v: %v", row, err)
		}
	}
	src, err := NewSQLiteSourceWithCursor(SQLiteAuth{Database: path}, "events", "", "event_time", cursorColumn, "test", fetchSize, "", "", nil)
	if err != nil {
		t.Fatalf("NewSQLiteSourceWithCursor: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	return src
}

// collectAll polls from cursor until a poll returns nothing, and returns
// the messages in delivery order.
func collectAll(t *testing.T, src core.Source, cursor string) []string {
	t.Helper()
	var got []string
	for i := 0; i < 20; i++ {
		entries, next, err := src.Collect(context.Background(), cursor)
		if err != nil {
			t.Fatalf("Collect(%q): %v", cursor, err)
		}
		if len(entries) == 0 {
			return got
		}
		for _, e := range entries {
			got = append(got, e.Fields["message"])
		}
		cursor = next
	}
	t.Fatalf("source never drained; delivered so far: %v", got)
	return nil
}

func assertMessages(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("delivered %v, want %v", got, want)
	}
}

const textEventsSchema = `CREATE TABLE events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_time TEXT NOT NULL,
	message TEXT NOT NULL
)`

const datetimeEventsSchema = `CREATE TABLE events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_time DATETIME NOT NULL,
	message TEXT NOT NULL
)`

// A DATETIME column is read as time.Time, so the cursor is
// written as "...T...Z" while SQLite stores "... ...". Compared as text, a
// space sorts before 'T', and later rows on the cursor's date were skipped.
func TestSQLiteSourceSpaceFormattedRowsAfterTFormattedCursor(t *testing.T) {
	src := newSQLiteTable(t, datetimeEventsSchema, "", 1,
		[]interface{}{"2024-01-01 10:00:00", "first"},
		[]interface{}{"2024-01-01 11:00:00", "second"},
		[]interface{}{"2024-01-02 09:00:00", "third"},
	)
	assertMessages(t, collectAll(t, src, ""), "first", "second", "third")
}

// "10:00:00Z" sorts after "10:00:00.5Z" as text ('Z' > '.').
func TestSQLiteSourceFractionalSecondsAfterWholeSecondCursor(t *testing.T) {
	src := newSQLiteTable(t, textEventsSchema, "", 10,
		[]interface{}{"2024-01-01T10:00:00Z", "at cursor"},
		[]interface{}{"2024-01-01T10:00:00.5Z", "half second later"},
	)
	assertMessages(t, collectAll(t, src, "v1:2024-01-01T10:00:00Z"), "half second later")
}

// julianday resolves only milliseconds. Rows inside one millisecond must
// each be delivered once, without and with a cursor column.
func TestSQLiteSourceSubMillisecondRows(t *testing.T) {
	rows := [][]interface{}{
		{"2024-01-01 10:00:00.0001", "a"},
		{"2024-01-01 10:00:00.0002", "b"},
		{"2024-01-01 10:00:00.0003", "c"},
	}
	for _, cursorColumn := range []string{"", "id"} {
		t.Run("cursor_column="+cursorColumn, func(t *testing.T) {
			src := newSQLiteTable(t, textEventsSchema, cursorColumn, 1, rows...)
			assertMessages(t, collectAll(t, src, ""), "a", "b", "c")
		})
	}
}

// go-sqlite3 writes time.Time with nanoseconds, while the cursor keeps
// 100ns. The last row must not be re-sent forever because its cursor was
// truncated.
func TestSQLiteSourceNanosecondTimesFromGo(t *testing.T) {
	base := time.Date(2024, 1, 1, 10, 0, 0, 123456789, time.UTC)
	src := newSQLiteTable(t, datetimeEventsSchema, "", 1,
		[]interface{}{base, "a"},
		[]interface{}{base.Add(150 * time.Nanosecond), "b"},
	)
	assertMessages(t, collectAll(t, src, ""), "a", "b")
}

// Integer epoch columns are not dates to julianday; they keep the numeric
// comparison.
func TestSQLiteSourceIntegerEpochTimestamps(t *testing.T) {
	src := newSQLiteTable(t, `CREATE TABLE events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_time INTEGER NOT NULL,
		message TEXT NOT NULL
	)`, "", 1,
		[]interface{}{1700000000, "first"},
		[]interface{}{1700000060, "second"},
		[]interface{}{1700000120, "third"},
	)
	assertMessages(t, collectAll(t, src, "v1:1700000000"), "second", "third")
}

// Past maxRowsAtCursor rows at the cursor, Collect fails and says how to
// fix it rather than stalling silently.
func TestSQLiteSourceTooManyRowsAtCursorFails(t *testing.T) {
	old := maxRowsAtCursor
	maxRowsAtCursor = 3
	t.Cleanup(func() { maxRowsAtCursor = old })
	src := newSQLiteTable(t, textEventsSchema, "", 1,
		[]interface{}{"2024-01-01 10:00:00", "a"},
		[]interface{}{"2024-01-01 10:00:00", "b"},
		[]interface{}{"2024-01-01 10:00:00", "c"},
		[]interface{}{"2024-01-01 10:00:00", "d"},
		[]interface{}{"2024-01-01 10:00:00", "e"},
	)
	_, _, err := src.Collect(context.Background(), "v1:2024-01-01 10:00:00")
	if err == nil || !strings.Contains(err.Error(), "configure cursor_column") {
		t.Fatalf("Collect error = %v, want one naming cursor_column", err)
	}
}
