package source

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func sqliteProps(t *testing.T, extra map[string]interface{}) map[string]interface{} {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	src := newSQLiteTableAt(t, path)
	src.Close()
	props := map[string]interface{}{
		"database":         path,
		"table":            "events",
		"timestamp_column": "event_time",
		"name":             "test",
		"fetch_size":       10,
	}
	for k, v := range extra {
		props[k] = v
	}
	return props
}

// newSQLiteTableAt creates the events table in the database at path.
func newSQLiteTableAt(t *testing.T, path string) *SQLiteSource {
	t.Helper()
	src, err := NewSQLiteSource(SQLiteAuth{Database: path}, "events", "", "event_time", "test", 10)
	if err != nil {
		t.Fatalf("NewSQLiteSource: %v", err)
	}
	if err := src.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := src.db.Exec(textEventsSchema); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return src
}

// Max_open_conns was decoded but never applied; the pool was
// always one connection.
func TestSQLSourceMaxOpenConnsApplied(t *testing.T) {
	for _, tc := range []struct {
		props map[string]interface{}
		want  int
	}{
		{nil, 1},
		{map[string]interface{}{"max_open_conns": 3}, 3},
	} {
		raw, err := Create("sqlite", context.Background(), sqliteProps(t, tc.props), nil)
		if err != nil {
			t.Fatalf("Create(%v): %v", tc.props, err)
		}
		src := raw.(*SQLiteSource)
		if _, _, err := src.Collect(context.Background(), ""); err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if got := src.db.Stats().MaxOpenConnections; got != tc.want {
			t.Errorf("props %v: pool max open connections = %d, want %d", tc.props, got, tc.want)
		}
		src.Close()
	}
}

func TestSQLSourceNegativeMaxOpenConnsRejected(t *testing.T) {
	if _, err := Create("sqlite", context.Background(), sqliteProps(t, map[string]interface{}{"max_open_conns": -1}), nil); err == nil || !strings.Contains(err.Error(), "max_open_conns") {
		t.Errorf("sqlite Create = %v, want an error naming max_open_conns", err)
	}
	props := map[string]interface{}{
		"auth":             map[string]interface{}{"method": "sql", "user": "u", "password": "p"},
		"server":           "localhost",
		"database":         "db",
		"table":            "events",
		"timestamp_column": "event_time",
		"fetch_size":       10,
		"max_open_conns":   -2,
	}
	if _, err := Create("mssql", context.Background(), props, nil); err == nil || !strings.Contains(err.Error(), "max_open_conns") {
		t.Errorf("mssql Create = %v, want an error naming max_open_conns", err)
	}
}

func TestMSSQLMaxOpenConnsApplied(t *testing.T) {
	props := map[string]interface{}{
		"auth":             map[string]interface{}{"method": "sql", "user": "u", "password": "p"},
		"server":           "localhost",
		"database":         "db",
		"table":            "events",
		"timestamp_column": "event_time",
		"fetch_size":       10,
		"max_open_conns":   4,
	}
	raw, err := Create("mssql", context.Background(), props, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer raw.Close()
	if got := raw.(*MSSQLSource).maxOpenConns; got != 4 {
		t.Errorf("maxOpenConns = %d, want 4", got)
	}
}

// Wait_for_change never had an effect. Old configs still load,
// with a warning instead of silence.
func TestSQLSourceWaitForChangeWarns(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	raw, err := Create("sqlite", context.Background(), sqliteProps(t, map[string]interface{}{"wait_for_change": "30s"}), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	raw.Close()
	if !strings.Contains(logs.String(), "wait_for_change has no effect") {
		t.Errorf("no deprecation warning logged; logs: %s", logs.String())
	}
}
