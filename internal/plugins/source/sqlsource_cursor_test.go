package source

import (
	"errors"
	"testing"
	"time"
)

func TestParseSQLCursor(t *testing.T) {
	valid := []struct {
		name, in string
		want     sqlCursor
	}{
		{"empty means no checkpoint", "", sqlCursor{}},
		{"v1 with tie breaker", `v1:{"timestamp":"2026-05-06T12:00:00Z","tie":"000123"}`, sqlCursor{Timestamp: "2026-05-06T12:00:00Z", Tie: "000123"}},
		{"v1 timestamp only", "v1:2026-05-06T12:00:00Z", sqlCursor{Timestamp: "2026-05-06T12:00:00Z"}},
		{"v1 source-local timestamp", "v1:2026-05-06 12:00:00", sqlCursor{Timestamp: "2026-05-06 12:00:00"}},
		{"v0 JSON", `{"timestamp":"2026-05-06T12:00:00Z","tie":"000123"}`, sqlCursor{Timestamp: "2026-05-06T12:00:00Z", Tie: "000123"}},
		{"v0 timestamp only", "2026-05-06T12:00:00Z", sqlCursor{Timestamp: "2026-05-06T12:00:00Z"}},
	}
	for _, tc := range valid {
		got, err := parseSQLCursor(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("%s: parseSQLCursor(%q) = %+v, %v; want %+v", tc.name, tc.in, got, err, tc.want)
		}
	}

	invalid := []struct{ name, in string }{
		{"v1 corrupt JSON", `v1:{"timestamp":"2026-05-06T12:00:00Z","tie":`},
		{"v1 JSON without timestamp", `v1:{"timestamp":"","tie":"000123"}`},
		{"v1 empty body", "v1:"},
		{"unknown version", `v2:{"timestamp":"2026-05-06T12:00:00Z"}`},
		{"v0 corrupt JSON", `{"timestamp":`},
		{"v0 JSON without timestamp", `{"timestamp":"","tie":"000123"}`},
	}
	for _, tc := range invalid {
		if got, err := parseSQLCursor(tc.in); !errors.Is(err, errInvalidCursor) {
			t.Errorf("%s: parseSQLCursor(%q) = %+v, %v; want errInvalidCursor", tc.name, tc.in, got, err)
		}
	}
}

func TestSQLCursorRoundTrip(t *testing.T) {
	for _, original := range []sqlCursor{
		{Timestamp: "2026-05-06T12:00:00Z", Tie: "000123"},
		{Timestamp: "2026-05-06T12:00:00Z"},
	} {
		parsed, err := parseSQLCursor(formatSQLCursor(original))
		if err != nil || parsed != original {
			t.Errorf("round trip of %+v = %+v, %v", original, parsed, err)
		}
	}
}

func TestFormatSQLCursorWithTieBreaker(t *testing.T) {
	cursor := sqlCursor{Timestamp: "2026-05-06T12:00:00Z", Tie: "000123"}
	formatted := formatSQLCursor(cursor)
	expected := `v1:{"timestamp":"2026-05-06T12:00:00Z","tie":"000123"}`
	if formatted != expected {
		t.Errorf("expected %s, got %s", expected, formatted)
	}
}

func TestFormatSQLCursorWithoutTieBreaker(t *testing.T) {
	cursor := sqlCursor{Timestamp: "2026-05-06T12:00:00Z"}
	formatted := formatSQLCursor(cursor)
	if formatted != "v1:2026-05-06T12:00:00Z" {
		t.Errorf("expected timestamp-only string with v1 prefix, got %s", formatted)
	}
}

func TestFormatSQLCursorEmptyTie(t *testing.T) {
	cursor := sqlCursor{Timestamp: "2026-05-06T12:00:00Z", Tie: ""}
	formatted := formatSQLCursor(cursor)
	if formatted != "v1:2026-05-06T12:00:00Z" {
		t.Errorf("expected timestamp-only string for empty tie, got %s", formatted)
	}
}

func TestCursorForQueryConvertsUTCCheckpointToSourceLocalTime(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatalf("failed to load location: %v", err)
	}

	src := &sqlSource{sourceLocation: loc}
	got := src.cursorForQuery(sqlCursor{
		Timestamp: "2026-05-18T09:07:58Z",
		Tie:       "123",
	}, loc)

	if got.Timestamp != "2026-05-18 11:07:58" {
		t.Fatalf("expected local query timestamp, got %q", got.Timestamp)
	}
	if got.Tie != "123" {
		t.Fatalf("expected tie breaker to be preserved, got %q", got.Tie)
	}
}

// Sub-second timestamps must survive into the cursor. They used to be
// formatted with whole seconds, so `event_time > cursor` matched the
// rows of the cursor's last second again on every poll (seen live
// against Azure SQL with DATETIME2).
func TestTimestampsKeepSubSecondPrecision(t *testing.T) {
	ts := time.Date(2026, 10, 28, 19, 6, 13, 1234500, time.UTC) // .0012345, DATETIME2 precision

	if got := formatValue(ts); got != "2026-10-28T19:06:13.0012345Z" {
		t.Errorf("formatValue = %q, want 2026-10-28T19:06:13.0012345Z", got)
	}
	if got := formatValue(ts.Truncate(time.Second)); got != "2026-10-28T19:06:13Z" {
		t.Errorf("formatValue of a whole second = %q, want no fraction", got)
	}

	src := &sqlSource{timestampColumn: "event_time", cursorColumn: "id"}
	entry := src.rowToLogEntry([]interface{}{"a", ts}, []string{"id", "event_time"}, 1, nil)
	cursor, err := parseSQLCursor(entry.Cursor)
	if err != nil || cursor.Timestamp != "2026-10-28T19:06:13.0012345Z" {
		t.Errorf("entry cursor = %q (%+v, %v), want the sub-second timestamp", entry.Cursor, cursor, err)
	}

	loc, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	got := src.cursorForQuery(sqlCursor{Timestamp: "2026-10-28T19:06:13.0012345Z"}, loc)
	if got.Timestamp != "2026-10-28 20:06:13.0012345" {
		t.Errorf("source-local query timestamp = %q, want 2026-10-28 20:06:13.0012345", got.Timestamp)
	}
}
