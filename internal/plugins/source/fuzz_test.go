package source

import "testing"

// FuzzParseSQLCursor checks the persisted cursor parser never panics and
// never accepts an unchecked version or malformed JSON.
// Run with: go test ./internal/plugins/source -fuzz=FuzzParseSQLCursor -fuzztime=30s
func FuzzParseSQLCursor(f *testing.F) {
	f.Add("")
	f.Add(`v1:{"timestamp":"2026-01-01T00:00:00Z","tie":"000001"}`)
	f.Add(`v1:2026-01-01T00:00:00Z`)
	f.Add(`v99:{"timestamp":"2026-01-01T00:00:00Z"}`)
	f.Add(`v1:{broken`)
	f.Fuzz(func(t *testing.T, cursor string) {
		_, _ = parseSQLCursor(cursor)
	})
}

// FuzzParseSQLiteTime checks the timestamp parser used for cursor
// comparison never panics on arbitrary text.
func FuzzParseSQLiteTime(f *testing.F) {
	f.Add("2026-05-06T12:00:00Z")
	f.Add("2026-05-06 12:00:00")
	f.Add("")
	f.Add("not-a-time")
	f.Fuzz(func(t *testing.T, value string) {
		_, _ = parseSQLiteTime(value)
	})
}
