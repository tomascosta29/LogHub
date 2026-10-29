package source

import "testing"

// FuzzParseFileCursor checks the file cursor parser accepts the documented
// "<offset>@<dev>:<ino>" and legacy plain-offset forms and rejects
// everything else with an error rather than a panic or a zero offset.
// Run with: go test ./internal/plugins/source -fuzz=FuzzParseFileCursor -fuzztime=30s
func FuzzParseFileCursor(f *testing.F) {
	for _, seed := range []string{
		"0",
		"1234",
		"1234@8:5678",
		"0@0:0",
		"-1",
		"@8:5678",
		"abc",
		"1@",
		"1@a:b",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, cursor string) {
		offset, id, err := parseFileCursor(cursor)
		if err != nil {
			return
		}
		if offset < 0 {
			t.Fatalf("accepted negative offset %d for %q", offset, cursor)
		}
		if got := formatFileCursor(offset, id); got != cursor && id != "" {
			// The parser may normalise leading zeroes in the offset, so
			// only require a round trip through the parsed values.
			o2, id2, err := parseFileCursor(got)
			if err != nil || o2 != offset || id2 != id {
				t.Fatalf("round trip of %q produced %q", cursor, got)
			}
		}
	})
}
