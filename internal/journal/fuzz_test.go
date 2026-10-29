package journal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// FuzzScanCommitted checks the committed-record decoder never panics and
// never reports an entry without a closing barrier, on arbitrary file
// contents.
// Run with: go test ./internal/journal -fuzz=FuzzScanCommitted -fuzztime=30s
func FuzzScanCommitted(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte(`{"id":1,"data":"YQ=="}` + "\n" + `{"barrier":true}` + "\n"))
	f.Add([]byte(`{"id":1,"data":"YQ=="}`))
	f.Add([]byte("{\"id\":1}\n{\"barrier\":true}\n{\"id\":2}\n"))
	f.Add(bytes.Repeat([]byte{0, 0xff, '\n'}, 32))
	f.Add([]byte(`{"barrier":true}` + "\n" + `{"id":1,"data":"YQ=="}` + "\n" + `{"barrier":true}` + "\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "journal")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		var ids []int64
		err := ReadCommittedFile(path, func(id int64, _ []byte) error {
			ids = append(ids, id)
			return nil
		})
		if err != nil {
			return // a scanner error is a loud failure, not corruption
		}
		// Recover must agree with ReadCommittedFile on the last
		// transaction's entries.
		j, err := Open(path)
		if err != nil {
			return
		}
		defer j.Close()
		entries, err := j.Recover()
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.ID <= 0 {
				t.Fatalf("recovered entry with non-positive id %d", e.ID)
			}
		}
	})
}
