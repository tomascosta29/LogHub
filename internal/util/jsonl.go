package util

import (
	"bufio"
	"bytes"
	"errors"
	"os"
)

// ErrBadLine is returned (wrapped) by an ImportJSONL callback for a line
// it cannot parse. Such lines are counted as skipped instead of failing
// the import; a v0.1.0 file can end in a partial line after a crash.
var ErrBadLine = errors.New("unreadable line")

// ImportJSONL calls fn for each non-empty line of the JSONL file at path,
// then renames the file with a ".migrated" suffix so it is imported only
// once. A missing file imports nothing. Any error from fn other than
// ErrBadLine stops the import and leaves the file in place.
func ImportJSONL(path string, fn func(line []byte) error) (imported, skipped int, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := fn(line); err != nil {
			if errors.Is(err, ErrBadLine) {
				skipped++
				continue
			}
			f.Close()
			return imported, skipped, err
		}
		imported++
	}
	scanErr := scanner.Err()
	f.Close()
	if scanErr != nil {
		return imported, skipped, scanErr
	}
	return imported, skipped, os.Rename(path, path+".migrated")
}
