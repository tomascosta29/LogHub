//go:build windows

package source

import "os"

// fileID returns "" on Windows, where os.FileInfo does not expose a file
// index. Cursors then hold only the offset, and rotation is detected
// with os.SameFile while LogHub runs.
func fileID(fi os.FileInfo) string {
	return ""
}
