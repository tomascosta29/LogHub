//go:build !windows

package source

import (
	"fmt"
	"os"
	"syscall"
)

// fileID identifies a file by device and inode, which survive a rename.
// The file source stores it in its cursor to tell the file it was reading
// from a new file created at the same path.
func fileID(fi os.FileInfo) string {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%d:%d", st.Dev, st.Ino)
}
