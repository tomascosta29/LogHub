//go:build !windows

package journal

import (
	"os"
	"syscall"
)

// openNoFollow makes opening a journal fail when its path is a symlink,
// so a link planted in the state directory cannot redirect writes.
const openNoFollow = syscall.O_NOFOLLOW

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
