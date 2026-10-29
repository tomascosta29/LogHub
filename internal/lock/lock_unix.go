//go:build !windows

package lock

import (
	"errors"
	"os"
	"syscall"
)

// flock applies flock(2) to the file. If nonBlocking is true, the call
// returns EWOULDBLOCK immediately when the lock is held by another
// process; otherwise it blocks until the lock is available.
func flock(f *os.File, nonBlocking bool) error {
	op := syscall.LOCK_EX
	if nonBlocking {
		op |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(f.Fd()), op); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return errAlreadyLocked
		}
		return err
	}
	return nil
}

func unflock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

var errAlreadyLocked = syscall.EWOULDBLOCK
