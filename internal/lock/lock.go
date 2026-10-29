// Package lock provides file-based advisory locking for the LogHub
// checkpointer. The lock is acquired at startup and released on shutdown.
// A crashed process's lock is automatically released by the OS when the
// file descriptor is closed (this is a property of flock(2) on Unix).
//
// The lock is currently Unix-only. On Windows, Acquire returns a
// non-blocking no-op lock; production deployments are expected to use
// Linux (the primary deployment target is Azure Container Apps / AKS).
package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileLock is an advisory exclusive lock on a file. The lock is acquired
// via flock(2) on Unix; see the package doc for the Windows behavior.
type FileLock struct {
	path string
	f    *os.File
	// dir holds a lock on the lock file's directory where the platform
	// supports it. The file lock alone follows the inode, so deleting the
	// lock file would let a second process lock a new file at the same
	// path; the directory cannot be swapped out from under a running
	// process the same way.
	dir  *os.File
	once sync.Once
}

// Acquire creates the lock file (and parent directory if needed) and
// tries to acquire an exclusive, non-blocking lock. If the lock is held
// by another process, returns a non-nil error describing the holder (the
// OS-level holder PID is not directly available from flock, so the
// error includes a hint to check `lsof` or `fuser`).
func Acquire(path string) (*FileLock, error) {
	if path == "" {
		return nil, fmt.Errorf("lock: path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("lock: failed to create directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|openNoFollow, 0o600)
	if err != nil {
		return nil, fmt.Errorf("lock: failed to open %q: %w", path, err)
	}
	if err := flock(f, true); err != nil {
		// Leave the file in place: it is the holder's lock. Removing it
		// would let the next process lock a new file at the same path
		// while the holder still runs.
		f.Close()
		return nil, fmt.Errorf("lock: %q is held by another process: %w", path, err)
	}
	dir, err := lockDir(filepath.Dir(path))
	if err != nil {
		_ = unflock(f)
		f.Close()
		return nil, fmt.Errorf("lock: directory of %q is held by another process: %w", path, err)
	}
	return &FileLock{path: path, f: f, dir: dir}, nil
}

// TryAcquire is the same as Acquire but does not create the lock file if
// it does not exist. Useful when the lock is meant to be created by the
// first process to start.
func TryAcquire(path string) (*FileLock, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("lock: %q does not exist", path)
	}
	return Acquire(path)
}

// Release releases the lock and closes the underlying file. Safe to call
// multiple times.
func (l *FileLock) Release() error {
	if l == nil {
		return nil
	}
	var err error
	l.once.Do(func() {
		if l.dir != nil {
			_ = unflock(l.dir)
			l.dir.Close()
			l.dir = nil
		}
		if l.f != nil {
			_ = unflock(l.f)
			err = l.f.Close()
			l.f = nil
		}
	})
	return err
}

// Path returns the lock file path.
func (l *FileLock) Path() string {
	return l.path
}
