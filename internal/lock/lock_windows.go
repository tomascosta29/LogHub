//go:build windows

package lock

import (
	"errors"
	"os"
	"sync"
	"time"
)

// Windows does not support flock(2). The fallback is a best-effort
// in-process lock combined with a sidecar file whose mtime is used as a
// soft TTL. This is not safe across hosts, but LogHub is targeted at
// Linux deployments. Production on Windows is unsupported.

var (
	windowsMu   sync.Mutex
	windowsHeld = make(map[string]time.Time)
)

var errAlreadyLocked = errors.New("lock: already held")

const openNoFollow = 0

// lockDir is a no-op: Windows does not lock directories, and an open file
// cannot be deleted there anyway.
func lockDir(string) (*os.File, error) { return nil, nil }

func flock(f *os.File, nonBlocking bool) error {
	windowsMu.Lock()
	defer windowsMu.Unlock()
	held, ok := windowsHeld[f.Name()]
	if ok && time.Since(held) < 24*time.Hour {
		return errAlreadyLocked
	}
	windowsHeld[f.Name()] = time.Now()
	return nil
}

func unflock(f *os.File) error {
	windowsMu.Lock()
	defer windowsMu.Unlock()
	delete(windowsHeld, f.Name())
	return nil
}
