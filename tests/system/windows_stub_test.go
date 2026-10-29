//go:build windows

package system_test

import "os/exec"

// The system suite drives real processes with Unix signals and inspects
// /proc, so it is not built on Windows. The unit and local end-to-end tests
// still run there.
type proc struct {
	cmd *exec.Cmd
}
