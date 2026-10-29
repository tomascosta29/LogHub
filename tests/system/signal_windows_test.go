//go:build windows

package system_test

import "syscall"

// signalProcess is the Windows fallback. Windows has no process groups, so
// the traced process is signalled directly.
func signalProcess(p *proc, sig syscall.Signal) error {
	return p.cmd.Process.Signal(sig)
}

func killProcess(p *proc) error {
	return p.cmd.Process.Kill()
}
