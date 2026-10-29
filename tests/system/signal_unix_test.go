//go:build !windows

package system_test

import "syscall"

// signalProcess sends sig to the process group started by
// startProcCommand, so a wrapper such as strace and the process it traces
// are signalled together.
func signalProcess(p *proc, sig syscall.Signal) error {
	return syscall.Kill(-p.cmd.Process.Pid, sig)
}

func killProcess(p *proc) error {
	return syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
}
