//go:build !windows

package system_test

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestShutdownTimeoutExitsNonZero reproduces the roadmap item that a lossy
// (timed-out) shutdown must be distinguishable from a clean one by the exit
// status. The process currently logs the pipeline error and exits 0.
func TestShutdownTimeoutExitsNonZero(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 20)
	s, srv := newSink(t, 200)
	s.setDelay(5 * time.Second)

	c := newTestConfig(t)
	p := newPipeline("shutdown-timeout", oracleFileSource(input, 5), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval": "10ms", "send_batch_size": 5, "buffer_size": 4,
		"checkpoint_interval": "0", "shutdown_timeout": "1s",
	}
	p["retry"] = map[string]any{"max_attempts": 10, "base_backoff": "1s"}
	p["processors"] = []any{oracleJSONParse()}
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "shutdown-timeout")
	time.Sleep(100 * time.Millisecond)
	proc.signal(syscall.SIGTERM)
	code, exited, logs := proc.exitedWithin(15 * time.Second)
	if !exited {
		proc.kill()
		t.Fatal("process did not exit after SIGTERM")
	}
	if code == 0 {
		t.Fatalf("timed-out shutdown exited 0; an orchestrator cannot distinguish a lossy stop from a clean one\n%s", logs)
	}
}
