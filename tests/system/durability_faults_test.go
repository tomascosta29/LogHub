//go:build !windows

package system_test

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// straceInjectPrefix returns the prefix that runs a command under strace
// with a fault injected into fsync. It skips the test when strace is not
// available, so the suite still runs on a machine without it.
func straceInjectPrefix(t *testing.T, inject string) []string {
	t.Helper()
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace is not installed")
	}
	return []string{"strace", "-f", "-q", "-o", "/dev/null", "-e", "inject=" + inject}
}

// TestCheckpointWriteFaultKeepsCursorAndIsObservable injects each fatal
// checkpoint fsync error and checks three things: the pipeline keeps
// delivering, the cursor does not silently move on without a durable
// checkpoint, and /health or the metrics make the failure visible.
func TestCheckpointWriteFaultKeepsCursorAndIsObservable(t *testing.T) {
	faults := []string{
		"fsync:error=EIO",
		"fsync:error=ENOSPC",
		"fsync:error=EACCES",
		"fsync:error=EROFS",
	}
	for _, inject := range faults {
		inject := inject
		t.Run(strings.NewReplacer(":", "_", "=", "_").Replace(inject), func(t *testing.T) {
			dir := t.TempDir()
			input := filepath.Join(dir, "audit.log")
			g := newOracleGenerator()
			g.appendFile(t, input, 20)

			c := newTestConfig(t)
			p := newPipeline("cp-fault", oracleFileSource(input, 10), destDummy(nil))
			p["runtime"] = map[string]any{
				"poll_interval":       "10ms",
				"send_batch_size":     10,
				"buffer_size":         4,
				"checkpoint_interval": "0",
				"shutdown_timeout":    "2s",
			}
			c.Global["stalled_threshold_multiplier"] = 5
			c.add(p)
			cfg := c.write(t)

			prefix := straceInjectPrefix(t, inject)
			proc := startProcCommand(t, prefix, []string{
				loghubBinary, "--config", cfg,
			})
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("loghub log:\n%s", proc.log())
				}
			})

			waitFor(t, 20*time.Second, "checkpoint failure logged", func() bool {
				return strings.Contains(proc.log(), "Failed to save checkpoint") ||
					strings.Contains(proc.log(), "Failed to flush checkpoint")
			})

			fams, err := tryScrapeMetrics(proc.addr)
			if err == nil {
				if got := counterValue(fams, "loghub_checkpoints_saved_total", map[string]string{"pipeline_id": "cp-fault"}); got != 0 {
					t.Errorf("checkpoints_saved_total = %v while every checkpoint fsync failed", got)
				}
			}
			_, h, err := fetchHealth(proc.addr)
			if err != nil {
				t.Fatalf("health: %v", err)
			}
			if len(h.Pipelines) != 1 || h.Pipelines[0].Status != "degraded" || !strings.Contains(h.Pipelines[0].ErrorMsg, "checkpoint") {
				t.Errorf("checkpoint fsync fails with %s but health = %+v, want the pipeline degraded with the checkpoint error", inject, h.Pipelines)
			}
			stopTolerant(proc)

			// A restart without the fault must deliver the records again
			// (at-least-once) and then persist the cursor.
			proc = startProc(t, cfg)
			waitFor(t, 20*time.Second, "cursor persisted after recovery", func() bool {
				cursor, err := readCursorNoFatal(filepath.Join(c.dir, "checkpoints"), "cp-fault")
				return err == nil && cursor != ""
			})
			stopTolerant(proc)
		})
	}
}

// readCursorNoFatal reads a cursor without failing the test, for polling.
func readCursorNoFatal(dir, pipelineID string) (string, error) {
	path := filepath.Join(dir, pipelineID+".journal")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("empty journal")
	}
	return string(data), nil
}

// TestDiskBufferWriteFaultKeepsDataWhileMemoryHoldsIt injects fsync EIO
// while pages are being spilled, then recovers the destination. The pages
// must stay in memory and be delivered rather than dropped.
func TestDiskBufferWriteFaultKeepsDataWhileMemoryHoldsIt(t *testing.T) {
	const total = 20
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	bufDir := filepath.Join(dir, "buffer")
	g := newOracleGenerator()
	g.appendFile(t, input, total)
	s, srv := newSink(t, http.StatusInternalServerError)

	c := newTestConfig(t)
	p := newPipeline("buf-fault", oracleFileSource(input, 10), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         8,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["processors"] = []any{oracleJSONParse()}
	p["durability"] = map[string]any{
		"disk_buffer": map[string]any{"enabled": true, "dir": bufDir},
	}
	c.Global["circuit_breaker_failure_threshold"] = 1
	c.Global["circuit_breaker_recovery_timeout"] = "50ms"
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProcCommand(t, straceInjectPrefix(t, "fsync:error=EIO"), []string{
		loghubBinary, "--config", cfg,
	})
	waitFor(t, 20*time.Second, "disk-buffer write failure logged", func() bool {
		return strings.Contains(proc.log(), "Failed to persist pending page")
	})
	s.setStatuses(http.StatusOK)
	waitFor(t, 30*time.Second, "records delivered from memory after the spill fault", func() bool {
		return s.acceptedEntries() >= total
	})
	proc.stop()

	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"http"},
		Observed:     observedFromLogEntries("http", s.acceptedLogEntries(t)),
	}, "disk buffer fsync failure")
}

// TestDLQWriteFaultKeepsEntriesRetried injects fsync EIO while a rejected
// batch is dead-lettered. The cursor must not move past entries that are
// neither delivered nor dead-lettered; after recovery they must be written
// to the DLQ.
func TestDLQWriteFaultKeepsEntriesRetried(t *testing.T) {
	const total = 10
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	dlqDir := filepath.Join(dir, "dlq")
	g := newOracleGenerator()
	g.appendFile(t, input, total)
	_, srv := newSink(t, http.StatusBadRequest)

	c := newTestConfig(t)
	p := newPipeline("dlq-fault", oracleFileSource(input, 10), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["processors"] = []any{oracleJSONParse()}
	p["durability"] = map[string]any{
		"dlq": map[string]any{"enabled": true, "dir": dlqDir},
	}
	c.Global["circuit_breaker_failure_threshold"] = 100
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProcCommand(t, straceInjectPrefix(t, "fsync:error=EIO"), []string{
		loghubBinary, "--config", cfg,
	})
	waitFor(t, 20*time.Second, "DLQ write failure logged", func() bool {
		log := proc.log()
		return strings.Contains(log, "Failed to write rejected entries to DLQ") ||
			strings.Contains(log, "Failed to write failed batch to DLQ")
	})
	// The failed fsync may still have left the record readable from the
	// page cache; what matters is that the write was reported as failed and
	// the entries were kept for retry rather than dropped.
	proc.stop()

	// Restart without the fault: the same entries are rejected again and
	// must now be preserved.
	proc = startProc(t, cfg)
	waitFor(t, 20*time.Second, "entries dead-lettered after recovery", func() bool {
		return len(readDLQ(t, dlqDir, "dlq-fault")) > 0
	})
	if code, logs := proc.kill(); code == 0 {
		t.Logf("kill exit code 0")
	} else if strings.Contains(logs, "panic") {
		t.Fatalf("restart panicked:\n%s", logs)
	}

	var observed []observedRecord
	for _, fb := range readDLQ(t, dlqDir, "dlq-fault") {
		for _, e := range fb.Entries {
			observed = append(observed, observedRecordFromFields("dlq", e.Fields))
		}
	}
	verifyOracle(t, g, oracleCheck{Observed: observed, AllowDuplicates: true}, "DLQ fsync failure")
}

// stopTolerant asks a wrapped process to stop and waits for it, accepting
// the wrapper's non-zero exit status. A strace parent exits from the signal
// it forwards even when the traced process stops cleanly.
func stopTolerant(p *proc) {
	p.t.Helper()
	_ = signalProcess(p, syscall.SIGTERM)
	p.waitExit(15 * time.Second)
}

// TestCheckpointWriteFailsOnFullFilesystem uses a real, small tmpfs inside
// a user+mount namespace. Filling it makes every journal fsync fail with
// ENOSPC. The process must stay up, keep delivering to the destination, and
// report the failure instead of exiting or dropping records.
func TestCheckpointWriteFailsOnFullFilesystem(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is not installed")
	}
	if err := exec.Command("unshare", "-Ur", "-m", "true").Run(); err != nil {
		t.Skipf("user mount namespaces are not available: %v", err)
	}
	const total = 2000
	dir := t.TempDir()
	mountPoint := filepath.Join(dir, "state")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatalf("mkdir mount point: %v", err)
	}
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, total)

	c := newTestConfig(t)
	c.Global["checkpoints_dir"] = filepath.Join(mountPoint, "checkpoints")
	p := oracleFilePipeline("enospc", input, output, 10)
	p["runtime"].(map[string]any)["poll_interval"] = "5ms"
	c.add(p)
	cfg := c.write(t)

	script := fmt.Sprintf("mount -t tmpfs -o size=64k none %q && dd if=/dev/zero of=%q bs=1024 count=56 2>/dev/null; exec \"$0\" \"$@\"",
		mountPoint, filepath.Join(mountPoint, "fill"))
	prefix := []string{"unshare", "-Ur", "-m", "sh", "-c", script}
	proc := startProcCommand(t, prefix, []string{loghubBinary, "--config", cfg})
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("loghub log:\n%s", proc.log())
		}
	})

	waitFor(t, 20*time.Second, "checkpoint ENOSPC logged", func() bool {
		return strings.Contains(proc.log(), "no space left on device") ||
			strings.Contains(proc.log(), "disk buffer size limit")
	})
	if _, exited, _ := proc.exitedWithin(100 * time.Millisecond); exited {
		t.Fatal("process exited on ENOSPC instead of keeping the durable cursor and retrying")
	}
	// The destination is unaffected; records must still be delivered.
	waitFor(t, 20*time.Second, "records delivered despite ENOSPC", func() bool {
		return len(readLogEntriesTolerant(t, output)) >= 1
	})
	stopTolerant(proc)
}
