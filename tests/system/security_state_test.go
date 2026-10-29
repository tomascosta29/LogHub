package system_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestStateFilesAreNotWorldReadable checks the permissions LogHub creates
// for cursors, journals and lock files. State is not a secret, but it names
// source records and pipelines and should not be readable by every local
// user by default.
func TestStateFilesAreNotWorldReadable(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 5)

	c := newTestConfig(t)
	p := oracleFilePipeline("modes", input, filepath.Join(dir, "out.jsonl"), 5)
	p["durability"] = map[string]any{
		"disk_buffer": map[string]any{"enabled": true, "dir": filepath.Join(dir, "buffer")},
		"dlq":         map[string]any{"enabled": true, "dir": filepath.Join(dir, "dlq")},
	}
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "modes")
	waitForJSONLCount(t, filepath.Join(dir, "out.jsonl"), 5, 10*time.Second)
	proc.stop()

	check := func(path string, want os.FileMode) {
		t.Helper()
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s mode = %04o, want %04o", path, got, want)
		}
	}
	cpDir := filepath.Join(c.dir, "checkpoints")
	check(filepath.Join(cpDir, "modes.journal"), 0o600)
	check(filepath.Join(cpDir, ".lock"), 0o600)
	check(filepath.Join(dir, "buffer", "modes.buffer.journal"), 0o600)
	check(filepath.Join(dir, "dlq", "modes.dlq.journal"), 0o600)
	check(cpDir, 0o700)
}

// TestSymlinkedCheckpointDoesNotWriteOutsideItsDirectory pre-creates a
// symlink where the checkpoint journal belongs. Following it lets a local
// attacker redirect durable state writes to any file the process can open.
func TestSymlinkedCheckpointDoesNotWriteOutsideItsDirectory(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("do not touch\n"), 0o600); err != nil {
		t.Fatalf("write victim: %v", err)
	}
	g := newOracleGenerator()
	g.appendFile(t, input, 5)

	c := newTestConfig(t)
	p := oracleFilePipeline("symlink", input, filepath.Join(dir, "out.jsonl"), 5)
	c.add(p)
	cfg := c.write(t)
	cpDir := filepath.Join(c.dir, "checkpoints")
	if err := os.MkdirAll(cpDir, 0o755); err != nil {
		t.Fatalf("mkdir checkpoints: %v", err)
	}
	if err := os.Symlink(victim, filepath.Join(cpDir, "symlink.journal")); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}

	proc := startProc(t, cfg)
	code, exited, _ := proc.exitedWithin(1500 * time.Millisecond)
	if !exited {
		time.Sleep(300 * time.Millisecond)
		proc.stop()
	} else if code == 0 {
		t.Fatal("process exited with status 0")
	}

	after, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("read victim: %v", err)
	}
	if string(after) != "do not touch\n" {
		t.Fatalf("checkpoint journal followed a symlink and appended to %s:\n%s", victim, after)
	}
}

// TestHeaderInjectionIsNeverSent gives the HTTP destination a header value
// containing CRLF. The client must reject it (and preserve the batch for the
// DLQ) rather than emit an injected header on the wire.
func TestHeaderInjectionIsNeverSent(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 3)
	var injected bool
	srv := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Injected") != "" {
			injected = true
		}
		w.WriteHeader(http.StatusOK)
	})

	c := newTestConfig(t)
	p := newPipeline("header-injection", oracleFileSource(input, 3), map[string]any{
		"type": "http",
		"http": map[string]any{
			"url":     srv,
			"timeout": "500ms",
			"headers": map[string]any{"X-Test": "value\r\nX-Injected: yes"},
		},
	})
	p["runtime"] = map[string]any{
		"poll_interval": "10ms", "send_batch_size": 3, "buffer_size": 4,
		"checkpoint_interval": "0", "shutdown_timeout": "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["processors"] = []any{oracleJSONParse()}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "header-injection")
	time.Sleep(500 * time.Millisecond)
	proc.stop()
	if injected {
		t.Fatal("CRLF in a configured header was sent as an injected header")
	}
	if !strings.Contains(proc.log(), "invalid header") && !strings.Contains(proc.log(), "Failed to send batch") {
		t.Logf("log did not mention the rejected header:\n%s", proc.log())
	}
}

// TestSourceNameWithNewlineIsNotSentAsHeader checks the other header source:
// the source name becomes X-LogSource.
func TestSourceNameWithNewlineIsNotSentAsHeader(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	if err := os.WriteFile(input, []byte("[INFO] one\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	var injected bool
	srv := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Injected") != "" {
			injected = true
		}
		w.WriteHeader(http.StatusOK)
	})
	c := newTestConfig(t)
	p := newPipeline("source-header", sourceFile(input, "name\r\nX-Injected: yes", 3), destHTTP(srv))
	p["runtime"] = map[string]any{"poll_interval": "10ms", "shutdown_timeout": "2s"}
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "source-header")
	time.Sleep(300 * time.Millisecond)
	proc.stop()
	if injected {
		t.Fatal("a newline in the source name was sent as an injected header")
	}
}

// TestLockFileDeletionDoesNotAllowSplitBrain deletes the held lock file and
// starts a second instance on the same checkpoint directory. The first
// process still holds a lock on the now-unlinked inode, so the second can
// create and lock a fresh file at the same path.
func TestLockFileDeletionDoesNotAllowSplitBrain(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 20)
	c := newTestConfig(t)
	c.add(oracleFilePipeline("split-brain", input, filepath.Join(dir, "out.jsonl"), 5))
	cfg := c.write(t)

	first := startProc(t, cfg)
	waitForPipelineUp(t, first, "split-brain")
	lockPath := filepath.Join(c.dir, "checkpoints", ".lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatalf("remove lock: %v", err)
	}

	second := startProc(t, cfg)
	code, exited, logs := second.exitedWithin(1500 * time.Millisecond)
	if !exited {
		second.stop()
		first.stop()
		t.Fatalf("a second instance started after the lock file was deleted; both processes now share the cursor state:\n%s", logs)
	}
	if code == 0 {
		t.Fatalf("second instance exited with status 0")
	}
	first.stop()
}

// TestKilledLockHolderReleasesLock checks that the flock is released by the
// kernel when its holder is SIGKILLed, without operator cleanup.
func TestKilledLockHolderReleasesLock(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 50)
	c := newTestConfig(t)
	c.add(oracleFilePipeline("lock-kill", input, filepath.Join(dir, "out.jsonl"), 5))
	cfg := c.write(t)

	first := startProc(t, cfg)
	waitForPipelineUp(t, first, "lock-kill")
	first.kill()

	second := startProc(t, cfg)
	waitForPipelineUp(t, second, "lock-kill")
	second.stop()
}

func httptestServer(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	s, srv := newSink(t)
	_ = s
	_ = srv
	// Use a dedicated server so tests can inspect headers without the
	// sink's scripted status behaviour.
	sg := httptest.NewServer(h)
	t.Cleanup(sg.Close)
	return sg.URL
}

var _ = syscall.SIGTERM
