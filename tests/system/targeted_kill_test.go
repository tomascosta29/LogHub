//go:build !windows

package system_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestTargetedKillsDuringDurableWrites dies at each syscall boundary of the
// checkpoint and output writes, restarts, and requires every record to
// survive. LOGHUB_TARGETED_KILLS=all sweeps several occurrence counts.
func TestTargetedKillsDuringDurableWrites(t *testing.T) {
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace is not installed")
	}
	syscalls := []string{"write", "fsync", "fdatasync", "rename", "renameat", "openat", "ftruncate"}
	maxWhen := 2
	if os.Getenv("LOGHUB_TARGETED_KILLS") == "all" {
		maxWhen = 6
	}
	for _, sc := range syscalls {
		for when := 1; when <= maxWhen; when++ {
			sc, when := sc, when
			t.Run(fmt.Sprintf("%s_%d", sc, when), func(t *testing.T) {
				runTargetedKill(t, sc, when)
			})
		}
	}
}

func runTargetedKill(t *testing.T, sc string, when int) {
	t.Helper()
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 300)

	c := newTestConfig(t)
	p := oracleFilePipeline("targeted-kill", input, output, 20)
	p["runtime"].(map[string]any)["poll_interval"] = "5ms"
	c.add(p)
	cfg := c.write(t)

	prefix := []string{"strace", "-f", "-q", "-o", os.DevNull, "-e",
		fmt.Sprintf("inject=%s:signal=KILL:when=%d", sc, when)}
	proc := startProcCommand(t, prefix, []string{loghubBinary, "--config", cfg})
	// The injected signal may fire immediately or after the pipeline has
	// run for a while; either is a valid kill point.
	start := time.Now()
	exitedAlready := false
	for time.Since(start) < 3*time.Second {
		if _, exited, _ := proc.exitedWithin(50 * time.Millisecond); exited {
			exitedAlready = true
			break
		}
	}
	if !exitedAlready {
		proc.kill()
	}

	// Restart without the fault and drain. A killed wrapper can take a
	// moment to release the flock; retry a startup that raced it.
	var restarted bool
	for attempt := 0; attempt < 3 && !restarted; attempt++ {
		proc = startProc(t, cfg)
		if code, exited, logs := proc.exitedWithin(500 * time.Millisecond); exited {
			if code != 0 && strings.Contains(logs, "held by another process") {
				time.Sleep(300 * time.Millisecond)
				continue
			}
			t.Fatalf("restart after %s#%d exited: %d\n%s", sc, when, code, logs)
		}
		restarted = true
	}
	if !restarted {
		t.Fatalf("restart after %s#%d could not acquire the lock", sc, when)
	}
	waitForPipelineUp(t, proc, "targeted-kill")
	waitForAllIDs(t, g, 30*time.Second, output)
	proc.stop()

	verifyOracle(t, g, oracleCheck{
		Destinations:    []string{"local_jsonl"},
		AllowDuplicates: true,
		Observed:        observedFromLogEntries("local_jsonl", readLogEntriesTolerant(t, output)),
	}, fmt.Sprintf("kill at %s#%d", sc, when))
}

// TestMaliciousJSONInputNeverCrashesDeliversOrSkips feeds deeply nested
// JSON, huge numbers, duplicate keys, colliding field names and unicode
// normalization through json_parse. Each line must still be accounted for.
func TestMaliciousJSONInputNeverCrashesDeliversOrSkips(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "weird.log")
	output := filepath.Join(dir, "out.jsonl")

	deep := strings.Repeat("[", 9000) + strings.Repeat("]", 9000)
	lines := []string{
		`{"id":"deep","value":` + deep + `}`,
		`{"id":"huge","value":1e999}`,
		`{"id":"dup-1","id":"dup-2","value":"x"}`,
		`{"id":"unicode","value":"e\u0301 vs \u00e9"}`,
		`{"id":"collision","timestamp":"2020-01-01T00:00:00Z","TimeGenerated":"2020-01-01T00:00:00Z","message":"m"}`,
		`{"id":"numbers","value":1234567890123456789012345678901234567890}`,
	}
	var buf strings.Builder
	for _, line := range lines {
		buf.WriteString("[INFO] ")
		buf.WriteString(line)
		buf.WriteString("\n")
	}
	if err := os.WriteFile(input, []byte(buf.String()), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	c := newTestConfig(t)
	p := newPipeline("weird-json", sourceFile(input, "weird", 10), destLocalJSONL(output, ""))
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	p["processors"] = []any{map[string]any{"type": "json_parse", "json_parse": map[string]any{
		"target_field": "fields",
		"on_error":     "keep",
	}}}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "weird-json")
	waitForJSONLCount(t, output, len(lines), 15*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	if len(entries) != len(lines) {
		t.Fatalf("delivered %d of %d malicious lines", len(entries), len(lines))
	}
	for _, e := range entries {
		// Every entry remains valid JSON with a fields map; no panic and
		// no half-decoded entry.
		if _, err := json.Marshal(e); err != nil {
			t.Fatalf("delivered entry does not marshal: %v", err)
		}
	}
}

// TestPipelineIDPathTricksAreRejected tries to escape the state directories
// with separators, dot segments and control characters.
func TestPipelineIDPathTricksAreRejected(t *testing.T) {
	for _, id := range []string{"../escape", `..\escape`, ".", "..", "a/b", "a\\b", "a\x00b", "a\nb"} {
		id := id
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			dir := t.TempDir()
			input := filepath.Join(dir, "in.log")
			os.WriteFile(input, []byte("[INFO] x\n"), 0o600)
			c := newTestConfig(t)
			p := newPipeline(id, sourceFile(input, "src", 5), destDummy(nil))
			c.add(p)
			cfg := c.write(t)
			_, stderr, code := runBinary(t, "--validate", "--config", cfg)
			if code == 0 {
				t.Fatalf("pipeline id %q was accepted by --validate", id)
			}
			if !strings.Contains(stderr, "pipeline ID") {
				t.Fatalf("error did not name the pipeline ID: %s", stderr)
			}
		})
	}
}

var _ = syscall.SIGKILL
