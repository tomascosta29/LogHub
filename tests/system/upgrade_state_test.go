//go:build !windows

package system_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildTagBinary builds ./cmd/loghub from a git tag in a temporary
// worktree. The tag's dependencies are downloaded by the normal module
// cache; the worktree is removed afterwards.
func buildTagBinary(t *testing.T, tag string) string {
	t.Helper()
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("not a git checkout: %v", err)
	}
	repo := strings.TrimSpace(string(root))
	dir := filepath.Join(t.TempDir(), "tag-build")
	if out, err := exec.Command("git", "-C", repo, "worktree", "add", "--detach", dir, tag).CombinedOutput(); err != nil {
		t.Skipf("cannot check out tag %s: %v\n%s", tag, err, out)
	}
	t.Cleanup(func() { exec.Command("git", "-C", repo, "worktree", "remove", "--force", dir).Run() })
	bin := filepath.Join(t.TempDir(), "loghub-"+tag)
	build := exec.Command("go", "build", "-o", bin, "./cmd/loghub")
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build tag %s: %v\n%s", tag, err, out)
	}
	return bin
}

// TestUpgradeAndRollbackStateCompatibility starts the current build on state
// written by an older release, then runs the older release on state the
// current build wrote. The default (cheap) form uses the current binary for
// both roles; set LOGHUB_UPGRADE_TAG to a real tag to exercise a release.
func TestUpgradeAndRollbackStateCompatibility(t *testing.T) {
	const total = 100
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, total)

	c := newTestConfig(t)
	p := oracleFilePipeline("upgrade", input, output, 10)
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	c.add(p)
	cfg := c.write(t)

	tag := os.Getenv("LOGHUB_UPGRADE_TAG")
	oldBin := ""
	if tag != "" {
		oldBin = buildTagBinary(t, tag)
	}

	runFor := func(bin string, add int, want int, label string) {
		t.Helper()
		if add > 0 {
			g.appendFile(t, input, add)
		}
		args := []string{loghubBinary, "--config", cfg}
		if bin != "" {
			args[0] = bin
		}
		proc := startProcCommand(t, nil, args)
		waitForPipelineUp(t, proc, "upgrade")
		waitForJSONLCount(t, output, want, 20*time.Second)
		proc.stop()
		got := len(readLogEntriesTolerant(t, output))
		if got != want {
			t.Fatalf("%s: delivered %d records, want %d", label, got, want)
		}
	}

	runFor(oldBin, 0, total, "old release on fresh state")
	runFor("", 50, total+50, "current build on old state")

	// Roll back. The old release must either read the newer cursor or refuse
	// to start; it must not silently restart at the beginning.
	before := len(readLogEntriesTolerant(t, output))
	g.appendFile(t, input, 25)
	args := []string{loghubBinary, "--config", cfg}
	if oldBin != "" {
		args[0] = oldBin
	}
	proc := startProcCommand(t, nil, args)
	code, exited, logs := proc.exitedWithin(1500 * time.Millisecond)
	if exited {
		if code == 0 {
			t.Fatalf("rollback process exited with status 0")
		}
		if !strings.Contains(logs, "cursor") {
			t.Fatalf("rollback refused to start without naming the cursor:\n%s", logs)
		}
		return
	}
	time.Sleep(800 * time.Millisecond)
	proc.stop()
	after := len(readLogEntriesTolerant(t, output))
	if after < before+25 {
		t.Fatalf("rollback did not deliver the 25 new records (%d -> %d); the older binary cannot read the newer cursor, so the pipeline is stalled",
			before, after)
	}
}

// TestLegacyCursorFileIsMigrated checks the v0.1.0 .cursor file upgrade on
// the current build: the offset is imported into the journal and the old
// file is kept with a .migrated suffix for rollback.
func TestLegacyCursorFileIsMigrated(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	first := "[INFO] already-read\n"
	if err := os.WriteFile(input, []byte(first+"[INFO] fresh\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	c := newTestConfig(t)
	c.add(oracleFilePipeline("legacy-migrate", input, filepath.Join(dir, "out.jsonl"), 10))
	cfg := c.write(t)
	cpDir := filepath.Join(c.dir, "checkpoints")
	if err := os.MkdirAll(cpDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cpDir, "legacy-migrate.cursor"), []byte(fmt.Sprintf("%d", len(first))), 0o600); err != nil {
		t.Fatalf("write legacy cursor: %v", err)
	}
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "legacy-migrate")
	waitForJSONLCount(t, filepath.Join(dir, "out.jsonl"), 1, 10*time.Second)
	proc.stop()
	if _, err := os.Stat(filepath.Join(cpDir, "legacy-migrate.cursor.migrated")); err != nil {
		t.Fatalf("legacy cursor was not retained as .migrated: %v", err)
	}
}
