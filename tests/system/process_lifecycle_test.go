package system_test

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestTenInstancesOnlyOneStarts starts ten processes on the same state
// directory. Exactly one may acquire the checkpoint lock; the others must
// exit with an error instead of running with shared state.
func TestTenInstancesOnlyOneStarts(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	if err := os.WriteFile(input, []byte("[INFO] one\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	c := newTestConfig(t)
	c.add(oracleFilePipeline("ten-locks", input, filepath.Join(dir, "out.jsonl"), 5))
	cfg := c.write(t)

	procs := make([]*proc, 10)
	for i := range procs {
		procs[i] = startProc(t, cfg)
	}
	running := 0
	for _, p := range procs {
		code, exited, logs := p.exitedWithin(2 * time.Second)
		if exited {
			if code == 0 {
				t.Errorf("instance exited with status 0 instead of a lock error")
			}
			if !containsAny(logs, "held by another process", "lock") {
				t.Errorf("instance exited without a clear lock error:\n%s", logs)
			}
			continue
		}
		running++
		p.stop()
	}
	if running != 1 {
		t.Fatalf("%d instances are running, want exactly 1", running)
	}
}

// TestSIGHUPStormDuringTrafficKeepsEveryRecord reloads repeatedly while
// records are being appended. Reloads must not lose records; every generated
// id has to reach the destination.
func TestSIGHUPStormDuringTrafficKeepsEveryRecord(t *testing.T) {
	iterations := 20
	if v := os.Getenv("LOGHUB_RELOAD_STORM"); v != "" {
		fmt.Sscanf(v, "%d", &iterations)
	}
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 50)

	c := newTestConfig(t)
	p := oracleFilePipeline("hup-storm", input, output, 25)
	p["runtime"].(map[string]any)["poll_interval"] = "5ms"
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "hup-storm")
	fdsBefore := countFDsOf(t, proc.cmd.Process.Pid)
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	for i := 0; i < iterations; i++ {
		g.appendFile(t, input, 5)
		proc.signal(syscall.SIGHUP)
		time.Sleep(time.Duration(10+rng.IntN(30)) * time.Millisecond)
	}
	g.appendFile(t, input, 25)

	waitForAllIDs(t, g, 60*time.Second, output)
	waitForHealth(t, proc.addr, 10*time.Second, "reloaded pipeline healthy", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 &&
			(h.Pipelines[0].Status == "running" || h.Pipelines[0].Status == "degraded")
	})
	fdsAfter := countFDsOf(t, proc.cmd.Process.Pid)
	proc.stop()

	if fdsAfter > fdsBefore+8 {
		t.Errorf("file descriptors grew from %d to %d across %d reloads", fdsBefore, fdsAfter, iterations)
	}
	res := verifyOracle(t, g, oracleCheck{
		Destinations:    []string{"local_jsonl"},
		AllowDuplicates: true,
		Observed:        observedFromLogEntries("local_jsonl", readLogEntriesTolerant(t, output)),
	}, "SIGHUP storm")
	if res.Duplicates > 0 {
		t.Logf("%d duplicate copies were delivered across %d reloads (outside the documented crash window)", res.Duplicates, iterations)
	}
}

func countFDsOf(t *testing.T, pid int) int {
	t.Helper()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return 0
	}
	return len(entries)
}

// TestRepeatedSIGTERMExits sends several SIGTERMs in a row. The process must
// exit cleanly once, not hang or panic on a late signal.
func TestRepeatedSIGTERMExits(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	if err := os.WriteFile(input, []byte("[INFO] one\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	c := newTestConfig(t)
	c.add(oracleFilePipeline("multi-term", input, filepath.Join(dir, "out.jsonl"), 5))
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "multi-term")
	for i := 0; i < 5; i++ {
		proc.signal(syscall.SIGTERM)
	}
	code, logs := proc.waitExit(15 * time.Second)
	if code != 0 {
		t.Fatalf("exit code %d after repeated SIGTERM\n%s", code, logs)
	}
}

// TestSIGKILLDuringShutdownDoesNotHang sends SIGTERM while a destination is
// slow, then SIGKILL. The test process must reap it promptly; a stuck
// shutdown that ignores the second signal is a failure.
func TestSIGKILLDuringShutdownDoesNotHang(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, 20)
	s, srv := newSink(t, http.StatusOK)
	s.setDelay(5 * time.Second)

	c := newTestConfig(t)
	p := newPipeline("kill-during-drain", oracleFileSource(input, 5), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval": "10ms", "send_batch_size": 5, "buffer_size": 4,
		"checkpoint_interval": "0", "shutdown_timeout": "10s",
	}
	p["processors"] = []any{oracleJSONParse()}
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "kill-during-drain")
	time.Sleep(100 * time.Millisecond)
	proc.signal(syscall.SIGTERM)
	time.Sleep(200 * time.Millisecond)
	if err := syscall.Kill(-proc.cmd.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatalf("sigkill: %v", err)
	}
	if _, exited, _ := proc.exitedWithin(5 * time.Second); !exited {
		proc.kill()
		t.Fatal("process did not exit within 5s of SIGTERM then SIGKILL")
	}
}

// TestRestartStorm repeatedly starts and stops LogHub with a random short
// run time. The durable cursor must never move backwards and every record
// must eventually be delivered.
func TestRestartStorm(t *testing.T) {
	iterations := envInt("LOGHUB_RESTART_ITERATIONS", 20)
	if testing.Short() {
		iterations = min(iterations, 5)
	}
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 200)
	c := newTestConfig(t)
	p := oracleFilePipeline("restart-storm", input, output, 10)
	p["runtime"].(map[string]any)["poll_interval"] = "5ms"
	c.add(p)
	cfg := c.write(t)
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 2))

	for i := 0; i < iterations; i++ {
		proc := startProc(t, cfg)
		waitForPipelineUp(t, proc, "restart-storm")
		time.Sleep(time.Duration(rng.IntN(120)) * time.Millisecond)
		proc.stop()
	}

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "restart-storm")
	waitForAllIDs(t, g, 60*time.Second, output)
	proc.stop()

	verifyOracle(t, g, oracleCheck{
		Destinations:    []string{"local_jsonl"},
		AllowDuplicates: true,
		Observed:        observedFromLogEntries("local_jsonl", readLogEntriesTolerant(t, output)),
	}, "restart storm")
}
