//go:build !windows

package system_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"loghub/internal/core"
)

// envInt reads a positive integer setting, falling back to def. Gated
// These settings let the same test run cheaply in CI and with large
// parameters on a qualification run.
func envInt(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// readLogEntriesTolerant reads a JSONL destination file, ignoring one torn
// trailing line. A SIGKILL can interrupt the final write of a test
// destination; the torn bytes were never fully written, and every complete
// line is still checked by the oracle.
func readLogEntriesTolerant(t *testing.T, path string) []core.LogEntry {
	t.Helper()
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []core.LogEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry core.LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			// Only the last line may be torn; a malformed line followed by
			// more data is corruption.
			if !scanner.Scan() {
				break
			}
			t.Fatalf("decode %s: %v (line %q)", path, err, line)
		}
		out = append(out, entry)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}

// waitForAllIDs waits until every generated record id is present in at
// least one of the given destination files.
func waitForAllIDs(t *testing.T, g *oracleGenerator, timeout time.Duration, paths ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		have := make(map[string]bool)
		for _, p := range paths {
			for _, e := range readLogEntriesTolerant(t, p) {
				have[e.Fields["id"]] = true
			}
		}
		missing := 0
		for _, rec := range g.all() {
			if !have[rec.ID] {
				missing++
			}
		}
		if missing == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d of %d generated records undelivered", missing, g.count())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// crashVariant is one combination from the crash-consistency matrix.
type crashVariant struct {
	name      string
	pipelines int
	// destinations is the number of JSONL outputs per pipeline. A failing
	// HTTP destination is added separately when failingHTTP is set.
	destinations int
	diskBuffer   bool
	dlq          bool
	checkpoint   string
	// failingHTTP adds a destination that returns 500 until the final
	// drain, exercising backlog recovery across crashes.
	failingHTTP bool
}

func crashVariants() []crashVariant {
	return []crashVariant{
		{name: "minimal", pipelines: 1, destinations: 1},
		{name: "disk-buffer", pipelines: 1, destinations: 1, diskBuffer: true},
		{name: "checkpoint-interval", pipelines: 1, destinations: 1, checkpoint: "50ms", dlq: true},
		{name: "two-destinations", pipelines: 1, destinations: 2, diskBuffer: true, dlq: true},
		{name: "two-pipelines", pipelines: 2, destinations: 1, diskBuffer: true},
		{name: "failing-destination-backlog", pipelines: 1, destinations: 2, diskBuffer: true, failingHTTP: true},
	}
}

// TestRandomSIGKILLLoopKeepsEveryRecord kills LogHub at random moments
// while a file source is growing and runs the oracle after every run. The
// recorded set must survive every crash: delivery may repeat, but no
// generated record may be lost, corrupted or reordered on first delivery.
func TestRandomSIGKILLLoopKeepsEveryRecord(t *testing.T) {
	iterations := envInt("LOGHUB_CRASH_ITERATIONS", 20)
	if testing.Short() {
		iterations = min(iterations, 5)
	}
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed>>1)))
	t.Logf("crash loop: %d iterations, seed %d", iterations, seed)

	only := os.Getenv("LOGHUB_CRASH_VARIANT")
	for _, v := range crashVariants() {
		v := v
		if only != "" && only != v.name {
			continue
		}
		t.Run(v.name, func(t *testing.T) {
			if testing.Short() && (v.name == "two-pipelines" || v.name == "failing-destination-backlog") {
				t.Skip("covered by the full qualification run")
			}
			runCrashVariant(t, v, iterations, rng)
		})
	}
}

type crashPipeline struct {
	id     string
	input  string
	output []string
	gen    *oracleGenerator
}

func runCrashVariant(t *testing.T, v crashVariant, iterations int, rng *rand.Rand) {
	t.Helper()
	dir := t.TempDir()
	c := newTestConfig(t)
	c.Global["stalled_threshold_multiplier"] = 1000
	c.Global["circuit_breaker_failure_threshold"] = 1
	c.Global["circuit_breaker_recovery_timeout"] = "50ms"

	var failSink *sink
	var failURL string
	if v.failingHTTP {
		s, srv := newSink(t, http.StatusInternalServerError)
		failSink, failURL = s, srv.URL
	}

	pipelines := make([]*crashPipeline, v.pipelines)
	for i := 0; i < v.pipelines; i++ {
		pf := &crashPipeline{
			id:    fmt.Sprintf("crash-%d", i),
			input: filepath.Join(dir, fmt.Sprintf("in-%d.log", i)),
			gen:   newOracleGenerator(),
		}
		for d := 0; d < v.destinations; d++ {
			pf.output = append(pf.output, filepath.Join(dir, fmt.Sprintf("out-%d-%d.jsonl", i, d)))
		}
		p := oracleFilePipeline(pf.id, pf.input, pf.output[0], 25)
		runtime := p["runtime"].(map[string]any)
		runtime["poll_interval"] = "5ms"
		runtime["send_batch_size"] = 7
		runtime["checkpoint_interval"] = v.checkpoint
		runtime["shutdown_timeout"] = "2s"
		p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
		if v.diskBuffer || v.dlq {
			durability := map[string]any{}
			if v.diskBuffer {
				durability["disk_buffer"] = map[string]any{
					"enabled": true,
					"dir":     filepath.Join(dir, fmt.Sprintf("buffer-%d", i)),
				}
			}
			if v.dlq {
				durability["dlq"] = map[string]any{
					"enabled": true,
					"dir":     filepath.Join(dir, fmt.Sprintf("dlq-%d", i)),
				}
			}
			p["durability"] = durability
		}
		dests := []any{}
		if v.failingHTTP {
			dests = append(dests, destHTTP(failURL))
		}
		for _, out := range pf.output {
			dests = append(dests, destLocalJSONL(out, ""))
		}
		p["destinations"] = dests
		c.add(p)
		pipelines[i] = pf
	}
	cfg := c.write(t)

	for iteration := 0; iteration < iterations; iteration++ {
		if iteration%1000 == 0 && iteration > 0 {
			t.Logf("crash iteration %d/%d", iteration, iterations)
		}
		for _, pf := range pipelines {
			pf.gen.appendFile(t, pf.input, 8+rng.IntN(20))
		}
		proc := startProc(t, cfg)
		t.Cleanup(func() {
			if t.Failed() {
				t.Logf("loghub log (iteration %d):\n%s", iteration, proc.log())
			}
		})
		for _, pf := range pipelines {
			waitForPipelineUp(t, proc, pf.id)
		}
		// A uniform pause covers startup, collection, delivery and
		// checkpoint writes without targeting one phase.
		time.Sleep(time.Duration(10+rng.IntN(200)) * time.Millisecond)
		if code, logs := proc.kill(); code == 0 {
			t.Fatalf("iteration %d: SIGKILL exited with code 0\n%s", iteration, logs)
		}
	}

	// Let the destinations recover, then drain the backlog before the
	// final accounting.
	if failSink != nil {
		failSink.setStatuses(http.StatusOK)
	}
	for _, pf := range pipelines {
		pf.gen.appendFile(t, pf.input, 30)
	}
	proc := startProc(t, cfg)
	for _, pf := range pipelines {
		waitForPipelineUp(t, proc, pf.id)
	}
	for _, pf := range pipelines {
		waitForAllIDs(t, pf.gen, 90*time.Second, pf.output...)
	}
	proc.stop()

	// Each crash can replay at most the two pages in flight, split into
	// sub-batches by send_batch_size. A page is at most fetch_size
	// entries, so this bound is deliberately generous while still
	// catching an unbounded replay loop.
	pageEntries := 25
	// With a non-zero checkpoint interval, every crash may replay what was
	// acknowledged since the last durable save. That is the documented
	// trade-off of a longer interval, not an unbounded replay.
	pagesPerInterval := 1
	if v.checkpoint != "" {
		pagesPerInterval = 16
	}
	allowedExtra := iterations * (2*pageEntries*pagesPerInterval + v.destinations)

	for i, pf := range pipelines {
		var observed []observedRecord
		var destNames []string
		for _, path := range pf.output {
			name := filepath.Base(path)
			destNames = append(destNames, name)
			observed = append(observed, observedFromLogEntries(name, readLogEntriesTolerant(t, path))...)
		}
		var dlqObserved []observedRecord
		for _, fb := range readDLQ(t, filepath.Join(dir, fmt.Sprintf("dlq-%d", i)), pf.id) {
			for _, e := range fb.Entries {
				dlqObserved = append(dlqObserved, observedRecordFromFields("dlq", e.Fields))
			}
		}
		res := verifyOracle(t, pf.gen, oracleCheck{
			Destinations:    destNames,
			AllowDuplicates: true,
			MaxDuplicates:   allowedExtra,
			Observed:        observed,
			DLQ:             dlqObserved,
		}, fmt.Sprintf("%s pipeline %d", v.name, i))
		if res.Duplicates > allowedExtra {
			t.Errorf("pipeline %d: %d duplicate copies exceed the %d allowed by the in-flight window", i, res.Duplicates, allowedExtra)
		}
	}
}
