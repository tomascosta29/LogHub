//go:build !windows

package system_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"loghub/internal/bootstrap"
)

// TestSoak10kRows is a short throughput/soak smoke test: ten thousand rows
// through SQLite, the timestamp processor and the sentinel JSONL writer.
// The full soak belongs in a nightly job; this catches gross regressions in
// the PR suite.
func TestSoak10kRows(t *testing.T) {
	const total = 10000
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit.db")
	output := filepath.Join(dir, "out.jsonl")
	fixture := newSQLiteFixture(t, dbPath)
	fixture.insert(t, total, 25)

	c := newTestConfig(t)
	p := sqliteSentinelPipeline("soak", dbPath, output, 500, 100)
	p["runtime"] = map[string]any{
		"poll_interval":       "5ms",
		"send_batch_size":     100,
		"buffer_size":         50,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "10s",
	}
	c.add(p)
	cfg := c.write(t)

	start := time.Now()
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "soak")
	waitForJSONLCount(t, output, total, 60*time.Second)
	elapsed := time.Since(start)
	proc.stop()

	records := readSentinelRecords(t, output)
	if len(records) != total {
		t.Fatalf("records = %d, want %d", len(records), total)
	}
	ids := idsFromSentinel(records)
	assertUnique(t, ids)
	assertSetContains(t, ids, fixtureIDs(1, total))
	t.Logf("delivered %d rows in %s (%.0f rows/s)", total, elapsed, float64(total)/elapsed.Seconds())
	if elapsed > 60*time.Second {
		t.Fatalf("10k-row soak took %s, want < 60s", elapsed)
	}
}

// soakSample is one resource observation.
type soakSample struct {
	Elapsed    time.Duration
	HeapAlloc  uint64
	HeapInuse  uint64
	Sys        uint64
	Goroutines int
	FDs        int
	StateBytes int64
}

func readSoakSample(root string, start time.Time) soakSample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fds := 0
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		fds = len(entries)
	}
	var state int64
	filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			state += info.Size()
		}
		return nil
	})
	return soakSample{
		Elapsed:    time.Since(start),
		HeapAlloc:  m.HeapAlloc,
		HeapInuse:  m.HeapInuse,
		Sys:        m.Sys,
		Goroutines: runtime.NumGoroutine(),
		FDs:        fds,
		StateBytes: state,
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// appendFileQuiet is appendFile without *testing.T, for use from the soak
// writer goroutine.
func (g *oracleGenerator) appendFileQuiet(path string, n int) {
	recs := g.generate(n)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	for _, r := range recs {
		line, err := json.Marshal(r.fields())
		if err != nil {
			return
		}
		fmt.Fprintf(f, "[INFO] %s\n", line)
	}
}

// insertOracleRows inserts records without failing the test from a
// goroutine.
func insertOracleRows(db *sql.DB, table string, recs []oracleRecord) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(fmt.Sprintf(`INSERT INTO %s (id, seq, event_time, generated_at, checksum, payload) VALUES (?, ?, ?, ?, ?, ?)`, table))
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, r := range recs {
		if _, err := stmt.Exec(r.ID, r.Seq, r.GeneratedAt.Format(time.RFC3339), r.GeneratedAt.Format(time.RFC3339Nano), r.Checksum, r.Payload); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// TestSoakResourceBehavior runs mixed sources against a destination that
// fails randomly, sampling memory, goroutines, file descriptors and state
// size. A monotonic rise in any of them after warm-up is a leak. The
// duration is short by default and is set with LOGHUB_SOAK_DURATION for a
// real soak; LOGHUB_SOAK_SAMPLE_INTERVAL and LOGHUB_SOAK_CSV record the
// samples.
func TestSoakResourceBehavior(t *testing.T) {
	duration := 6 * time.Second
	if testing.Short() {
		duration = 2 * time.Second
	}
	if raw := os.Getenv("LOGHUB_SOAK_DURATION"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			t.Fatalf("invalid LOGHUB_SOAK_DURATION %q", raw)
		}
		duration = d
	}
	sampleEvery := duration / 10
	if raw := os.Getenv("LOGHUB_SOAK_SAMPLE_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			t.Fatalf("invalid LOGHUB_SOAK_SAMPLE_INTERVAL %q", raw)
		}
		sampleEvery = d
	}
	if sampleEvery < 200*time.Millisecond {
		sampleEvery = 200 * time.Millisecond
	}
	t.Logf("soak: duration=%s sample_interval=%s", duration, sampleEvery)

	dir := t.TempDir()
	input := filepath.Join(dir, "soak.log")
	dbPath := filepath.Join(dir, "soak.db")
	jsonlOut := filepath.Join(dir, "sqlite.jsonl")
	db := openOracleDB(t, dbPath)

	fileGen := newOracleGenerator()
	sqlGen := newOracleGenerator()
	sqlGen.createSQLiteTable(t, db)

	s, srv := newSink(t)
	// Random, recoverable failures: retries make progress, and the breaker
	// threshold is high enough that the destination is not shed entirely.
	s.setStatusFunc(func() int {
		if os.Getenv("LOGHUB_SOAK_NO_FAULTS") != "" {
			return http.StatusOK
		}
		switch n := rand.IntN(100); {
		case n < 6:
			return http.StatusInternalServerError
		case n < 10:
			return http.StatusTooManyRequests
		default:
			return http.StatusOK
		}
	})
	_ = io.Discard

	global := map[string]any{
		"checkpoints_dir":              filepath.Join(dir, "checkpoints"),
		"data_dir":                     filepath.Join(dir, "data"),
		"log_level":                    "error",
		"metrics_enabled":              false,
		"secret_refresh_interval":      "0",
		"stalled_threshold_multiplier": 100000,
	}
	pFile := newPipeline("soak-file", oracleFileSource(input, 50), destHTTP(srv.URL))
	pFile["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         10,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "10s",
	}
	pFile["retry"] = map[string]any{"max_attempts": 5, "base_backoff": "1ms"}
	pFile["processors"] = []any{oracleJSONParse()}
	pFile["durability"] = map[string]any{
		"disk_buffer": map[string]any{"enabled": true, "dir": filepath.Join(dir, "buffer")},
		"dlq":         map[string]any{"enabled": true, "dir": filepath.Join(dir, "dlq")},
	}

	pSQL := oracleSQLitePipeline("soak-sqlite", dbPath, jsonlOut, 50)
	pSQL["runtime"].(map[string]any)["poll_interval"] = "5ms"
	pSQL["runtime"].(map[string]any)["send_batch_size"] = 25

	cfg := &testConfig{dir: dir, Global: global, Pipelines: []map[string]any{pFile, pSQL}}
	cfg.Global["circuit_breaker_failure_threshold"] = 50
	cfgPath := cfg.write(t)

	// Keep the test log readable and the run cheap; the soak checks
	// behaviour, not log volume.
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(previousLogger)

	app, err := bootstrap.NewApp(context.Background(), cfgPath)
	if err != nil {
		t.Fatalf("build app: %v", err)
	}
	defer app.Close()
	if err := app.Orchestrator.Start(); err != nil {
		t.Fatalf("start orchestrator: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			app.Orchestrator.Shutdown()
		}
	}()

	var csv *os.File
	if path := os.Getenv("LOGHUB_SOAK_CSV"); path != "" {
		csv, err = os.Create(path)
		if err != nil {
			t.Fatalf("create soak csv: %v", err)
		}
		defer csv.Close()
		fmt.Fprintln(csv, "elapsed_s,heap_alloc_mb,heap_inuse_mb,sys_mb,goroutines,fds,state_bytes")
	}

	done := make(chan struct{})
	writerErr := make(chan error, 1)
	go func() {
		// A modest rate keeps the in-process oracle's own record set
		// bounded during an 8 h run; the fault and resource behaviour does
		// not depend on saturating the pipeline.
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fileGen.appendFileQuiet(input, 1)
				recs := sqlGen.generate(1)
				if err := insertOracleRows(db, "oracle_events", recs); err != nil {
					select {
					case writerErr <- err:
					default:
					}
					return
				}
			}
		}
	}()

	var samples []soakSample
	start := time.Now()
	next := start.Add(sampleEvery)
	for time.Since(start) < duration {
		time.Sleep(minDuration(50*time.Millisecond, time.Until(start.Add(duration))))
		if !time.Now().Before(next) {
			next = next.Add(sampleEvery)
			sample := readSoakSample(dir, start)
			samples = append(samples, sample)
			if csv != nil {
				fmt.Fprintf(csv, "%.1f,%.1f,%.1f,%.1f,%d,%d,%d\n",
					sample.Elapsed.Seconds(), float64(sample.HeapAlloc)/(1<<20),
					float64(sample.HeapInuse)/(1<<20), float64(sample.Sys)/(1<<20),
					sample.Goroutines, sample.FDs, sample.StateBytes)
			}
			t.Logf("sample t=%s heap=%.1fMB inuse=%.1fMB sys=%.1fMB goroutines=%d fds=%d state=%.1fKB",
				sample.Elapsed.Round(time.Second), float64(sample.HeapAlloc)/(1<<20),
				float64(sample.HeapInuse)/(1<<20), float64(sample.Sys)/(1<<20),
				sample.Goroutines, sample.FDs, float64(sample.StateBytes)/1024)
		}
	}
	close(done)
	select {
	case err := <-writerErr:
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
	default:
	}
	fileTotal := fileGen.count()
	sqlTotal := sqlGen.count()

	// Drain before the accounting check. The destination is random but
	// always eventually succeeds.
	waitFor(t, 60*time.Second, "HTTP destination drained", func() bool {
		return s.acceptedEntries() >= fileTotal
	})
	waitForJSONLCount(t, jsonlOut, sqlTotal, 60*time.Second)

	if err := app.Orchestrator.Shutdown(); err != nil {
		t.Logf("shutdown returned: %v", err)
	}
	stopped = true

	httpObserved := observedFromLogEntries("http", s.acceptedLogEntries(t))
	var dlqObserved []observedRecord
	for _, fb := range readDLQ(t, filepath.Join(dir, "dlq", "soak-file"), "soak-file") {
		for _, e := range fb.Entries {
			dlqObserved = append(dlqObserved, observedRecordFromFields("dlq", e.Fields))
		}
	}
	verifyOracle(t, fileGen, oracleCheck{
		Destinations: []string{"http"},
		Observed:     httpObserved,
		DLQ:          dlqObserved,
	}, "soak file pipeline")
	verifyOracle(t, sqlGen, oracleCheck{
		Destinations: []string{"local_jsonl"},
		Observed:     observedFromLogEntries("local_jsonl", readLogEntries(t, jsonlOut)),
	}, "soak sqlite pipeline")

	// Resource growth after the warm-up quarter must stay bounded. The
	// thresholds are deliberately loose; only a leak should trip them.
	if len(samples) >= 8 {
		warm := samples[len(samples)/4]
		for _, s := range samples[len(samples)/4:] {
			if s.FDs > warm.FDs+32 {
				t.Errorf("open file descriptors grew from %d to %d", warm.FDs, s.FDs)
			}
			if s.Goroutines > warm.Goroutines+64 {
				t.Errorf("goroutine count grew from %d to %d", warm.Goroutines, s.Goroutines)
			}
			if s.HeapAlloc > warm.HeapAlloc*4+(128<<20) {
				t.Errorf("heap grew from %d to %d bytes", warm.HeapAlloc, s.HeapAlloc)
			}
		}
	}
}

// compile-time assertion that the fault server helper is linked in.
var _ = httptest.NewServer
