package system_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"

	"loghub/internal/checkpointer"
	"loghub/internal/core"
	"loghub/internal/dlq"
)

var (
	loghubBinary string
	loghubBinDir string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "loghub-system-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
		os.Exit(1)
	}
	loghubBinDir = dir
	loghubBinary = filepath.Join(dir, "loghub")
	build := exec.Command("go", "build", "-o", loghubBinary, "./cmd/loghub")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build loghub: %v\n%s", err, out)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(loghubBinDir)
	os.Exit(code)
}

// --- config builder ---

type testConfig struct {
	Global    map[string]any   `yaml:"global"`
	Secrets   map[string]any   `yaml:"secrets,omitempty"`
	Pipelines []map[string]any `yaml:"pipelines"`
	dir       string
}

func newTestConfig(t *testing.T) *testConfig {
	t.Helper()
	dir := t.TempDir()
	return &testConfig{
		dir: dir,
		Global: map[string]any{
			"checkpoints_dir":         filepath.Join(dir, "checkpoints"),
			"log_level":               "warn",
			"metrics_enabled":         true,
			"secret_refresh_interval": "0",
		},
	}
}

func (c *testConfig) add(p map[string]any) *testConfig {
	c.Pipelines = append(c.Pipelines, p)
	return c
}

func (c *testConfig) write(t *testing.T) string {
	t.Helper()
	data, err := yaml.Marshal(c)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	path := filepath.Join(c.dir, "loghub.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func newPipeline(id string, src map[string]any, dests ...map[string]any) map[string]any {
	return map[string]any{
		"id": id,
		"runtime": map[string]any{
			"poll_interval":       "20ms",
			"send_batch_size":     100,
			"buffer_size":         20,
			"checkpoint_interval": "0",
			"shutdown_timeout":    "3s",
		},
		"retry": map[string]any{
			"max_attempts": 3,
			"base_backoff": "1ms",
		},
		"source":       src,
		"destinations": dests,
	}
}

func sourceSQLite(database, table, timestampColumn, cursorColumn string, fetchSize int, extra map[string]any) map[string]any {
	props := map[string]any{
		"database":         database,
		"table":            table,
		"timestamp_column": timestampColumn,
		"cursor_column":    cursorColumn,
		"fetch_size":       fetchSize,
	}
	for k, v := range extra {
		props[k] = v
	}
	return map[string]any{"type": "sqlite", "sqlite": props}
}

func sourceFile(path, name string, fetchSize int) map[string]any {
	return map[string]any{"type": "file", "file": map[string]any{
		"path":       path,
		"name":       name,
		"fetch_size": fetchSize,
	}}
}

func destLocalJSONL(path, format string) map[string]any {
	props := map[string]any{"path": path}
	if format != "" {
		props["format"] = format
	}
	return map[string]any{"type": "local_jsonl", "local_jsonl": props}
}

func destHTTP(url string) map[string]any {
	return map[string]any{"type": "http", "http": map[string]any{
		"url":     url,
		"timeout": "500ms",
	}}
}

func destDummy(extra map[string]any) map[string]any {
	props := map[string]any{}
	for k, v := range extra {
		props[k] = v
	}
	return map[string]any{"type": "dummy", "dummy": props}
}

func timestampProcessor(sourceField, layout, targetField string) map[string]any {
	return map[string]any{"type": "timestamp", "timestamp": map[string]any{
		"source_field":  sourceField,
		"source_layout": layout,
		"target_field":  targetField,
	}}
}

// --- SQLite fixture ---

type sqliteFixture struct {
	path string
	db   *sql.DB
	next int
	base time.Time
}

func newSQLiteFixture(t *testing.T, path string) *sqliteFixture {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=5000"} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			t.Fatalf("%s: %v", pragma, err)
		}
	}
	if _, err := db.Exec(`
		CREATE TABLE audit_events (
			id TEXT PRIMARY KEY,
			event_time TEXT NOT NULL,
			user_name TEXT NOT NULL,
			action TEXT NOT NULL,
			payload TEXT NOT NULL
		)`); err != nil {
		db.Close()
		t.Fatalf("create audit_events: %v", err)
	}
	f := &sqliteFixture{
		path: path,
		db:   db,
		base: time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC),
	}
	t.Cleanup(func() { db.Close() })
	return f
}

func (f *sqliteFixture) insert(t *testing.T, n, groupSize int) []string {
	t.Helper()
	if groupSize <= 0 {
		groupSize = 1
	}
	tx, err := f.db.Begin()
	if err != nil {
		t.Fatalf("begin insert: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO audit_events (id, event_time, user_name, action, payload) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("prepare insert: %v", err)
	}
	defer stmt.Close()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		f.next++
		id := fmt.Sprintf("%06d", f.next)
		eventTime := f.base.Add(time.Duration((f.next-1)/groupSize) * time.Second).Format(time.RFC3339)
		action := "login"
		if f.next%7 == 0 {
			action = "denied"
		}
		if _, err := stmt.Exec(id, eventTime, "user-"+id, action, "payload-"+id); err != nil {
			tx.Rollback()
			t.Fatalf("insert row %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit insert: %v", err)
	}
	return ids
}

func (f *sqliteFixture) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := f.db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func (f *sqliteFixture) replaceTimestamps(t *testing.T, format string) {
	t.Helper()
	if _, err := f.db.Exec(`UPDATE audit_events SET event_time = ` + format); err != nil {
		t.Fatalf("replace timestamps: %v", err)
	}
}

// --- process runner ---

type proc struct {
	t       *testing.T
	cmd     *exec.Cmd
	addr    string
	logPath string
	logFile *os.File
	waitCh  chan error
	done    bool
}

func startProc(t *testing.T, cfgPath string, env ...string) *proc {
	t.Helper()
	addr := freeAddr(t)
	logPath := filepath.Join(t.TempDir(), "loghub.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create log file: %v", err)
	}
	cmd := exec.Command(loghubBinary, "--config", cfgPath, "--metrics", addr)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start loghub: %v", err)
	}
	p := &proc{t: t, cmd: cmd, addr: addr, logPath: logPath, logFile: logFile, waitCh: make(chan error, 1)}
	go func() { p.waitCh <- cmd.Wait() }()
	t.Cleanup(func() {
		if p.done {
			return
		}
		_ = p.cmd.Process.Kill()
		<-p.waitCh
		p.logFile.Close()
		p.done = true
	})
	return p
}

func (p *proc) signal(sig syscall.Signal) {
	p.t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		p.t.Fatalf("signal %v: %v", sig, err)
	}
}

func (p *proc) waitExit(timeout time.Duration) (int, string) {
	p.t.Helper()
	select {
	case err := <-p.waitCh:
		p.done = true
		p.logFile.Close()
		code := 0
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else {
				code = -1
			}
		}
		return code, p.log()
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		<-p.waitCh
		p.done = true
		p.logFile.Close()
		p.t.Fatalf("loghub did not exit within %s\n%s", timeout, p.log())
		return -1, ""
	}
}

func (p *proc) stop() {
	p.t.Helper()
	p.signal(syscall.SIGTERM)
	code, logs := p.waitExit(15 * time.Second)
	if code != 0 {
		p.t.Fatalf("loghub exited with code %d\n%s", code, logs)
	}
}

func (p *proc) kill() (int, string) {
	p.t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		p.t.Fatalf("kill: %v", err)
	}
	return p.waitExit(10 * time.Second)
}

func (p *proc) log() string {
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		return fmt.Sprintf("<read log: %v>", err)
	}
	const max = 16 << 10
	if len(data) > max {
		data = data[len(data)-max:]
	}
	return string(data)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, desc)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- health and metrics ---

type healthPipeline struct {
	ID          string    `json:"ID"`
	Status      string    `json:"Status"`
	LastCollect time.Time `json:"LastCollect"`
	LastSend    time.Time `json:"LastSend"`
	LastError   time.Time `json:"LastError"`
	ErrorMsg    string    `json:"ErrorMsg"`
}

type healthResponse struct {
	Status    string           `json:"status"`
	Running   bool             `json:"running"`
	Uptime    string           `json:"uptime"`
	Reason    string           `json:"reason"`
	Pipelines []healthPipeline `json:"pipelines"`
}

var httpClient = &http.Client{Timeout: time.Second}

func fetchHealth(addr string) (int, healthResponse, error) {
	resp, err := httpClient.Get("http://" + addr + "/health")
	if err != nil {
		return 0, healthResponse{}, err
	}
	defer resp.Body.Close()
	var h healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return resp.StatusCode, h, err
	}
	return resp.StatusCode, h, nil
}

func waitForHealth(t *testing.T, addr string, timeout time.Duration, desc string, cond func(int, healthResponse) bool) healthResponse {
	t.Helper()
	var last healthResponse
	waitFor(t, timeout, desc, func() bool {
		code, h, err := fetchHealth(addr)
		if err != nil {
			return false
		}
		last = h
		return cond(code, h)
	})
	return last
}

func waitForRunning(t *testing.T, p *proc, pipelineID string) {
	t.Helper()
	waitForHealth(t, p.addr, 10*time.Second, "pipeline "+pipelineID+" running", func(code int, h healthResponse) bool {
		if code != http.StatusOK || !h.Running {
			return false
		}
		for _, ph := range h.Pipelines {
			if ph.ID == pipelineID && ph.Status == "running" {
				return true
			}
		}
		return false
	})
}

func waitForPipelineUp(t *testing.T, p *proc, pipelineID string) {
	t.Helper()
	waitForHealth(t, p.addr, 10*time.Second, "pipeline "+pipelineID+" up", func(code int, h healthResponse) bool {
		if code != http.StatusOK || !h.Running {
			return false
		}
		for _, ph := range h.Pipelines {
			if ph.ID == pipelineID && (ph.Status == "running" || ph.Status == "degraded") {
				return true
			}
		}
		return false
	})
}

type metricFamilies map[string]*dto.MetricFamily

func scrapeMetrics(t *testing.T, addr string) metricFamilies {
	t.Helper()
	resp, err := httpClient.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status %d", resp.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		t.Fatalf("parse metrics: %v", err)
	}
	return metricFamilies(fams)
}

func tryScrapeMetrics(addr string) (metricFamilies, error) {
	resp, err := httpClient.Get("http://" + addr + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, err
	}
	return metricFamilies(fams), nil
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	if len(m.GetLabel()) != len(want) {
		return false
	}
	for _, lp := range m.GetLabel() {
		if want[lp.GetName()] != lp.GetValue() {
			return false
		}
	}
	return true
}

func sampleValue(fams metricFamilies, name string, labels map[string]string) (float64, bool) {
	fam, ok := fams[name]
	if !ok {
		return 0, false
	}
	for _, m := range fam.GetMetric() {
		if !labelsMatch(m, labels) {
			continue
		}
		switch fam.GetType() {
		case dto.MetricType_COUNTER:
			return m.GetCounter().GetValue(), true
		case dto.MetricType_GAUGE:
			return m.GetGauge().GetValue(), true
		case dto.MetricType_HISTOGRAM:
			return float64(m.GetHistogram().GetSampleCount()), true
		}
	}
	return 0, false
}

func counterValue(fams metricFamilies, name string, labels map[string]string) float64 {
	v, _ := sampleValue(fams, name, labels)
	return v
}

func mustValue(t *testing.T, fams metricFamilies, name string, labels map[string]string) float64 {
	t.Helper()
	v, ok := sampleValue(fams, name, labels)
	if !ok {
		t.Fatalf("metric %s%v not found", name, labels)
	}
	return v
}

func waitForMetric(t *testing.T, addr, name string, labels map[string]string, timeout time.Duration, desc string, cond func(float64) bool) {
	t.Helper()
	waitFor(t, timeout, desc, func() bool {
		fams, err := tryScrapeMetrics(addr)
		if err != nil {
			return false
		}
		v, ok := sampleValue(fams, name, labels)
		return ok && cond(v)
	})
}

// --- scripted HTTP sink ---

type sink struct {
	mu       sync.Mutex
	statuses []int
	delay    time.Duration
	requests int
	entries  int
	accepted int
	bodies   [][]byte
	headers  []http.Header
	methods  []string
}

func newSink(t *testing.T, statuses ...int) (*sink, *httptest.Server) {
	t.Helper()
	s := &sink{statuses: statuses}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		status := http.StatusOK
		if len(s.statuses) > 0 {
			idx := s.requests
			if idx >= len(s.statuses) {
				idx = len(s.statuses) - 1
			}
			status = s.statuses[idx]
		}
		s.requests++
		var entries []json.RawMessage
		_ = json.Unmarshal(body, &entries)
		s.entries += len(entries)
		if status < 400 {
			s.accepted += len(entries)
		}
		s.bodies = append(s.bodies, body)
		s.headers = append(s.headers, r.Header.Clone())
		s.methods = append(s.methods, r.Method)
		delay := s.delay
		s.mu.Unlock()
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if status >= 400 {
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return s, srv
}

func (s *sink) setStatuses(statuses ...int) {
	s.mu.Lock()
	s.statuses = statuses
	s.mu.Unlock()
}

func (s *sink) setDelay(d time.Duration) {
	s.mu.Lock()
	s.delay = d
	s.mu.Unlock()
}

func (s *sink) counts() (requests, entries int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.entries
}

func (s *sink) acceptedEntries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

func (s *sink) lastHeader() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.headers) == 0 {
		return nil
	}
	return s.headers[len(s.headers)-1]
}

func (s *sink) lastMethod() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.methods) == 0 {
		return ""
	}
	return s.methods[len(s.methods)-1]
}

// --- destination readers ---

func readLogEntries(t *testing.T, path string) []core.LogEntry {
	t.Helper()
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	var out []core.LogEntry
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var entry core.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		out = append(out, entry)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}

func readSentinelRecords(t *testing.T, path string) []map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	var out []map[string]string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var record map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		out = append(out, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}

func tryCountJSONL(path string) int {
	file, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer file.Close()
	count := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var v any
		if json.Unmarshal(line, &v) != nil {
			return count
		}
		count++
	}
	return count
}

func waitForJSONLCount(t *testing.T, path string, want int, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, fmt.Sprintf("%d records in %s", want, path), func() bool {
		return tryCountJSONL(path) >= want
	})
}

func idsFromEntries(entries []core.LogEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Fields["id"])
	}
	return out
}

func idsFromSentinel(records []map[string]string) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, r["id"])
	}
	return out
}

func assertUnique(t *testing.T, ids []string) {
	t.Helper()
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func assertSetContains(t *testing.T, ids, want []string) {
	t.Helper()
	got := make(map[string]bool, len(ids))
	for _, id := range ids {
		got[id] = true
	}
	for _, id := range want {
		if !got[id] {
			t.Fatalf("missing id %q", id)
		}
	}
}

func readCursor(t *testing.T, dir, pipelineID string) string {
	t.Helper()
	cp, err := checkpointer.NewFileCheckpointer(dir)
	if err != nil {
		t.Fatalf("open checkpointer: %v", err)
	}
	defer cp.Close()
	cursor, err := cp.Get(pipelineID)
	if err != nil {
		t.Fatalf("read cursor for %s: %v", pipelineID, err)
	}
	return cursor
}

func readDLQ(t *testing.T, dir, pipelineID string) []dlq.FailedBatch {
	t.Helper()
	var out []dlq.FailedBatch
	err := dlq.ReadFile(dlq.JournalPath(dir, pipelineID), func(fb dlq.FailedBatch) error {
		out = append(out, fb)
		return nil
	})
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("read DLQ: %v", err)
	}
	return out
}

func runBinary(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(loghubBinary, args...)
	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	code = 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			code = -1
		}
	}
	return out.String(), errOut.String(), code
}

func runBinaryFor(t *testing.T, timeout time.Duration, env []string, args ...string) (stdout, stderr string, code int, timedOut bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, loghubBinary, args...)
	cmd.Env = append(os.Environ(), env...)
	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return out.String(), errOut.String(), -1, true
	}
	code = 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			code = -1
		}
	}
	return out.String(), errOut.String(), code, false
}

func waitForLog(t *testing.T, p *proc, substr string, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, "log containing "+strconvQuote(substr), func() bool {
		return strings.Contains(p.log(), substr)
	})
}

func strconvQuote(s string) string {
	return fmt.Sprintf("%q", s)
}
