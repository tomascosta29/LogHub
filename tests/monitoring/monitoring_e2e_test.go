package monitoring_test

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
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

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"loghub/internal/metrics"
)

var (
	loghubBinary string
	loghubBinDir string
)

func TestMain(m *testing.M) {
	metrics.Register()

	dir, err := os.MkdirTemp("", "loghub-monitoring-*")
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

// --- process harness ---

type loghubProcess struct {
	t       *testing.T
	cmd     *exec.Cmd
	addr    string
	logPath string
	logFile *os.File
	done    bool
}

func startLogHub(t *testing.T, cfgPath string, extraEnv ...string) *loghubProcess {
	t.Helper()
	addr := freeAddr(t)
	logPath := filepath.Join(t.TempDir(), "loghub.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create log file: %v", err)
	}
	cmd := exec.Command(loghubBinary, "--config", cfgPath, "--metrics", addr)
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start loghub: %v", err)
	}
	p := &loghubProcess{t: t, cmd: cmd, addr: addr, logPath: logPath, logFile: logFile}
	t.Cleanup(func() {
		if p.done {
			return
		}
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
		p.logFile.Close()
	})
	return p
}

func (p *loghubProcess) stop() {
	p.t.Helper()
	if p.done {
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatalf("send SIGTERM: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		p.done = true
		p.logFile.Close()
		if err != nil {
			p.t.Fatalf("loghub exited with error: %v\n%s", err, p.logTail())
		}
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		p.done = true
		p.logFile.Close()
		p.t.Fatalf("loghub did not stop within 15s\n%s", p.logTail())
	}
}

func (p *loghubProcess) logTail() string {
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		return fmt.Sprintf("<read log: %v>", err)
	}
	const max = 8 << 10
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
	if err := l.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
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

// --- health endpoint ---

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

var healthClient = &http.Client{Timeout: time.Second}

func fetchHealth(addr string) (int, healthResponse, error) {
	resp, err := healthClient.Get("http://" + addr + "/health")
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

func singlePipeline(t *testing.T, h healthResponse) healthPipeline {
	t.Helper()
	if len(h.Pipelines) != 1 {
		t.Fatalf("health reports %d pipelines, want 1: %+v", len(h.Pipelines), h.Pipelines)
	}
	return h.Pipelines[0]
}

// --- metrics scraping ---

type metricFamilies map[string]*dto.MetricFamily

var metricsClient = &http.Client{Timeout: 2 * time.Second}

func scrapeMetrics(t *testing.T, addr string) metricFamilies {
	t.Helper()
	fams, err := tryScrape(addr)
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	return fams
}

func tryScrape(addr string) (metricFamilies, error) {
	resp, err := metricsClient.Get("http://" + addr + "/metrics")
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
		return nil, fmt.Errorf("parse metrics: %w", err)
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

func histogramStats(fams metricFamilies, name string, labels map[string]string) (uint64, float64, bool) {
	fam, ok := fams[name]
	if !ok {
		return 0, 0, false
	}
	for _, m := range fam.GetMetric() {
		if labelsMatch(m, labels) {
			return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum(), true
		}
	}
	return 0, 0, false
}

func mustValue(t *testing.T, fams metricFamilies, name string, labels map[string]string) float64 {
	t.Helper()
	v, ok := sampleValue(fams, name, labels)
	if !ok {
		t.Fatalf("metric %s%v not found", name, labels)
	}
	return v
}

func counterOrZero(fams metricFamilies, name string, labels map[string]string) float64 {
	v, _ := sampleValue(fams, name, labels)
	return v
}

func waitForMetric(t *testing.T, addr, name string, labels map[string]string, timeout time.Duration, desc string, cond func(float64) bool) {
	t.Helper()
	last := "metric absent"
	waitFor(t, timeout, desc, func() bool {
		fams, err := tryScrape(addr)
		if err != nil {
			last = err.Error()
			return false
		}
		v, ok := sampleValue(fams, name, labels)
		if !ok {
			return false
		}
		last = fmt.Sprintf("%g", v)
		return cond(v)
	})
	t.Logf("%s observed at %s", name, last)
}

// --- HTTP sink ---

type sink struct {
	mu       sync.Mutex
	status   int
	requests int
	entries  int
}

func newSink(t *testing.T, status int) (*sink, *httptest.Server) {
	t.Helper()
	s := &sink{status: status}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload []json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&payload)
		s.mu.Lock()
		s.requests++
		s.entries += len(payload)
		status := s.status
		s.mu.Unlock()
		if status >= http.StatusBadRequest {
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return s, srv
}

func (s *sink) counts() (requests, entries int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.entries
}

func (s *sink) setStatus(status int) {
	s.mu.Lock()
	s.status = status
	s.mu.Unlock()
}

// --- config helpers ---

func writeTestFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	return writeTestFile(t, "loghub.yaml", content)
}

// --- tests ---

func TestMonitoringHealthyPipeline(t *testing.T) {
	const pipelineID = "healthy-monitor"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	var lines strings.Builder
	for i := 0; i < 60; i++ {
		fmt.Fprintf(&lines, "[INFO] healthy-%03d\n", i)
	}
	if err := os.WriteFile(input, []byte(lines.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	_, srv := newSink(t, http.StatusOK)

	cfg := writeConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: true
  health_staleness_threshold: "30s"
  stalled_threshold_multiplier: 10
  secret_refresh_interval: "0"
secrets:
  env:
    type: env
pipelines:
  - id: %s
    runtime:
      poll_interval: 20ms
      send_batch_size: 10
      buffer_size: 10
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 2
      base_backoff: 1ms
    source:
      type: file
      file:
        path: "${env:LOGHUB_MONITOR_INPUT}"
        name: monitor-input
        fetch_size: 10
    destinations:
      - type: http
        http:
          url: %q
          timeout: 500ms
`, filepath.Join(dir, "checkpoints"), pipelineID, srv.URL))

	p := startLogHub(t, cfg, "LOGHUB_MONITOR_INPUT="+input)

	pid := map[string]string{"pipeline_id": pipelineID}
	httpDest := map[string]string{"pipeline_id": pipelineID, "destination_type": "http"}
	waitForMetric(t, p.addr, "loghub_entries_sent_total", httpDest, 10*time.Second, "healthy entries delivered", func(v float64) bool { return v >= 60 })

	h := waitForHealth(t, p.addr, 5*time.Second, "healthy /health 200", func(code int, h healthResponse) bool {
		return code == http.StatusOK && h.Status == "up" && h.Running && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "running"
	})
	if _, err := time.ParseDuration(h.Uptime); err != nil {
		t.Errorf("health uptime %q is not a duration: %v", h.Uptime, err)
	}
	ph := singlePipeline(t, h)
	if ph.ID != pipelineID {
		t.Errorf("health pipeline ID = %q, want %q", ph.ID, pipelineID)
	}
	if ph.LastCollect.IsZero() || ph.LastSend.IsZero() {
		t.Errorf("health LastCollect/LastSend not populated: %+v", ph)
	}
	if !ph.LastError.IsZero() || ph.ErrorMsg != "" {
		t.Errorf("healthy pipeline reports an error: %+v", ph)
	}

	resp, err := healthClient.Get("http://" + p.addr + "/health")
	if err != nil {
		t.Fatalf("health request: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("health Content-Type = %q, want application/json", ct)
	}

	// The send counter advances before the durable checkpoint completes;
	// wait for the checkpoint counter so this check does not race the last
	// fsync.
	waitForMetric(t, p.addr, "loghub_checkpoints_saved_total", pid, 10*time.Second,
		"checkpoints caught up with sends", func(v float64) bool { return v >= 6 })
	fams := scrapeMetrics(t, p.addr)
	if got := mustValue(t, fams, "loghub_pipeline_running", pid); got != 1 {
		t.Errorf("loghub_pipeline_running = %v, want 1", got)
	}
	if got := mustValue(t, fams, "loghub_pipeline_status", map[string]string{"pipeline_id": pipelineID, "status": "running"}); got != 1 {
		t.Errorf("loghub_pipeline_status{running} = %v, want 1", got)
	}
	if got := mustValue(t, fams, "loghub_pipeline_status", map[string]string{"pipeline_id": pipelineID, "status": "starting"}); got != 0 {
		t.Errorf("loghub_pipeline_status{starting} = %v, want 0", got)
	}
	if got := mustValue(t, fams, "loghub_poll_interval_seconds", pid); math.Abs(got-0.02) > 1e-9 {
		t.Errorf("loghub_poll_interval_seconds = %v, want 0.02", got)
	}
	if got := mustValue(t, fams, "loghub_entries_collected_total", map[string]string{"pipeline_id": pipelineID, "source_name": "monitor-input"}); got < 60 {
		t.Errorf("loghub_entries_collected_total = %v, want >= 60", got)
	}
	if got := mustValue(t, fams, "loghub_entries_sent_total", httpDest); got < 60 {
		t.Errorf("loghub_entries_sent_total = %v, want >= 60", got)
	}
	if got := mustValue(t, fams, "loghub_batches_collected_total", pid); got < 6 {
		t.Errorf("loghub_batches_collected_total = %v, want >= 6", got)
	}
	if got := mustValue(t, fams, "loghub_batches_sent_total", pid); got < 6 {
		t.Errorf("loghub_batches_sent_total = %v, want >= 6", got)
	}
	if got := mustValue(t, fams, "loghub_checkpoints_saved_total", pid); got < 6 {
		t.Errorf("loghub_checkpoints_saved_total = %v, want >= 6", got)
	}
	count, sum, ok := histogramStats(fams, "loghub_batch_size", pid)
	if !ok || count < 6 || sum < 60 {
		t.Errorf("loghub_batch_size count/sum = %d/%v, want >= 6/60", count, sum)
	}
	count, _, ok = histogramStats(fams, "loghub_collect_duration_seconds", pid)
	if !ok || count < 6 {
		t.Errorf("loghub_collect_duration_seconds count = %d, want >= 6", count)
	}
	count, _, ok = histogramStats(fams, "loghub_send_duration_seconds", pid)
	if !ok || count < 6 {
		t.Errorf("loghub_send_duration_seconds count = %d, want >= 6", count)
	}
	if got := counterOrZero(fams, "loghub_retries_total", httpDest); got != 0 {
		t.Errorf("loghub_retries_total = %v, want 0", got)
	}
	if got := counterOrZero(fams, "loghub_entries_failed_total", httpDest); got != 0 {
		t.Errorf("loghub_entries_failed_total = %v, want 0", got)
	}
	if got := mustValue(t, fams, "loghub_circuit_breaker_state", httpDest); got != 0 {
		t.Errorf("loghub_circuit_breaker_state = %v, want 0 (closed)", got)
	}
	if got := counterOrZero(fams, "loghub_secret_refresh_total", map[string]string{"provider": "env"}); got < 1 {
		t.Errorf("loghub_secret_refresh_total{env} = %v, want >= 1", got)
	}
	if got := counterOrZero(fams, "loghub_secret_refresh_errors_total", map[string]string{"provider": "env"}); got != 0 {
		t.Errorf("loghub_secret_refresh_errors_total{env} = %v, want 0", got)
	}

	p.stop()
}

func TestMonitoringDegradedPipelineMetrics(t *testing.T) {
	const pipelineID = "degraded-monitor"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	var lines strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&lines, "[ERROR] degraded-%03d\n", i)
	}
	if err := os.WriteFile(input, []byte(lines.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	// Always fail at the destination.
	_, srv := newSink(t, http.StatusInternalServerError)

	cfg := writeConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: true
  stalled_threshold_multiplier: 1000
  circuit_breaker_failure_threshold: 2
  circuit_breaker_recovery_timeout: "30s"
  secret_refresh_interval: "0"
pipelines:
  - id: %s
    runtime:
      poll_interval: 20ms
      send_batch_size: 5
      buffer_size: 20
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 2
      base_backoff: 1ms
    source:
      type: file
      file:
        path: %q
        name: degraded-input
        fetch_size: 5
    destinations:
      - type: http
        http:
          url: %q
          timeout: 500ms
    durability:
      dlq:
        enabled: true
        dir: %q
      disk_buffer:
        enabled: true
        dir: %q
`, filepath.Join(dir, "checkpoints"), pipelineID, input, srv.URL,
		filepath.Join(dir, "dlq"), filepath.Join(dir, "disk-buffer")))

	p := startLogHub(t, cfg)

	httpDest := map[string]string{"pipeline_id": pipelineID, "destination_type": "http"}
	pid := map[string]string{"pipeline_id": pipelineID}

	waitForMetric(t, p.addr, "loghub_entries_failed_total", httpDest, 10*time.Second, "destination failures recorded", func(v float64) bool { return v >= 5 })
	waitForMetric(t, p.addr, "loghub_dlq_entries_total", pid, 10*time.Second, "DLQ entries recorded", func(v float64) bool { return v >= 5 })
	waitForMetric(t, p.addr, "loghub_circuit_breaker_state", httpDest, 10*time.Second, "circuit breaker open", func(v float64) bool { return v == 1 })
	waitForMetric(t, p.addr, "loghub_disk_buffer_batches", pid, 10*time.Second, "disk buffer spill", func(v float64) bool { return v >= 1 })

	h := waitForHealth(t, p.addr, 5*time.Second, "degraded /health 200", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "degraded"
	})
	if h.Status != "up" || !h.Running {
		t.Errorf("degraded health = %+v, want status up and running", h)
	}
	ph := singlePipeline(t, h)
	if ph.LastError.IsZero() || ph.ErrorMsg == "" {
		t.Errorf("degraded pipeline does not report LastError/ErrorMsg: %+v", ph)
	}

	fams := scrapeMetrics(t, p.addr)
	if got := mustValue(t, fams, "loghub_pipeline_status", map[string]string{"pipeline_id": pipelineID, "status": "degraded"}); got != 1 {
		t.Errorf("loghub_pipeline_status{degraded} = %v, want 1", got)
	}
	if got := mustValue(t, fams, "loghub_pipeline_running", pid); got != 1 {
		t.Errorf("loghub_pipeline_running = %v, want 1", got)
	}
	if got := mustValue(t, fams, "loghub_entries_failed_total", httpDest); got < 5 {
		t.Errorf("loghub_entries_failed_total = %v, want >= 5", got)
	}
	if got := mustValue(t, fams, "loghub_retries_total", httpDest); got < 2 {
		t.Errorf("loghub_retries_total = %v, want >= 2", got)
	}
	if got := mustValue(t, fams, "loghub_dlq_entries_total", pid); got < 5 {
		t.Errorf("loghub_dlq_entries_total = %v, want >= 5", got)
	}
	if got := mustValue(t, fams, "loghub_circuit_breaker_tripped_total", httpDest); got < 1 {
		t.Errorf("loghub_circuit_breaker_tripped_total = %v, want >= 1", got)
	}
	if got := mustValue(t, fams, "loghub_disk_buffer_batches", pid); got < 1 {
		t.Errorf("loghub_disk_buffer_batches = %v, want >= 1", got)
	}

	p.stop()
}

func TestMonitoringDestinationRecovery(t *testing.T) {
	const pipelineID = "recovery-monitor"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	var lines strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&lines, "[INFO] recovery-%02d\n", i)
	}
	if err := os.WriteFile(input, []byte(lines.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	s, srv := newSink(t, http.StatusInternalServerError)

	cfg := writeConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: true
  stalled_threshold_multiplier: 50
  circuit_breaker_failure_threshold: 2
  circuit_breaker_recovery_timeout: "150ms"
  secret_refresh_interval: "0"
pipelines:
  - id: %s
    runtime:
      poll_interval: 20ms
      send_batch_size: 5
      buffer_size: 20
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 1
      base_backoff: 1ms
    source:
      type: file
      file:
        path: %q
        name: recovery-input
        fetch_size: 5
    destinations:
      - type: http
        http:
          url: %q
          timeout: 500ms
    durability:
      disk_buffer:
        enabled: true
        dir: %q
`, filepath.Join(dir, "checkpoints"), pipelineID, input, srv.URL, filepath.Join(dir, "disk-buffer")))

	p := startLogHub(t, cfg)
	pid := map[string]string{"pipeline_id": pipelineID}
	httpDest := map[string]string{"pipeline_id": pipelineID, "destination_type": "http"}

	waitForMetric(t, p.addr, "loghub_circuit_breaker_state", httpDest, 10*time.Second, "breaker open", func(v float64) bool { return v == 1 })
	waitForMetric(t, p.addr, "loghub_disk_buffer_batches", pid, 10*time.Second, "batch spilled to disk", func(v float64) bool { return v >= 1 })
	waitForHealth(t, p.addr, 5*time.Second, "degraded before recovery", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "degraded"
	})

	// Destination recovers. The breaker must probe, close, the disk buffer
	// must drain, and the pipeline must return to running.
	s.setStatus(http.StatusOK)
	waitForMetric(t, p.addr, "loghub_circuit_breaker_state", httpDest, 10*time.Second, "breaker closed after probe", func(v float64) bool { return v == 0 })
	waitForMetric(t, p.addr, "loghub_disk_buffer_batches", pid, 10*time.Second, "disk buffer drained", func(v float64) bool { return v == 0 })
	waitForHealth(t, p.addr, 10*time.Second, "running after recovery", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "running"
	})

	fams := scrapeMetrics(t, p.addr)
	if got := mustValue(t, fams, "loghub_circuit_breaker_tripped_total", httpDest); got < 1 {
		t.Errorf("loghub_circuit_breaker_tripped_total = %v, want >= 1", got)
	}
	if got := mustValue(t, fams, "loghub_circuit_breaker_state", httpDest); got != 0 {
		t.Errorf("loghub_circuit_breaker_state = %v, want 0 after recovery", got)
	}
	if got := mustValue(t, fams, "loghub_pipeline_status", map[string]string{"pipeline_id": pipelineID, "status": "running"}); got != 1 {
		t.Errorf("loghub_pipeline_status{running} = %v, want 1 after recovery", got)
	}
	if got := counterOrZero(fams, "loghub_entries_sent_total", httpDest); got < 5 {
		t.Errorf("loghub_entries_sent_total = %v, want the retained batch delivered after recovery", got)
	}

	p.stop()
}

func TestMonitoringStalledPipelineHealth(t *testing.T) {
	const pipelineID = "stalled-monitor"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	if err := os.WriteFile(input, []byte("[INFO] before-outage\n[INFO] second\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, srv := newSink(t, http.StatusOK)

	cfg := writeConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: true
  stalled_threshold_multiplier: 3
  secret_refresh_interval: "0"
pipelines:
  - id: %s
    runtime:
      poll_interval: 50ms
      send_batch_size: 10
      buffer_size: 10
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 2
      base_backoff: 1ms
    source:
      type: file
      file:
        path: %q
        name: stalled-input
        fetch_size: 10
    destinations:
      - type: http
        http:
          url: %q
          timeout: 500ms
`, filepath.Join(dir, "checkpoints"), pipelineID, input, srv.URL))

	p := startLogHub(t, cfg)
	httpDest := map[string]string{"pipeline_id": pipelineID, "destination_type": "http"}
	waitForMetric(t, p.addr, "loghub_entries_sent_total", httpDest, 10*time.Second, "initial entries delivered", func(v float64) bool { return v >= 2 })

	// Remove the source file: every subsequent collect fails, so no
	// successful collect or send happens and the pipeline must stall.
	if err := os.Remove(input); err != nil {
		t.Fatal(err)
	}

	h := waitForHealth(t, p.addr, 10*time.Second, "stalled /health 503", func(code int, h healthResponse) bool {
		return code == http.StatusServiceUnavailable && strings.Contains(h.Reason, "stalled")
	})
	if h.Status != "down" || !h.Running {
		t.Errorf("stalled health = %+v, want status down and running true", h)
	}
	ph := singlePipeline(t, h)
	if ph.Status != "stalled" {
		t.Errorf("stalled pipeline status = %q, want stalled", ph.Status)
	}
	if !strings.Contains(ph.ErrorMsg, "failed to stat file") {
		t.Errorf("stalled pipeline ErrorMsg = %q, want the source error", ph.ErrorMsg)
	}

	fams := scrapeMetrics(t, p.addr)
	if got := mustValue(t, fams, "loghub_pipeline_status", map[string]string{"pipeline_id": pipelineID, "status": "stalled"}); got != 1 {
		t.Errorf("loghub_pipeline_status{stalled} = %v, want 1", got)
	}
	if got := mustValue(t, fams, "loghub_pipeline_running", map[string]string{"pipeline_id": pipelineID}); got != 1 {
		t.Errorf("loghub_pipeline_running = %v, want 1 while stalled", got)
	}
	// A failing source is counted in loghub_collect_errors_total, which the
	// LogHubCollectionErrors alert watches; it is not a send failure.
	if got := mustValue(t, fams, "loghub_collect_errors_total", map[string]string{"pipeline_id": pipelineID, "source_name": "stalled-input"}); got < 1 {
		t.Errorf("loghub_collect_errors_total = %v, want >= 1 during a source outage", got)
	}
	if got := counterOrZero(fams, "loghub_entries_failed_total", httpDest); got != 0 {
		t.Errorf("loghub_entries_failed_total = %v, want 0 (the destination did not fail)", got)
	}

	// Recreating the file makes collect succeed again; the pipeline must
	// leave the stalled state on its own.
	if err := os.WriteFile(input, []byte("[INFO] after-recovery\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForHealth(t, p.addr, 10*time.Second, "recovered /health 200 running", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "running"
	})

	p.stop()
}

func TestMonitoringHealthStaleness(t *testing.T) {
	const pipelineID = "stale-monitor"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	if err := os.WriteFile(input, []byte("[INFO] stale-check\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, srv := newSink(t, http.StatusOK)

	// A 1s poll interval with a 150ms staleness budget means the health
	// endpoint must report stale for most of every poll cycle.
	cfg := writeConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: true
  health_staleness_threshold: "150ms"
  stalled_threshold_multiplier: 100
  secret_refresh_interval: "0"
pipelines:
  - id: %s
    runtime:
      poll_interval: 1s
      send_batch_size: 10
      buffer_size: 10
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 2
      base_backoff: 1ms
    source:
      type: file
      file:
        path: %q
        name: stale-input
        fetch_size: 10
    destinations:
      - type: http
        http:
          url: %q
          timeout: 500ms
`, filepath.Join(dir, "checkpoints"), pipelineID, input, srv.URL))

	p := startLogHub(t, cfg)
	httpDest := map[string]string{"pipeline_id": pipelineID, "destination_type": "http"}
	waitForMetric(t, p.addr, "loghub_entries_sent_total", httpDest, 10*time.Second, "first entry delivered", func(v float64) bool { return v >= 1 })

	h := waitForHealth(t, p.addr, 10*time.Second, "stale /health 503", func(code int, h healthResponse) bool {
		return code == http.StatusServiceUnavailable && strings.Contains(h.Reason, "is stale")
	})
	if h.Status != "down" || !h.Running {
		t.Errorf("stale health = %+v, want status down and running true", h)
	}
	ph := singlePipeline(t, h)
	if ph.Status != "running" {
		t.Errorf("stale pipeline status = %q, want running (staleness is a health verdict, not a pipeline state)", ph.Status)
	}
	if ph.LastCollect.IsZero() || ph.LastSend.IsZero() {
		t.Errorf("stale health is missing LastCollect/LastSend: %+v", ph)
	}

	p.stop()
}

func TestMonitoringMetricsDisabled(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	if err := os.WriteFile(input, []byte("[INFO] metrics-disabled\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, srv := newSink(t, http.StatusOK)

	cfg := writeConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: false
  secret_refresh_interval: "0"
pipelines:
  - id: disabled-monitor
    runtime:
      poll_interval: 20ms
      send_batch_size: 10
      buffer_size: 10
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 2
      base_backoff: 1ms
    source:
      type: file
      file:
        path: %q
        fetch_size: 10
    destinations:
      - type: http
        http:
          url: %q
          timeout: 500ms
`, filepath.Join(dir, "checkpoints"), input, srv.URL))

	p := startLogHub(t, cfg)
	waitFor(t, 10*time.Second, "pipeline activity with metrics disabled", func() bool {
		_, entries := s.counts()
		return entries >= 1
	})

	if _, _, err := fetchHealth(p.addr); err == nil {
		t.Errorf("GET /health succeeded although metrics_enabled is false")
	}
	conn, err := net.DialTimeout("tcp", p.addr, 300*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Errorf("metrics address %s is listening although metrics_enabled is false", p.addr)
	}

	p.stop()
}

func TestMonitoringDroppedEntries(t *testing.T) {
	const pipelineID = "dropped-monitor"
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	var lines strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&lines, "[WARN] rejected-%02d\n", i)
	}
	if err := os.WriteFile(input, []byte(lines.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	// HTTP 400 is a deterministic per-entry rejection (poison pill), not a
	// retryable outage. Without a DLQ the entries are dropped.
	_, srv := newSink(t, http.StatusBadRequest)

	cfg := writeConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: true
  stalled_threshold_multiplier: 1000
  secret_refresh_interval: "0"
pipelines:
  - id: %s
    runtime:
      poll_interval: 20ms
      send_batch_size: 5
      buffer_size: 20
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 5
      base_backoff: 1ms
    source:
      type: file
      file:
        path: %q
        name: dropped-input
        fetch_size: 5
    destinations:
      - type: http
        http:
          url: %q
          timeout: 500ms
`, filepath.Join(dir, "checkpoints"), pipelineID, input, srv.URL))

	p := startLogHub(t, cfg)
	httpDest := map[string]string{"pipeline_id": pipelineID, "destination_type": "http"}

	waitForMetric(t, p.addr, "loghub_entries_dropped_total", map[string]string{"pipeline_id": pipelineID, "reason": "destination_rejected"}, 10*time.Second, "dropped entry metric", func(v float64) bool { return v >= 20 })

	fams := scrapeMetrics(t, p.addr)
	if got := mustValue(t, fams, "loghub_entries_dropped_total", map[string]string{"pipeline_id": pipelineID, "reason": "destination_rejected"}); got < 20 {
		t.Errorf("loghub_entries_dropped_total = %v, want >= 20", got)
	}
	if got := mustValue(t, fams, "loghub_entries_failed_total", httpDest); got < 20 {
		t.Errorf("loghub_entries_failed_total = %v, want >= 20", got)
	}
	if got := counterOrZero(fams, "loghub_retries_total", httpDest); got != 0 {
		t.Errorf("loghub_retries_total = %v, want 0 for a poison-pill rejection", got)
	}
	if got := counterOrZero(fams, "loghub_entries_sent_total", httpDest); got != 0 {
		t.Errorf("loghub_entries_sent_total = %v, want 0", got)
	}
	// Per-entry rejections degrade the pipeline: /health stays 200, but
	// the pipeline reports degraded with the rejection as its last error.
	h := waitForHealth(t, p.addr, 5*time.Second, "degraded after rejections", func(code int, h healthResponse) bool {
		return code == http.StatusOK && len(h.Pipelines) == 1 && h.Pipelines[0].Status == "degraded"
	})
	if ph := singlePipeline(t, h); ph.LastError.IsZero() || !strings.Contains(ph.ErrorMsg, "rejected") {
		t.Errorf("pipeline LastError=%v ErrorMsg=%q, want the rejection recorded", ph.LastError, ph.ErrorMsg)
	}
	fams = scrapeMetrics(t, p.addr)
	if got := mustValue(t, fams, "loghub_pipeline_status", map[string]string{"pipeline_id": pipelineID, "status": "degraded"}); got != 1 {
		t.Errorf("loghub_pipeline_status{degraded} = %v, want 1", got)
	}

	p.stop()
}
