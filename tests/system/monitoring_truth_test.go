//go:build !windows

package system_test

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestCountersArePublishedAtZero checks the claim that Prometheus counters
// exist at 0 before their first event, so rate() and increase() see the
// first sample. A series that appears only after the first event is
// invisible to alerting for the first scrape interval.
func TestCountersArePublishedAtZero(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "missing.log") // no source data, no sends
	c := newTestConfig(t)
	p := newPipeline("zero-counters", sourceFile(input, "zero-source", 10), destDummy(nil))
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitFor(t, 10*time.Second, "pipeline to start", func() bool {
		_, h, err := fetchHealth(proc.addr)
		return err == nil && len(h.Pipelines) == 1
	})
	fams := scrapeMetrics(t, proc.addr)
	proc.stop()

	want := []struct {
		name   string
		labels map[string]string
	}{
		{"loghub_entries_collected_total", map[string]string{"pipeline_id": "zero-counters", "source_name": "zero-source"}},
		{"loghub_entries_sent_total", map[string]string{"pipeline_id": "zero-counters", "destination_type": "dummy"}},
		{"loghub_entries_failed_total", map[string]string{"pipeline_id": "zero-counters", "destination_type": "dummy"}},
		{"loghub_batches_collected_total", map[string]string{"pipeline_id": "zero-counters"}},
		{"loghub_batches_sent_total", map[string]string{"pipeline_id": "zero-counters"}},
		{"loghub_retries_total", map[string]string{"pipeline_id": "zero-counters", "destination_type": "dummy"}},
		{"loghub_checkpoints_saved_total", map[string]string{"pipeline_id": "zero-counters"}},
		{"loghub_dlq_entries_total", map[string]string{"pipeline_id": "zero-counters"}},
		{"loghub_circuit_breaker_tripped_total", map[string]string{"pipeline_id": "zero-counters", "destination_type": "dummy"}},
	}
	for _, m := range want {
		v, ok := sampleValue(fams, m.name, m.labels)
		if !ok {
			t.Errorf("%s%v is absent before its first event, so a rate/increase alert cannot see the first sample", m.name, m.labels)
			continue
		}
		if v != 0 {
			t.Errorf("%s%v = %v before its first event, want 0", m.name, m.labels, v)
		}
	}
}

// TestHealthDoesNotReportHealthyWhileDropping asks whether /health tells an
// operator that records were lost when a destination rejects them and no
// DLQ is configured. The documented alert is the dropped counter; this test
// records whether the health endpoint also changes.
func TestHealthDoesNotReportHealthyWhileDropping(t *testing.T) {
	const total = 5
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, total)
	_, srv := newSink(t, http.StatusBadRequest)

	c := newTestConfig(t)
	p := newPipeline("drop-health", oracleFileSource(input, 10), destHTTP(srv.URL))
	p["runtime"] = map[string]any{
		"poll_interval":       "50ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 2, "base_backoff": "1ms"}
	p["processors"] = []any{oracleJSONParse()}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForMetric(t, proc.addr, "loghub_entries_dropped_total",
		map[string]string{"pipeline_id": "drop-health", "reason": "destination_rejected"},
		15*time.Second, "entries dropped", func(v float64) bool { return v >= total })
	code, h, err := fetchHealth(proc.addr)
	proc.stop()
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if code == http.StatusOK {
		status := ""
		if len(h.Pipelines) > 0 {
			status = h.Pipelines[0].Status
		}
		t.Fatalf("/health returns 200 (pipeline %q) in the same instant that %d records were dropped without a DLQ; "+
			"only the dropped counter alerts, and an operator watching liveness would see a healthy process", status, total)
	}
}

// TestCheckpointFailureIsObservable injects EIO on fsync so every
// checkpoint write fails while delivery succeeds. The send counter must
// advance, the saved-checkpoint counter must not, the error must be logged
// and /health must reflect the failure.
func TestCheckpointFailureIsObservable(t *testing.T) {
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace is not installed")
	}
	const total = 20
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	g := newOracleGenerator()
	g.appendFile(t, input, total)

	c := newTestConfig(t)
	p := newPipeline("eio-checkpoint", oracleFileSource(input, 10), destDummy(nil))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "2s",
	}
	p["processors"] = []any{oracleJSONParse()}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	prefix := []string{"strace", "-f", "-qq", "-o", os.DevNull, "-e", "inject=fsync:error=EIO"}
	proc := startProcCommand(t, prefix, []string{loghubBinary, "--config", cfg})
	waitForPipelineUp(t, proc, "eio-checkpoint")
	waitForMetric(t, proc.addr, "loghub_entries_sent_total",
		map[string]string{"pipeline_id": "eio-checkpoint", "destination_type": "dummy"},
		15*time.Second, "entries delivered", func(v float64) bool { return v >= total })
	waitForLog(t, proc, "Failed to save checkpoint", 10*time.Second)

	fams := scrapeMetrics(t, proc.addr)
	if got := counterValue(fams, "loghub_checkpoints_saved_total", map[string]string{"pipeline_id": "eio-checkpoint"}); got != 0 {
		t.Errorf("checkpoints_saved_total = %v despite every fsync failing, want 0", got)
	}
	code, h, err := fetchHealth(proc.addr)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if code == http.StatusOK {
		status := ""
		if len(h.Pipelines) > 0 {
			status = h.Pipelines[0].Status
		}
		t.Fatalf("checkpoint writes are failing but /health reports 200 (status %q); the only signal is a log line and a counter that stopped increasing",
			status)
	}
	proc.stop()
}
