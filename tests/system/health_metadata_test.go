//go:build !windows

package system_test

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// TestHealthReportsStartedAtAndAppliesStaleness checks two documented
// /health fields: StartedAt must be populated, and when no successful
// collect or send has happened since startup, staleness must be measured
// from that start time.
func TestHealthReportsStartedAtAndAppliesStaleness(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "missing.log") // never created
	c := newTestConfig(t)
	p := newPipeline("health-start", sourceFile(input, "health-source", 10), destDummy(nil))
	p["runtime"] = map[string]any{
		"poll_interval":       "100ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "2s",
	}
	c.Global["health_staleness_threshold"] = "1s"
	// Keep the stall transition out of the way so this checks staleness,
	// not the separate Stalled state.
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("loghub log:\n%s", proc.log())
		}
	})
	waitForHealth(t, proc.addr, 10*time.Second, "pipeline appears", func(code int, h healthResponse) bool {
		return len(h.Pipelines) == 1
	})
	_, h, err := fetchHealth(proc.addr)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if len(h.Pipelines) != 1 {
		t.Fatalf("pipelines = %d", len(h.Pipelines))
	}
	if h.Pipelines[0].StartedAt.IsZero() {
		t.Errorf("/health StartedAt is zero; staleness cannot be measured from startup")
	}

	// With no collect or send and a 1 s staleness threshold, the endpoint
	// must become 503 within a few seconds.
	waitForHealth(t, proc.addr, 10*time.Second, "stale pipeline reports 503", func(code int, h healthResponse) bool {
		return code == http.StatusServiceUnavailable
	})
	proc.stop()
}
