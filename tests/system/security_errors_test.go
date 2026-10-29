//go:build !windows

package system_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestHealthDoesNotExposeDestinationURLSecrets uses a destination URL that
// carries a secret in its query string and a closed port. Connection errors
// from net/http include the URL; /health's ErrorMsg and the DLQ error must
// not repeat it.
func TestHealthDoesNotExposeDestinationURLSecrets(t *testing.T) {
	const secret = "url-secret-token-123"
	dir := t.TempDir()
	input := filepath.Join(dir, "audit.log")
	dlqDir := filepath.Join(dir, "dlq")
	g := newOracleGenerator()
	g.appendFile(t, input, 3)
	url := "http://" + freeAddr(t) + "/ingest?token=" + secret

	c := newTestConfig(t)
	p := newPipeline("url-secret", oracleFileSource(input, 3), map[string]any{
		"type": "http",
		"http": map[string]any{"url": url, "timeout": "200ms"},
	})
	p["runtime"] = map[string]any{
		"poll_interval": "10ms", "send_batch_size": 3, "buffer_size": 4,
		"checkpoint_interval": "0", "shutdown_timeout": "2s",
	}
	p["retry"] = map[string]any{"max_attempts": 1, "base_backoff": "1ms"}
	p["processors"] = []any{oracleJSONParse()}
	p["durability"] = map[string]any{"dlq": map[string]any{"enabled": true, "dir": dlqDir}}
	c.Global["stalled_threshold_multiplier"] = 1000
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "url-secret")
	waitFor(t, 10*time.Second, "DLQ written", func() bool {
		return len(readDLQ(t, dlqDir, "url-secret")) > 0
	})
	_, h, err := fetchHealth(proc.addr)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	proc.stop()

	if strings.Contains(proc.log(), secret) {
		t.Fatalf("log exposes the destination URL secret")
	}
	for _, ph := range h.Pipelines {
		if strings.Contains(ph.ErrorMsg, secret) {
			t.Fatalf("/health ErrorMsg exposes the destination URL secret: %q", ph.ErrorMsg)
		}
	}
	for _, fb := range readDLQ(t, dlqDir, "url-secret") {
		if strings.Contains(fb.Error, secret) {
			t.Fatalf("DLQ error exposes the destination URL secret: %q", fb.Error)
		}
	}
}
