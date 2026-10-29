//go:build !windows

package system_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeRawConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "loghub.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func minimalRawConfig(t *testing.T, runtimeBlock string) string {
	t.Helper()
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	if err := os.WriteFile(input, []byte("[INFO] seed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: false
pipelines:
  - id: cfg-test
%s
    source:
      type: file
      file:
        path: %q
    destinations:
      - type: dummy
`, filepath.Join(dir, "checkpoints"), runtimeBlock, input)
}

func TestCLIVersion(t *testing.T) {
	stdout, stderr, code := runBinary(t, "--version")
	if code != 0 {
		t.Fatalf("--version exit %d, stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "loghub") {
		t.Fatalf("--version output = %q", stdout)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want int
	}{
		{
			name: "valid minimal",
			yaml: minimalRawConfig(t, `    runtime:
      poll_interval: 20ms`),
			want: 0,
		},
		{
			name: "unknown top-level field",
			yaml: minimalRawConfig(t, "    runtime:\n      poll_interval: 20ms\n  bogus: true"),
			want: 1,
		},
		{
			name: "unknown plugin property",
			yaml: strings.Replace(minimalRawConfig(t, "    runtime:\n      poll_interval: 20ms"),
				"        path:", "        bogus: true\n        path:", 1),
			want: 1,
		},
		{
			name: "missing destinations",
			yaml: `global:
  checkpoints_dir: "/tmp/loghub-test-cp"
  metrics_enabled: false
pipelines:
  - id: no-dest
    source:
      type: file
      file:
        path: "/tmp/nope"
`,
			want: 1,
		},
		{
			name: "invalid log level",
			yaml: `global:
  checkpoints_dir: "/tmp/loghub-test-cp"
  log_level: verbose
  metrics_enabled: false
pipelines:
  - id: bad-level
    source:
      type: file
      file:
        path: "/tmp/nope"
    destinations:
      - type: dummy
`,
			want: 1,
		},
		{
			name: "negative runtime buffer",
			yaml: minimalRawConfig(t, "    runtime:\n      poll_interval: 20ms\n      buffer_size: -1"),
			want: 1,
		},
		{
			name: "removed flat pipeline field",
			yaml: strings.Replace(minimalRawConfig(t, "    runtime:\n      poll_interval: 20ms"),
				"    runtime:", "    poll_interval: 1s\n    runtime:", 1),
			want: 1,
		},
		{
			name: "negative max_retries",
			yaml: minimalRawConfig(t, "    runtime:\n      poll_interval: 20ms\n    retry:\n      max_attempts: -1"),
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := writeRawConfig(t, tt.yaml)
			_, stderr, code := runBinary(t, "--config", cfg, "--validate")
			if code != tt.want {
				t.Fatalf("--validate exit = %d, want %d\n%s", code, tt.want, stderr)
			}
		})
	}
}

func TestPollIntervalZeroRejected(t *testing.T) {
	cfg := writeRawConfig(t, minimalRawConfig(t, "    runtime:\n      poll_interval: \"0\""))
	_, stderr, code := runBinary(t, "--config", cfg, "--validate")
	if code != 1 {
		t.Fatalf("--validate exit = %d, want 1 (non-positive poll_interval must be rejected)\n%s", code, stderr)
	}
}

func TestNegativePollIntervalRejected(t *testing.T) {
	cfg := writeRawConfig(t, minimalRawConfig(t, "    runtime:\n      poll_interval: \"-1s\""))
	_, stderr, code := runBinary(t, "--config", cfg, "--validate")
	if code != 1 {
		t.Fatalf("--validate exit = %d, want 1 (negative poll_interval must be rejected)\n%s", code, stderr)
	}
}

func TestQueryOnlySQLiteConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := writeRawConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  metrics_enabled: false
pipelines:
  - id: query-only
    source:
      type: sqlite
      sqlite:
        database: %q
        query: "SELECT id, event_time, message FROM events"
    destinations:
      - type: dummy
`, filepath.Join(dir, "checkpoints"), filepath.Join(dir, "db.sqlite")))
	_, stderr, code := runBinary(t, "--config", cfg, "--validate")
	if code != 0 {
		t.Fatalf("--validate exit = %d, want 0 for query-only SQLite\n%s", code, stderr)
	}
}

func TestSyslogSourceRejected(t *testing.T) {
	dir := t.TempDir()
	cfg := writeRawConfig(t, fmt.Sprintf(`global:
  checkpoints_dir: %q
  metrics_enabled: false
pipelines:
  - id: syslog
    source:
      type: syslog
      syslog:
        network: tcp
        address: "127.0.0.1:5514"
    destinations:
      - type: dummy
`, filepath.Join(dir, "checkpoints")))
	_, stderr, code := runBinary(t, "--config", cfg, "--validate")
	if code == 0 {
		t.Fatalf("--validate accepted syslog although no implementation exists\n%s", stderr)
	}
}

func TestUnknownProcessorFailsStartup(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	if err := os.WriteFile(input, []byte("[INFO] seed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newTestConfig(t)
	p := newPipeline("unknown-processor", sourceFile(input, "in", 10), destDummy(nil))
	p["processors"] = []any{map[string]any{"type": "does_not_exist", "does_not_exist": map[string]any{}}}
	c.add(p)
	cfg := c.write(t)
	addr := freeAddr(t)
	_, stderr, code, timedOut := runBinaryFor(t, 3*time.Second, nil,
		"--config", cfg, "--metrics", addr)
	if timedOut {
		t.Fatalf("pipeline started without a valid processor; expected startup failure\n%s", stderr)
	}
	if code == 0 {
		t.Fatalf("startup exit = 0, want non-zero for an unknown processor\n%s", stderr)
	}
}

func TestUnknownSourceTypeFailsStartup(t *testing.T) {
	c := newTestConfig(t)
	c.add(newPipeline("unknown-source",
		map[string]any{"type": "no_such_source", "no_such_source": map[string]any{}},
		destDummy(nil)))
	cfg := c.write(t)
	addr := freeAddr(t)
	_, stderr, code, timedOut := runBinaryFor(t, 5*time.Second, nil, "--config", cfg, "--metrics", addr)
	if timedOut {
		t.Fatalf("process hung for an unknown source type\n%s", stderr)
	}
	if code == 0 {
		t.Fatalf("startup exit = 0, want non-zero\n%s", stderr)
	}
}

func TestPipelineIDPathTraversalRejected(t *testing.T) {
	dir := t.TempDir()
	c := newTestConfig(t)
	p := newPipeline("../escape", sourceFile(filepath.Join(dir, "in.log"), "in", 10), destDummy(nil))
	p["durability"] = map[string]any{
		"dlq":         map[string]any{"enabled": true, "dir": filepath.Join(dir, "dlq")},
		"disk_buffer": map[string]any{"enabled": true, "dir": filepath.Join(dir, "buffer")},
	}
	c.add(p)
	cfg := c.write(t)
	_, stderr, code := runBinary(t, "--config", cfg, "--validate")
	if code == 0 {
		t.Fatalf("--validate accepted a traversal pipeline ID\n%s", stderr)
	}
}

func TestStrictResolutionMissingSecretFailsStartup(t *testing.T) {
	c := newTestConfig(t)
	c.Global["strict_resolution"] = true
	c.Secrets = map[string]any{"env": map[string]any{"type": "env"}}
	c.add(newPipeline("strict-secret",
		sourceFile("${env:LOGHUB_MISSING_INPUT}", "secret-input", 10),
		destDummy(nil)))
	cfg := c.write(t)
	addr := freeAddr(t)
	_, stderr, code, timedOut := runBinaryFor(t, 5*time.Second,
		[]string{"LOGHUB_MISSING_INPUT="}, "--config", cfg, "--metrics", addr)
	if timedOut {
		t.Fatalf("strict secret resolution did not fail startup\n%s", stderr)
	}
	if code == 0 {
		t.Fatalf("strict secret resolution exit = 0, want non-zero\n%s", stderr)
	}
}

func TestNonStrictResolutionStartsAndReportsCollectError(t *testing.T) {
	c := newTestConfig(t)
	c.Global["strict_resolution"] = false
	c.Secrets = map[string]any{"env": map[string]any{"type": "env"}}
	c.add(newPipeline("nonstrict-secret",
		sourceFile("${env:LOGHUB_MISSING_INPUT}", "secret-input", 10),
		destDummy(nil)))
	cfg := c.write(t)
	p := startProc(t, cfg, "LOGHUB_MISSING_INPUT=")
	waitForMetric(t, p.addr, "loghub_collect_errors_total",
		map[string]string{"pipeline_id": "nonstrict-secret", "source_name": "secret-input"},
		10*time.Second, "collect error for unresolved path", func(v float64) bool { return v >= 1 })
	code, h, err := fetchHealth(p.addr)
	if err != nil || code != 200 {
		t.Fatalf("health = %d, %v; want 200 while degraded\n%s", code, err, p.log())
	}
	ph := h.Pipelines[0]
	if ph.ErrorMsg == "" {
		t.Fatalf("health does not report the unresolved source error: %+v", ph)
	}
	p.stop()
}

func TestDataDirLayout(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.log")
	if err := os.WriteFile(input, []byte("[INFO] data-dir\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newTestConfig(t)
	c.Global["data_dir"] = filepath.Join(dir, "data")
	p := newPipeline("data-dir", sourceFile(input, "in", 10), destDummy(nil))
	p["durability"] = map[string]any{
		"dlq":         map[string]any{"enabled": true},
		"disk_buffer": map[string]any{"enabled": true},
	}
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForRunning(t, proc, "data-dir")
	proc.stop()
	for _, sub := range []string{"dlq/data-dir", "disk-buffer/data-dir"} {
		path := filepath.Join(dir, "data", sub)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected %s: %v", path, err)
		}
	}
}
