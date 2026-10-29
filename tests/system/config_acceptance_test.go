//go:build !windows

package system_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestValidateAcceptsExactlyWhatTheRuntimeStarts enforces the configuration
// contract: `loghub --validate` must accept a config if and only if the
// runtime can start with it. Validation that disagrees with startup makes a
// pre-flight check useless.
func TestValidateAcceptsExactlyWhatTheRuntimeStarts(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return path
	}
	input := filepath.Join(dir, "input.log")
	if err := os.WriteFile(input, []byte("[INFO] one\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	output := filepath.Join(dir, "out.jsonl")
	checkpoints := filepath.Join(dir, "checkpoints")

	valid := map[string]string{
		"file-local": `
global: {checkpoints_dir: "%[1]s", log_level: warn, metrics_enabled: false}
pipelines:
  - id: p
    runtime: {poll_interval: 20ms, shutdown_timeout: 1s}
    source: {type: file, file: {path: "%[2]s", fetch_size: 10}}
    destinations: [{type: local_jsonl, local_jsonl: {path: "%[3]s"}}]
`,
		"sqlite-dummy": `
global: {checkpoints_dir: "%[1]s", log_level: warn, metrics_enabled: false}
pipelines:
  - id: p
    runtime: {poll_interval: 20ms, shutdown_timeout: 1s}
    source: {type: sqlite, sqlite: {database: "%[2]s", table: t, timestamp_column: ts, fetch_size: 10}}
    destinations: [{type: dummy, dummy: {}}]
`,
		"two-pipelines": `
global: {checkpoints_dir: "%[1]s", log_level: warn, metrics_enabled: false}
pipelines:
  - id: a
    source: {type: file, file: {path: "%[2]s", fetch_size: 10}}
    destinations: [{type: dummy, dummy: {}}]
  - id: b
    source: {type: file, file: {path: "%[2]s", fetch_size: 10}}
    destinations: [{type: dummy, dummy: {}}]
`,
	}
	_ = output
	for name, tmpl := range valid {
		name, tmpl := name, tmpl
		t.Run("valid/"+name, func(t *testing.T) {
			cfg := write(name+".yaml", sprintfConfig(tmpl, checkpoints, input, output))
			assertValidateMatchesStart(t, cfg, true)
		})
	}

	invalid := map[string]string{
		"unknown-source": `
global: {checkpoints_dir: "%[1]s"}
pipelines:
  - id: p
    source: {type: nope, nope: {}}
    destinations: [{type: dummy, dummy: {}}]
`,
		"missing-destination": `
global: {checkpoints_dir: "%[1]s"}
pipelines:
  - id: p
    source: {type: file, file: {path: "%[2]s"}}
`,
		"path-traversal-id": `
global: {checkpoints_dir: "%[1]s"}
pipelines:
  - id: "../escape"
    source: {type: file, file: {path: "%[2]s"}}
    destinations: [{type: dummy, dummy: {}}]
`,
		"duplicate-id": `
global: {checkpoints_dir: "%[1]s"}
pipelines:
  - id: p
    source: {type: file, file: {path: "%[2]s"}}
    destinations: [{type: dummy, dummy: {}}]
  - id: p
    source: {type: file, file: {path: "%[2]s"}}
    destinations: [{type: dummy, dummy: {}}]
`,
		"bad-duration": `
global: {checkpoints_dir: "%[1]s"}
pipelines:
  - id: p
    runtime: {poll_interval: nonsense}
    source: {type: file, file: {path: "%[2]s"}}
    destinations: [{type: dummy, dummy: {}}]
`,
		"unknown-property": `
global: {checkpoints_dir: "%[1]s"}
pipelines:
  - id: p
    source: {type: file, file: {path: "%[2]s", mystery: 1}}
    destinations: [{type: dummy, dummy: {}}]
`,
		"mssql-without-auth": `
global: {checkpoints_dir: "%[1]s"}
pipelines:
  - id: p
    source: {type: mssql, mssql: {server: x, database: d, table: t, timestamp_column: ts}}
    destinations: [{type: dummy, dummy: {}}]
`,
	}
	for name, tmpl := range invalid {
		name, tmpl := name, tmpl
		t.Run("invalid/"+name, func(t *testing.T) {
			cfg := write(name+".yaml", sprintfConfig(tmpl, checkpoints, input, output))
			assertValidateMatchesStart(t, cfg, false)
		})
	}
}

func sprintfConfig(tmpl, checkpoints, input, output string) string {
	return fmt.Sprintf(tmpl, checkpoints, input, output)
}

func assertValidateMatchesStart(t *testing.T, cfgPath string, wantStart bool) {
	t.Helper()
	_, stderr, code := runBinary(t, "--validate", "--config", cfgPath)
	validateOK := code == 0
	if validateOK != wantStart {
		t.Fatalf("--validate exit %d (accepts=%v), want accepts=%v\n%s", code, validateOK, wantStart, stderr)
	}

	// Start the binary. A valid config must stay up; an invalid one must
	// exit non-zero instead of starting a partial runtime.
	proc := startProc(t, cfgPath)
	code, exited, logs := proc.exitedWithin(1200 * time.Millisecond)
	if wantStart {
		if exited {
			t.Fatalf("validated config failed to start: exit %d\n%s", code, logs)
		}
		proc.stop()
		return
	}
	if !exited {
		proc.stop()
		t.Fatalf("config rejected by --validate but the runtime started anyway")
	}
	if code == 0 {
		t.Fatalf("invalid config exited with status 0")
	}
}

// TestValidateDoesNotAcceptUnresolvableSecrets checks the other direction of
// the validation invariant: --validate must reject a config that the runtime
// cannot start because a strict secret reference is unresolvable.
func TestValidateDoesNotAcceptUnresolvableSecrets(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "in.log")
	if err := os.WriteFile(input, []byte("[INFO] x\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	cfg := filepath.Join(dir, "loghub.yaml")
	content := `
global:
  checkpoints_dir: "` + filepath.Join(dir, "cp") + `"
  strict_resolution: true
  secret_refresh_interval: "0"
pipelines:
  - id: p
    runtime: {poll_interval: 20ms, shutdown_timeout: 1s}
    source:
      type: file
      file:
        path: "${env:LOGHUB_TEST_MISSING_SECRET}"
    destinations: [{type: dummy, dummy: {}}]
`
	if err := os.WriteFile(cfg, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, stderr, code := runBinary(t, "--validate", "--config", cfg); code != 0 {
		t.Logf("--validate rejected the unresolvable secret (expected behaviour): %s", stderr)
		return
	}
	proc := startProc(t, cfg)
	code, exited, logs := proc.exitedWithin(2 * time.Second)
	if !exited {
		proc.stop()
		t.Fatalf("--validate accepted a config with an unresolvable strict secret, and the runtime started instead of failing")
	}
	if code == 0 {
		t.Fatalf("--validate accepted an unresolvable strict secret and the runtime exited 0\n%s", logs)
	}
}
