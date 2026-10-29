package bootstrap

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"loghub/internal/config"
	_ "loghub/internal/schemas"
)

func TestNewAppValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "loghub.yaml")
	configContent := `
global:
  checkpoints_dir: "` + tmpDir + `/checkpoints"
  log_level: "info"
  metrics_enabled: false

pipelines:
  - id: "test-pipeline"
    source:
      type: "file"
      file:
        path: "` + tmpDir + `/test.log"
        name: "test"
    destinations:
      - type: "dummy"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	app, err := NewApp(context.Background(), configPath)
	if err != nil {
		t.Fatalf("NewApp failed: %v", err)
	}
	if app == nil {
		t.Fatal("expected non-nil app")
	}
	if app.Orchestrator == nil {
		t.Fatal("expected non-nil orchestrator")
	}
	if app.Checkpointer == nil {
		t.Fatal("expected non-nil checkpointer")
	}
	if len(app.Config.Pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(app.Config.Pipelines))
	}
	if err := app.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNewAppInvalidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "bad.yaml")
	configContent := `
global:
  checkpoints_dir: "` + tmpDir + `/checkpoints"

pipelines:
  - id: "test-pipeline"
    source:
      type: "unknown-plugin"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	app, err := NewApp(context.Background(), configPath)
	if err == nil {
		if app != nil {
			app.Close()
		}
		t.Fatal("expected NewApp to fail with invalid config")
	}
}

func TestBuildPipelineSettings(t *testing.T) {
	g := &config.GlobalConfig{
		PollInterval:       "5s",
		SendBatchSize:      50,
		BufferSize:         200,
		MaxRetries:         3,
		RetryBaseBackoff:   "2s",
		ShutdownTimeout:    "60s",
		CheckpointInterval: "0",
	}
	pc := &config.PipelineConfig{
		Runtime: config.RuntimeConfig{
			PollInterval:       "10s",
			SendBatchSize:      100,
			BufferSize:         50,
			ShutdownTimeout:    "30s",
			CheckpointInterval: "0",
		},
		Retry: config.RetryConfig{
			MaxAttempts: 5,
			BaseBackoff: "1s",
		},
	}

	settings, err := BuildPipelineSettings(g, pc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if settings.PollInterval != 10*time.Second {
		t.Errorf("PollInterval: expected 10s, got %v", settings.PollInterval)
	}
	if settings.SendBatchSize != 100 {
		t.Errorf("SendBatchSize: expected 100, got %d", settings.SendBatchSize)
	}
	if settings.BufferSize != 50 {
		t.Errorf("BufferSize: expected 50, got %d", settings.BufferSize)
	}
	if settings.MaxRetries != 5 {
		t.Errorf("MaxRetries: expected 5, got %d", settings.MaxRetries)
	}
	if settings.RetryBaseBackoff != 1*time.Second {
		t.Errorf("RetryBaseBackoff: expected 1s, got %v", settings.RetryBaseBackoff)
	}
	if settings.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout: expected 30s, got %v", settings.ShutdownTimeout)
	}
}

func TestResolveProperties(t *testing.T) {
	props := map[string]interface{}{
		"plain": "value",
		"nested": map[string]interface{}{
			"inner": "value2",
		},
	}
	resolved, err := ResolveProperties(context.Background(), props, &mockResolver{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved["plain"] != "value" {
		t.Errorf("expected plain=value, got %v", resolved["plain"])
	}
}

type mockResolver struct{}

func (m *mockResolver) Resolve(ctx context.Context, value string) (string, error) {
	return value, nil
}

// writeSecretConfig writes a config whose source name comes from the
// LOGHUB_TEST_SOURCE_NAME environment secret.
func writeSecretConfig(t *testing.T, reloadOnChange bool) string {
	t.Helper()
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "loghub.yaml")
	reload := "false"
	if reloadOnChange {
		reload = "true"
	}
	configContent := `
global:
  checkpoints_dir: "` + tmpDir + `/checkpoints"
  metrics_enabled: false
  secret_refresh_interval: "20ms"
  reload_on_secret_change: ` + reload + `

secrets:
  env:
    type: "env"

pipelines:
  - id: "test-pipeline"
    source:
      type: "file"
      file:
        path: "` + tmpDir + `/test.log"
        name: "${env:LOGHUB_TEST_SOURCE_NAME}"
    destinations:
      - type: "dummy"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	return configPath
}

// The background refresh notices a rotated secret and, with
// reload_on_secret_change, reloads the pipelines; without it, it only logs.
func TestSecretChangeTriggersReload(t *testing.T) {
	for _, reloadOnChange := range []bool{true, false} {
		t.Run(fmt.Sprintf("reload_on_secret_change=%v", reloadOnChange), func(t *testing.T) {
			t.Setenv("LOGHUB_TEST_SOURCE_NAME", "before")
			app, err := NewApp(context.Background(), writeSecretConfig(t, reloadOnChange))
			if err != nil {
				t.Fatalf("NewApp failed: %v", err)
			}
			defer app.Close()
			reloaded := make(chan struct{}, 1)
			app.Orchestrator.SetReloadFunc(func() error {
				select {
				case reloaded <- struct{}{}:
				default:
				}
				return nil
			})
			if err := app.Orchestrator.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer app.Orchestrator.Shutdown()

			os.Setenv("LOGHUB_TEST_SOURCE_NAME", "after")
			select {
			case <-reloaded:
				if !reloadOnChange {
					t.Error("reloaded although reload_on_secret_change is false")
				}
			case <-time.After(500 * time.Millisecond):
				if reloadOnChange {
					t.Error("no reload after the secret changed")
				}
			}
		})
	}
}
