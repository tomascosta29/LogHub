package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"loghub/internal/plugins/destination"
	"loghub/internal/plugins/processor"
)

func TestLoadValidConfig(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
  log_level: "debug"
  metrics_enabled: true

pipelines:
  - id: "test-pipeline"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
        name: "test-source"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Global.CheckpointsDir != "./data" {
		t.Errorf("Expected checkpoints_dir './data', got '%s'", cfg.Global.CheckpointsDir)
	}
	if cfg.Global.LogLevel != "debug" {
		t.Errorf("Expected log_level 'debug', got '%s'", cfg.Global.LogLevel)
	}
	if !cfg.Global.MetricsEnabled {
		t.Error("Expected metrics_enabled true")
	}
	if len(cfg.Pipelines) != 1 {
		t.Fatalf("Expected 1 pipeline, got %d", len(cfg.Pipelines))
	}
	if cfg.Pipelines[0].ID != "test-pipeline" {
		t.Errorf("Expected pipeline ID 'test-pipeline', got '%s'", cfg.Pipelines[0].ID)
	}
	if len(cfg.Pipelines[0].Destinations) != 1 {
		t.Fatalf("Expected 1 destination, got %d", len(cfg.Pipelines[0].Destinations))
	}
}

func TestLoadConfigMetricsDisabledByDefault(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"

pipelines:
  - id: "test-pipeline"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Global.MetricsEnabled {
		t.Error("Expected metrics_enabled to default to false")
	}
}

func TestValidateEmptyPipelines(t *testing.T) {
	cfg := &Config{
		Pipelines: []PipelineConfig{},
	}
	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for empty pipelines")
	}
}

func TestValidateDuplicateIDs(t *testing.T) {
	cfg := &Config{
		Pipelines: []PipelineConfig{
			{ID: "pipeline1"},
			{ID: "pipeline1"},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for duplicate IDs")
	}
}

func TestValidateMissingSourceType(t *testing.T) {
	cfg := &Config{
		Pipelines: []PipelineConfig{
			{
				ID: "p1",
				Source: PluginConfig{
					Properties: map[string]interface{}{"path": "/path"},
				},
				Destinations: []PluginConfig{
					{Type: "http", Properties: map[string]interface{}{"url": "http://x"}},
				},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for missing source type")
	}
}

func TestValidateMissingDestinations(t *testing.T) {
	cfg := &Config{
		Pipelines: []PipelineConfig{
			{
				ID: "p1",
				Source: PluginConfig{
					Type: "file",
				},
				Destinations: []PluginConfig{},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for missing destinations")
	}
}

func TestValidateMissingDestType(t *testing.T) {
	cfg := &Config{
		Pipelines: []PipelineConfig{
			{
				ID: "p1",
				Source: PluginConfig{
					Type: "file",
				},
				Destinations: []PluginConfig{
					{Properties: map[string]interface{}{"url": "http://x"}},
				},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for missing destination type")
	}
}

func TestLoadConfigWithMSSQLSourceAndCursorColumn(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
  log_level: "info"

pipelines:
  - id: "mssql-pipeline"
    source:
      type: "mssql"
      mssql:
        server: "localhost"
        database: "logsdb"
        table: "events"
        timestamp_column: "created_at"
        cursor_column: "id"
        batch_size: 100
        auth:
          method: "sql"
          user: "sa"
          password: "secret"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(cfg.Pipelines) != 1 {
		t.Fatalf("Expected 1 pipeline, got %d", len(cfg.Pipelines))
	}

	pipeline := cfg.Pipelines[0]
	if pipeline.ID != "mssql-pipeline" {
		t.Errorf("Expected pipeline ID 'mssql-pipeline', got '%s'", pipeline.ID)
	}

	if pipeline.Source.Type != "mssql" {
		t.Errorf("Expected source type 'mssql', got '%s'", pipeline.Source.Type)
	}

	cursorColumn, ok := pipeline.Source.Properties["cursor_column"].(string)
	if !ok || cursorColumn != "id" {
		t.Errorf("Expected cursor_column 'id', got '%v'", pipeline.Source.Properties["cursor_column"])
	}

	timestampColumn, ok := pipeline.Source.Properties["timestamp_column"].(string)
	if !ok || timestampColumn != "created_at" {
		t.Errorf("Expected timestamp_column 'created_at', got '%v'", pipeline.Source.Properties["timestamp_column"])
	}

	auth, ok := pipeline.Source.Properties["auth"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected auth property to be a map")
	}
	if auth["method"] != "sql" {
		t.Errorf("Expected auth method 'sql', got '%v'", auth["method"])
	}
}

func TestLoadConfigWithHTTPAndHeaders(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"

pipelines:
  - id: "http-pipeline"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
        name: "test-source"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
          timeout: "10s"
          headers:
            Authorization: "Bearer token123"
            X-Custom-Header: "custom-value"
            X-Request-ID: "req-456"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(cfg.Pipelines) != 1 {
		t.Fatalf("Expected 1 pipeline, got %d", len(cfg.Pipelines))
	}

	pipeline := cfg.Pipelines[0]
	if len(pipeline.Destinations) != 1 {
		t.Fatalf("Expected 1 destination, got %d", len(pipeline.Destinations))
	}

	dest := pipeline.Destinations[0]
	if dest.Type != "http" {
		t.Errorf("Expected destination type 'http', got '%s'", dest.Type)
	}

	url, ok := dest.Properties["url"].(string)
	if !ok || url != "http://localhost:8080/logs" {
		t.Errorf("Expected url 'http://localhost:8080/logs', got '%v'", dest.Properties["url"])
	}

	timeout, ok := dest.Properties["timeout"].(string)
	if !ok || timeout != "10s" {
		t.Errorf("Expected timeout '10s', got '%v'", dest.Properties["timeout"])
	}

	headers, ok := dest.Properties["headers"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected headers property to be a map")
	}

	expectedHeaders := map[string]string{
		"Authorization":   "Bearer token123",
		"X-Custom-Header": "custom-value",
		"X-Request-ID":    "req-456",
	}
	for k, v := range expectedHeaders {
		if got, ok := headers[k].(string); !ok || got != v {
			t.Errorf("Expected header %s='%s', got '%v'", k, v, headers[k])
		}
	}
}

func TestLoadConfigWithProcessorsSnakeCase(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"

pipelines:
  - id: "processor-pipeline"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
        name: "test-source"
    processors:
      - type: "filter"
        filter:
          include_level:
            - "ERROR"
            - "WARN"
          exclude_message: "test.*debug"
          include_fields:
            env: "prod"
      - type: "json_parse"
        json_parse:
          source_field: "payload"
          target_field: "message"
          keep_source: false
          on_error: "drop"
      - type: "timestamp"
        timestamp:
          source_field: "ts"
          source_layout: "2006-01-02 15:04:05"
          target_layout: "2006-01-02T15:04:05Z07:00"
          target_field: "timestamp"
          on_error: "keep"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(cfg.Pipelines) != 1 {
		t.Fatalf("Expected 1 pipeline, got %d", len(cfg.Pipelines))
	}

	pipeline := cfg.Pipelines[0]
	if len(pipeline.Processors) != 3 {
		t.Fatalf("Expected 3 processors, got %d", len(pipeline.Processors))
	}

	filterProc := pipeline.Processors[0]
	if filterProc.Type != "filter" {
		t.Errorf("Expected processor type 'filter', got '%s'", filterProc.Type)
	}

	includeLevel, ok := filterProc.Properties["include_level"].([]interface{})
	if !ok || len(includeLevel) != 2 {
		t.Errorf("Expected include_level with 2 items, got %v", filterProc.Properties["include_level"])
	}

	excludeMessage, ok := filterProc.Properties["exclude_message"].(string)
	if !ok || excludeMessage != "test.*debug" {
		t.Errorf("Expected exclude_message 'test.*debug', got '%v'", filterProc.Properties["exclude_message"])
	}

	includeFields, ok := filterProc.Properties["include_fields"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected include_fields to be a map")
	}
	if includeFields["env"] != "prod" {
		t.Errorf("Expected include_fields.env='prod', got '%v'", includeFields["env"])
	}

	jsonProc := pipeline.Processors[1]
	if jsonProc.Type != "json_parse" {
		t.Errorf("Expected processor type 'json_parse', got '%s'", jsonProc.Type)
	}

	sourceField, ok := jsonProc.Properties["source_field"].(string)
	if !ok || sourceField != "payload" {
		t.Errorf("Expected source_field 'payload', got '%v'", jsonProc.Properties["source_field"])
	}

	keepSource, ok := jsonProc.Properties["keep_source"].(bool)
	if !ok || keepSource != false {
		t.Errorf("Expected keep_source false, got '%v'", jsonProc.Properties["keep_source"])
	}

	onError, ok := jsonProc.Properties["on_error"].(string)
	if !ok || onError != "drop" {
		t.Errorf("Expected on_error 'drop', got '%v'", jsonProc.Properties["on_error"])
	}

	tsProc := pipeline.Processors[2]
	if tsProc.Type != "timestamp" {
		t.Errorf("Expected processor type 'timestamp', got '%s'", tsProc.Type)
	}

	sourceLayout, ok := tsProc.Properties["source_layout"].(string)
	if !ok || sourceLayout != "2006-01-02 15:04:05" {
		t.Errorf("Expected source_layout '2006-01-02 15:04:05', got '%v'", tsProc.Properties["source_layout"])
	}

	targetField, ok := tsProc.Properties["target_field"].(string)
	if !ok || targetField != "timestamp" {
		t.Errorf("Expected target_field 'timestamp', got '%v'", tsProc.Properties["target_field"])
	}
}

func TestLoadConfigWithDLQAndDiskBuffer(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
  log_level: "info"

pipelines:
  - id: "dlq-pipeline"
    durability:
      dlq:
        enabled: true
        dir: "/var/loghub/dlq"
      disk_buffer:
        enabled: true
        dir: "/var/loghub/buffer"
        max_size_mb: 1024
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
        name: "test-source"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(cfg.Pipelines) != 1 {
		t.Fatalf("Expected 1 pipeline, got %d", len(cfg.Pipelines))
	}

	pipeline := cfg.Pipelines[0]

	if !pipeline.Durability.DLQ.Enabled {
		t.Error("Expected durability.dlq.enabled to be true")
	}
	if pipeline.Durability.DLQ.Dir != "/var/loghub/dlq" {
		t.Errorf("Expected durability.dlq.dir '/var/loghub/dlq', got '%s'", pipeline.Durability.DLQ.Dir)
	}

	if !pipeline.Durability.DiskBuffer.Enabled {
		t.Error("Expected durability.disk_buffer.enabled to be true")
	}
	if pipeline.Durability.DiskBuffer.Dir != "/var/loghub/buffer" {
		t.Errorf("Expected durability.disk_buffer.dir '/var/loghub/buffer', got '%s'", pipeline.Durability.DiskBuffer.Dir)
	}
	if pipeline.Durability.DiskBuffer.MaxSizeMB != 1024 {
		t.Errorf("Expected durability.disk_buffer.max_size_mb 1024, got %d", pipeline.Durability.DiskBuffer.MaxSizeMB)
	}
}

func TestInstantiateProcessorsFromConfig(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"

pipelines:
  - id: "processor-pipeline"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
        name: "test-source"
    processors:
      - type: "filter"
        filter:
          include_level:
            - "ERROR"
            - "WARN"
          exclude_message: "test.*debug"
      - type: "json_parse"
        json_parse:
          source_field: "payload"
          keep_source: false
      - type: "timestamp"
        timestamp:
          source_field: "ts"
          source_layout: "2006-01-02 15:04:05"
          target_layout: "2006-01-02T15:04:05Z07:00"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	pipeline := cfg.Pipelines[0]

	for i, procConfig := range pipeline.Processors {
		proc, err := processor.Create(procConfig.Type, procConfig.Properties)
		if err != nil {
			t.Fatalf("Failed to create processor %d (%s): %v", i, procConfig.Type, err)
		}
		if proc == nil {
			t.Fatalf("Processor %d (%s) is nil", i, procConfig.Type)
		}
	}
}

func TestInstantiateHTTPWithHeadersFromConfig(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"

pipelines:
  - id: "http-pipeline"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
        name: "test-source"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
          timeout: "5s"
          headers:
            Authorization: "Bearer test-token"
            X-Custom-Header: "CustomValue"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	pipeline := cfg.Pipelines[0]
	if len(pipeline.Destinations) != 1 {
		t.Fatalf("Expected 1 destination, got %d", len(pipeline.Destinations))
	}

	dest, err := destination.Create(pipeline.Destinations[0].Type, pipeline.Destinations[0].Properties)
	if err != nil {
		t.Fatalf("Failed to create destination: %v", err)
	}
	if dest == nil {
		t.Fatal("Destination is nil")
	}

	httpDest, ok := dest.(*destination.HTTP)
	if !ok {
		t.Fatalf("Expected *destination.HTTP, got %T", dest)
	}

	if httpDest.Type() != "http" {
		t.Errorf("Expected type 'http', got '%s'", httpDest.Type())
	}
}

func TestLoadNamespacedProductionShape(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"

pipelines:
  - id: "mssql-sentinel"
    runtime:
      poll_interval: "10s"
      send_batch_size: 25
      buffer_size: 50
      checkpoint_interval: "30s"
      shutdown_timeout: "60s"
    retry:
      max_attempts: 5
      base_backoff: "2s"
    durability:
      dlq:
        enabled: true
        dir: "/var/loghub/dlq"
      disk_buffer:
        enabled: true
        dir: "/var/loghub/buffer"
        max_size_mb: 2048
    source:
      type: "mssql"
      mssql:
        server: "sql.example.database.windows.net"
        database: "AuditDB"
        table: "AuditEvents"
        timestamp_column: "event_time"
        cursor_column: "id"
        auth:
          method: "ad-managed-identity"
    processors:
      - type: "timestamp"
        timestamp:
          source_field: "event_time"
          source_layout: "2006-01-02T15:04:05Z07:00"
          target_field: "timestamp"
    destinations:
      - type: "sentinel"
        max_retries: 3
        sentinel:
          dce: "https://dce.eastus.ingest.monitor.azure.com"
          dcr: "dcr-id"
          stream: "Custom-MSSQLAudit_CL"
          timeout: "30s"
`
	cfg := loadConfigFromString(t, content)
	p := cfg.Pipelines[0]

	if p.Runtime.PollInterval != "10s" || p.Runtime.SendBatchSize != 25 || p.Runtime.BufferSize != 50 || p.Runtime.CheckpointInterval != "30s" || p.Runtime.ShutdownTimeout != "60s" {
		t.Fatalf("runtime settings not loaded correctly: %+v", p.Runtime)
	}
	if p.Retry.MaxAttempts != 5 || p.Retry.BaseBackoff != "2s" {
		t.Fatalf("retry settings not loaded correctly: %+v", p.Retry)
	}
	if !p.Durability.DLQ.Enabled || p.Durability.DLQ.Dir != "/var/loghub/dlq" {
		t.Fatalf("dlq settings not loaded correctly: %+v", p.Durability.DLQ)
	}
	if !p.Durability.DiskBuffer.Enabled || p.Durability.DiskBuffer.Dir != "/var/loghub/buffer" || p.Durability.DiskBuffer.MaxSizeMB != 2048 {
		t.Fatalf("disk buffer settings not loaded correctly: %+v", p.Durability.DiskBuffer)
	}
	if p.Source.Properties["server"] != "sql.example.database.windows.net" {
		t.Fatalf("mssql typed properties were not normalized: %+v", p.Source.Properties)
	}
	if p.Source.Properties["fetch_size"] != 100 {
		t.Fatalf("expected default fetch_size 100, got %v", p.Source.Properties["fetch_size"])
	}
	if p.Destinations[0].Properties["dce"] != "https://dce.eastus.ingest.monitor.azure.com" {
		t.Fatalf("sentinel typed properties were not normalized: %+v", p.Destinations[0].Properties)
	}
	if p.Destinations[0].MaxRetries != 3 {
		t.Fatalf("expected destination max_retries 3, got %d", p.Destinations[0].MaxRetries)
	}
}

func TestLoadRejectsUnknownTopLevelField(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
unexpected: true
pipelines:
  - id: "p"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestLoadRejectsUnknownPluginProperty(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
        typo_path: "/oops"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected unknown plugin property error")
	}
}

func TestLoadRejectsFetchSizeWithLegacyBatchSize(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "sqlite"
      sqlite:
        database: "/tmp/test.db"
        table: "events"
        timestamp_column: "event_time"
        fetch_size: 200
        batch_size: 100
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected config error")
	}
	if got := err.Error(); got != `invalid config: pipeline p source: use fetch_size or batch_size, not both` {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestLoadAcceptsSQLMaxLookback(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "mssql"
      mssql:
        server: "localhost"
        database: "logs"
        query: "SELECT 1 WHERE created_at > @cursor"
        timestamp_column: "created_at"
        max_lookback: "5m"
        auth:
          method: "sql"
          user: "sa"
          password: "secret"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	cfg := loadConfigFromString(t, content)
	if got := cfg.Pipelines[0].Source.Properties["max_lookback"]; got != "5m" {
		t.Fatalf("expected max_lookback to be preserved, got %v", got)
	}
}

func TestLoadRejectsInvalidSQLMaxLookback(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "sqlite"
      sqlite:
        database: "/tmp/test.db"
        table: "events"
        timestamp_column: "event_time"
        max_lookback: "five minutes"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestLoadAcceptsSQLSourceTimezone(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "sqlite"
      sqlite:
        database: "/tmp/test.db"
        table: "events"
        timestamp_column: "event_time"
        source_timezone: "Europe/Berlin"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	cfg := loadConfigFromString(t, content)
	if got := cfg.Pipelines[0].Source.Properties["source_timezone"]; got != "Europe/Berlin" {
		t.Fatalf("expected source_timezone to be preserved, got %v", got)
	}
}

func TestLoadRejectsInvalidSQLSourceTimezone(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "mssql"
      mssql:
        server: "localhost"
        database: "logs"
        query: "SELECT 1 WHERE created_at > @cursor"
        timestamp_column: "created_at"
        source_timezone: "Mars/Olympus"
        auth:
          method: "sql"
          user: "sa"
          password: "secret"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected invalid timezone error")
	}
}

func TestLoadRejectsDestinationRetriesInsideProperties(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
          max_retries: 2
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected max_retries placement error")
	}
}

func TestLoadRejectsMissingSentinelDCE(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "sentinel"
        sentinel:
          dcr: "dcr-id"
          stream: "Custom-MSSQLAudit_CL"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected missing dce error")
	}
}

func TestLoadRejectsInvalidSecretProvider(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
secrets:
  kv:
    type: "keyvault"
pipelines:
  - id: "p"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected missing vault_url error")
	}
}

func TestLoadRejectsInvalidLogLevel(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
  log_level: "verbose"
pipelines:
  - id: "p"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "http"
        http:
          url: "http://localhost:8080/logs"
`
	_, err := loadConfigFromStringErr(t, content)
	if err == nil {
		t.Fatal("expected invalid log level error")
	}
}

func loadConfigFromString(t *testing.T, content string) *Config {
	t.Helper()
	cfg, err := loadConfigFromStringErr(t, content)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	return cfg
}

func loadConfigFromStringErr(t *testing.T, content string) (*Config, error) {
	t.Helper()
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}
	return Load(tmpfile.Name())
}

func TestPluginSchemaValidation(t *testing.T) {
	RegisterPluginSchema("custom_src", SimplePluginSchema{
		Allowed:  []string{"required_field", "optional_field"},
		Required: []string{"required_field"},
	})

	// Valid config
	validContent := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p1"
    source:
      type: "custom_src"
      custom_src:
        required_field: "foo"
        optional_field: "bar"
    destinations:
      - type: "dummy"
`
	if _, err := loadConfigFromStringErr(t, validContent); err != nil {
		t.Errorf("expected valid config to pass, got: %v", err)
	}

	// Missing required field
	missingRequired := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p1"
    source:
      type: "custom_src"
      custom_src:
        optional_field: "bar"
    destinations:
      - type: "dummy"
`
	if _, err := loadConfigFromStringErr(t, missingRequired); err == nil {
		t.Error("expected error due to missing required field, got nil")
	}

	// Unknown field
	unknownField := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p1"
    source:
      type: "custom_src"
      custom_src:
        required_field: "foo"
        invalid_field: "baz"
    destinations:
      - type: "dummy"
`
	if _, err := loadConfigFromStringErr(t, unknownField); err == nil {
		t.Error("expected error due to unknown field, got nil")
	}
}

func TestStalledThresholdMultiplier_Default(t *testing.T) {
	// When not specified, the global stalled_threshold_multiplier
	// defaults to 5.
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p1"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "dummy"
`
	tmpfile, err := os.CreateTemp("", "config-stalled-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Global.StalledThresholdMultiplier != 5 {
		t.Errorf("StalledThresholdMultiplier = %d, want 5 (default)", cfg.Global.StalledThresholdMultiplier)
	}
}

func TestStalledThresholdMultiplier_Explicit(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
  stalled_threshold_multiplier: 7
pipelines:
  - id: "p1"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "dummy"
`
	tmpfile, err := os.CreateTemp("", "config-stalled-explicit-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Global.StalledThresholdMultiplier != 7 {
		t.Errorf("StalledThresholdMultiplier = %d, want 7 (explicit)", cfg.Global.StalledThresholdMultiplier)
	}
}

// TestCircuitBreakerConfig_Defaults verifies the documented
// defaults (failure_threshold=5, recovery_timeout=30s,
// half_open_max_calls=1) when the config does not specify them.
func TestCircuitBreakerConfig_Defaults(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
pipelines:
  - id: "p1"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "dummy"
`
	tmpfile, err := os.CreateTemp("", "config-cb-default-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Global.CircuitBreakerFailureThreshold != 5 {
		t.Errorf("CircuitBreakerFailureThreshold = %d, want 5 (default)", cfg.Global.CircuitBreakerFailureThreshold)
	}
	if cfg.Global.CircuitBreakerRecoveryTimeout != "30s" {
		t.Errorf("CircuitBreakerRecoveryTimeout = %q, want \"30s\" (default)", cfg.Global.CircuitBreakerRecoveryTimeout)
	}
	if cfg.Global.CircuitBreakerHalfOpenMaxCalls != 1 {
		t.Errorf("CircuitBreakerHalfOpenMaxCalls = %d, want 1 (default)", cfg.Global.CircuitBreakerHalfOpenMaxCalls)
	}
}

// TestCircuitBreakerConfig_Explicit verifies the configured values
// are honored by Load.
func TestCircuitBreakerConfig_Explicit(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
  circuit_breaker_failure_threshold: 10
  circuit_breaker_recovery_timeout: "5m"
  circuit_breaker_half_open_max_calls: 2
pipelines:
  - id: "p1"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "dummy"
`
	tmpfile, err := os.CreateTemp("", "config-cb-explicit-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Global.CircuitBreakerFailureThreshold != 10 {
		t.Errorf("CircuitBreakerFailureThreshold = %d, want 10 (explicit)", cfg.Global.CircuitBreakerFailureThreshold)
	}
	if cfg.Global.CircuitBreakerRecoveryTimeout != "5m" {
		t.Errorf("CircuitBreakerRecoveryTimeout = %q, want \"5m\" (explicit)", cfg.Global.CircuitBreakerRecoveryTimeout)
	}
	if cfg.Global.CircuitBreakerHalfOpenMaxCalls != 2 {
		t.Errorf("CircuitBreakerHalfOpenMaxCalls = %d, want 2 (explicit)", cfg.Global.CircuitBreakerHalfOpenMaxCalls)
	}
}

// TestCircuitBreakerConfig_InvalidDuration verifies that an
// unparseable recovery_timeout is rejected at config load time.
func TestCircuitBreakerConfig_InvalidDuration(t *testing.T) {
	content := `
global:
  checkpoints_dir: "./data"
  circuit_breaker_recovery_timeout: "not-a-duration"
pipelines:
  - id: "p1"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "dummy"
`
	tmpfile, err := os.CreateTemp("", "config-cb-bad-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpfile.Close()

	if _, err := Load(tmpfile.Name()); err == nil {
		t.Error("expected error for invalid circuit_breaker_recovery_timeout, got nil")
	}
}

// TestPipelineSettingsFromConfig_CircuitBreakerFields verifies the
// config-side PipelineSettings mirror carries the circuit breaker
// fields.
func TestPipelineSettingsFromConfig_CircuitBreakerFields(t *testing.T) {
	g := &GlobalConfig{
		CircuitBreakerFailureThreshold: 8,
		CircuitBreakerRecoveryTimeout:  "15s",
		CircuitBreakerHalfOpenMaxCalls: 3,
	}
	s := PipelineSettingsFromConfig(g, nil)
	if s.CircuitBreakerFailureThreshold != 8 {
		t.Errorf("FailureThreshold = %d, want 8", s.CircuitBreakerFailureThreshold)
	}
	if s.CircuitBreakerRecoveryTimeout != 15*time.Second {
		t.Errorf("RecoveryTimeout = %v, want 15s", s.CircuitBreakerRecoveryTimeout)
	}
	if s.CircuitBreakerHalfOpenMaxCalls != 3 {
		t.Errorf("HalfOpenMaxCalls = %d, want 3", s.CircuitBreakerHalfOpenMaxCalls)
	}
}

func TestLoadRejectsRemovedFlatPipelineFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loghub.yaml")
	content := `
pipelines:
  - id: "p1"
    dlq_enabled: true
    source:
      type: "file"
      file:
        path: "/tmp/in.log"
    destinations:
      - type: "dummy"
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load accepted a removed flat field")
	}
	for _, want := range []string{"pipeline p1", "line 4", `"dlq_enabled"`, `"durability.dlq.enabled"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadValidatesSentinelAuth(t *testing.T) {
	load := func(auth string) error {
		path := filepath.Join(t.TempDir(), "loghub.yaml")
		content := `
pipelines:
  - id: "p1"
    source:
      type: "file"
      file:
        path: "/tmp/in.log"
    destinations:
      - type: "sentinel"
        sentinel:
          dce: "https://dce.example.com"
          dcr: "dcr-id"
          stream: "Custom-Test_CL"
` + auth
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		_, err := Load(path)
		return err
	}

	valid := map[string]string{
		"no auth (managed identity)": "",
		"user-assigned client_id":    "          client_id: \"11111111-1111-1111-1111-111111111111\"\n",
		"service principal": `          auth:
            method: "ad-service-principal"
            tenant_id: "t"
            client_id: "c"
            client_secret: "${env:SENTINEL_CLIENT_SECRET}"
`,
	}
	for name, auth := range valid {
		if err := load(auth); err != nil {
			t.Errorf("%s: Load = %v, want success", name, err)
		}
	}

	invalid := []struct{ name, auth, want string }{
		{"service principal without secret", `          auth:
            method: "ad-service-principal"
            tenant_id: "t"
            client_id: "c"
`, "client_secret"},
		{"unsupported method", `          auth:
            method: "sql"
`, `unsupported sentinel auth method "sql"`},
		{"unknown auth key", `          auth:
            method: "ad-managed-identity"
            password: "x"
`, `unknown sentinel auth property "password"`},
		{"client_id next to auth", `          client_id: "c"
          auth:
            method: "ad-managed-identity"
`, "set client_id inside auth"},
	}
	for _, tc := range invalid {
		err := load(tc.auth)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Load = %v, want an error containing %q", tc.name, err, tc.want)
		}
	}
}

func loadGlobalConfig(t *testing.T, global string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "global:\n  checkpoints_dir: \"./data\"\n" + global + `
pipelines:
  - id: "p1"
    source:
      type: "file"
      file:
        path: "/var/log/test.log"
    destinations:
      - type: "dummy"
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return Load(path)
}

func TestSecretRefreshConfig(t *testing.T) {
	cfg, err := loadGlobalConfig(t, "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Global.SecretRefreshInterval != "5m" || cfg.Global.ReloadOnSecretChange {
		t.Errorf("defaults = %q, %v; want 5m, false", cfg.Global.SecretRefreshInterval, cfg.Global.ReloadOnSecretChange)
	}

	cfg, err = loadGlobalConfig(t, "  secret_refresh_interval: \"0\"\n  reload_on_secret_change: true\n")
	if err != nil {
		t.Fatalf("Load with refresh disabled: %v", err)
	}
	if cfg.Global.SecretRefreshInterval != "0" || !cfg.Global.ReloadOnSecretChange {
		t.Errorf("explicit = %q, %v; want 0, true", cfg.Global.SecretRefreshInterval, cfg.Global.ReloadOnSecretChange)
	}

	for _, bad := range []string{"soon", "-1m"} {
		if _, err := loadGlobalConfig(t, "  secret_refresh_interval: \""+bad+"\"\n"); err == nil {
			t.Errorf("secret_refresh_interval %q: Load succeeded, want an error", bad)
		}
	}
}
