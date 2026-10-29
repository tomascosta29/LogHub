package config

import (
	"loghub/internal/plugins/destination"
	"loghub/internal/plugins/processor"
	"loghub/internal/plugins/source"
)

func init() {
	// Register all built-in plugin schemas for test validation.
	// Fields must match the mapstructure tags in each plugin's config struct.
	RegisterPluginSchema("file", SimplePluginSchema{
		Allowed:  []string{"path", "name", "fetch_size", "batch_size"},
		Required: []string{"path"},
	})
	RegisterPluginSchema("sqlite", SimplePluginSchema{
		Allowed:  []string{"database", "table", "query", "timestamp_column", "cursor_column", "name", "fetch_size", "batch_size", "max_lookback", "source_timezone", "max_open_conns", "columns", "wait_for_change"},
		Required: []string{"database"},
	})
	RegisterPluginSchema("mssql", SimplePluginSchema{
		Allowed:  []string{"auth", "server", "database", "table", "query", "timestamp_column", "cursor_column", "name", "fetch_size", "batch_size", "max_lookback", "source_timezone", "trust_server_certificate", "max_open_conns", "columns", "wait_for_change", "encrypt"},
		Required: []string{"server", "database"},
	})
	RegisterPluginSchema("http", SimplePluginSchema{
		Allowed:  []string{"url", "timeout", "headers"},
		Required: []string{"url"},
	})
	RegisterPluginSchema("local_jsonl", SimplePluginSchema{
		Allowed:  []string{"path", "format"},
		Required: []string{"path"},
	})
	RegisterPluginSchema("dummy", SimplePluginSchema{
		Allowed: []string{"drop_rate", "log_messages", "latency"},
	})
	schema := SimplePluginSchema{
		Allowed:  []string{"dce", "dcr", "stream", "timeout", "client_id", "auth"},
		Required: []string{"dce", "dcr", "stream"},
	}
	RegisterPluginSchema("sentinel", schema)
	RegisterPluginSchema("azure_monitor", schema)
	RegisterPluginSchema("filter", SimplePluginSchema{
		Allowed: []string{"include_level", "exclude_level", "include_message", "exclude_message", "include_fields", "exclude_fields"},
	})
	RegisterPluginSchema("json_parse", SimplePluginSchema{
		Allowed: []string{"source_field", "target_field", "keep_source", "on_error"},
	})
	RegisterPluginSchema("timestamp", SimplePluginSchema{
		Allowed:  []string{"source_field", "source_layout", "target_layout", "target_field", "on_error"},
		Required: []string{"source_layout"},
	})

	// Mark built-in plugins.
	for _, name := range []string{"http", "local_jsonl", "dummy", "sentinel", "azure_monitor"} {
		destination.RegisterBuiltIn(name)
	}
	for _, name := range []string{"file", "sqlite", "mssql"} {
		source.RegisterBuiltIn(name)
	}
	for _, name := range []string{"filter", "json_parse", "timestamp"} {
		processor.RegisterBuiltIn(name)
	}
}
