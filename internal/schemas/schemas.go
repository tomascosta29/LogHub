// Package schemas registers built-in plugin schemas in a central location.
// Importing this package triggers init() side effects that register schemas.
// This package is imported by main and bootstrap; it should NOT be imported by
// internal/config or by any plugin package to avoid import cycles.
package schemas

import (
	"loghub/internal/config"
	"loghub/internal/plugins/destination"
	"loghub/internal/plugins/processor"
	"loghub/internal/plugins/source"
)

func init() {
	// Register all built-in plugin schemas.
	// Fields must match the mapstructure tags in each plugin's config struct.
	config.RegisterPluginSchema("file", config.SimplePluginSchema{
		Allowed:  []string{"path", "name", "fetch_size", "batch_size"},
		Required: []string{"path"},
	})
	config.RegisterPluginSchema("sqlite", config.SimplePluginSchema{
		Allowed:  []string{"database", "table", "query", "timestamp_column", "cursor_column", "name", "fetch_size", "batch_size", "max_lookback", "source_timezone", "max_open_conns", "columns", "wait_for_change"},
		Required: []string{"database"},
	})
	config.RegisterPluginSchema("mssql", config.SimplePluginSchema{
		Allowed:  []string{"auth", "server", "database", "table", "query", "timestamp_column", "cursor_column", "name", "fetch_size", "batch_size", "max_lookback", "source_timezone", "trust_server_certificate", "max_open_conns", "columns", "wait_for_change", "encrypt"},
		Required: []string{"server", "database"},
	})
	config.RegisterPluginSchema("http", config.SimplePluginSchema{
		Allowed:  []string{"url", "timeout", "headers"},
		Required: []string{"url"},
	})
	config.RegisterPluginSchema("local_jsonl", config.SimplePluginSchema{
		Allowed:  []string{"path", "format"},
		Required: []string{"path"},
	})
	config.RegisterPluginSchema("dummy", config.SimplePluginSchema{
		Allowed: []string{"drop_rate", "log_messages", "latency"},
	})
	schema := config.SimplePluginSchema{
		Allowed:  []string{"dce", "dcr", "stream", "timeout", "client_id", "auth"},
		Required: []string{"dce", "dcr", "stream"},
	}
	config.RegisterPluginSchema("sentinel", schema)
	config.RegisterPluginSchema("azure_monitor", schema)
	config.RegisterPluginSchema("filter", config.SimplePluginSchema{
		Allowed: []string{"include_level", "exclude_level", "include_message", "exclude_message", "include_fields", "exclude_fields"},
	})
	config.RegisterPluginSchema("json_parse", config.SimplePluginSchema{
		Allowed: []string{"source_field", "target_field", "keep_source", "on_error"},
	})
	config.RegisterPluginSchema("timestamp", config.SimplePluginSchema{
		Allowed:  []string{"source_field", "source_layout", "target_layout", "target_field", "on_error"},
		Required: []string{"source_layout"},
	})

	// Mark built-in plugins as registered
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
