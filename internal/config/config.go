package config

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Global    GlobalConfig     `yaml:"global"`
	Secrets   SecretsConfig    `yaml:"secrets"`
	Pipelines []PipelineConfig `yaml:"pipelines"`
}

type GlobalConfig struct {
	// HTTPMaxIdleConnsPerHost bounds each shared HTTP destination idle pool.
	// Zero selects the default of 100.
	HTTPMaxIdleConnsPerHost int `yaml:"http_max_idle_conns_per_host"`
	// DataDir is the root for per-pipeline on-disk state
	// (checkpoints, disk buffer, DLQ). When set, per-pipeline dirs
	// default to <DataDir>/<subsystem>/<pipelineID>. When empty,
	// each subsystem must specify its own dir explicitly.
	DataDir            string `yaml:"data_dir"`
	CheckpointsDir     string `yaml:"checkpoints_dir"`
	LogLevel           string `yaml:"log_level"`
	MetricsEnabled     bool   `yaml:"metrics_enabled"`
	PollInterval       string `yaml:"poll_interval"`
	SendBatchSize      int    `yaml:"send_batch_size"`
	ShutdownTimeout    string `yaml:"shutdown_timeout"`
	CheckpointInterval string `yaml:"checkpoint_interval"`
	MaxRetries         int    `yaml:"max_retries"`
	RetryBaseBackoff   string `yaml:"retry_base_backoff"`
	BufferSize         int    `yaml:"buffer_size"`
	StrictResolution   bool   `yaml:"strict_resolution"`

	// SecretRefreshInterval is how often cached secrets are looked up
	// again in the background, to notice rotation. "0" disables the
	// background refresh. Defaults to 5m.
	SecretRefreshInterval string `yaml:"secret_refresh_interval"`

	// ReloadOnSecretChange reloads, as on SIGHUP, when the background
	// refresh sees a secret's value change. Off by default: the change
	// is logged and applied on the next SIGHUP or restart.
	ReloadOnSecretChange bool `yaml:"reload_on_secret_change"`

	// HealthStalenessThreshold is the maximum duration since the last
	// successful collect or send before a pipeline is considered unhealthy
	// by the /health endpoint. Zero disables staleness checks.
	HealthStalenessThreshold string `yaml:"health_staleness_threshold"`

	// StalledThresholdMultiplier is the number of PollInterval ticks
	// without a successful collect or send that must elapse before a
	// pipeline is marked Stalled. Defaults to 5 in
	// setDefaults.
	StalledThresholdMultiplier int `yaml:"stalled_threshold_multiplier"`

	// CircuitBreakerFailureThreshold is the number of consecutive
	// destination failures that trip the per-destination circuit
	// breaker from closed to open. Defaults to 5 in setDefaults.
	CircuitBreakerFailureThreshold int `yaml:"circuit_breaker_failure_threshold"`

	// CircuitBreakerRecoveryTimeout is the duration the breaker
	// stays open before transitioning to half-open to admit a
	// single probe call. Defaults to "30s" in setDefaults.
	CircuitBreakerRecoveryTimeout string `yaml:"circuit_breaker_recovery_timeout"`

	// CircuitBreakerHalfOpenMaxCalls is the maximum number of
	// concurrent probe calls admitted while the breaker is in the
	// half-open state. Defaults to 1 in setDefaults.
	CircuitBreakerHalfOpenMaxCalls int `yaml:"circuit_breaker_half_open_max_calls"`
}

type SecretsConfig map[string]SecretProviderConfig

type SecretProviderConfig struct {
	Type     string `yaml:"type"`
	VaultURL string `yaml:"vault_url"`
	TenantID string `yaml:"tenant_id"`
	ClientID string `yaml:"client_id"`
}

type PluginConfig struct {
	Type       string                 `yaml:"type"`
	Properties map[string]interface{} `yaml:"-"`

	Extra map[string]interface{} `yaml:",inline"`

	MaxRetries int `yaml:"max_retries"`
}

type PipelineConfig struct {
	ID           string         `yaml:"id"`
	Source       PluginConfig   `yaml:"source"`
	Processors   []PluginConfig `yaml:"processors"`
	Destinations []PluginConfig `yaml:"destinations"`

	Runtime    RuntimeConfig    `yaml:"runtime"`
	Retry      RetryConfig      `yaml:"retry"`
	Durability DurabilityConfig `yaml:"durability"`
}

type RuntimeConfig struct {
	PollInterval       string `yaml:"poll_interval"`
	SendBatchSize      int    `yaml:"send_batch_size"`
	BufferSize         int    `yaml:"buffer_size"`
	CheckpointInterval string `yaml:"checkpoint_interval"`
	ShutdownTimeout    string `yaml:"shutdown_timeout"`
}

type RetryConfig struct {
	MaxAttempts int    `yaml:"max_attempts"`
	BaseBackoff string `yaml:"base_backoff"`
}

type DurabilityConfig struct {
	DLQ        QueueConfig      `yaml:"dlq"`
	DiskBuffer DiskBufferConfig `yaml:"disk_buffer"`
}

type QueueConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Dir       string `yaml:"dir"`
	MaxSizeMB int64  `yaml:"max_size_mb"`
}

type DiskBufferConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Dir       string `yaml:"dir"`
	MaxSizeMB int64  `yaml:"max_size_mb"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := checkRemovedPipelineFields(data); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := normalizeConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	setDefaults(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// removedPipelineFields maps the flat per-pipeline keys used by v0.1.0
// to the nested key that replaces each one.
var removedPipelineFields = map[string]string{
	"poll_interval":           "runtime.poll_interval",
	"send_batch_size":         "runtime.send_batch_size",
	"buffer_size":             "runtime.buffer_size",
	"checkpoint_interval":     "runtime.checkpoint_interval",
	"shutdown_timeout":        "runtime.shutdown_timeout",
	"max_retries":             "retry.max_attempts",
	"retry_base_backoff":      "retry.base_backoff",
	"dlq_enabled":             "durability.dlq.enabled",
	"dlq_dir":                 "durability.dlq.dir",
	"disk_buffer_enabled":     "durability.disk_buffer.enabled",
	"disk_buffer_dir":         "durability.disk_buffer.dir",
	"disk_buffer_max_size_mb": "durability.disk_buffer.max_size_mb",
}

// checkRemovedPipelineFields reports a flat per-pipeline key with the
// nested key that replaces it, which is more useful than the generic
// unknown-field error the strict decoder would give.
func checkRemovedPipelineFields(data []byte) error {
	var root struct {
		Pipelines []yaml.Node `yaml:"pipelines"`
	}
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil // the strict decode reports syntax errors
	}
	for i, p := range root.Pipelines {
		if p.Kind != yaml.MappingNode {
			continue
		}
		id := fmt.Sprintf("#%d", i)
		for k := 0; k+1 < len(p.Content); k += 2 {
			if p.Content[k].Value == "id" {
				id = p.Content[k+1].Value
			}
		}
		for k := 0; k+1 < len(p.Content); k += 2 {
			key := p.Content[k].Value
			if nested, ok := removedPipelineFields[key]; ok {
				return fmt.Errorf("pipeline %s (line %d): %q is no longer supported; use %q", id, p.Content[k].Line, key, nested)
			}
		}
	}
	return nil
}

func setDefaults(cfg *Config) {
	if cfg.Global.HTTPMaxIdleConnsPerHost == 0 {
		cfg.Global.HTTPMaxIdleConnsPerHost = 100
	}
	if cfg.Global.CheckpointsDir == "" {
		cfg.Global.CheckpointsDir = "./data/checkpoints"
	}
	if cfg.Global.LogLevel == "" {
		cfg.Global.LogLevel = "info"
	}
	if cfg.Global.PollInterval == "" {
		cfg.Global.PollInterval = "1s"
	}
	if cfg.Global.ShutdownTimeout == "" {
		cfg.Global.ShutdownTimeout = "30s"
	}
	if cfg.Global.SecretRefreshInterval == "" {
		cfg.Global.SecretRefreshInterval = "5m"
	}
	if cfg.Global.CheckpointInterval == "" {
		cfg.Global.CheckpointInterval = "0"
	}
	if cfg.Global.MaxRetries == 0 {
		cfg.Global.MaxRetries = 4
	}
	if cfg.Global.RetryBaseBackoff == "" {
		cfg.Global.RetryBaseBackoff = "1s"
	}
	if cfg.Global.BufferSize == 0 {
		cfg.Global.BufferSize = 100
	}
	if cfg.Global.StalledThresholdMultiplier == 0 {
		cfg.Global.StalledThresholdMultiplier = 5
	}
	if cfg.Global.CircuitBreakerFailureThreshold == 0 {
		cfg.Global.CircuitBreakerFailureThreshold = 5
	}
	if cfg.Global.CircuitBreakerRecoveryTimeout == "" {
		cfg.Global.CircuitBreakerRecoveryTimeout = "30s"
	}
	if cfg.Global.CircuitBreakerHalfOpenMaxCalls == 0 {
		cfg.Global.CircuitBreakerHalfOpenMaxCalls = 1
	}
}

func normalizeConfig(cfg *Config) error {
	for i := range cfg.Pipelines {
		p := &cfg.Pipelines[i]

		if err := normalizePlugin(&p.Source, fmt.Sprintf("pipeline %s source", p.ID)); err != nil {
			return err
		}
		for j := range p.Processors {
			if err := normalizePlugin(&p.Processors[j], fmt.Sprintf("pipeline %s processor %d", p.ID, j)); err != nil {
				return err
			}
		}
		for j := range p.Destinations {
			if err := normalizePlugin(&p.Destinations[j], fmt.Sprintf("pipeline %s destination %d", p.ID, j)); err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizePlugin(pc *PluginConfig, context string) error {
	typedName, typed, err := pc.typedProperties()
	if err != nil {
		return fmt.Errorf("%s: %w", context, err)
	}
	if typed != nil && typedName != pc.Type {
		return fmt.Errorf("%s: typed section %s does not match type %s", context, typedName, pc.Type)
	}
	if typed != nil {
		pc.Properties = typed
	} else {
		// Flat properties: use all Extra entries as properties.
		pc.Properties = make(map[string]interface{})
		for k, v := range pc.Extra {
			pc.Properties[k] = v
		}
	}
	if pc.Properties == nil {
		pc.Properties = map[string]interface{}{}
	}
	defaultPluginProperties(pc)
	return validatePluginProperties(pc, context)
}

func (pc *PluginConfig) typedProperties() (string, map[string]interface{}, error) {
	var foundName string
	var found map[string]interface{}
	for name, props := range pc.Extra {
		if props == nil {
			continue
		}
		m, ok := props.(map[string]interface{})
		if !ok {
			continue // scalar value, skip
		}
		if found != nil {
			return "", nil, fmt.Errorf("multiple typed plugin sections: %s and %s", foundName, name)
		}
		foundName = name
		found = m
	}
	return foundName, found, nil
}

func defaultPluginProperties(pc *PluginConfig) {
	switch pc.Type {
	case "file", "sqlite", "mssql":
		if isEmptyValue(pc.Properties["fetch_size"]) && isEmptyValue(pc.Properties["batch_size"]) {
			pc.Properties["fetch_size"] = 100
		}
	}
}

func validatePluginProperties(pc *PluginConfig, context string) error {
	if _, ok := pc.Properties["max_retries"]; ok {
		return fmt.Errorf("%s: max_retries belongs next to type, not inside properties", context)
	}
	if !isEmptyValue(pc.Properties["fetch_size"]) && !isEmptyValue(pc.Properties["batch_size"]) {
		return fmt.Errorf("%s: use fetch_size or batch_size, not both", context)
	}

	// Validate that fetch_size / batch_size is a positive integer.
	for _, sizeKey := range []string{"fetch_size", "batch_size"} {
		if isEmptyValue(pc.Properties[sizeKey]) {
			continue
		}
		var n int
		switch v := pc.Properties[sizeKey].(type) {
		case int:
			n = v
		case float64:
			n = int(v)
		default:
			return fmt.Errorf("%s: %s must be an integer", context, sizeKey)
		}
		if n <= 0 {
			return fmt.Errorf("%s: %s must be a positive integer, got %d", context, sizeKey, n)
		}
	}

	allowed, strict := allowedPluginKeys(pc.Type)
	if !strict {
		return nil
	}
	for key := range pc.Properties {
		if !allowed[key] {
			return fmt.Errorf("%s: unknown %s property %q", context, pc.Type, key)
		}
	}
	return validatePluginRequiredFields(pc, context)
}

var (
	schemasMu     sync.RWMutex
	pluginSchemas = make(map[string]PluginSchema)
)

type PluginSchema interface {
	AllowedKeys() []string
	RequiredKeys() []string
}

type SimplePluginSchema struct {
	Allowed  []string
	Required []string
}

func (s SimplePluginSchema) AllowedKeys() []string  { return s.Allowed }
func (s SimplePluginSchema) RequiredKeys() []string { return s.Required }

func RegisterPluginSchema(pluginType string, schema PluginSchema) {
	schemasMu.Lock()
	defer schemasMu.Unlock()
	pluginSchemas[pluginType] = schema
}

func allowedPluginKeys(pluginType string) (map[string]bool, bool) {
	schemasMu.RLock()
	schema, registered := pluginSchemas[pluginType]
	schemasMu.RUnlock()
	if !registered {
		return nil, false
	}
	allowed := make(map[string]bool)
	for _, key := range schema.AllowedKeys() {
		allowed[key] = true
	}
	return allowed, true
}

func validatePluginRequiredFields(pc *PluginConfig, context string) error {
	required := func(keys ...string) error {
		for _, key := range keys {
			if isEmptyValue(pc.Properties[key]) {
				return fmt.Errorf("%s: %s property %q is required", context, pc.Type, key)
			}
		}
		return nil
	}

	// Schema-based required fields.
	schemasMu.RLock()
	schema, registered := pluginSchemas[pc.Type]
	schemasMu.RUnlock()
	if registered {
		if err := required(schema.RequiredKeys()...); err != nil {
			return err
		}
	}

	// Type-specific validations (always run regardless of schema registration).
	switch pc.Type {
	case "file":
		if !registered {
			if err := required("path"); err != nil {
				return err
			}
		}
	case "sqlite":
		if !registered {
			if err := required("database"); err != nil {
				return err
			}
		}
		if err := validateSQLShape(pc, context); err != nil {
			return err
		}
	case "mssql":
		if !registered {
			if err := required("server", "database"); err != nil {
				return err
			}
		}
		if err := validateSQLShape(pc, context); err != nil {
			return err
		}
		if err := validateMSSQLAuth(pc, context); err != nil {
			return err
		}
	case "http":
		if !registered {
			if err := required("url"); err != nil {
				return err
			}
		}
	case "local_jsonl":
		if !registered {
			if err := required("path"); err != nil {
				return err
			}
		}
	case "sentinel", "azure_monitor":
		if !registered {
			if err := required("dce", "dcr", "stream"); err != nil {
				return err
			}
		}
		if err := validateSentinelAuth(pc, context); err != nil {
			return err
		}
	case "timestamp":
		if !registered {
			if err := required("source_layout"); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateSQLShape(pc *PluginConfig, context string) error {
	hasQuery := !isEmptyValue(pc.Properties["query"])
	hasTable := !isEmptyValue(pc.Properties["table"])
	if !hasQuery && !hasTable {
		return fmt.Errorf("%s: %s requires either query or table", context, pc.Type)
	}
	if !hasQuery && isEmptyValue(pc.Properties["timestamp_column"]) {
		return fmt.Errorf("%s: %s property %q is required when table is used", context, pc.Type, "timestamp_column")
	}
	if rawLookback, ok := pc.Properties["max_lookback"]; ok && !isEmptyValue(rawLookback) {
		lookback, ok := rawLookback.(string)
		if !ok {
			return fmt.Errorf("%s: %s property %q must be a duration string", context, pc.Type, "max_lookback")
		}
		if _, err := time.ParseDuration(lookback); err != nil {
			return fmt.Errorf("%s: invalid duration %s property %q=%q: %w", context, pc.Type, "max_lookback", lookback, err)
		}
	}
	if rawTimezone, ok := pc.Properties["source_timezone"]; ok && !isEmptyValue(rawTimezone) {
		tz, ok := rawTimezone.(string)
		if !ok {
			return fmt.Errorf("%s: %s property %q must be a timezone string", context, pc.Type, "source_timezone")
		}
		if _, err := time.LoadLocation(tz); err != nil {
			return fmt.Errorf("%s: invalid timezone %s property %q=%q: %w", context, pc.Type, "source_timezone", tz, err)
		}
	}
	return nil
}

// azureAuthMethods maps each auth method a plugin accepts to the auth
// properties it requires. ad-managed-identity takes an optional
// client_id, which selects a user-assigned identity.
var (
	mssqlAuthMethods = map[string][]string{
		"sql":                  {"user", "password"},
		"ad-password":          {"user", "password", "client_id"},
		"ad-managed-identity":  nil,
		"ad-service-principal": {"tenant_id", "client_id", "client_secret"},
	}
	sentinelAuthMethods = map[string][]string{
		"ad-managed-identity":  nil,
		"ad-service-principal": {"tenant_id", "client_id", "client_secret"},
	}
)

func validateMSSQLAuth(pc *PluginConfig, context string) error {
	return validateAuth(pc, context, "mssql", mssqlAuthMethods, true)
}

// validateSentinelAuth checks the optional auth block of a sentinel or
// azure_monitor destination. Without one, the destination uses a managed
// identity, selected by the top-level client_id if set.
func validateSentinelAuth(pc *PluginConfig, context string) error {
	if _, ok := pc.Properties["auth"]; ok && !isEmptyValue(pc.Properties["client_id"]) {
		return fmt.Errorf("%s: set client_id inside auth, not next to it", context)
	}
	return validateAuth(pc, context, pc.Type, sentinelAuthMethods, false)
}

// validateAuth checks pc's auth block against methods. The allowed auth
// properties are "method", "client_id" and every property some method
// requires.
func validateAuth(pc *PluginConfig, context, plugin string, methods map[string][]string, required bool) error {
	rawAuth, ok := pc.Properties["auth"]
	if !ok {
		if required {
			return fmt.Errorf("%s: %s property %q is required", context, plugin, "auth")
		}
		return nil
	}
	auth, ok := rawAuth.(map[string]interface{})
	if !ok {
		return fmt.Errorf("%s: %s auth must be a map", context, plugin)
	}
	allowed := map[string]bool{"method": true, "client_id": true}
	for _, keys := range methods {
		for _, key := range keys {
			allowed[key] = true
		}
	}
	for key := range auth {
		if !allowed[key] {
			return fmt.Errorf("%s: unknown %s auth property %q", context, plugin, key)
		}
	}
	method, _ := auth["method"].(string)
	if method == "" {
		return fmt.Errorf("%s: %s auth property %q is required", context, plugin, "method")
	}
	keys, ok := methods[method]
	if !ok {
		return fmt.Errorf("%s: unsupported %s auth method %q", context, plugin, method)
	}
	for _, key := range keys {
		if isEmptyValue(auth[key]) {
			return fmt.Errorf("%s: %s auth property %q is required for method %q", context, plugin, key, method)
		}
	}
	return nil
}

func isEmptyValue(value interface{}) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(v) == ""
	default:
		return false
	}
}

func (c *Config) Validate() error {
	if err := c.Global.validateLimits(); err != nil {
		return err
	}
	if len(c.Pipelines) == 0 {
		return fmt.Errorf("no pipelines defined")
	}
	if err := c.Global.validateDurations(); err != nil {
		return err
	}
	if err := validateSecretProviders(c.Secrets); err != nil {
		return err
	}

	seen := make(map[string]bool)
	for _, p := range c.Pipelines {
		if p.ID == "" {
			return fmt.Errorf("pipeline missing ID")
		}
		if seen[p.ID] {
			return fmt.Errorf("duplicate pipeline ID: %s", p.ID)
		}
		seen[p.ID] = true
		if err := p.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (g *GlobalConfig) validateLimits() error {
	if g.HTTPMaxIdleConnsPerHost < 0 {
		return fmt.Errorf("global.http_max_idle_conns_per_host cannot be negative")
	}
	if g.LogLevel != "" {
		switch g.LogLevel {
		case "debug", "info", "warn", "error":
		default:
			return fmt.Errorf("log_level must be one of debug, info, warn, error, got %q", g.LogLevel)
		}
	}
	if g.BufferSize <= 0 {
		return fmt.Errorf("buffer_size must be positive, got %d", g.BufferSize)
	}
	if g.MaxRetries < 0 {
		return fmt.Errorf("max_retries cannot be negative, got %d", g.MaxRetries)
	}
	if d, err := time.ParseDuration(g.SecretRefreshInterval); err == nil && d < 0 {
		return fmt.Errorf("secret_refresh_interval cannot be negative, got %s", g.SecretRefreshInterval)
	}
	return nil
}

func (g *GlobalConfig) validateDurations() error {
	return validateDurations("global.", []namedValue{
		{"poll_interval", g.PollInterval},
		{"shutdown_timeout", g.ShutdownTimeout},
		{"checkpoint_interval", g.CheckpointInterval},
		{"retry_base_backoff", g.RetryBaseBackoff},
		{"health_staleness_threshold", g.HealthStalenessThreshold},
		{"circuit_breaker_recovery_timeout", g.CircuitBreakerRecoveryTimeout},
		{"secret_refresh_interval", g.SecretRefreshInterval},
	})
}

func validateSecretProviders(providers map[string]SecretProviderConfig) error {
	for name, provider := range providers {
		switch provider.Type {
		case "env":
		case "keyvault":
			if strings.TrimSpace(provider.VaultURL) == "" {
				return fmt.Errorf("secret provider %s: vault_url is required for keyvault", name)
			}
		case "":
			return fmt.Errorf("secret provider %s: type is required", name)
		default:
			return fmt.Errorf("secret provider %s: unsupported type %q", name, provider.Type)
		}
	}
	return nil
}

// validate checks one pipeline's own settings. ID presence and
// uniqueness are checked by Config.Validate.
func (p *PipelineConfig) validate() error {
	if p.Source.Type == "" {
		return fmt.Errorf("pipeline %s: source type is required", p.ID)
	}
	if len(p.Destinations) == 0 {
		return fmt.Errorf("pipeline %s: at least one destination is required", p.ID)
	}
	for _, d := range p.Destinations {
		if d.Type == "" {
			return fmt.Errorf("pipeline %s: destination type is required", p.ID)
		}
		if d.MaxRetries < 0 {
			return fmt.Errorf("pipeline %s: destination %s max_retries cannot be negative, got %d", p.ID, d.Type, d.MaxRetries)
		}
	}

	if err := validateDurations(fmt.Sprintf("pipeline %s.", p.ID), []namedValue{
		{"runtime.poll_interval", p.Runtime.PollInterval},
		{"runtime.shutdown_timeout", p.Runtime.ShutdownTimeout},
		{"runtime.checkpoint_interval", p.Runtime.CheckpointInterval},
		{"retry.base_backoff", p.Retry.BaseBackoff},
	}); err != nil {
		return err
	}

	if p.Runtime.BufferSize < 0 {
		return fmt.Errorf("pipeline %s: runtime.buffer_size cannot be negative, got %d", p.ID, p.Runtime.BufferSize)
	}
	if p.Retry.MaxAttempts < 0 {
		return fmt.Errorf("pipeline %s: retry.max_attempts cannot be negative, got %d", p.ID, p.Retry.MaxAttempts)
	}
	if strings.TrimSpace(p.ID) != p.ID {
		return fmt.Errorf("pipeline ID %q must not have leading or trailing whitespace", p.ID)
	}
	return nil
}

type namedValue struct{ name, val string }

// validateDurations checks that every non-empty value parses as a
// time.Duration. Errors name the field as prefix+name.
func validateDurations(prefix string, fields []namedValue) error {
	for _, f := range fields {
		if f.val == "" {
			continue
		}
		if _, err := time.ParseDuration(f.val); err != nil {
			return fmt.Errorf("invalid duration %s%s=%q: %w", prefix, f.name, f.val, err)
		}
	}
	return nil
}

// PipelineSettings is a mirror of the pipeline package's settings
// struct, used by PipelineSettingsFromConfig to build runtime
// settings without importing the pipeline package. The conversion
// happens in main.go.
type PipelineSettings struct {
	PollInterval       time.Duration
	SendBatchSize      int
	BufferSize         int
	MaxRetries         int
	RetryBaseBackoff   time.Duration
	CheckpointInterval time.Duration
	ShutdownTimeout    time.Duration
	// StalledThresholdMultiplier mirrors pipeline.PipelineSettings.
	// Zero means "use the default" (5).
	StalledThresholdMultiplier int
	// CircuitBreakerFailureThreshold is the per-destination
	// circuit breaker failure threshold. Zero means
	// "use the default" (5).
	CircuitBreakerFailureThreshold int
	// CircuitBreakerRecoveryTimeout is the per-destination
	// circuit breaker cool-off before transitioning to
	// half-open. Zero means "use the default" (30s).
	CircuitBreakerRecoveryTimeout time.Duration
	// CircuitBreakerHalfOpenMaxCalls is the maximum number of
	// concurrent probe calls admitted in the half-open state. Zero
	// means "use the default" (1).
	CircuitBreakerHalfOpenMaxCalls int
}

// PipelineSettingsFromConfig builds a config-package PipelineSettings
// from the global config and per-pipeline overrides. The defaults
// match the pipeline package's DefaultSettings. The returned value is
// then converted to *pipeline.PipelineSettings in main.go.
func PipelineSettingsFromConfig(g *GlobalConfig, pc *PipelineConfig) *PipelineSettings {
	s := &PipelineSettings{
		PollInterval:                   1 * time.Second,
		SendBatchSize:                  0,
		BufferSize:                     100,
		MaxRetries:                     4,
		RetryBaseBackoff:               1 * time.Second,
		CheckpointInterval:             0,
		ShutdownTimeout:                30 * time.Second,
		StalledThresholdMultiplier:     5,
		CircuitBreakerFailureThreshold: 5,
		CircuitBreakerRecoveryTimeout:  30 * time.Second,
		CircuitBreakerHalfOpenMaxCalls: 1,
	}
	if g != nil {
		if g.PollInterval != "" {
			if d, err := time.ParseDuration(g.PollInterval); err == nil {
				s.PollInterval = d
			}
		}
		if g.ShutdownTimeout != "" {
			if d, err := time.ParseDuration(g.ShutdownTimeout); err == nil {
				s.ShutdownTimeout = d
			}
		}
		if g.CheckpointInterval != "" {
			if d, err := time.ParseDuration(g.CheckpointInterval); err == nil {
				s.CheckpointInterval = d
			}
		}
		if g.RetryBaseBackoff != "" {
			if d, err := time.ParseDuration(g.RetryBaseBackoff); err == nil {
				s.RetryBaseBackoff = d
			}
		}
		if g.MaxRetries > 0 {
			s.MaxRetries = g.MaxRetries
		}
		if g.SendBatchSize > 0 {
			s.SendBatchSize = g.SendBatchSize
		}
		if g.BufferSize > 0 {
			s.BufferSize = g.BufferSize
		}
		if g.StalledThresholdMultiplier > 0 {
			s.StalledThresholdMultiplier = g.StalledThresholdMultiplier
		}
		if g.CircuitBreakerFailureThreshold > 0 {
			s.CircuitBreakerFailureThreshold = g.CircuitBreakerFailureThreshold
		}
		if g.CircuitBreakerRecoveryTimeout != "" {
			if d, err := time.ParseDuration(g.CircuitBreakerRecoveryTimeout); err == nil {
				s.CircuitBreakerRecoveryTimeout = d
			}
		}
		if g.CircuitBreakerHalfOpenMaxCalls > 0 {
			s.CircuitBreakerHalfOpenMaxCalls = g.CircuitBreakerHalfOpenMaxCalls
		}
	}
	if pc != nil {
		if pc.Runtime.PollInterval != "" {
			if d, err := time.ParseDuration(pc.Runtime.PollInterval); err == nil {
				s.PollInterval = d
			}
		}
		if pc.Runtime.ShutdownTimeout != "" {
			if d, err := time.ParseDuration(pc.Runtime.ShutdownTimeout); err == nil {
				s.ShutdownTimeout = d
			}
		}
		if pc.Runtime.CheckpointInterval != "" {
			if d, err := time.ParseDuration(pc.Runtime.CheckpointInterval); err == nil {
				s.CheckpointInterval = d
			}
		}
		if pc.Retry.BaseBackoff != "" {
			if d, err := time.ParseDuration(pc.Retry.BaseBackoff); err == nil {
				s.RetryBaseBackoff = d
			}
		}
		if pc.Retry.MaxAttempts > 0 {
			s.MaxRetries = pc.Retry.MaxAttempts
		}
		if pc.Runtime.SendBatchSize > 0 {
			s.SendBatchSize = pc.Runtime.SendBatchSize
		}
		if pc.Runtime.BufferSize > 0 {
			s.BufferSize = pc.Runtime.BufferSize
		}
	}
	return s
}

// ToPipelineSettings converts this config-side settings struct to a
// pipeline.PipelineSettings. Defined in main.go (the only place that
// imports both packages).
// ToPipelineSettings is a small adapter that lives in main.go because
// it needs both packages.
