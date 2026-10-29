// Package bootstrap contains the application wiring logic extracted from
// cmd/loghub/main.go. It is responsible for reading config,
// resolving secrets, building the checkpointer, constructing pipelines, and
// starting the orchestrator. Keeping this in a separate package makes the
// entry point thin and allows the bootstrap logic to be unit-tested.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	"loghub/internal/buffer"
	"loghub/internal/checkpointer"
	"loghub/internal/config"
	"loghub/internal/core"
	"loghub/internal/dlq"
	"loghub/internal/orchestrator"
	"loghub/internal/pipeline"
	"loghub/internal/plugins/destination"
	"loghub/internal/plugins/processor"
	"loghub/internal/plugins/secrets"
	"loghub/internal/plugins/source"
	_ "loghub/internal/schemas"
)

// App holds the fully wired LogHub runtime.
type App struct {
	Config       *config.Config
	Checkpointer *checkpointer.FileCheckpointer
	Orchestrator *orchestrator.Orchestrator
	secretHolder *secrets.CachedSecretHolder
	configPath   string

	// reloadOnSecretChange mirrors Config.Global.ReloadOnSecretChange
	// for the background secret refresh, which runs concurrently with
	// reloads that replace Config.
	reloadOnSecretChange atomic.Bool
}

// NewApp reads configuration, resolves secrets, and wires all pipelines into
// an orchestrator. It does not start the orchestrator; call Run() for that.
func NewApp(ctx context.Context, configPath string) (*App, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	rawSecretHolder, err := secrets.NewSecretHolder(secretsMapFromConfig(cfg.Secrets))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize secrets provider: %w", err)
	}
	rawSecretHolder.Strict = cfg.Global.StrictResolution
	secretHolder := secrets.NewCachedSecretHolder(rawSecretHolder, 5*time.Minute)
	refreshInterval, err := time.ParseDuration(cfg.Global.SecretRefreshInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid secret_refresh_interval: %w", err)
	}

	cp, err := checkpointer.NewFileCheckpointer(cfg.Global.CheckpointsDir)
	if err != nil {
		rawSecretHolder.Close()
		return nil, fmt.Errorf("failed to create checkpointer: %w", err)
	}

	orch := orchestrator.NewOrchestrator()

	app := &App{
		Config:       cfg,
		Checkpointer: cp,
		Orchestrator: orch,
		secretHolder: secretHolder,
		configPath:   configPath,
	}

	if err := app.buildPipelines(ctx); err != nil {
		cp.Close()
		rawSecretHolder.Close()
		return nil, err
	}

	orch.SetReloadFunc(app.buildReloadFunc(ctx))
	app.reloadOnSecretChange.Store(cfg.Global.ReloadOnSecretChange)
	if refreshInterval > 0 {
		secretHolder.StartRefresh(refreshInterval, app.onSecretChange)
	}

	return app, nil
}

// onSecretChange handles a secret rotation seen by the background refresh.
// Pipelines hold the values resolved when they were built, so the new
// value applies only on a reload.
func (a *App) onSecretChange(changed []string) {
	if a.reloadOnSecretChange.Load() {
		slog.Info("Secret value changed; reloading pipelines", "secrets", changed)
		a.Orchestrator.RequestReload()
		return
	}
	slog.Warn("Secret value changed; send SIGHUP or restart to apply it (or set global.reload_on_secret_change)", "secrets", changed)
}

// Run starts the orchestrator and blocks until shutdown.
func (a *App) Run() error {
	if err := a.Orchestrator.Start(); err != nil {
		return fmt.Errorf("failed to start orchestrator: %w", err)
	}
	a.Orchestrator.Wait()
	return nil
}

// Close cleans up resources held by the App.
func (a *App) Close() error {
	var firstErr error
	if err := a.Checkpointer.Close(); err != nil {
		firstErr = err
	}
	a.secretHolder.Close()
	return firstErr
}

// ValidatePluginTypes checks that every source, destination and processor
// type in cfg has a registered factory. config.Load cannot check this,
// because the config package does not import the plugins; without it, a
// misspelled type passes --validate and fails only at startup.
func ValidatePluginTypes(cfg *config.Config) error {
	for _, pc := range cfg.Pipelines {
		if !source.IsRegistered(pc.Source.Type) {
			return fmt.Errorf("pipeline %s: unknown source type %q", pc.ID, pc.Source.Type)
		}
		for _, dc := range pc.Destinations {
			if !destination.IsRegistered(dc.Type) {
				return fmt.Errorf("pipeline %s: unknown destination type %q", pc.ID, dc.Type)
			}
		}
		for _, proc := range pc.Processors {
			if !processor.IsRegistered(proc.Type) {
				return fmt.Errorf("pipeline %s: unknown processor type %q", pc.ID, proc.Type)
			}
		}
	}
	return nil
}

// buildPipelines constructs each configured pipeline and registers it with
// the orchestrator. It logs and skips pipelines that fail to build.
func (a *App) buildPipelines(ctx context.Context) error {
	pipelines, err := a.buildPipelineList(ctx, a.Config, a.Checkpointer, a.secretHolder, false)
	if err != nil {
		return err
	}
	for _, p := range pipelines {
		a.Orchestrator.AddPipeline(p)
	}
	return nil
}

// buildPipelineList constructs pipelines from the given config without
// registering them with an orchestrator. At startup (allOrNothing false)
// a pipeline that fails to build is skipped; on reload (allOrNothing
// true) any failure fails the whole build, so the running pipelines are
// kept instead of being replaced by a partial set.
func (a *App) buildPipelineList(ctx context.Context, cfg *config.Config, cp *checkpointer.FileCheckpointer, sh secrets.Resolver, allOrNothing bool) ([]*pipeline.Pipeline, error) {
	var failedPipelines []string
	var succeededPipelines []string
	var pipelines []*pipeline.Pipeline

	for _, pc := range cfg.Pipelines {
		slog.Info("Configuring pipeline", "pipeline_id", pc.ID, "source_type", pc.Source.Type, "destinations", len(pc.Destinations), "processors", len(pc.Processors))
		pipelineCtx, pipelineCancel := context.WithCancel(ctx)
		defer pipelineCancel()

		resolvedProps, err := ResolveProperties(pipelineCtx, pc.Source.Properties, sh)
		if err != nil {
			slog.Error("Failed to resolve source properties", "pipeline_id", pc.ID, "error", err)
			failedPipelines = append(failedPipelines, pc.ID)
			continue
		}

		src, err := source.Create(pc.Source.Type, pipelineCtx, resolvedProps, sh)
		if err != nil {
			slog.Error("Failed to create source for pipeline", "pipeline_id", pc.ID, "error", err)
			failedPipelines = append(failedPipelines, pc.ID)
			continue
		}

		var dests []pipeline.DestinationInfo
		var destClosers []core.Destination
		destFailed := false
		for _, destConfig := range pc.Destinations {
			resolvedDestProps, err := ResolveProperties(pipelineCtx, destConfig.Properties, sh)
			if err != nil {
				slog.Error("Failed to resolve destination properties", "pipeline_id", pc.ID, "type", destConfig.Type, "error", err)
				destFailed = true
				break
			}
			dest, err := destination.CreateWithHTTPPool(destConfig.Type, resolvedDestProps, cfg.Global.HTTPMaxIdleConnsPerHost)
			if err != nil {
				slog.Error("Failed to create destination for pipeline", "pipeline_id", pc.ID, "type", destConfig.Type, "error", err)
				destFailed = true
				break
			}
			// HTTP-based destinations send the source name in X-LogSource.
			if named, ok := dest.(interface{ SetSourceName(string) }); ok {
				named.SetSourceName(src.Name())
			}
			destClosers = append(destClosers, dest)
			dests = append(dests, pipeline.DestinationInfo{
				Destination: dest,
				MaxRetries:  destConfig.MaxRetries,
			})
		}

		if destFailed || len(dests) == 0 {
			slog.Error("No valid destinations created for pipeline, rolling back", "pipeline_id", pc.ID)
			for _, d := range destClosers {
				d.Close()
			}
			if src != nil {
				src.Close()
			}
			failedPipelines = append(failedPipelines, pc.ID)
			continue
		}

		// A processor that cannot be built fails the whole pipeline, like a
		// source or destination. Running without it would deliver records
		// that a filter was meant to remove.
		var procs []core.Processor
		procFailed := false
		for _, procConfig := range pc.Processors {
			proc, err := processor.Create(procConfig.Type, procConfig.Properties)
			if err != nil {
				slog.Error("Failed to create processor for pipeline", "pipeline_id", pc.ID, "type", procConfig.Type, "error", err)
				procFailed = true
				break
			}
			procs = append(procs, proc)
		}
		if procFailed {
			src.Close()
			for _, d := range destClosers {
				d.Close()
			}
			failedPipelines = append(failedPipelines, pc.ID)
			continue
		}

		settings, err := BuildPipelineSettings(&cfg.Global, &pc)
		if err != nil {
			slog.Error("Failed to parse pipeline settings", "pipeline_id", pc.ID, "error", err)
			src.Close()
			for _, d := range destClosers {
				d.Close()
			}
			failedPipelines = append(failedPipelines, pc.ID)
			continue
		}

		diskBuf, dlqWriter, err := buildDurability(cfg, pc, src, destClosers, cp)
		if err != nil {
			failedPipelines = append(failedPipelines, pc.ID)
			continue
		}

		p := pipeline.NewPipeline(pc.ID, src, procs, dests, cp, settings, diskBuf, dlqWriter)
		pipelines = append(pipelines, p)
		succeededPipelines = append(succeededPipelines, pc.ID)
	}

	if len(succeededPipelines) == 0 {
		return nil, fmt.Errorf("no pipelines were successfully configured; failed=%v", failedPipelines)
	}
	if allOrNothing && len(failedPipelines) > 0 {
		for _, p := range pipelines {
			if err := p.Close(); err != nil {
				slog.Error("Error closing pipeline that was not started", "pipeline_id", p.ID, "error", err)
			}
		}
		return nil, fmt.Errorf("some pipelines failed to configure; failed=%v", failedPipelines)
	}
	if len(failedPipelines) > 0 {
		slog.Warn("Some pipelines failed to configure and were skipped", "failed", failedPipelines, "succeeded", succeededPipelines)
	}
	return pipelines, nil
}

func buildDurability(cfg *config.Config, pc config.PipelineConfig, src core.Source, destClosers []core.Destination, cp core.Checkpointer) (*buffer.DiskBuffer, *dlq.DeadLetterQueue, error) {
	var diskBuf *buffer.DiskBuffer
	var dlqWriter *dlq.DeadLetterQueue

	cleanup := func() {
		if src != nil {
			src.Close()
		}
		for _, d := range destClosers {
			d.Close()
		}
		if diskBuf != nil {
			diskBuf.Close()
		}
		if dlqWriter != nil {
			dlqWriter.Close()
		}
	}

	if pc.Durability.DiskBuffer.Enabled {
		dir, err := DiskBufferDir(&cfg.Global, &pc)
		if err != nil {
			slog.Error("Cannot place disk buffer; refusing to start", "pipeline_id", pc.ID, "error", err)
			cleanup()
			return nil, nil, fmt.Errorf("pipeline %s durability setup failed", pc.ID)
		}
		maxSize := pc.Durability.DiskBuffer.MaxSizeMB
		if maxSize == 0 {
			maxSize = 1024
		}
		diskBuf, err = buffer.NewDiskBuffer(dir, pc.ID, maxSize)
		if err != nil {
			slog.Error("Failed to create disk buffer", "pipeline_id", pc.ID, "error", err)
			cleanup()
			return nil, nil, fmt.Errorf("pipeline %s durability setup failed", pc.ID)
		}
	}

	if pc.Durability.DLQ.Enabled {
		dir, err := DLQDir(&cfg.Global, &pc)
		if err != nil {
			slog.Error("Cannot place DLQ; refusing to start", "pipeline_id", pc.ID, "error", err)
			cleanup()
			return nil, nil, fmt.Errorf("pipeline %s durability setup failed", pc.ID)
		}
		maxSize := pc.Durability.DLQ.MaxSizeMB
		if maxSize == 0 {
			maxSize = 1024
		}
		dlqWriter, err = dlq.New(dir, pc.ID, maxSize)
		if err != nil {
			slog.Error("Failed to create DLQ", "pipeline_id", pc.ID, "error", err)
			cleanup()
			return nil, nil, fmt.Errorf("pipeline %s durability setup failed", pc.ID)
		}
	}

	if err := importUnifiedJournal(cfg.Global.CheckpointsDir, pc.ID, cp, diskBuf, dlqWriter); err != nil {
		slog.Error("Failed to import shared pipeline journal", "pipeline_id", pc.ID, "error", err)
		cleanup()
		return nil, nil, fmt.Errorf("pipeline %s durability setup failed: %w", pc.ID, err)
	}

	return diskBuf, dlqWriter, nil
}

// DiskBufferDir returns the directory that holds pc's disk buffer:
// durability.disk_buffer.dir, or <global.data_dir>/disk-buffer/<id>.
func DiskBufferDir(g *config.GlobalConfig, pc *config.PipelineConfig) (string, error) {
	return durabilityDir(g, pc, pc.Durability.DiskBuffer.Dir, "durability.disk_buffer.dir", "disk-buffer")
}

// DLQDir returns the directory that holds pc's dead-letter queue:
// durability.dlq.dir, or <global.data_dir>/dlq/<id>.
func DLQDir(g *config.GlobalConfig, pc *config.PipelineConfig) (string, error) {
	return durabilityDir(g, pc, pc.Durability.DLQ.Dir, "durability.dlq.dir", "dlq")
}

func durabilityDir(g *config.GlobalConfig, pc *config.PipelineConfig, explicit, field, subdir string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if g.DataDir == "" {
		return "", fmt.Errorf("pipeline %s: %s is required when global.data_dir is not set", pc.ID, field)
	}
	return filepath.Join(g.DataDir, subdir, pc.ID), nil
}

// ResolveProperties walks a property map and resolves any secret references
// using the provided resolver.
func ResolveProperties(ctx context.Context, props map[string]interface{}, sh secrets.Resolver) (map[string]interface{}, error) {
	resolved := make(map[string]interface{}, len(props))
	for k, v := range props {
		switch val := v.(type) {
		case string:
			resolvedVal, err := sh.Resolve(ctx, val)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve property %s: %w", k, err)
			}
			resolved[k] = resolvedVal
		case map[string]interface{}:
			resolvedVal, err := ResolveProperties(ctx, val, sh)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve property %s: %w", k, err)
			}
			resolved[k] = resolvedVal
		default:
			resolved[k] = v
		}
	}
	return resolved, nil
}

// BuildPipelineSettings converts config-level pipeline settings into the
// runtime struct used by the pipeline package.
func BuildPipelineSettings(g *config.GlobalConfig, pc *config.PipelineConfig) (*pipeline.PipelineSettings, error) {
	cs := config.PipelineSettingsFromConfig(g, pc)
	return &pipeline.PipelineSettings{
		PollInterval:                   cs.PollInterval,
		SendBatchSize:                  cs.SendBatchSize,
		BufferSize:                     cs.BufferSize,
		MaxRetries:                     cs.MaxRetries,
		RetryBaseBackoff:               cs.RetryBaseBackoff,
		CheckpointInterval:             cs.CheckpointInterval,
		ShutdownTimeout:                cs.ShutdownTimeout,
		StalledThresholdMultiplier:     cs.StalledThresholdMultiplier,
		CircuitBreakerFailureThreshold: cs.CircuitBreakerFailureThreshold,
		CircuitBreakerRecoveryTimeout:  cs.CircuitBreakerRecoveryTimeout,
		CircuitBreakerHalfOpenMaxCalls: cs.CircuitBreakerHalfOpenMaxCalls,
	}, nil
}

// buildReloadFunc returns a closure that reloads config and rebuilds
// pipelines on SIGHUP. It loads the latest config file, looks up every
// cached secret again (so a rotated secret applies now; a failed lookup
// keeps the last value), and calls ReplacePipelines on the orchestrator.
func (a *App) buildReloadFunc(ctx context.Context) func() error {
	return func() error {
		cfg, err := config.Load(a.configPath)
		if err != nil {
			return fmt.Errorf("failed to reload config: %w", err)
		}

		if changed, err := a.secretHolder.Refresh(ctx); err != nil {
			slog.Warn("Some secrets could not be looked up; using their last resolved values", "error", err)
		} else if len(changed) > 0 {
			slog.Info("Reload picked up changed secrets", "secrets", changed)
		}

		pipelines, err := a.buildPipelineList(ctx, cfg, a.Checkpointer, a.secretHolder, true)
		if err != nil {
			return fmt.Errorf("failed to rebuild pipelines: %w", err)
		}

		if err := a.Orchestrator.ReplacePipelines(pipelines); err != nil {
			// The old pipelines are already stopped and closed, so they
			// are rebuilt from the previous config.
			slog.Error("New pipelines failed to start; restoring the previous configuration", "error", err)
			previous, buildErr := a.buildPipelineList(ctx, a.Config, a.Checkpointer, a.secretHolder, false)
			if buildErr != nil {
				return errors.Join(fmt.Errorf("failed to replace pipelines: %w", err), fmt.Errorf("failed to restore previous pipelines: %w", buildErr))
			}
			if restoreErr := a.Orchestrator.ReplacePipelines(previous); restoreErr != nil {
				return errors.Join(fmt.Errorf("failed to replace pipelines: %w", err), fmt.Errorf("failed to restore previous pipelines: %w", restoreErr))
			}
			return fmt.Errorf("failed to replace pipelines; previous configuration restored: %w", err)
		}

		a.Config = cfg
		a.reloadOnSecretChange.Store(cfg.Global.ReloadOnSecretChange)
		return nil
	}
}

func secretsMapFromConfig(cfg config.SecretsConfig) map[string]secrets.ProviderConfig {
	result := make(map[string]secrets.ProviderConfig)
	for name, pcfg := range cfg {
		result[name] = secrets.ProviderConfig{
			Type:     pcfg.Type,
			VaultURL: pcfg.VaultURL,
			TenantID: pcfg.TenantID,
			ClientID: pcfg.ClientID,
		}
	}
	return result
}
