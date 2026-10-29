package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"loghub/internal/bootstrap"
	"loghub/internal/config"
	"loghub/internal/metrics"
)

// version is the release version, set at build time with
// -ldflags "-X main.version=<version>".
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "dlq" {
		if err := runDLQ(os.Args[2:], os.Stdout, os.Stderr); err != nil {
			fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
			os.Exit(1)
		}
		return
	}

	configPath := flag.String("config", "loghub.yaml", "Path to configuration file")
	metricsAddr := flag.String("metrics", ":9090", "Address to expose Prometheus metrics")
	validateOnly := flag.Bool("validate", false, "Validate configuration and exit")
	showVersion := flag.Bool("version", false, "Print the version and exit")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: loghub [flags]\n       loghub dlq <list|export> [flags]\n\nflags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *showVersion {
		fmt.Printf("loghub %s\n", version)
		return
	}

	metrics.Register()

	if err := run(*configPath, *metricsAddr, *validateOnly); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath, metricsAddr string, validateOnly bool) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	if err := configureLogger(cfg.Global.LogLevel); err != nil {
		return err
	}
	slog.Info("Loaded configuration", "version", version, "config", configPath, "pipelines", len(cfg.Pipelines), "log_level", cfg.Global.LogLevel)
	if validateOnly {
		fmt.Printf("config %s is valid\n", configPath)
		return nil
	}

	app, err := bootstrap.NewApp(context.Background(), configPath)
	if err != nil {
		return err
	}
	defer app.Close()

	if cfg.Global.MetricsEnabled {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", metrics.Handler())
			mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
				running, uptime := app.Orchestrator.Health()
				status := http.StatusOK
				if !running {
					status = http.StatusServiceUnavailable
				}
				resp := map[string]interface{}{
					"status":    "up",
					"running":   running,
					"uptime":    uptime.String(),
					"pipelines": app.Orchestrator.PipelineHealths(),
				}
				if !running {
					resp["status"] = "down"
				}

				// Staleness check: if health_staleness_threshold is set, mark
				// the overall response unhealthy if any pipeline is stale.
				// A pipeline in the Stalled state is always reported unhealthy
				// — the operator should investigate.
				var threshold time.Duration
				if cfg.Global.HealthStalenessThreshold != "" {
					if d, err := time.ParseDuration(cfg.Global.HealthStalenessThreshold); err == nil {
						threshold = d
					}
				}
				now := time.Now()
				for _, ph := range app.Orchestrator.PipelineHealths() {
					if ph.Status == "stalled" {
						status = http.StatusServiceUnavailable
						resp["status"] = "down"
						resp["reason"] = "pipeline " + ph.ID + " is stalled"
						break
					}
					if threshold <= 0 {
						continue
					}
					if ph.Status != "running" && ph.Status != "degraded" {
						continue
					}
					stale := false
					if !ph.LastCollect.IsZero() && now.Sub(ph.LastCollect) > threshold {
						stale = true
					}
					if !ph.LastSend.IsZero() && now.Sub(ph.LastSend) > threshold {
						stale = true
					}
					if stale {
						status = http.StatusServiceUnavailable
						resp["status"] = "down"
						resp["reason"] = "pipeline " + ph.ID + " is stale"
						break
					}
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				if err := json.NewEncoder(w).Encode(resp); err != nil {
					slog.Error("Failed to encode health response", "error", err)
				}
			})
			slog.Info("Starting metrics server", "addr", metricsAddr)
			if err := http.ListenAndServe(metricsAddr, mux); err != nil {
				slog.Error("Metrics server error", "error", err)
			}
		}()
	}

	return app.Run()
}

func configureLogger(level string) error {
	slogLevel, err := parseLogLevel(level)
	if err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slogLevel,
		AddSource: slogLevel <= slog.LevelDebug,
	})))
	return nil
}

func parseLogLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unsupported log level %q", level)
	}
}
