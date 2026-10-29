package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"loghub/internal/pipeline"
)

type Orchestrator struct {
	// pipelines is guarded by mu: /health reads it while a reload
	// replaces it.
	pipelines    []*pipeline.Pipeline
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	reloadCh     chan struct{}
	running      bool
	reload       ReloadFunc
	startedAt    time.Time
	mu           sync.Mutex
}

func NewOrchestrator() *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Orchestrator{
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
		reloadCh:   make(chan struct{}, 1),
	}
}

// RequestReload asks for a reload, as SIGHUP does. The reload runs on the
// goroutine that handles signals, so it never overlaps a SIGHUP reload or
// shutdown. A request made while one is pending is merged into it. It has
// no effect before Start.
func (o *Orchestrator) RequestReload() {
	select {
	case o.reloadCh <- struct{}{}:
	default:
	}
}

func (o *Orchestrator) AddPipeline(p *pipeline.Pipeline) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pipelines = append(o.pipelines, p)
}

// snapshot returns a copy of the pipeline list.
func (o *Orchestrator) snapshot() []*pipeline.Pipeline {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*pipeline.Pipeline(nil), o.pipelines...)
}

// ReplacePipelines stops all existing pipelines and starts the provided
// set in their place. It is used by the reload path. The old pipelines
// are stopped first because the new ones reuse their checkpoints, disk
// buffers and DLQs.
//
// If a new pipeline fails to start, the ones already started are
// stopped, the rest are closed, and the orchestrator is left with no
// pipelines; the caller restores the previous configuration.
func (o *Orchestrator) ReplacePipelines(pipelines []*pipeline.Pipeline) error {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return errors.New("orchestrator not running")
	}
	old := o.pipelines
	o.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range old {
		wg.Add(1)
		go func(p *pipeline.Pipeline) {
			defer wg.Done()
			p.Stop()
		}(p)
	}
	wg.Wait()

	o.mu.Lock()
	o.pipelines = pipelines
	o.mu.Unlock()

	for i, p := range pipelines {
		if err := p.Start(o.ctx); err != nil {
			for _, sp := range pipelines[:i] {
				sp.Stop()
			}
			for _, up := range pipelines[i:] {
				if closeErr := up.Close(); closeErr != nil {
					slog.Error("Error closing pipeline that was not started", "pipeline_id", up.ID, "error", closeErr)
				}
			}
			o.mu.Lock()
			o.pipelines = nil
			o.mu.Unlock()
			return fmt.Errorf("pipeline %s: %w", p.ID, err)
		}
	}
	return nil
}

func (o *Orchestrator) Running() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.running
}

// PipelineHealth aggregates per-pipeline health for the /health endpoint.
type PipelineHealth struct {
	ID          string
	Status      string
	StartedAt   time.Time
	LastCollect time.Time
	LastSend    time.Time
	LastError   time.Time
	ErrorMsg    string
}

// Health returns a snapshot of the orchestrator's health for use by
// the /health and /ready HTTP endpoints. Reports uptime since the
// last Start.
func (o *Orchestrator) Health() (running bool, uptime time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.running, time.Since(o.startedAt)
}

// PipelineHealths returns a snapshot of each pipeline's health state.
func (o *Orchestrator) PipelineHealths() []PipelineHealth {
	var out []PipelineHealth
	for _, p := range o.snapshot() {
		h := p.Health()
		out = append(out, PipelineHealth{
			ID:          p.ID,
			Status:      string(h.Status),
			StartedAt:   h.StartedAt,
			LastCollect: h.LastCollect,
			LastSend:    h.LastSend,
			LastError:   h.LastError,
			ErrorMsg:    h.ErrorMsg,
		})
	}
	return out
}

func (o *Orchestrator) Start() error {
	o.mu.Lock()
	if o.running {
		o.mu.Unlock()
		return nil
	}
	o.running = true
	o.startedAt = time.Now()
	o.mu.Unlock()

	// If Start returns without launching signalHandler
	// (e.g. a pipeline failed to start), close shutdownCh so Wait
	// unblocks. On the success path, signalHandler takes
	// responsibility for the close (via Shutdown or the
	// <-o.shutdownCh branch), and the sync.Once makes this defer a
	// no-op.
	success := false
	defer func() {
		if !success {
			o.shutdownOnce.Do(func() { close(o.shutdownCh) })
		}
	}()

	var started []*pipeline.Pipeline

	for _, p := range o.snapshot() {
		if err := p.Start(o.ctx); err != nil {
			for _, sp := range started {
				sp.Stop()
			}
			o.mu.Lock()
			o.running = false
			o.mu.Unlock()
			return err
		}
		started = append(started, p)
	}

	o.wg.Add(1)
	success = true
	go o.signalHandler()

	return nil
}

// ReloadFunc is called when a SIGHUP is received. It should reload
// any external config (config file, secrets, etc.) and return a new
// config or an error. The orchestrator logs the result and continues
// running with the existing config on error.
type ReloadFunc func() error

// SetReloadFunc registers a callback to be invoked on SIGHUP. This
// enables hot-reload of configuration without process restart.
func (o *Orchestrator) SetReloadFunc(fn ReloadFunc) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reload = fn
}

func (o *Orchestrator) signalHandler() {
	defer o.wg.Done()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for {
		// Also exit when shutdownCh is closed by
		// Shutdown() so signalHandler's wg.Done() fires and
		// Wait() can return. The signal-driven path is still
		// handled by the `sig` branch below.
		select {
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				o.runReload("SIGHUP")
				continue
			}
			slog.Info("Received signal, initiating graceful shutdown...", "signal", sig)
			o.cancel()
			// Shutdown logs the outcome, including per-pipeline errors.
			_ = o.Shutdown()
			return
		case <-o.reloadCh:
			o.runReload("request")
		case <-o.shutdownCh:
			// Shutdown was called directly; signalHandler's job
			// is done.
			return
		}
	}
}

// runReload calls the registered reload function, logging the outcome.
func (o *Orchestrator) runReload(trigger string) {
	o.mu.Lock()
	fn := o.reload
	o.mu.Unlock()
	if fn == nil {
		slog.Warn("Reload requested but no reload function is registered", "trigger", trigger)
		return
	}
	if err := fn(); err != nil {
		slog.Error("Config reload failed; continuing with current config", "trigger", trigger, "error", err)
		return
	}
	slog.Info("Config reloaded", "trigger", trigger)
}

func (o *Orchestrator) Wait() {
	<-o.shutdownCh
	o.wg.Wait()
}

// shutdownCloseAllowance is the time Shutdown allows, beyond the longest
// pipeline MaxStopDuration, for closing sources and destinations.
const shutdownCloseAllowance = 5 * time.Second

// Shutdown stops all pipelines in parallel. See ShutdownContext.
func (o *Orchestrator) Shutdown() error {
	return o.ShutdownContext(context.Background())
}

// ShutdownContext stops all pipelines in parallel under one deadline: the
// longest pipeline MaxStopDuration plus shutdownCloseAllowance, or ctx's
// if earlier. It returns each pipeline's stop error, joined; a pipeline
// still stopping at the deadline is reported and left behind, so the
// process can exit within its termination grace period.
func (o *Orchestrator) ShutdownContext(ctx context.Context) error {
	slog.Info("Waiting for pipelines to flush buffers...")
	pipelines := o.snapshot()
	var budget time.Duration
	for _, p := range pipelines {
		budget = max(budget, p.MaxStopDuration())
	}
	ctx, cancel := context.WithTimeout(ctx, budget+shutdownCloseAllowance)
	defer cancel()

	errs := make([]error, len(pipelines))
	stopped := make([]bool, len(pipelines))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, p := range pipelines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.StopContext(ctx)
			mu.Lock()
			defer mu.Unlock()
			stopped[i] = true
			if err != nil {
				errs[i] = fmt.Errorf("pipeline %s: %w", p.ID, err)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}

	mu.Lock()
	for i, p := range pipelines {
		if !stopped[i] {
			errs[i] = fmt.Errorf("pipeline %s: did not stop before the shutdown deadline: %w", p.ID, pipeline.ErrShutdownTimeout)
		}
	}
	err := errors.Join(errs...)
	mu.Unlock()

	o.mu.Lock()
	o.running = false
	o.mu.Unlock()

	// Close shutdownCh here so direct Shutdown() callers (e.g. tests)
	// unblock Wait(). The
	// sync.Once makes this safe to call alongside signalHandler's
	// close.
	o.shutdownOnce.Do(func() { close(o.shutdownCh) })

	if err != nil {
		slog.Error("Pipelines stopped with errors", "error", err)
		return err
	}
	slog.Info("All pipelines stopped, exiting cleanly.")
	return nil
}
