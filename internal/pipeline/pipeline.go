package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"loghub/internal/buffer"
	"loghub/internal/core"
	"loghub/internal/dlq"
	"loghub/internal/metrics"
	"loghub/internal/retry"
)

// ErrDestinationUnreachable is returned when a destination's retries are
// exhausted without a more specific error to report.
var ErrDestinationUnreachable = errors.New("destination unreachable after retries")

type PipelineSettings struct {
	PollInterval       time.Duration
	SendBatchSize      int
	BufferSize         int
	MaxRetries         int
	RetryBaseBackoff   time.Duration
	CheckpointInterval time.Duration
	ShutdownTimeout    time.Duration
	// StalledThresholdMultiplier is the number of PollInterval ticks
	// without a successful collect or send that must elapse before the
	// pipeline is marked Stalled. Defaults to 5.
	StalledThresholdMultiplier int
	// CircuitBreakerFailureThreshold is the number of consecutive
	// destination send failures that trip the per-destination
	// circuit breaker from closed to open. Zero means "use the
	// default" (5).
	CircuitBreakerFailureThreshold int
	// CircuitBreakerRecoveryTimeout is the cool-off window the
	// breaker spends in the open state before transitioning to
	// half-open to admit a probe. Zero means "use the default"
	// (30s).
	CircuitBreakerRecoveryTimeout time.Duration
	// CircuitBreakerHalfOpenMaxCalls is the maximum number of
	// concurrent probe calls admitted while the breaker is
	// half-open. Zero means "use the default" (1).
	CircuitBreakerHalfOpenMaxCalls int
}

func DefaultSettings() *PipelineSettings {
	return &PipelineSettings{
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
}

type Batch struct {
	Entries      []*core.LogEntry
	EntryCursors []string
	NextCursor   string
	// Destinations lists the keys of the destinations that still need
	// this batch. Empty means every destination.
	Destinations []string
}

type DestinationInfo struct {
	Destination core.Destination
	MaxRetries  int
	Breaker     *retry.Breaker

	// key identifies the destination within its pipeline, e.g.
	// "sentinel#0". It is set by NewPipeline and is what batches and DLQ
	// records use to name a destination.
	key string
}

// destinationKey returns the identifier for the destination of type
// destType at position idx in the pipeline's destination list.
func destinationKey(destType string, idx int) string {
	return fmt.Sprintf("%s#%d", destType, idx)
}

type PipelineStatus string

const (
	PipelineStatusStopped  PipelineStatus = "stopped"
	PipelineStatusStarting PipelineStatus = "starting"
	PipelineStatusRunning  PipelineStatus = "running"
	// PipelineStatusDegraded means the pipeline is still progressing
	// (collects / sends are still happening) but recent errors have
	// been observed (e.g. a destination returned 5xx, a batch was
	// DLQ'd). It is a side-channel flag — the pipeline can recover to
	// Running on the next clean cycle, or transition to Stalled if
	// activity stops.
	PipelineStatusDegraded PipelineStatus = "degraded"
	// PipelineStatusStalled means no successful collect or send has
	// happened for StalledThresholdMultiplier × PollInterval. The
	// pipeline is alive but not making progress. The stallMonitor
	// goroutine owns this transition; the next successful collect or
	// send transitions back to Running (or Degraded if errors are
	// also present).
	PipelineStatusStalled PipelineStatus = "stalled"
	// PipelineStatusDraining is the pre-stop state. It is set when
	// Stop() is called and remains until the consumer goroutine has
	// drained the in-memory buffer and the disk buffer. Transitions
	// to Stopped when the goroutines exit.
	PipelineStatusDraining PipelineStatus = "draining"
	PipelineStatusStopping PipelineStatus = "stopping"
)

type PipelineHealth struct {
	// StartedAt is when the pipeline last started. It is the baseline for
	// stall and staleness checks until the first successful read or send,
	// so a source that fails from startup still becomes stalled.
	StartedAt   time.Time
	LastCollect time.Time
	LastSend    time.Time
	LastError   time.Time
	ErrorMsg    string
	Status      PipelineStatus
}

type Pipeline struct {
	ID           string
	Source       core.Source
	Processors   []core.Processor
	Destinations []DestinationInfo
	Checkpointer core.Checkpointer
	Buffer       chan Batch
	producerWg   sync.WaitGroup
	consumerWg   sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	// pushCtx bounds the pusher's processing and sends. Stop cancels ctx
	// first so the puller stops collecting, but keeps pushCtx alive until
	// the pusher has drained or shutdown_timeout expires, so in-flight
	// batches are delivered instead of failing with context.Canceled.
	pushCtx    context.Context
	cancelPush context.CancelFunc
	// readCursor is owned by the puller; cursor is the acknowledged prefix.
	readCursor string
	cursor     *cursorState
	flight     *flightState
	mu         sync.Mutex
	running    bool
	status     PipelineStatus
	settings   *PipelineSettings
	diskBuffer *buffer.DiskBuffer
	dlq        *dlq.DeadLetterQueue

	// health tracks per-pipeline last-success and last-error timestamps
	// for the improved /health endpoint.
	health PipelineHealth
}

// Status returns the current pipeline state.
func (p *Pipeline) Status() PipelineStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status
}

// SetStatus transitions the pipeline to the given state, logs the
// transition, and updates the PipelineStatus gauge metric. It is a
// no-op when the status is unchanged. Caller must NOT hold p.mu
// (this function takes it).
//
// The metric is emitted as loghub_pipeline_status{pipeline_id, status}
// = 1 for the active state and 0 for the previous state. Operators
// can query `loghub_pipeline_status == 1` to see the current state
// per pipeline.
func (p *Pipeline) SetStatus(next PipelineStatus) {
	p.mu.Lock()
	prev := p.status
	if prev == next {
		p.mu.Unlock()
		return
	}
	p.status = next
	p.mu.Unlock()

	if prev != "" {
		metrics.PipelineStatus.WithLabelValues(p.ID, string(prev)).Set(0)
	}
	metrics.PipelineStatus.WithLabelValues(p.ID, string(next)).Set(1)
	slog.Info("Pipeline state transition", "pipeline_id", p.ID, "from", prev, "to", next)
}

// Health returns a snapshot of the pipeline's health state.
func (p *Pipeline) Health() PipelineHealth {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PipelineHealth{
		LastCollect: p.health.LastCollect,
		LastSend:    p.health.LastSend,
		LastError:   p.health.LastError,
		ErrorMsg:    p.health.ErrorMsg,
		Status:      p.status,
	}
}

// cursorState encapsulates the in-memory cursor. Access goes through
// Snapshot/Advance so the mutex is owned by the type, not the Pipeline
// struct.
type cursorState struct {
	mu   sync.Mutex
	curr string
}

func (c *cursorState) Snapshot() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curr
}

func (c *cursorState) Advance(cursor string) {
	c.mu.Lock()
	c.curr = cursor
	c.mu.Unlock()
}

// flightState counts batches handed to the pusher that it has not
// finished yet. blocked prevents reads while an older page needs recovery.
type flightState struct {
	mu      sync.Mutex
	n       int
	blocked bool
}

func (f *flightState) Inc() {
	f.mu.Lock()
	f.n++
	f.mu.Unlock()
}

func (f *flightState) Dec() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n > 0 {
		f.n--
	}
	return f.n
}

func (f *flightState) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

// Snapshot returns a coherent view of outstanding pages and recovery state.
func (f *flightState) Snapshot() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n, f.blocked
}

func (f *flightState) Block() {
	f.mu.Lock()
	f.blocked = true
	f.mu.Unlock()
}

func (f *flightState) Unblock() {
	f.mu.Lock()
	f.blocked = false
	f.mu.Unlock()
}

func NewPipeline(
	id string,
	source core.Source,
	processors []core.Processor,
	destinations []DestinationInfo,
	checkpointer core.Checkpointer,
	settings *PipelineSettings,
	diskBuffer *buffer.DiskBuffer,
	dlq *dlq.DeadLetterQueue,
) *Pipeline {
	if settings == nil {
		settings = DefaultSettings()
	}

	for i := range destinations {
		destinations[i].key = destinationKey(destTypeOf(destinations[i].Destination), i)
		if destinations[i].Breaker == nil {
			destinations[i].Breaker = newBreakerForDest(id, destTypeOf(destinations[i].Destination), settings)
		}
		// Apply current settings to a pre-existing breaker as well so
		// hot-reload and tests that construct a breaker manually still
		// respect the configured thresholds.
		applyBreakerSettings(destinations[i].Breaker, settings)
		destinations[i].Breaker.OnStateChange = breakerMetricHook(id, destTypeOf(destinations[i].Destination))
		// Publish the initial state so dashboards see a closed breaker
		// (0) before it has ever tripped. The hook only fires on
		// transitions, so without this the series would be absent.
		metrics.CircuitBreakerState.WithLabelValues(id, destTypeOf(destinations[i].Destination)).Set(float64(destinations[i].Breaker.State()))
	}
	// Publish the collect-error counter at 0 for the same reason: rate()
	// and increase() cannot see the first error of a series that does not
	// exist yet.
	if source != nil {
		metrics.CollectErrors.WithLabelValues(id, source.Name()).Add(0)
	}

	return &Pipeline{
		ID:           id,
		Source:       source,
		Processors:   processors,
		Destinations: destinations,
		Checkpointer: checkpointer,
		Buffer:       make(chan Batch, settings.BufferSize),
		settings:     settings,
		diskBuffer:   diskBuffer,
		dlq:          dlq,
		cursor:       &cursorState{},
		flight:       &flightState{},
		status:       PipelineStatusStopped,
	}
}

// newBreakerForDest returns a fresh Breaker populated with the
// pipeline's configured thresholds and the metric-emission hook.
func newBreakerForDest(pipelineID, destType string, settings *PipelineSettings) *retry.Breaker {
	b := retry.NewBreaker()
	applyBreakerSettings(b, settings)
	b.OnStateChange = breakerMetricHook(pipelineID, destType)
	return b
}

// applyBreakerSettings copies the configured thresholds onto b.
// Fields that are zero in settings retain the breaker's existing
// (default) values.
func applyBreakerSettings(b *retry.Breaker, settings *PipelineSettings) {
	if b == nil || settings == nil {
		return
	}
	if settings.CircuitBreakerFailureThreshold > 0 {
		b.FailureThreshold = settings.CircuitBreakerFailureThreshold
	}
	if settings.CircuitBreakerRecoveryTimeout > 0 {
		b.OpenDuration = settings.CircuitBreakerRecoveryTimeout
	}
	if settings.CircuitBreakerHalfOpenMaxCalls > 0 {
		b.HalfOpenMaxCalls = settings.CircuitBreakerHalfOpenMaxCalls
	}
}

// breakerMetricHook returns an OnStateChange callback that updates
// the circuit_breaker_state gauge and increments the
// circuit_breaker_tripped_total counter on every transition to
// open. The hook is invoked by the breaker while it
// holds its mutex; it must therefore be quick and non-blocking,
// which is satisfied by Prometheus's atomic metric updates.
func breakerMetricHook(pipelineID, destType string) func(prev, next retry.State) {
	return func(_, next retry.State) {
		metrics.CircuitBreakerState.WithLabelValues(pipelineID, destType).Set(float64(next))
		if next == retry.StateOpen {
			metrics.CircuitBreakerTrippedTotal.WithLabelValues(pipelineID, destType).Inc()
		}
	}
}

// destTypeOf returns the destination's type, defaulting to "unknown"
// when the destination is nil (defensive; should not happen in
// production).
func destTypeOf(d core.Destination) string {
	if d == nil {
		return "unknown"
	}
	return d.Type()
}

func (p *Pipeline) Start(parentCtx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return errors.New("pipeline already running")
	}
	// Load the checkpoint before touching p.running / p.status /
	// p.cancel, so a startup failure leaves the pipeline stopped and
	// a later Start can try again.
	cursor, err := p.Checkpointer.Get(p.ID)
	p.mu.Unlock()
	if err != nil && !errors.Is(err, core.ErrCursorNotFound) {
		return err
	}
	if errors.Is(err, core.ErrCursorNotFound) {
		cursor = ""
	}

	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return errors.New("pipeline already running")
	}
	p.running = true
	p.health.StartedAt = time.Now()
	p.ctx, p.cancel = context.WithCancel(parentCtx)
	p.pushCtx, p.cancelPush = context.WithCancel(context.WithoutCancel(parentCtx))
	p.mu.Unlock()

	p.SetStatus(PipelineStatusStarting)
	metrics.PipelineRunning.WithLabelValues(p.ID).Set(1)
	metrics.PollInterval.WithLabelValues(p.ID).Set(p.settings.PollInterval.Seconds())

	p.cursor.Advance(cursor)
	p.readCursor = cursor
	slog.Info("Starting pipeline", "pipeline_id", p.ID, "poll_interval", p.settings.PollInterval, "send_batch_size", p.settings.SendBatchSize, "buffer_size", p.settings.BufferSize, "checkpoint_cursor", cursor)

	p.producerWg.Add(1)
	go p.pullRoutine()
	p.consumerWg.Add(1)
	go p.pushRoutine()

	// Start the stall monitor. It transitions the pipeline to
	// Stalled when no successful collect or send has happened for
	// StalledThresholdMultiplier × PollInterval.
	go p.stallMonitorRoutine()

	p.SetStatus(PipelineStatusRunning)
	return nil
}

// stallMonitorRoutine runs in a background goroutine and transitions
// the pipeline to Stalled when no successful collect or send has
// happened for StalledThresholdMultiplier × PollInterval. It also
// recovers to Running/Degraded when activity resumes.
func (p *Pipeline) stallMonitorRoutine() {
	if p.settings.PollInterval <= 0 {
		return
	}
	ticker := time.NewTicker(p.settings.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case now := <-ticker.C:
			p.evaluateStatus(now)
		}
	}
}

// stallThreshold returns the duration after which the pipeline is
// considered stalled (no successful collect or send). Defaults to
// 5 × PollInterval when StalledThresholdMultiplier is unset.
func (p *Pipeline) stallThreshold() time.Duration {
	n := p.settings.StalledThresholdMultiplier
	if n <= 0 {
		n = 5
	}
	return time.Duration(n) * p.settings.PollInterval
}

// evaluateStatus inspects the pipeline's health timestamps and
// transitions to Stalled, Degraded, or Running as appropriate. It is
// the single state-update function for the active states and is
// called by the puller (on collect success/failure), the pusher (on
// send success/failure), and the stall monitor (on each tick).
//
//   - Stalled: no successful collect or send for stallThreshold().
//   - Degraded: a recent error was observed (lastError within
//     stallThreshold() of now), but activity is recent.
//   - Running: recent activity, no recent errors.
//
// Terminal / transitional states (Stopped, Stopping, Draining,
// Starting) are left alone.
func (p *Pipeline) evaluateStatus(now time.Time) {
	p.mu.Lock()
	cur := p.status
	threshold := p.stallThreshold()
	lastCollect := p.health.LastCollect
	lastSend := p.health.LastSend
	lastError := p.health.LastError
	startedAt := p.health.StartedAt
	p.mu.Unlock()

	switch cur {
	case PipelineStatusStopped, PipelineStatusStopping,
		PipelineStatusDraining, PipelineStatusStarting:
		return
	}

	lastActivity := startedAt
	switch {
	case !lastCollect.IsZero() && !lastSend.IsZero():
		if lastCollect.After(lastSend) {
			lastActivity = lastCollect
		} else {
			lastActivity = lastSend
		}
	case !lastCollect.IsZero():
		lastActivity = lastCollect
	case !lastSend.IsZero():
		lastActivity = lastSend
	}

	var next PipelineStatus
	switch {
	case !lastActivity.IsZero() && now.Sub(lastActivity) >= threshold:
		// No activity for the stall window.
		next = PipelineStatusStalled
	case !lastError.IsZero() && now.Sub(lastError) < threshold:
		// Recent error: degraded.
		next = PipelineStatusDegraded
	default:
		// Recent activity, no recent error: running.
		next = PipelineStatusRunning
	}
	if next == PipelineStatusRunning && !lastError.IsZero() {
		// The error is older than the stall window and the pipeline has
		// recovered: clear its message so /health does not show it as
		// current. LastError keeps the time. Skip it if a new error was
		// recorded since the snapshot above.
		p.mu.Lock()
		if p.health.LastError.Equal(lastError) {
			p.health.ErrorMsg = ""
		}
		p.mu.Unlock()
	}
	p.SetStatus(next)
}

// shutdownGrace is how long Stop waits, after abandoning in-flight sends
// at shutdown_timeout, for the pusher to record the interrupted batches.
const shutdownGrace = 5 * time.Second

// pushContext returns the context for processing and sending batches.
// Tests that drive processBatch directly set only ctx.
func (p *Pipeline) pushContext() context.Context {
	if p.pushCtx != nil {
		return p.pushCtx
	}
	return p.ctx
}

func (p *Pipeline) cancelPushContext() {
	if p.cancelPush != nil {
		p.cancelPush()
	}
}

// shuttingDown reports whether the pusher's context has been cancelled,
// i.e. Stop gave up waiting for in-flight sends.
func (p *Pipeline) shuttingDown() bool {
	return p.pushContext().Err() != nil
}

// ErrShutdownTimeout is wrapped by StopContext when the pipeline did not
// drain before its deadline.
var ErrShutdownTimeout = errors.New("shutdown timed out")

// MaxStopDuration is the longest StopContext waits when its context has
// no earlier deadline: shutdown_timeout, then shutdownGrace for the pusher
// to record interrupted batches. Closing the source and destinations comes
// on top and is not bounded here.
func (p *Pipeline) MaxStopDuration() time.Duration {
	return p.settings.ShutdownTimeout + shutdownGrace
}

// Close releases the source, destinations, disk buffer and DLQ of a
// pipeline that was built but never started, such as one discarded by a
// failed reload. A started pipeline releases them in Stop; Close on it
// returns an error and changes nothing.
func (p *Pipeline) Close() error {
	p.mu.Lock()
	running := p.running
	p.mu.Unlock()
	if running {
		return errors.New("pipeline is running; use Stop")
	}
	var errs []error
	if p.Source != nil {
		if err := p.Source.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close source: %w", err))
		}
	}
	for _, dest := range p.Destinations {
		if err := dest.Destination.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close destination %s: %w", dest.Destination.Type(), err))
		}
	}
	if p.diskBuffer != nil {
		if err := p.diskBuffer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close disk buffer: %w", err))
		}
	}
	if p.dlq != nil {
		if err := p.dlq.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close DLQ: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Stop stops the pipeline within its shutdown_timeout. See StopContext.
func (p *Pipeline) Stop() error {
	return p.StopContext(context.Background())
}

// StopContext stops the pipeline and returns what went wrong, if anything.
// One deadline, shutdown_timeout or ctx's if earlier, covers both waiting
// for the producer and draining the consumer. After it, in-flight sends are
// abandoned and the pusher gets shutdownGrace (cut short if ctx ends) to
// keep the interrupted batches for retry. Errors are also logged.
func (p *Pipeline) StopContext(ctx context.Context) error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}
	p.running = false
	p.mu.Unlock()

	p.SetStatus(PipelineStatusDraining)
	metrics.PipelineRunning.WithLabelValues(p.ID).Set(0)
	p.cancel()

	deadlineCtx, cancelDeadline := context.WithTimeout(ctx, p.settings.ShutdownTimeout)
	defer cancelDeadline()
	var errs []error

	// Bound the producer wait. The producer can be blocked in a
	// non-context-aware diskBuffer.Write under disk pressure (ENOSPC,
	// slow fs, etc.); without a deadline, Stop would hang indefinitely.
	producerDone := make(chan struct{})
	go func() {
		p.producerWg.Wait()
		close(producerDone)
	}()
	select {
	case <-producerDone:
		// good
	case <-deadlineCtx.Done():
		slog.Error("Pipeline producer wait timed out — puller may be stuck in disk I/O", "pipeline_id", p.ID, "timeout", p.settings.ShutdownTimeout)
		errs = append(errs, fmt.Errorf("producer did not stop: %w", ErrShutdownTimeout))
		// Fall through: the producer goroutine is leaked but the
		// process must not hang. The puller's Collect is
		// context-aware (or has a defer that unblocks on
		// cancellation), so it should exit on the next iteration.
	}
	close(p.Buffer)

	consumerDone := make(chan struct{})
	go func() {
		p.consumerWg.Wait()
		close(consumerDone)
	}()

	consumerTimedOut := false
	select {
	case <-consumerDone:
		slog.Info("Pipeline stopped gracefully", "pipeline_id", p.ID)
	case <-deadlineCtx.Done():
		slog.Error("Pipeline consumer drain timed out; abandoning in-flight sends", "pipeline_id", p.ID, "timeout", p.settings.ShutdownTimeout)
		errs = append(errs, fmt.Errorf("drain did not finish; in-flight sends abandoned: %w", ErrShutdownTimeout))
		// Abort the in-flight sends. The pusher keeps the interrupted
		// batch for retry (in the disk buffer, if configured) rather than
		// dead-lettering it; give it a moment to record that.
		p.cancelPushContext()
		grace := time.NewTimer(shutdownGrace)
		select {
		case <-consumerDone:
		case <-grace.C:
			consumerTimedOut = true
		case <-ctx.Done():
			consumerTimedOut = true
		}
		grace.Stop()
		if p.diskBuffer != nil {
			if count := p.diskBuffer.Count(); count > 0 {
				slog.Warn("Disk buffer still contains undelivered batches at shutdown", "pipeline_id", p.ID, "pending_batches", count)
			}
		}
	}
	p.cancelPushContext()

	if err := p.Source.Close(); err != nil {
		slog.Error("Error closing source", "pipeline_id", p.ID, "error", err)
		errs = append(errs, fmt.Errorf("close source: %w", err))
	}
	for _, dest := range p.Destinations {
		if err := dest.Destination.Close(); err != nil {
			slog.Error("Error closing destination", "pipeline_id", p.ID, "error", err)
			errs = append(errs, fmt.Errorf("close destination %s: %w", dest.Destination.Type(), err))
		}
	}
	// Do NOT close the disk buffer or DLQ if the consumer timed out. The consumer may still be inside drainFromPJ /
	// drainFromLegacy holding db.mu, and closing here would
	// deadlock. Closing the source and destinations is enough to
	// unblock the consumer's next Collect; the leaked consumer
	// goroutine will eventually exit and release the locks. The
	// disk buffer's journal fd will be released when the process
	// exits (the OS reaps it).
	if !consumerTimedOut {
		if p.diskBuffer != nil {
			if err := p.diskBuffer.Close(); err != nil {
				slog.Error("Error closing disk buffer", "pipeline_id", p.ID, "error", err)
				errs = append(errs, fmt.Errorf("close disk buffer: %w", err))
			}
		}
		if p.dlq != nil {
			if err := p.dlq.Close(); err != nil {
				slog.Error("Error closing DLQ", "pipeline_id", p.ID, "error", err)
				errs = append(errs, fmt.Errorf("close DLQ: %w", err))
			}
		}
	} else {
		slog.Warn("Skipping disk buffer / DLQ close due to consumer timeout — consumer may still hold db.mu", "pipeline_id", p.ID)
		errs = append(errs, errors.New("consumer still running after the grace period; disk buffer and DLQ left open"))
	}

	p.SetStatus(PipelineStatusStopped)
	return errors.Join(errs...)
}
