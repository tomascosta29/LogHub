package retry

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrFatal = errors.New("retry: fatal error, stop retrying")
var ErrCircuitOpen = errors.New("retry: circuit breaker open")

const maxBackoff = 5 * time.Minute

// Retrier executes an operation with exponential backoff retries.
type Retrier struct {
	MaxRetries  int
	BaseBackoff time.Duration
}

// Do repeatedly calls op until it succeeds, the context is cancelled,
// the operation returns ErrFatal, or MaxRetries total attempts have been made.
// MaxRetries is the maximum number of attempts (not initial + retries).
// After each failed attempt, it sleeps for BaseBackoff * 2^attempt, capped at 5 minutes.
func (r *Retrier) Do(ctx context.Context, op func() error) error {
	var lastErr error
	for i := 0; i < r.MaxRetries; i++ {
		// Check for cancellation before each attempt so we don't call op after
		// the context is already done.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := op()
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(err, ErrFatal) {
			return err
		}

		backoff := r.BaseBackoff * time.Duration(1<<uint(i))
		// Cap backoff to avoid overflow for large retry counts.
		if backoff > maxBackoff || backoff <= 0 {
			backoff = maxBackoff
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return lastErr
}

// State enumerates the three breaker states.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// Breaker is a per-destination circuit breaker. After
// FailureThreshold consecutive failures, the circuit opens for
// OpenDuration. While open, calls fail fast with ErrCircuitOpen
// until the cool-off elapses, at which point the breaker transitions
// to half-open and allows at most HalfOpenMaxCalls probe calls. A
// probe success closes the circuit; a probe failure re-opens it
// for another OpenDuration window. Counters and trip events can
// be observed via the OnTrip / OnStateChange hooks.
type Breaker struct {
	mu sync.Mutex

	consecutiveFails int
	halfOpenInFlight int
	openUntil        time.Time
	state            State

	FailureThreshold int
	OpenDuration     time.Duration
	HalfOpenMaxCalls int

	// now is a clock indirection used by tests to drive transitions
	// deterministically. Defaults to time.Now.
	now func() time.Time

	// OnStateChange, if non-nil, is invoked synchronously on every
	// state transition. Callers may use it to emit metrics or logs.
	// Implementations must be fast and non-blocking — the breaker
	// holds its mutex when the hook fires.
	OnStateChange func(prev, next State)
}

// NewBreaker returns a closed breaker with the documented defaults
// (FailureThreshold=5, OpenDuration=30s, HalfOpenMaxCalls=1).
func NewBreaker() *Breaker {
	return &Breaker{
		FailureThreshold: 5,
		OpenDuration:     30 * time.Second,
		HalfOpenMaxCalls: 1,
		now:              time.Now,
	}
}

// Allow returns nil if the call should proceed, or ErrCircuitOpen if
// the breaker is open. A success should be reported via RecordSuccess
// and a failure via RecordFailure.
//
// State machine:
//
//   - Closed: returns nil. The caller proceeds and reports
//     success/failure normally.
//   - Open: returns ErrCircuitOpen until the cool-off elapses. The
//     first call after the cool-off transitions the breaker to
//     half-open and returns nil (a single probe).
//   - Half-Open: returns nil for the first HalfOpenMaxCalls callers
//     (the probe window). Subsequent callers in the same probe window
//     receive ErrCircuitOpen.
//
// Each successful call should be paired with RecordSuccess; each
// failed call with RecordFailure. Callers that did not actually
// invoke the destination (because Allow returned ErrCircuitOpen)
// must not call RecordSuccess or RecordFailure.
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return nil
	case StateOpen:
		if b.now().Before(b.openUntil) {
			return ErrCircuitOpen
		}
		// Cool-off elapsed: transition to half-open. The very call
		// that triggered the transition becomes the first probe
		// (we return nil below), as long as HalfOpenMaxCalls > 0.
		b.transitionLocked(StateHalfOpen)
		if b.HalfOpenMaxCalls <= 0 {
			// Defensive: a zero/negative probe budget is treated as
			// "no probe allowed" and we remain effectively open.
			return ErrCircuitOpen
		}
		b.halfOpenInFlight = 1
		return nil
	case StateHalfOpen:
		if b.halfOpenInFlight < b.HalfOpenMaxCalls {
			b.halfOpenInFlight++
			return nil
		}
		return ErrCircuitOpen
	}
	return nil
}

// RecordSuccess closes the breaker (from either closed or half-open).
// Callers MUST have called Allow and received nil before calling
// RecordSuccess; calling RecordSuccess without a prior successful
// Allow would inflate the success counter incorrectly.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFails = 0
	b.halfOpenInFlight = 0
	if b.state != StateClosed {
		b.transitionLocked(StateClosed)
	}
}

// RecordFailure increments the consecutive failure count. If the
// threshold is reached, the breaker opens for OpenDuration. In
// half-open, any failure re-opens the breaker immediately (the probe
// failed) and the cool-off window restarts. Callers MUST have
// called Allow and received nil before calling RecordFailure.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		b.consecutiveFails++
		if b.consecutiveFails >= b.FailureThreshold {
			b.openUntil = b.now().Add(b.OpenDuration)
			b.transitionLocked(StateOpen)
		}
	case StateHalfOpen:
		// Probe failed: re-open the circuit for another full cool-off.
		b.openUntil = b.now().Add(b.OpenDuration)
		b.halfOpenInFlight = 0
		b.transitionLocked(StateOpen)
	case StateOpen:
		// Defensive: callers should not call RecordFailure while the
		// breaker is open (they got ErrCircuitOpen from Allow). But
		// if they do, treat it as a confirmation that the destination
		// is still down and extend the cool-off.
		b.openUntil = b.now().Add(b.OpenDuration)
	}
}

// State returns the breaker's current state. Safe for concurrent use.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// transitionLocked changes state and invokes the OnStateChange hook.
// Caller MUST hold b.mu.
func (b *Breaker) transitionLocked(next State) {
	if b.state == next {
		return
	}
	prev := b.state
	b.state = next
	if next == StateClosed {
		b.openUntil = time.Time{}
	}
	if b.OnStateChange != nil {
		b.OnStateChange(prev, next)
	}
}
