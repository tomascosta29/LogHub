package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetrierSuccess(t *testing.T) {
	r := Retrier{MaxRetries: 3, BaseBackoff: 10 * time.Millisecond}
	attempts := 0
	err := r.Do(context.Background(), func() error {
		attempts++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestRetrierEventualSuccess(t *testing.T) {
	r := Retrier{MaxRetries: 3, BaseBackoff: 10 * time.Millisecond}
	attempts := 0
	err := r.Do(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts (MaxRetries=3 means 3 total attempts), got %d", attempts)
	}
}

func TestRetrierExhausted(t *testing.T) {
	r := Retrier{MaxRetries: 2, BaseBackoff: 10 * time.Millisecond}
	attempts := 0
	err := r.Do(context.Background(), func() error {
		attempts++
		return errors.New("permanent")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts (MaxRetries=2 means 2 total attempts), got %d", attempts)
	}
}

func TestRetrierContextCancel(t *testing.T) {
	r := Retrier{MaxRetries: 10, BaseBackoff: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := r.Do(ctx, func() error {
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestBreakerClosedByDefault(t *testing.T) {
	b := NewBreaker()
	if err := b.Allow(); err != nil {
		t.Errorf("Allow on fresh breaker = %v, want nil", err)
	}
}

func TestBreakerOpensAfterFailures(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 3
	b.OpenDuration = 100 * time.Millisecond
	for i := 0; i < 3; i++ {
		b.RecordFailure()
	}
	if err := b.Allow(); err != ErrCircuitOpen {
		t.Errorf("Allow after 3 failures = %v, want ErrCircuitOpen", err)
	}
}

func TestBreakerClosesAfterSuccess(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 2
	b.RecordFailure()
	b.RecordFailure()
	if err := b.Allow(); err != ErrCircuitOpen {
		t.Errorf("Allow after 2 failures = %v, want ErrCircuitOpen", err)
	}
	b.RecordSuccess()
	if err := b.Allow(); err != nil {
		t.Errorf("Allow after success = %v, want nil", err)
	}
}

func TestBreakerRecoversAfterOpenDuration(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 2
	b.OpenDuration = 50 * time.Millisecond
	b.RecordFailure()
	b.RecordFailure()
	if err := b.Allow(); err != ErrCircuitOpen {
		t.Errorf("Allow immediately after 2 failures = %v, want ErrCircuitOpen", err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := b.Allow(); err != nil {
		t.Errorf("Allow after cool-off = %v, want nil", err)
	}
}

// TestBreakerStateClosedByDefault verifies the State() method
// returns StateClosed for a fresh breaker.
func TestBreakerStateClosedByDefault(t *testing.T) {
	b := NewBreaker()
	if got := b.State(); got != StateClosed {
		t.Errorf("State() = %d, want %d (StateClosed)", got, StateClosed)
	}
}

// TestBreakerStateOpenAfterFailures verifies State() returns
// StateOpen after FailureThreshold consecutive failures.
func TestBreakerStateOpenAfterFailures(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 2
	b.RecordFailure()
	if got := b.State(); got != StateClosed {
		t.Errorf("State() after 1 failure = %d, want StateClosed", got)
	}
	b.RecordFailure()
	if got := b.State(); got != StateOpen {
		t.Errorf("State() after threshold reached = %d, want StateOpen", got)
	}
}

// TestBreakerHalfOpenAfterCoolOff verifies that Allow() transitions
// the breaker from open to half-open when the cool-off elapses, and
// that the same Allow() call is admitted as the probe.
func TestBreakerHalfOpenAfterCoolOff(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 1
	b.OpenDuration = 30 * time.Millisecond
	b.RecordFailure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %d, want StateOpen", got)
	}
	if err := b.Allow(); err != ErrCircuitOpen {
		t.Fatalf("Allow() while open = %v, want ErrCircuitOpen", err)
	}
	time.Sleep(40 * time.Millisecond)
	// First Allow after cool-off: must transition to half-open and
	// admit the call as the probe.
	if err := b.Allow(); err != nil {
		t.Fatalf("Allow() after cool-off = %v, want nil (probe admitted)", err)
	}
	if got := b.State(); got != StateHalfOpen {
		t.Errorf("State() after probe admitted = %d, want StateHalfOpen", got)
	}
}

// TestBreakerHalfOpenAdmitsSingleCallByDefault verifies the default
// HalfOpenMaxCalls=1: only one probe is admitted and subsequent
// calls in the same probe window fail fast.
func TestBreakerHalfOpenAdmitsSingleCallByDefault(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 1
	b.OpenDuration = 10 * time.Millisecond
	b.RecordFailure()
	time.Sleep(15 * time.Millisecond)
	// First Allow: probe admitted.
	if err := b.Allow(); err != nil {
		t.Fatalf("first Allow after cool-off = %v, want nil", err)
	}
	// Second Allow: probe window exhausted.
	if err := b.Allow(); err != ErrCircuitOpen {
		t.Errorf("second Allow in half-open window = %v, want ErrCircuitOpen", err)
	}
}

// TestBreakerHalfOpenAdmitsMultipleProbes verifies the
// HalfOpenMaxCalls=N config: N concurrent probes are admitted and
// the (N+1)th fails fast.
func TestBreakerHalfOpenAdmitsMultipleProbes(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 1
	b.OpenDuration = 10 * time.Millisecond
	b.HalfOpenMaxCalls = 3
	b.RecordFailure()
	time.Sleep(15 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := b.Allow(); err != nil {
			t.Errorf("Allow #%d in half-open = %v, want nil", i+1, err)
		}
	}
	if err := b.Allow(); err != ErrCircuitOpen {
		t.Errorf("Allow #4 in half-open (budget exhausted) = %v, want ErrCircuitOpen", err)
	}
}

// TestBreakerHalfOpenProbeSuccessClosesCircuit verifies that a
// successful probe in half-open transitions the breaker to closed
// and resets the failure counter.
func TestBreakerHalfOpenProbeSuccessClosesCircuit(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 1
	b.OpenDuration = 10 * time.Millisecond
	b.RecordFailure()
	time.Sleep(15 * time.Millisecond)
	if err := b.Allow(); err != nil {
		t.Fatalf("Allow() after cool-off = %v, want nil", err)
	}
	b.RecordSuccess()
	if got := b.State(); got != StateClosed {
		t.Errorf("State() after probe success = %d, want StateClosed", got)
	}
	// Closed again: many more failures should be required to trip it.
	for i := 0; i < b.FailureThreshold-1; i++ {
		b.RecordFailure()
	}
	if got := b.State(); got != StateClosed {
		t.Errorf("State() after %d fresh failures (below threshold) = %d, want StateClosed", b.FailureThreshold-1, got)
	}
}

// TestBreakerHalfOpenProbeFailureReopensCircuit verifies that a
// failed probe in half-open transitions the breaker back to open
// and resets the cool-off window.
func TestBreakerHalfOpenProbeFailureReopensCircuit(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 1
	b.OpenDuration = 10 * time.Millisecond
	b.RecordFailure()
	time.Sleep(15 * time.Millisecond)
	if err := b.Allow(); err != nil {
		t.Fatalf("Allow() after cool-off = %v, want nil", err)
	}
	b.RecordFailure()
	if got := b.State(); got != StateOpen {
		t.Errorf("State() after probe failure = %d, want StateOpen", got)
	}
	// Cool-off window must have been restarted.
	if err := b.Allow(); err != ErrCircuitOpen {
		t.Errorf("Allow() immediately after probe-failure re-open = %v, want ErrCircuitOpen", err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := b.Allow(); err != nil {
		t.Errorf("Allow() after restart of cool-off = %v, want nil (new probe)", err)
	}
}

// TestBreakerOnStateChangeHook verifies that the OnStateChange hook
// fires on every transition. The hook is the integration
// point used by the pipeline to update Prometheus metrics.
func TestBreakerOnStateChangeHook(t *testing.T) {
	b := NewBreaker()
	b.FailureThreshold = 1
	b.OpenDuration = 5 * time.Millisecond

	type transition struct {
		prev, next State
	}
	var got []transition
	b.OnStateChange = func(prev, next State) {
		got = append(got, transition{prev, next})
	}

	b.RecordFailure()                 // closed -> open
	time.Sleep(10 * time.Millisecond) // cool-off elapses
	b.Allow()                         // open -> half-open (probe admitted)
	b.RecordSuccess()                 // half-open -> closed
	b.RecordFailure()                 // closed -> open

	want := []transition{
		{StateClosed, StateOpen},
		{StateOpen, StateHalfOpen},
		{StateHalfOpen, StateClosed},
		{StateClosed, StateOpen},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d transitions, want %d (%v)", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("transitions[%d] = %+v, want %+v", i, got[i], w)
		}
	}
}

// TestBreakerOpenDurationZeroDefaultsAreSafe verifies that
// constructing a Breaker with no explicit thresholds produces the
// documented defaults (5 failures, 30s recovery, 1 probe).
func TestBreakerOpenDurationZeroDefaultsAreSafe(t *testing.T) {
	b := NewBreaker()
	if b.FailureThreshold != 5 {
		t.Errorf("FailureThreshold = %d, want 5", b.FailureThreshold)
	}
	if b.OpenDuration != 30*time.Second {
		t.Errorf("OpenDuration = %v, want 30s", b.OpenDuration)
	}
	if b.HalfOpenMaxCalls != 1 {
		t.Errorf("HalfOpenMaxCalls = %d, want 1", b.HalfOpenMaxCalls)
	}
}

func TestRetrierZeroMaxRetriesStillCallsOnce(t *testing.T) {
	for _, n := range []int{0, -1} {
		calls := 0
		err := (&Retrier{MaxRetries: n, BaseBackoff: time.Millisecond}).Do(context.Background(), func() error {
			calls++
			return errors.New("fail")
		})
		if calls != 1 || err == nil {
			t.Errorf("MaxRetries %d: calls = %d, err = %v; want one call and its error", n, calls, err)
		}
	}
}
