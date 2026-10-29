package pipeline

import (
	"testing"
	"time"
)

// TestBackwardClockJumpDoesNotHideStall models a wall-clock jump backwards
// after a pipeline has been reading successfully: the recorded activity is
// now in the future. Staleness must still be measured against the current
// clock, otherwise a dead source looks healthy for as long as the jump.
func TestBackwardClockJumpDoesNotHideStall(t *testing.T) {
	p := NewPipeline("clock-back", nil, nil, nil, &mockCheckpointer{}, &PipelineSettings{
		PollInterval:               time.Second,
		StalledThresholdMultiplier: 5,
	}, nil, nil)
	p.SetStatus(PipelineStatusRunning)

	now := time.Now()
	p.mu.Lock()
	p.health.StartedAt = now.Add(-time.Hour)
	p.health.LastCollect = now.Add(time.Hour)
	p.health.LastSend = now.Add(time.Hour)
	p.mu.Unlock()

	// No collect or send has happened for the threshold measured from the
	// current clock; the future timestamps are an artifact of the jump.
	p.evaluateStatus(now)
	if got := p.Status(); got != PipelineStatusStalled {
		t.Fatalf("status = %s after a backward clock jump, want stalled: the future activity timestamp hid a stalled pipeline", got)
	}
}

// TestFutureLastErrorDoesNotProlongDegraded models the same jump with an
// error recorded just before it. The pipeline must not stay degraded for
// the duration of the jump once activity has resumed.
func TestFutureLastErrorDoesNotProlongDegraded(t *testing.T) {
	p := NewPipeline("clock-error", nil, nil, nil, &mockCheckpointer{}, &PipelineSettings{
		PollInterval:               time.Second,
		StalledThresholdMultiplier: 5,
	}, nil, nil)
	p.SetStatus(PipelineStatusRunning)

	now := time.Now()
	p.mu.Lock()
	p.health.StartedAt = now.Add(-time.Hour)
	p.health.LastCollect = now
	p.health.LastSend = now
	p.health.LastError = now.Add(time.Hour)
	p.health.ErrorMsg = "destination unreachable"
	p.mu.Unlock()

	p.evaluateStatus(now)
	if got := p.Status(); got != PipelineStatusRunning {
		t.Fatalf("status = %s with recent activity and an error timestamp one hour in the future, want running; the pipeline stays degraded until the wall clock catches up", got)
	}
}
