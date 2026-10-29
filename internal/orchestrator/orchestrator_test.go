package orchestrator

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"loghub/internal/core"
	"loghub/internal/pipeline"
)

type mockSource struct {
	closed atomic.Bool
}

func (m *mockSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	return nil, cursor, nil
}

func (m *mockSource) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockSource) Name() string {
	return "mock-source"
}

type mockDestination struct {
	closed atomic.Bool
}

func (m *mockDestination) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	out := make([]core.EntryStatus, len(logs))
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
	}
	return out, nil
}

func (m *mockDestination) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockDestination) Type() string {
	return "mock"
}

type mockCheckpointer struct{}

func (m *mockCheckpointer) Get(pipelineID string) (string, error) {
	return "", nil
}

func (m *mockCheckpointer) Set(pipelineID string, cursor string) error {
	return nil
}

func (m *mockCheckpointer) Close() error {
	return nil
}

func TestOrchestratorAddPipelineAndStartStop(t *testing.T) {
	o := NewOrchestrator()

	src1 := &mockSource{}
	dest1 := &mockDestination{}
	cp := &mockCheckpointer{}

	settings := pipeline.DefaultSettings()
	p1 := pipeline.NewPipeline("p1", src1, nil, []pipeline.DestinationInfo{{Destination: dest1}}, cp, settings, nil, nil)

	src2 := &mockSource{}
	dest2 := &mockDestination{}
	p2 := pipeline.NewPipeline("p2", src2, nil, []pipeline.DestinationInfo{{Destination: dest2}}, cp, settings, nil, nil)

	o.AddPipeline(p1)
	o.AddPipeline(p2)

	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	o.Shutdown()

	if !src1.closed.Load() || !dest1.closed.Load() {
		t.Errorf("Expected pipeline 1 source and destination to be closed")
	}

	if !src2.closed.Load() || !dest2.closed.Load() {
		t.Errorf("Expected pipeline 2 source and destination to be closed")
	}
}

func TestOrchestratorSignalHandling(t *testing.T) {
	o := NewOrchestrator()

	src := &mockSource{}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}

	p := pipeline.NewPipeline("test-sig", src, nil, []pipeline.DestinationInfo{{Destination: dest}}, cp, pipeline.DefaultSettings(), nil, nil)
	o.AddPipeline(p)

	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	pProcess, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess failed: %v", err)
	}

	err = pProcess.Signal(syscall.SIGINT)
	if err != nil {
		t.Fatalf("Signal failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !src.closed.Load() {
		t.Errorf("Expected source to be closed by signal handler")
	}
}

func TestOrchestratorShutdownDoesNotHangWait(t *testing.T) {
	// Regression test: calling Shutdown() directly (not via signal)
	// must close shutdownCh so Wait() returns. Before the fix, Wait
	// blocked forever because only signalHandler closed shutdownCh.
	orch := NewOrchestrator()
	src := &mockSource{}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}
	settings := pipeline.DefaultSettings()
	p := pipeline.NewPipeline("p1", src, nil, []pipeline.DestinationInfo{{Destination: dest}}, cp, settings, nil, nil)
	orch.AddPipeline(p)

	if err := orch.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		orch.Wait()
		close(done)
	}()

	// Give Start a moment to spin up signalHandler, then call
	// Shutdown directly. Wait must return promptly.
	time.Sleep(50 * time.Millisecond)
	orch.Shutdown()

	select {
	case <-done:
		// good — Wait returned
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after Shutdown")
	}
}

func TestPipelineHealths_ReportsPerPipelineState(t *testing.T) {
	// The /health endpoint relies on PipelineHealths()
	// returning the per-pipeline state. The test puts one pipeline
	// in Running and another in Stalled and asserts both are
	// surfaced.
	orch := NewOrchestrator()
	cp := &mockCheckpointer{}

	settings := pipeline.DefaultSettings()
	settings.PollInterval = time.Hour
	settings.StalledThresholdMultiplier = 5

	p1 := pipeline.NewPipeline("p1", &mockSource{}, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, cp, settings, nil, nil)
	p1.SetStatus(pipeline.PipelineStatusRunning)

	p2 := pipeline.NewPipeline("p2", &mockSource{}, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, cp, settings, nil, nil)
	p2.SetStatus(pipeline.PipelineStatusStalled)

	orch.AddPipeline(p1)
	orch.AddPipeline(p2)

	hs := orch.PipelineHealths()
	if len(hs) != 2 {
		t.Fatalf("expected 2 healths, got %d", len(hs))
	}
	statuses := map[string]string{}
	for _, h := range hs {
		statuses[h.ID] = h.Status
	}
	if statuses["p1"] != "running" {
		t.Errorf("p1 status = %q, want running", statuses["p1"])
	}
	if statuses["p2"] != "stalled" {
		t.Errorf("p2 status = %q, want stalled", statuses["p2"])
	}
}

type failingCloseDestination struct {
	mockDestination
	err error
}

func (d *failingCloseDestination) Close() error { return d.err }

// hangingCloseSource never returns from Close, like a driver stuck on the
// network.
type hangingCloseSource struct {
	mockSource
}

func (s *hangingCloseSource) Close() error { select {} }

// Shutdown returns each pipeline's stop error, naming the pipeline.
func TestShutdownCollectsPipelineErrors(t *testing.T) {
	errClose := errors.New("close failed")
	orch := NewOrchestrator()
	settings := pipeline.DefaultSettings()
	orch.AddPipeline(pipeline.NewPipeline("good", &mockSource{}, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, settings, nil, nil))
	orch.AddPipeline(pipeline.NewPipeline("bad", &mockSource{}, nil, []pipeline.DestinationInfo{{Destination: &failingCloseDestination{err: errClose}}}, &mockCheckpointer{}, settings, nil, nil))
	if err := orch.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	err := orch.Shutdown()
	if !errors.Is(err, errClose) {
		t.Fatalf("Shutdown = %v, want it to wrap the close error", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "pipeline bad") || strings.Contains(msg, "pipeline good") {
		t.Errorf("Shutdown error %q should name pipeline bad only", msg)
	}
}

// One deadline bounds the whole shutdown. A pipeline stuck past
// it is reported, and Shutdown returns instead of hanging.
func TestShutdownContextReturnsAtDeadline(t *testing.T) {
	orch := NewOrchestrator()
	settings := pipeline.DefaultSettings()
	orch.AddPipeline(pipeline.NewPipeline("stuck", &hangingCloseSource{}, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, settings, nil, nil))
	orch.AddPipeline(pipeline.NewPipeline("fine", &mockSource{}, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, settings, nil, nil))
	if err := orch.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := orch.ShutdownContext(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("ShutdownContext took %v, want it bounded by the 300ms deadline", elapsed)
	}
	if !errors.Is(err, pipeline.ErrShutdownTimeout) || !strings.Contains(err.Error(), "pipeline stuck") {
		t.Fatalf("ShutdownContext = %v, want ErrShutdownTimeout naming pipeline stuck", err)
	}
	if strings.Contains(err.Error(), "pipeline fine") {
		t.Errorf("ShutdownContext error %q should not name pipeline fine", err)
	}
	if orch.Running() {
		t.Error("orchestrator still reports running after shutdown")
	}
}

// RequestReload runs the reload function on the signal goroutine, as
// SIGHUP does, so it cannot overlap a SIGHUP reload or shutdown.
func TestRequestReloadRunsReloadFunc(t *testing.T) {
	orch := NewOrchestrator()
	orch.AddPipeline(pipeline.NewPipeline("p1", &mockSource{}, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, pipeline.DefaultSettings(), nil, nil))
	reloaded := make(chan struct{}, 1)
	orch.SetReloadFunc(func() error {
		reloaded <- struct{}{}
		return nil
	})
	if err := orch.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Shutdown()

	orch.RequestReload()
	select {
	case <-reloaded:
	case <-time.After(2 * time.Second):
		t.Fatal("RequestReload did not run the reload function")
	}
}

type failingGetCheckpointer struct{ mockCheckpointer }

func (m *failingGetCheckpointer) Get(pipelineID string) (string, error) {
	return "", errors.New("checkpoint unreadable")
}

func TestReplacePipelinesStartFailureCleansUp(t *testing.T) {
	o := NewOrchestrator()
	settings := pipeline.DefaultSettings()
	oldSrc := &mockSource{}
	o.AddPipeline(pipeline.NewPipeline("old", oldSrc, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, settings, nil, nil))
	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Shutdown()

	goodSrc, badSrc, laterSrc := &mockSource{}, &mockSource{}, &mockSource{}
	good := pipeline.NewPipeline("good", goodSrc, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, settings, nil, nil)
	bad := pipeline.NewPipeline("bad", badSrc, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &failingGetCheckpointer{}, settings, nil, nil)
	later := pipeline.NewPipeline("later", laterSrc, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, settings, nil, nil)

	err := o.ReplacePipelines([]*pipeline.Pipeline{good, bad, later})
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("ReplacePipelines error = %v, want the failing pipeline named", err)
	}
	if !oldSrc.closed.Load() {
		t.Error("old pipeline was not stopped")
	}
	if !goodSrc.closed.Load() || !badSrc.closed.Load() || !laterSrc.closed.Load() {
		t.Errorf("new pipelines left open: good=%v bad=%v later=%v", goodSrc.closed.Load(), badSrc.closed.Load(), laterSrc.closed.Load())
	}
	if n := len(o.PipelineHealths()); n != 0 {
		t.Errorf("orchestrator reports %d pipelines after a failed replace, want 0", n)
	}
}

// TestPipelineHealthsDuringReplace runs under -race: /health reads the
// pipeline list while a reload replaces it.
func TestPipelineHealthsDuringReplace(t *testing.T) {
	o := NewOrchestrator()
	settings := pipeline.DefaultSettings()
	newPipeline := func(id string) *pipeline.Pipeline {
		return pipeline.NewPipeline(id, &mockSource{}, nil, []pipeline.DestinationInfo{{Destination: &mockDestination{}}}, &mockCheckpointer{}, settings, nil, nil)
	}
	o.AddPipeline(newPipeline("p"))
	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Shutdown()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			if err := o.ReplacePipelines([]*pipeline.Pipeline{newPipeline("p")}); err != nil {
				t.Errorf("ReplacePipelines: %v", err)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
			o.PipelineHealths()
		}
	}
}
