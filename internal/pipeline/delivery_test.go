package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"loghub/internal/buffer"
	"loghub/internal/core"
	"loghub/internal/dlq"
	"loghub/internal/metrics"
	"loghub/internal/retry"
)

func twoEntryBatch() Batch {
	return Batch{
		Entries: []*core.LogEntry{
			{Message: "1", Cursor: "c1"},
			{Message: "2", Cursor: "c2"},
		},
		EntryCursors: []string{"c1", "c2"},
		NextCursor:   "c2",
	}
}

// When one destination's breaker is open, the sub-batch is kept for that
// destination only; retrying it must not resend to the destination that
// already accepted it.
func TestProcessBatch_RetriesOnlyDestinationsThatNeedIt(t *testing.T) {
	healthy := &orderedDest{}
	down := &orderedDest{}
	p, dlqWriter := makePipelineWithDests(t, []core.Destination{healthy, down})
	p.cursor.Advance("start")
	p.Destinations[1].Breaker.FailureThreshold = 1
	p.Destinations[1].Breaker.OpenDuration = time.Hour
	p.Destinations[1].Breaker.RecordFailure()

	remaining, ok := p.processBatch(&checkpointState{}, twoEntryBatch())
	if ok || len(remaining) != 1 {
		t.Fatalf("processBatch = (%d remaining, ok=%v), want (1, false)", len(remaining), ok)
	}
	if got := remaining[0].Destinations; len(got) != 1 || got[0] != "ordered#1" {
		t.Fatalf("remaining destinations = %v, want [ordered#1]", got)
	}
	if got := p.cursor.Snapshot(); got != "start" {
		t.Errorf("cursor = %q, want start (a destination still needs the batch)", got)
	}

	// The destination recovers; retry what was returned.
	p.Destinations[1].Breaker = retry.NewBreaker()
	if _, ok := p.processBatch(&checkpointState{}, remaining[0]); !ok {
		t.Fatal("retry processBatch returned ok=false")
	}
	if _, calls := healthy.snapshot(); calls != 1 {
		t.Errorf("healthy destination received %d sends, want 1 (no duplicate on retry)", calls)
	}
	if got, _ := down.snapshot(); len(got) != 2 {
		t.Errorf("recovered destination received %d entries, want 2", len(got))
	}
	if got := p.cursor.Snapshot(); got != "c2" {
		t.Errorf("cursor = %q, want c2", got)
	}
	if recovered, _ := dlqWriter.Recover(); len(recovered) != 0 {
		t.Errorf("DLQ has %d records, want 0", len(recovered))
	}
}

// A batch-level failure on one destination is dead-lettered for that
// destination alone, so a replay cannot duplicate it at the others.
func TestProcessBatch_DeadLettersOnlyTheFailedDestination(t *testing.T) {
	ok1 := &orderedDest{}
	failing := &orderedDest{batchErr: errors.New("503")}
	p, dlqWriter := makePipelineWithDests(t, []core.Destination{ok1, failing})

	if _, ok := p.processBatch(&checkpointState{}, twoEntryBatch()); !ok {
		t.Fatal("processBatch returned ok=false")
	}
	recovered, err := dlqWriter.Recover()
	if err != nil {
		t.Fatalf("dlq.Recover: %v", err)
	}
	if len(recovered) != 1 || recovered[0].Destination != "ordered#1" {
		t.Fatalf("DLQ records = %+v, want one record for ordered#1", recovered)
	}
	if got := p.cursor.Snapshot(); got != "c2" {
		t.Errorf("cursor = %q, want c2", got)
	}
}

// Per-entry rejections that are dead-lettered must not hold the cursor
// back, or the rejected entries are collected and rejected again.
func TestProcessBatch_CursorPassesDeadLetteredTrailingRejections(t *testing.T) {
	dest := &orderedDest{perEntryErrs: []error{nil, errors.New("400")}}
	p, dlqWriter := makePipelineWithDests(t, []core.Destination{dest})

	if _, ok := p.processBatch(&checkpointState{}, twoEntryBatch()); !ok {
		t.Fatal("processBatch returned ok=false")
	}
	if got := p.cursor.Snapshot(); got != "c2" {
		t.Errorf("cursor = %q, want c2", got)
	}
	recovered, _ := dlqWriter.Recover()
	if len(recovered) != 1 || len(recovered[0].Entries) != 1 || recovered[0].Destination != "ordered#0" {
		t.Errorf("DLQ records = %+v, want one record holding the rejected entry for ordered#0", recovered)
	}
}

func TestProcessBatch_CountsDropsWithoutDLQ(t *testing.T) {
	t.Run("destination rejection", func(t *testing.T) {
		dest := &orderedDest{perEntryErrs: []error{errors.New("400"), nil}}
		p := NewPipeline("drop-dest", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, &mockCheckpointer{}, &PipelineSettings{MaxRetries: 1, RetryBaseBackoff: time.Millisecond}, nil, nil)
		p.ctx = context.Background()
		before := testutil.ToFloat64(metrics.EntriesDropped.WithLabelValues("drop-dest", dropDestinationRejected))

		p.processBatch(&checkpointState{}, twoEntryBatch())

		if got := testutil.ToFloat64(metrics.EntriesDropped.WithLabelValues("drop-dest", dropDestinationRejected)) - before; got != 1 {
			t.Errorf("dropped entries = %v, want 1", got)
		}
	})
	t.Run("processor rejection", func(t *testing.T) {
		proc := &failingProcessor{err: errors.New("bad json")}
		p := NewPipeline("drop-proc", &mockSource{}, []core.Processor{proc}, []DestinationInfo{makeDestInfo(&orderedDest{}, 0)}, &mockCheckpointer{}, &PipelineSettings{MaxRetries: 1, RetryBaseBackoff: time.Millisecond}, nil, nil)
		p.ctx = context.Background()
		before := testutil.ToFloat64(metrics.EntriesDropped.WithLabelValues("drop-proc", dropProcessorRejected))

		p.processBatch(&checkpointState{}, twoEntryBatch())

		if got := testutil.ToFloat64(metrics.EntriesDropped.WithLabelValues("drop-proc", dropProcessorRejected)) - before; got != 2 {
			t.Errorf("dropped entries = %v, want 2", got)
		}
	})
	t.Run("no disk buffer retains pending pages", func(t *testing.T) {
		p := NewPipeline("pending", &mockSource{}, nil, []DestinationInfo{makeDestInfo(&orderedDest{}, 0)}, &mockCheckpointer{}, nil, nil, nil)
		remaining := p.spillRemaining([]Batch{twoEntryBatch()})
		if len(remaining) != 1 || len(remaining[0].Entries) != 2 {
			t.Fatalf("pending pages were discarded: %+v", remaining)
		}
	})
}

func TestTargetDestinations(t *testing.T) {
	p, _ := makePipelineWithDests(t, []core.Destination{&orderedDest{}, &orderedDest{}})

	if got := p.targetDestinations(nil); len(got) != 2 {
		t.Errorf("no keys: got %d targets, want all 2", len(got))
	}
	if got := p.targetDestinations([]string{"ordered#1"}); len(got) != 1 || got[0] != &p.Destinations[1] {
		t.Errorf("known key: got %v, want only destination 1", got)
	}
	if got := p.targetDestinations([]string{"gone#7"}); len(got) != 2 {
		t.Errorf("unknown key: got %d targets, want fallback to all 2", len(got))
	}
}

// The puller must mark a batch in flight before handing it over;
// otherwise a fast pusher can finish it first and leave the counter at 1,
// and the puller never polls again.
func TestPull_InFlightCountSettlesAfterFastPusher(t *testing.T) {
	src := &mockSource{batch: []*core.LogEntry{{Message: "x", Cursor: "c"}}, nextCursor: "c"}
	p := NewPipeline("inflight", src, nil, []DestinationInfo{makeDestInfo(&orderedDest{}, 0)}, &mockCheckpointer{}, &PipelineSettings{BufferSize: 0}, nil, nil)
	p.ctx = context.Background()

	for i := 0; i < 200; i++ {
		done := make(chan struct{})
		go func() {
			<-p.Buffer
			p.markDone()
			close(done)
		}()
		// With no disk buffer, pull blocks until the receiver takes
		// the batch.
		if err := p.pull(); err != nil {
			t.Fatalf("pull: %v", err)
		}
		<-done
		if n := p.flight.Count(); n != 0 {
			t.Fatalf("iteration %d: in-flight count = %d after the batch was processed, want 0", i, n)
		}
	}
}

func TestPull_SkipsWhileDiskBufferHasBatches(t *testing.T) {
	diskBuf, err := buffer.NewDiskBuffer(t.TempDir(), "skip", 0)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	t.Cleanup(func() { _ = diskBuf.Close() })
	if err := diskBuf.Write(buffer.DiskBatch{Entries: []*core.LogEntry{{Message: "spilled"}}, NextCursor: "c1"}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	src := &mockSource{}
	p := NewPipeline("skip", src, nil, []DestinationInfo{makeDestInfo(&orderedDest{}, 0)}, &mockCheckpointer{}, nil, diskBuf, nil)
	p.ctx = context.Background()

	if err := p.pull(); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(src.cursors) != 0 {
		t.Errorf("source was polled %d times while the disk buffer held a batch, want 0", len(src.cursors))
	}
}

func TestDiskBatch_OldFormatTargetsAllDestinations(t *testing.T) {
	var b buffer.DiskBatch
	if err := json.Unmarshal([]byte(`{"entries":[{"message":"m"}],"next_cursor":"c"}`), &b); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if b.Destinations != nil {
		t.Errorf("Destinations = %v, want nil (every destination)", b.Destinations)
	}
}

// onceSource returns one batch on the first Collect and nothing after.
type onceSource struct {
	mu   sync.Mutex
	done bool
}

func (s *onceSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil, cursor, nil
	}
	s.done = true
	return []*core.LogEntry{{Message: "m", Cursor: "c1", Timestamp: time.Now()}}, "c1", nil
}
func (s *onceSource) Close() error { return nil }
func (s *onceSource) Name() string { return "once" }

// slowDest takes delay per send, and reports when a send has started.
type slowDest struct {
	delay    time.Duration
	started  chan struct{}
	once     sync.Once
	mu       sync.Mutex
	received int
}

func (d *slowDest) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	d.once.Do(func() { close(d.started) })
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	d.mu.Lock()
	d.received += len(logs)
	d.mu.Unlock()
	out := make([]core.EntryStatus, len(logs))
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
	}
	return out, nil
}
func (d *slowDest) Close() error { return nil }
func (d *slowDest) Type() string { return "slow" }

func newStopTestPipeline(t *testing.T, dest core.Destination, shutdownTimeout time.Duration) (*Pipeline, *dlq.DeadLetterQueue, *buffer.DiskBuffer, string) {
	t.Helper()
	dir := t.TempDir()
	dlqWriter, err := dlq.New(dir, "stop", 0)
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	diskBuf, err := buffer.NewDiskBuffer(dir, "stop", 0)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	p := NewPipeline("stop", &onceSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, &mockCheckpointer{}, &PipelineSettings{
		PollInterval:     10 * time.Millisecond,
		BufferSize:       1,
		MaxRetries:       1,
		RetryBaseBackoff: time.Millisecond,
		ShutdownTimeout:  shutdownTimeout,
	}, diskBuf, dlqWriter)
	return p, dlqWriter, diskBuf, dir
}

// A graceful stop must finish the in-flight send. It used to cancel the
// send's context first, so the batch failed with context.Canceled and
// was dead-lettered (seen live against Azure Monitor).
func TestStopDeliversInFlightBatch(t *testing.T) {
	dest := &slowDest{delay: 300 * time.Millisecond, started: make(chan struct{})}
	p, dlqWriter, _, _ := newStopTestPipeline(t, dest, 5*time.Second)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-dest.started
	if err := p.Stop(); err != nil {
		t.Errorf("Stop = %v, want nil for a graceful stop", err)
	}

	if dest.received != 1 {
		t.Errorf("destination received %d entries, want 1 (the in-flight batch)", dest.received)
	}
	if recs, _ := dlqWriter.Recover(); len(recs) != 0 {
		t.Errorf("DLQ has %d records after a graceful stop, want 0", len(recs))
	}
}

// When shutdown_timeout expires, the abandoned batch must be kept for
// retry in the disk buffer, not dead-lettered.
func TestStopTimeoutKeepsInterruptedBatch(t *testing.T) {
	dest := &slowDest{delay: time.Hour, started: make(chan struct{})}
	p, dlqWriter, _, dir := newStopTestPipeline(t, dest, 100*time.Millisecond)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-dest.started
	if err := p.Stop(); !errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("Stop = %v, want ErrShutdownTimeout", err)
	}

	if recs, _ := dlqWriter.Recover(); len(recs) != 0 {
		t.Errorf("DLQ has %d records, want 0 (a shutdown is not a destination failure)", len(recs))
	}
	reopened, err := buffer.NewDiskBuffer(dir, "stop", 0)
	if err != nil {
		t.Fatalf("reopen disk buffer: %v", err)
	}
	defer reopened.Close()
	if got := reopened.Count(); got != 1 {
		t.Errorf("disk buffer holds %d batches after the timed-out stop, want 1", got)
	}
}

// stuckSource returns one entry, then blocks in Collect and ignores
// cancellation, like a driver stuck in I/O. It never unblocks.
type stuckSource struct {
	onceSource
	block chan struct{}
}

func (s *stuckSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	entries, next, err := s.onceSource.Collect(ctx, cursor)
	if len(entries) > 0 {
		return entries, next, err
	}
	<-s.block
	return nil, cursor, nil
}

// One shutdown_timeout covers the producer wait and the drain.
// Each used to get its own, so a stuck producer and a stuck send doubled
// the stop time (past Kubernetes' default 30s grace at the 30s default).
func TestStopSharesOneDeadlineAcrossProducerAndDrain(t *testing.T) {
	const timeout = 400 * time.Millisecond
	dest := &slowDest{delay: time.Hour, started: make(chan struct{})}
	p, _, _, _ := newStopTestPipeline(t, dest, timeout)
	p.Source = &stuckSource{block: make(chan struct{})}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-dest.started

	start := time.Now()
	err := p.Stop()
	elapsed := time.Since(start)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("Stop = %v, want ErrShutdownTimeout", err)
	}
	if elapsed > timeout+timeout/2 {
		t.Errorf("Stop took %v, want about one shutdown_timeout (%v)", elapsed, timeout)
	}
}

// A caller's earlier deadline also bounds the stop.
func TestStopContextHonorsCallerDeadline(t *testing.T) {
	dest := &slowDest{delay: time.Hour, started: make(chan struct{})}
	p, _, _, _ := newStopTestPipeline(t, dest, time.Hour)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-dest.started

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.StopContext(ctx)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("StopContext = %v, want ErrShutdownTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("StopContext took %v, want it bounded by the caller's 200ms deadline", elapsed)
	}
}
