package pipeline

import (
	"context"
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

// orderedDest records the order in which entries were received and
// returns per-entry statuses based on configured per-entry errors.
// All entries are recorded; the test asserts that the order in which
// the destination received the entries matches the input order. Used
// to verify that the fan-out preserves per-destination ordering.
type orderedDest struct {
	mu             sync.Mutex
	received       []*core.LogEntry
	perEntryErrs   []error
	batchErr       error
	receivedCalls  int
	receivedOrder  []int
	latencyPerCall time.Duration
}

func (d *orderedDest) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	d.mu.Lock()
	d.receivedCalls++
	startIdx := len(d.received)
	for i, e := range logs {
		d.received = append(d.received, e)
		d.receivedOrder = append(d.receivedOrder, startIdx+i)
	}
	if d.latencyPerCall > 0 {
		time.Sleep(d.latencyPerCall)
	}
	batchErr := d.batchErr
	perEntry := d.perEntryErrs
	d.mu.Unlock()

	if batchErr != nil {
		return nil, batchErr
	}
	out := make([]core.EntryStatus, len(logs))
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
		if i < len(perEntry) && perEntry[i] != nil {
			out[i].Error = perEntry[i]
		}
	}
	return out, nil
}

func (d *orderedDest) Close() error { return nil }
func (d *orderedDest) Type() string { return "ordered" }

func (d *orderedDest) snapshot() ([]*core.LogEntry, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*core.LogEntry, len(d.received))
	copy(out, d.received)
	calls := d.receivedCalls
	return out, calls
}

// sendWithRetry sends entries to every destination and merges the
// results the way a single-destination caller sees them: the first
// batch-level error (in destination order) wins, otherwise an entry
// fails if any destination rejected it.
func (p *Pipeline) sendWithRetry(entries []*core.LogEntry) ([]core.EntryStatus, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	results := p.sendToDestinations(entries, p.targetDestinations(nil))
	merged := make([]core.EntryStatus, len(entries))
	for i := range merged {
		merged[i] = core.EntryStatus{Index: i}
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for i, s := range r.statuses {
			if i < len(merged) && s.Error != nil {
				merged[i].Error = s.Error
			}
		}
	}
	return merged, nil
}

// makePipelineWithDests builds a pipeline with the supplied destinations
// (and the test defaults) and a context the test can control. The
// returned pipeline has a real DLQ (so DLQ-write paths can be exercised)
// and starts with the cursor at "start".
func makePipelineWithDests(t *testing.T, dests []core.Destination) (*Pipeline, *dlq.DeadLetterQueue) {
	t.Helper()
	dlqDir := t.TempDir()
	dlqWriter, err := dlq.New(dlqDir, "test", 0)
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	t.Cleanup(func() { _ = dlqWriter.Close() })

	cp := &mockCheckpointer{cursor: "start"}
	infos := make([]DestinationInfo, 0, len(dests))
	for _, d := range dests {
		infos = append(infos, makeDestInfo(d, 0))
	}
	p := NewPipeline("test", &mockSource{}, nil, infos, cp, &PipelineSettings{
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, dlqWriter)
	p.ctx = context.Background()
	return p, dlqWriter
}

// TestSendWithRetry_FanOutToThreeDestinationsAllSucceed verifies that
// sendWithRetry dispatches a single batch to three destinations in
// parallel, and that a successful path through all three returns nil
// error with the expected per-entry statuses.
func TestSendWithRetry_FanOutToThreeDestinationsAllSucceed(t *testing.T) {
	d1 := &orderedDest{}
	d2 := &orderedDest{}
	d3 := &orderedDest{}

	p, _ := makePipelineWithDests(t, []core.Destination{d1, d2, d3})

	entries := []*core.LogEntry{
		{Message: "a", Cursor: "c1"},
		{Message: "b", Cursor: "c2"},
		{Message: "c", Cursor: "c3"},
	}
	statuses, err := p.sendWithRetry(entries)
	if err != nil {
		t.Fatalf("sendWithRetry returned error: %v", err)
	}
	if len(statuses) != len(entries) {
		t.Fatalf("got %d statuses, want %d", len(statuses), len(entries))
	}
	for i, s := range statuses {
		if s.Error != nil {
			t.Errorf("statuses[%d].Error = %v, want nil", i, s.Error)
		}
	}

	for i, d := range []*orderedDest{d1, d2, d3} {
		recv, calls := d.snapshot()
		if calls != 1 {
			t.Errorf("dest %d: Send called %d times, want 1", i, calls)
		}
		if len(recv) != len(entries) {
			t.Errorf("dest %d: received %d entries, want %d", i, len(recv), len(entries))
			continue
		}
		for j, e := range recv {
			if e.Message != entries[j].Message {
				t.Errorf("dest %d: received entry %d message = %q, want %q", i, j, e.Message, entries[j].Message)
			}
		}
	}
}

// TestSendWithRetry_PartialFailureBlocksCursor verifies that when one
// of three destinations fails at the batch level, sendWithRetry
// returns the failure to the caller, and the pusher's processBatch
// only advances the cursor past the failed sub-batch once it is in the
// DLQ.
func TestSendWithRetry_PartialFailureBlocksCursor(t *testing.T) {
	// d2 returns a batch-level error on every call. The other two
	// succeed. The pusher must see a non-nil error and not advance
	// the cursor past the failed sub-batch.
	d1 := &orderedDest{}
	d2 := &orderedDest{batchErr: errors.New("dest2 down")}
	d3 := &orderedDest{}

	p, dlqWriter := makePipelineWithDests(t, []core.Destination{d1, d2, d3})

	// Drive the same code path the pusher uses: processBatch. If
	// sendWithRetry returns an error, the pusher DLQs the entire
	// sub-batch and (if the DLQ write succeeds) advances the cursor
	// to the sub-batch's NextCursor. Either way, the cursor must
	// not stay at "start" without a forward signal that reflects
	// the partial failure.
	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "a", Cursor: "c1"},
			{Message: "b", Cursor: "c2"},
		},
		EntryCursors: []string{"c1", "c2"},
		NextCursor:   "c2",
	}
	_, ok := p.processBatch(&checkpointState{}, batch)
	if !ok {
		t.Fatal("processBatch should return true (DLQ'd batch is considered handled)")
	}

	// d2's batch error must have caused a non-nil error from
	// sendWithRetry; the pusher DLQ'd the sub-batch and the cursor
	// advanced past it (to "c2"). When any destination fails at the
	// batch level, the cursor does not stay at "start" because the
	// DLQ caught the batch.
	if p.cursor.Snapshot() == "start" {
		t.Fatal("cursor did not advance after partial failure DLQ")
	}
	if p.cursor.Snapshot() != "c2" {
		t.Errorf("cursor = %q, want c2 (DLQ'd past sub-batch)", p.cursor.Snapshot())
	}

	// d1 and d3 received the entries before d2 failed (concurrent
	// fan-out means they all see the same call).
	if recv, _ := d1.snapshot(); len(recv) != 2 {
		t.Errorf("d1: got %d entries, want 2", len(recv))
	}
	if recv, _ := d3.snapshot(); len(recv) != 2 {
		t.Errorf("d3: got %d entries, want 2", len(recv))
	}
	// d2 also saw the entries — Send is called even when it returns
	// an error.
	if recv, calls := d2.snapshot(); calls == 0 {
		t.Error("d2: Send was never called (should have been called once)")
	} else if len(recv) != 2 {
		t.Errorf("d2: got %d entries, want 2", len(recv))
	}

	// DLQ contains the entire sub-batch (batch-level failure path).
	recovered, err := dlqWriter.Recover()
	if err != nil {
		t.Fatalf("dlq.Recover: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(recovered))
	}
	if len(recovered[0].Entries) != 2 {
		t.Errorf("expected DLQ entry to contain both entries, got %d", len(recovered[0].Entries))
	}
}

// TestSendWithRetry_AllDestinationsFailCursorStaysPut verifies the
// all-fail path: with no DLQ, the cursor MUST NOT advance when every
// destination fails.
func TestSendWithRetry_AllDestinationsFailCursorStaysPut(t *testing.T) {
	d1 := &orderedDest{batchErr: errors.New("d1 down")}
	d2 := &orderedDest{batchErr: errors.New("d2 down")}
	d3 := &orderedDest{batchErr: errors.New("d3 down")}

	// No DLQ — the pusher must return the sub-batches for retry.
	// Track Set() calls explicitly so we can detect that the
	// checkpointer was never written.
	setCalled := 0
	cp := &countingCheckpointer{cursor: "start", setCalls: &setCalled}
	infos := []DestinationInfo{
		makeDestInfo(d1, 0),
		makeDestInfo(d2, 0),
		makeDestInfo(d3, 0),
	}
	p := NewPipeline("test", &mockSource{}, nil, infos, cp, &PipelineSettings{
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	// Pre-seed the cursor to "start" so we can detect any forward
	// movement. NewPipeline leaves the cursor empty; without an
	// explicit seed, "advanced" and "stayed empty" are
	// indistinguishable.
	p.cursor.Advance("start")

	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "a", Cursor: "c1"},
			{Message: "b", Cursor: "c2"},
		},
		EntryCursors: []string{"c1", "c2"},
		NextCursor:   "c2",
	}
	remaining, ok := p.processBatch(&checkpointState{}, batch)
	if ok {
		t.Fatal("processBatch should return false when all destinations fail and no DLQ is configured")
	}
	if len(remaining) == 0 {
		t.Fatal("processBatch should return the sub-batches for disk spillover")
	}
	if p.cursor.Snapshot() != "start" {
		t.Fatalf("cursor advanced despite all-destinations-failure: got %q, want %q", p.cursor.Snapshot(), "start")
	}
	if setCalled != 0 {
		t.Errorf("checkpointer.Set was called %d times, want 0 (cursor must not advance on all-fail)", setCalled)
	}
}

// TestSendWithRetry_PreservesPerDestinationOrdering verifies that the
// concurrent fan-out preserves per-destination entry ordering: each
// destination receives the entries in the same order they were given
// to sendWithRetry, and the per-entry statuses are merged in input
// order.
func TestSendWithRetry_PreservesPerDestinationOrdering(t *testing.T) {
	// d1 sleeps a bit on every call so d2 and d3 finish first. If
	// fan-out is truly parallel, d1 must still see the entries in
	// input order despite the latency.
	d1 := &orderedDest{latencyPerCall: 20 * time.Millisecond}
	d2 := &orderedDest{}
	d3 := &orderedDest{}

	p, _ := makePipelineWithDests(t, []core.Destination{d1, d2, d3})

	entries := []*core.LogEntry{
		{Message: "first", Cursor: "c1"},
		{Message: "second", Cursor: "c2"},
		{Message: "third", Cursor: "c3"},
		{Message: "fourth", Cursor: "c4"},
	}
	statuses, err := p.sendWithRetry(entries)
	if err != nil {
		t.Fatalf("sendWithRetry returned error: %v", err)
	}
	if len(statuses) != len(entries) {
		t.Fatalf("got %d statuses, want %d", len(statuses), len(entries))
	}
	for i, e := range entries {
		if statuses[i].Index != i {
			t.Errorf("statuses[%d].Index = %d, want %d", i, statuses[i].Index, i)
		}
		if e.Message == "" {
			t.Errorf("statuses[%d] has empty input entry", i)
		}
	}
	for i, d := range []*orderedDest{d1, d2, d3} {
		recv, _ := d.snapshot()
		if len(recv) != len(entries) {
			t.Errorf("dest %d: got %d entries, want %d", i, len(recv), len(entries))
			continue
		}
		for j, e := range recv {
			if e.Message != entries[j].Message {
				t.Errorf("dest %d: entry %d = %q, want %q (ordering broken)", i, j, e.Message, entries[j].Message)
			}
			if e.Cursor != entries[j].Cursor {
				t.Errorf("dest %d: entry %d cursor = %q, want %q", i, j, e.Cursor, entries[j].Cursor)
			}
		}
	}
}

// TestSendWithRetry_PerEntryErrorsMergedAcrossDestinations verifies
// that per-entry errors from one destination are reflected in the
// merged status slice even when another destination accepted the same
// entry. This is the "per-destination results reconciled" deliverable.
func TestSendWithRetry_PerEntryErrorsMergedAcrossDestinations(t *testing.T) {
	// d2 rejects the second entry; d1 and d3 accept everything.
	d1 := &orderedDest{}
	d2 := &orderedDest{perEntryErrs: []error{nil, errors.New("rejected by d2")}}
	d3 := &orderedDest{}

	p, dlqWriter := makePipelineWithDests(t, []core.Destination{d1, d2, d3})

	entries := []*core.LogEntry{
		{Message: "a", Cursor: "c1"},
		{Message: "b", Cursor: "c2"},
		{Message: "c", Cursor: "c3"},
	}
	statuses, err := p.sendWithRetry(entries)
	if err != nil {
		t.Fatalf("sendWithRetry returned error: %v", err)
	}
	// Entry 1 was accepted by every destination -> no merged error.
	if statuses[0].Error != nil {
		t.Errorf("statuses[0].Error = %v, want nil", statuses[0].Error)
	}
	// Entry 1 (index 1) was rejected by d2 -> merged error must
	// reflect that.
	if statuses[1].Error == nil {
		t.Error("statuses[1].Error = nil, want non-nil (d2 rejected)")
	}
	// Entry 2 (index 2) was accepted by every destination -> no error.
	if statuses[2].Error != nil {
		t.Errorf("statuses[2].Error = %v, want nil", statuses[2].Error)
	}

	// Pusher path: processBatch sees the per-entry error, DLQs
	// entry 1 individually, and advances the cursor to c3 (the
	// last successful entry's cursor).
	batch := Batch{
		Entries:      entries,
		EntryCursors: []string{"c1", "c2", "c3"},
		NextCursor:   "c3",
	}
	_, ok := p.processBatch(&checkpointState{}, batch)
	if !ok {
		t.Fatal("processBatch should return true (per-entry DLQ path)")
	}
	if p.cursor.Snapshot() != "c3" {
		t.Errorf("cursor = %q, want c3 (last successful entry)", p.cursor.Snapshot())
	}
	recovered, err := dlqWriter.Recover()
	if err != nil {
		t.Fatalf("dlq.Recover: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(recovered))
	}
	if len(recovered[0].Entries) != 1 {
		t.Fatalf("expected 1 rejected entry in DLQ, got %d", len(recovered[0].Entries))
	}
	// The cursor is internal to the pipeline (json:"-" on
	// LogEntry.Cursor) and is dropped on DLQ round-trip. Verify by
	// message instead.
	if recovered[0].Entries[0].Message != "b" {
		t.Errorf("DLQ entry message = %q, want %q", recovered[0].Entries[0].Message, "b")
	}
}

// TestSendWithRetry_ConcurrentFanOutLatencyUnderCount verifies that
// the parallel fan-out is in fact parallel: with three destinations
// each sleeping for 50ms, the total wall time must be roughly one
// sleep window (not three). This is a wall-clock sanity check; on a
// loaded CI machine it may be slightly more, so the test allows up
// to 130ms.
func TestSendWithRetry_ConcurrentFanOutLatencyUnderCount(t *testing.T) {
	const sleep = 50 * time.Millisecond
	d1 := &orderedDest{latencyPerCall: sleep}
	d2 := &orderedDest{latencyPerCall: sleep}
	d3 := &orderedDest{latencyPerCall: sleep}

	p, _ := makePipelineWithDests(t, []core.Destination{d1, d2, d3})

	entries := []*core.LogEntry{{Message: "a", Cursor: "c1"}}
	start := time.Now()
	if _, err := p.sendWithRetry(entries); err != nil {
		t.Fatalf("sendWithRetry: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 3*sleep-10*time.Millisecond {
		t.Errorf("fan-out took %v, expected ~%v (parallel), suggesting destinations were serialized", elapsed, sleep)
	}
	if elapsed < sleep/2 {
		t.Errorf("fan-out took %v, expected at least ~%v (suspiciously fast)", elapsed, sleep/2)
	}
}

// countingCheckpointer is a Checkpointer that counts Set calls so a
// test can assert that no checkpoint was written.
type countingCheckpointer struct {
	cursor   string
	setCalls *int
}

func (c *countingCheckpointer) Get(pipelineID string) (string, error) {
	return c.cursor, nil
}

func (c *countingCheckpointer) Set(pipelineID string, cursor string) error {
	*c.setCalls++
	c.cursor = cursor
	return nil
}

func (c *countingCheckpointer) Close() error { return nil }

// TestSendWithRetry_FailsFastWhenBreakerOpen verifies that when the
// destination's circuit breaker is already open, sendWithRetry returns
// retry.ErrCircuitOpen without ever calling destination.Send.
func TestSendWithRetry_FailsFastWhenBreakerOpen(t *testing.T) {
	d := &orderedDest{}
	infos := []DestinationInfo{makeDestInfo(d, 0)}
	// Pre-open the breaker so the very first Allow returns ErrCircuitOpen.
	infos[0].Breaker = retry.NewBreaker()
	infos[0].Breaker.FailureThreshold = 1
	infos[0].Breaker.OpenDuration = time.Hour
	infos[0].Breaker.RecordFailure()
	if got := infos[0].Breaker.State(); got != retry.StateOpen {
		t.Fatalf("breaker pre-open: State() = %d, want StateOpen", got)
	}

	p := NewPipeline("test", &mockSource{}, nil, infos, &mockCheckpointer{}, &PipelineSettings{
		MaxRetries:         5,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	entries := []*core.LogEntry{{Message: "a", Cursor: "c1"}}
	_, err := p.sendWithRetry(entries)
	if !errors.Is(err, retry.ErrCircuitOpen) {
		t.Fatalf("sendWithRetry = %v, want retry.ErrCircuitOpen", err)
	}
	// Critical: the destination must NOT have been called.
	if _, calls := d.snapshot(); calls != 0 {
		t.Errorf("destination Send was called %d times while breaker was open, want 0", calls)
	}
}

// TestSendWithRetry_BreakerTripsAfterThresholdFailures verifies that
// after FailureThreshold consecutive destination failures, the breaker
// trips and subsequent calls fail fast.
func TestSendWithRetry_BreakerTripsAfterThresholdFailures(t *testing.T) {
	d := &orderedDest{batchErr: errors.New("transient failure")}
	infos := []DestinationInfo{makeDestInfo(d, 0)}
	infos[0].Breaker = retry.NewBreaker()
	infos[0].Breaker.FailureThreshold = 2
	infos[0].Breaker.OpenDuration = time.Hour

	p := NewPipeline("test", &mockSource{}, nil, infos, &mockCheckpointer{}, &PipelineSettings{
		MaxRetries:         1, // one attempt; breaker does the heavy lifting
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	entries := []*core.LogEntry{{Message: "a", Cursor: "c1"}}
	// First call: 1 send attempt -> 1 failure. State still closed
	// (threshold=2).
	_, _ = p.sendWithRetry(entries)
	if got := infos[0].Breaker.State(); got != retry.StateClosed {
		t.Errorf("after 1 failure: State() = %d, want StateClosed", got)
	}
	// Second call: another failure -> threshold reached -> open.
	_, err := p.sendWithRetry(entries)
	if err == nil {
		t.Error("second sendWithRetry: expected error, got nil")
	}
	if got := infos[0].Breaker.State(); got != retry.StateOpen {
		t.Errorf("after 2 failures: State() = %d, want StateOpen", got)
	}
	// Third call: must fail fast; destination must NOT be invoked.
	_, callsBefore := d.snapshot()
	_, err = p.sendWithRetry(entries)
	if !errors.Is(err, retry.ErrCircuitOpen) {
		t.Errorf("third sendWithRetry: err = %v, want ErrCircuitOpen", err)
	}
	_, callsAfter := d.snapshot()
	if callsAfter != callsBefore {
		t.Errorf("destination was called %d more times while breaker open, want 0", callsAfter-callsBefore)
	}
}

// TestSendWithRetry_BreakerRecoversViaHalfOpenProbe verifies that
// after the cool-off elapses, a successful probe call closes the
// circuit.
func TestSendWithRetry_BreakerRecoversViaHalfOpenProbe(t *testing.T) {
	// Destination that fails the first 2 calls then succeeds. With
	// FailureThreshold=2 and a short cool-off, the third call must
	// be a successful probe in half-open and close the circuit.
	var calls int32
	d := &switchingDest{
		failForFirstN: 2,
		calls:         &calls,
	}
	infos := []DestinationInfo{makeDestInfo(d, 0)}
	infos[0].Breaker = retry.NewBreaker()
	infos[0].Breaker.FailureThreshold = 2
	infos[0].Breaker.OpenDuration = 20 * time.Millisecond
	infos[0].Breaker.HalfOpenMaxCalls = 1

	p := NewPipeline("test", &mockSource{}, nil, infos, &mockCheckpointer{}, &PipelineSettings{
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	entries := []*core.LogEntry{{Message: "a", Cursor: "c1"}}

	// Call 1: destination fails; breaker still closed.
	if _, err := p.sendWithRetry(entries); err == nil {
		t.Fatal("call 1: expected error, got nil")
	}
	if got := infos[0].Breaker.State(); got != retry.StateClosed {
		t.Fatalf("call 1: State() = %d, want StateClosed", got)
	}
	// Call 2: destination fails; threshold reached -> open.
	if _, err := p.sendWithRetry(entries); err == nil {
		t.Fatal("call 2: expected error, got nil")
	}
	if got := infos[0].Breaker.State(); got != retry.StateOpen {
		t.Fatalf("call 2: State() = %d, want StateOpen", got)
	}
	// Wait for cool-off to elapse.
	time.Sleep(30 * time.Millisecond)
	// Call 3: probe in half-open; destination now succeeds. Breaker
	// must transition to closed.
	if _, err := p.sendWithRetry(entries); err != nil {
		t.Fatalf("call 3 (probe): err = %v, want nil", err)
	}
	if got := infos[0].Breaker.State(); got != retry.StateClosed {
		t.Errorf("call 3: State() = %d, want StateClosed (probe success)", got)
	}
}

// TestProcessBatch_RoutesToDiskBufferWhenBreakerOpen verifies the
// load-shedding behavior: when a destination's circuit breaker is
// open, processBatch returns the sub-batches for the caller to spill
// to the disk buffer, the cursor does NOT advance, and the DLQ is
// NOT written.
func TestProcessBatch_RoutesToDiskBufferWhenBreakerOpen(t *testing.T) {
	dir := t.TempDir()
	dlqDir := t.TempDir()

	// DLQ that records how many entries it received. The test
	// asserts it received NONE — the breaker-open path bypasses the
	// DLQ entirely.
	dlqWriter, err := dlq.New(dlqDir, "test", 0)
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	t.Cleanup(func() { _ = dlqWriter.Close() })

	// Disk buffer to verify the sub-batches end up there.
	diskBuf, err := buffer.NewDiskBuffer(dir, "test", 100)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	t.Cleanup(func() { _ = diskBuf.Close() })

	dest := &orderedDest{batchErr: errors.New("dead destination")}
	infos := []DestinationInfo{makeDestInfo(dest, 0)}
	infos[0].Breaker = retry.NewBreaker()
	infos[0].Breaker.FailureThreshold = 1
	infos[0].Breaker.OpenDuration = time.Hour
	infos[0].Breaker.RecordFailure() // pre-open

	cp := &mockCheckpointer{cursor: "start"}
	p := NewPipeline("test", &mockSource{}, nil, infos, cp, &PipelineSettings{
		SendBatchSize:      2,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, diskBuf, dlqWriter)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "1", Cursor: "c1"},
			{Message: "2", Cursor: "c2"},
			{Message: "3", Cursor: "c3"},
			{Message: "4", Cursor: "c4"},
		},
		EntryCursors: []string{"c1", "c2", "c3", "c4"},
		NextCursor:   "c4",
	}

	remaining, ok := p.processBatch(&checkpointState{}, batch)
	if ok {
		t.Fatal("processBatch returned ok=true; want false (sub-batches must be returned for spilling)")
	}
	if len(remaining) == 0 {
		t.Fatal("processBatch returned no remaining sub-batches; want the un-sent sub-batches")
	}

	// The caller (pushRoutine) calls spillRemaining on a non-ok
	// return. Simulate that here.
	p.spillRemaining(remaining)

	// Cursor must not have advanced — the data is on disk, awaiting
	// the breaker's recovery.
	if got := p.cursor.Snapshot(); got != "start" {
		t.Errorf("cursor = %q, want %q (must not advance when shedding)", got, "start")
	}
	// DLQ must not have been written to.
	recovered, err := dlqWriter.Recover()
	if err != nil {
		t.Fatalf("dlq.Recover: %v", err)
	}
	if len(recovered) != 0 {
		t.Errorf("DLQ received %d entries; want 0 (breaker-open path bypasses DLQ)", len(recovered))
	}
	// Disk buffer must contain the spilled sub-batches.
	if got := diskBuf.Count(); got == 0 {
		t.Error("disk buffer is empty; want the spilled sub-batches")
	}
}

// TestSendWithRetry_BreakerDefaultsAppliedByNewPipeline verifies
// that NewPipeline applies the configured breaker thresholds.
func TestSendWithRetry_BreakerDefaultsAppliedByNewPipeline(t *testing.T) {
	d := &orderedDest{}
	infos := []DestinationInfo{makeDestInfo(d, 0)}

	p := NewPipeline("test", &mockSource{}, nil, infos, &mockCheckpointer{}, &PipelineSettings{
		CircuitBreakerFailureThreshold: 7,
		CircuitBreakerRecoveryTimeout:  123 * time.Millisecond,
		CircuitBreakerHalfOpenMaxCalls: 3,
	}, nil, nil)
	p.ctx = context.Background()

	b := p.Destinations[0].Breaker
	if b.FailureThreshold != 7 {
		t.Errorf("FailureThreshold = %d, want 7", b.FailureThreshold)
	}
	if b.OpenDuration != 123*time.Millisecond {
		t.Errorf("OpenDuration = %v, want 123ms", b.OpenDuration)
	}
	if b.HalfOpenMaxCalls != 3 {
		t.Errorf("HalfOpenMaxCalls = %d, want 3", b.HalfOpenMaxCalls)
	}
}

// switchingDest fails its first failForFirstN calls and then
// succeeds. The counter is incremented exactly once per Send call.
type switchingDest struct {
	mu            sync.Mutex
	failForFirstN int
	calls         *int32
}

func (d *switchingDest) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	d.mu.Lock()
	(*d.calls)++
	n := *d.calls
	d.mu.Unlock()
	if n <= int32(d.failForFirstN) {
		return nil, errors.New("transient")
	}
	out := make([]core.EntryStatus, len(logs))
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
	}
	return out, nil
}

func (d *switchingDest) Close() error { return nil }
func (d *switchingDest) Type() string { return "switching" }

// TestBreakerMetricEmissionOnTransition verifies that the
// circuit_breaker_state gauge and the circuit_breaker_tripped_total
// counter are updated on every breaker state transition.
func TestBreakerMetricEmissionOnTransition(t *testing.T) {
	// Use a dedicated pipeline ID + dest type to keep the metric
	// series isolated from any other test in the package.
	const pipelineID = "metric-test"
	dest := &orderedDest{batchErr: errors.New("transient failure")}
	// orderedDest.Type() returns "ordered".
	const destType = "ordered"
	reset := func() {
		metrics.CircuitBreakerState.Reset()
		metrics.CircuitBreakerTrippedTotal.Reset()
	}
	reset()
	t.Cleanup(reset)

	infos := []DestinationInfo{makeDestInfo(dest, 0)}
	infos[0].Breaker = retry.NewBreaker()
	infos[0].Breaker.FailureThreshold = 1
	infos[0].Breaker.OpenDuration = time.Hour

	p := NewPipeline(pipelineID, &mockSource{}, nil, infos, &mockCheckpointer{}, &PipelineSettings{
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	entries := []*core.LogEntry{{Message: "a", Cursor: "c1"}}

	// Closed -> Open: one failed send trips the breaker (threshold=1).
	if _, err := p.sendWithRetry(entries); err == nil {
		t.Fatal("expected error from sendWithRetry")
	}
	if got := testutil.ToFloat64(metrics.CircuitBreakerState.WithLabelValues(pipelineID, destType)); got != float64(retry.StateOpen) {
		t.Errorf("CircuitBreakerState after trip = %v, want %v (StateOpen)", got, retry.StateOpen)
	}
	if got := testutil.ToFloat64(metrics.CircuitBreakerTrippedTotal.WithLabelValues(pipelineID, destType)); got != 1 {
		t.Errorf("CircuitBreakerTrippedTotal after first trip = %v, want 1", got)
	}
	// Subsequent calls while open must NOT increment the counter
	// (they don't transition state).
	_, _ = p.sendWithRetry(entries)
	if got := testutil.ToFloat64(metrics.CircuitBreakerTrippedTotal.WithLabelValues(pipelineID, destType)); got != 1 {
		t.Errorf("CircuitBreakerTrippedTotal after second call (no new trip) = %v, want 1", got)
	}
}
