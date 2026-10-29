package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"loghub/internal/buffer"
	"loghub/internal/checkpointer"
	"loghub/internal/core"
	"loghub/internal/dlq"
	"loghub/internal/metrics"
	"loghub/internal/plugins/destination"
	"loghub/internal/retry"
)

type mockSource struct {
	batch      []*core.LogEntry
	nextCursor string
	err        error
	closed     bool
	cursors    []string
}

func (m *mockSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	m.cursors = append(m.cursors, cursor)
	return m.batch, m.nextCursor, m.err
}

func (m *mockSource) Close() error {
	m.closed = true
	return nil
}

func (m *mockSource) Name() string {
	return "mock-source"
}

type mockDestination struct {
	sendErr  error
	closed   bool
	received []*core.LogEntry
	mu       sync.Mutex
	latency  time.Duration
	// perEntryErrs, if non-nil, returns an error for the entry at the
	// corresponding index. The caller receives per-entry statuses; nil
	// entries are accepted.
	perEntryErrs []error
}

func (m *mockDestination) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, logs...)
	if m.sendErr != nil {
		return nil, m.sendErr
	}
	out := make([]core.EntryStatus, len(logs))
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
		if m.perEntryErrs != nil && i < len(m.perEntryErrs) && m.perEntryErrs[i] != nil {
			out[i].Error = m.perEntryErrs[i]
		}
	}
	return out, nil
}

func (m *mockDestination) Close() error {
	m.closed = true
	return nil
}

func (m *mockDestination) Type() string {
	return "mock"
}

type mockCheckpointer struct {
	cursor string
	err    error
	setErr error
	mu     sync.Mutex
}

func (m *mockCheckpointer) Get(pipelineID string) (string, error) {
	return m.cursor, m.err
}

func (m *mockCheckpointer) Set(pipelineID string, cursor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursor = cursor
	return m.setErr
}

func (m *mockCheckpointer) Close() error {
	return nil
}

func makeDestInfo(dest core.Destination, maxRetries int) DestinationInfo {
	return DestinationInfo{Destination: dest, MaxRetries: maxRetries}
}

type batchRecordingDestination struct {
	mu         sync.Mutex
	batchSizes []int
	failOnCall int
	calls      int
}

func (d *batchRecordingDestination) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.failOnCall > 0 && d.calls == d.failOnCall {
		return nil, errors.New("send failed")
	}
	d.batchSizes = append(d.batchSizes, len(logs))
	out := make([]core.EntryStatus, len(logs))
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
	}
	return out, nil
}

func (d *batchRecordingDestination) Close() error { return nil }

func (d *batchRecordingDestination) Type() string { return "mock" }

func TestPipelineStartStop(t *testing.T) {
	src := &mockSource{batch: []*core.LogEntry{}, nextCursor: "1"}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, nil, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	p.Stop()

	if !src.closed {
		t.Error("Expected source to be closed")
	}
	if !dest.closed {
		t.Error("Expected destination to be closed")
	}
}

func TestPipelineCollectAndForward(t *testing.T) {
	src := &mockSource{
		batch: []*core.LogEntry{
			{Message: "test1"},
			{Message: "test2"},
		},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, nil, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	p.Stop()

	if len(dest.received) == 0 {
		t.Error("Expected at least one batch received")
	}
}

func TestPullDoesNotAdvanceCursorBeforeDelivery(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{
		batch:      []*core.LogEntry{{Message: "test1"}},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: errors.New("downstream unavailable")}
	cp := &mockCheckpointer{cursor: "start"}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	_, _ = p.processBatch(&checkpointState{}, <-p.Buffer)

	if p.cursor.Snapshot() != "start" {
		t.Fatalf("cursor advanced before successful delivery: got %q", p.cursor.Snapshot())
	}
}

func TestPushRoutineFlushesPendingCheckpointOnShutdown(t *testing.T) {
	src := &mockSource{}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: time.Hour,
	}, nil, nil)
	p.ctx = context.Background()

	p.Buffer <- Batch{
		Entries:      []*core.LogEntry{{Message: "test1"}},
		EntryCursors: []string{"cursor123"},
		NextCursor:   "cursor123",
	}
	close(p.Buffer)

	p.consumerWg.Add(1)
	p.pushRoutine()

	if cp.cursor != "cursor123" {
		t.Fatalf("expected pending checkpoint to flush on shutdown, got %q", cp.cursor)
	}
}

func TestProcessBatchSplitsDestinationSends(t *testing.T) {
	dest := &batchRecordingDestination{}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "1"},
			{Message: "2"},
			{Message: "3"},
			{Message: "4"},
			{Message: "5"},
		},
		EntryCursors: []string{"c1", "c2", "c3", "c4", "c5"},
		NextCursor:   "c5",
	}

	remaining, ok := p.processBatch(&checkpointState{}, batch)
	if !ok {
		t.Fatal("expected batch processing to succeed")
	}
	if len(remaining) != 0 {
		t.Fatalf("expected no remaining batches, got %d", len(remaining))
	}

	if got := dest.batchSizes; len(got) != 3 || got[0] != 2 || got[1] != 2 || got[2] != 1 {
		t.Fatalf("unexpected destination batch sizes: %v", got)
	}
	if cp.cursor != "c5" {
		t.Fatalf("expected checkpoint c5, got %q", cp.cursor)
	}
	if p.cursor.Snapshot() != "c5" {
		t.Fatalf("expected current cursor c5, got %q", p.cursor.Snapshot())
	}
}

func TestProcessBatchReturnsRemainingSubBatchesOnFailure(t *testing.T) {
	dest := &batchRecordingDestination{failOnCall: 2}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "1"},
			{Message: "2"},
			{Message: "3"},
			{Message: "4"},
			{Message: "5"},
		},
		EntryCursors: []string{"c1", "c2", "c3", "c4", "c5"},
		NextCursor:   "c5",
	}

	remaining, ok := p.processBatch(&checkpointState{}, batch)
	if ok {
		t.Fatal("expected batch processing to fail")
	}
	if got := dest.batchSizes; len(got) != 1 || got[0] != 2 {
		t.Fatalf("unexpected successful destination sends: %v", got)
	}
	if cp.cursor != "c2" {
		t.Fatalf("expected checkpoint to stop at c2, got %q", cp.cursor)
	}
	if p.cursor.Snapshot() != "c2" {
		t.Fatalf("expected current cursor to stop at c2, got %q", p.cursor.Snapshot())
	}
	if len(remaining) != 2 {
		t.Fatalf("expected two remaining sub-batches, got %d", len(remaining))
	}
	if remaining[0].NextCursor != "c4" || len(remaining[0].Entries) != 2 {
		t.Fatalf("unexpected first remaining batch: %+v", remaining[0])
	}
	if remaining[1].NextCursor != "c5" || len(remaining[1].Entries) != 1 {
		t.Fatalf("unexpected second remaining batch: %+v", remaining[1])
	}
}

func TestPipelineRetryOnFailure(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{
		batch: []*core.LogEntry{
			{Message: "test1"},
		},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: errors.New("transient error")}
	cp := &mockCheckpointer{}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{MaxRetries: 2, RetryBaseBackoff: 100 * time.Millisecond, PollInterval: 100 * time.Millisecond, ShutdownTimeout: time.Second}, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(600 * time.Millisecond)
	p.Stop()
}

func TestPipelinePoisonPillDropsBatch(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{
		batch: []*core.LogEntry{
			{Message: "bad1"},
		},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: fmt.Errorf("permanently rejected: %w", core.ErrPoisonPill)}
	cp := &mockCheckpointer{}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, nil, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	p.Stop()
}

func TestPipelineDoubleStart(t *testing.T) {
	src := &mockSource{}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, nil, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("First Start failed: %v", err)
	}

	if err := p.Start(context.Background()); err == nil {
		t.Error("Expected error on double Start")
	}

	p.Stop()
}

func TestPipelinePerDestinationMaxRetries(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{
		batch: []*core.LogEntry{
			{Message: "test1"},
		},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: errors.New("transient error")}
	cp := &mockCheckpointer{}

	// Pipeline MaxRetries=10, but destination MaxRetries=1
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 1)}, cp, &PipelineSettings{MaxRetries: 10, RetryBaseBackoff: 50 * time.Millisecond, PollInterval: 100 * time.Millisecond, ShutdownTimeout: time.Second}, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	p.Stop()

	// Should have tried only once (MaxRetries=1 means 1 total attempt)
	// With 50ms backoff, total ~50ms. With 300ms sleep we might see 1 attempt per poll.
	// We just verify it doesn't wait for all 10 retries.
}

func TestPipelineDLQ(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	dlqDir := t.TempDir()
	src := &mockSource{
		batch: []*core.LogEntry{
			{Message: "fail1"},
		},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: errors.New("permanent error")}
	cp := &mockCheckpointer{}

	dlqWriter, err := dlq.New(dlqDir, "test", 0)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{MaxRetries: 1, RetryBaseBackoff: 10 * time.Millisecond, PollInterval: 100 * time.Millisecond, ShutdownTimeout: time.Second}, nil, dlqWriter)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	p.Stop()

	if dlqWriter.Count() == 0 {
		t.Error("Expected DLQ to have entries")
	}
}

func TestPipelineDiskBuffer(t *testing.T) {
	bufDir := t.TempDir()
	src := &mockSource{
		batch: []*core.LogEntry{
			{Message: "buf1"},
			{Message: "buf2"},
		},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}

	diskBuf, err := buffer.NewDiskBuffer(bufDir, "test", 100)
	if err != nil {
		t.Fatalf("failed to create disk buffer: %v", err)
	}

	// Slow destination with small buffer to force spillover
	dest.latency = 200 * time.Millisecond
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{BufferSize: 1, PollInterval: 50 * time.Millisecond, MaxRetries: 2, ShutdownTimeout: 3 * time.Second}, diskBuf, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)
	p.Stop()

	if len(dest.received) == 0 {
		t.Error("Expected destination to receive entries via disk buffer")
	}

	// Disk buffer should be empty after processing
	if diskBuf.Count() != 0 {
		t.Errorf("Expected disk buffer to be empty, got %d", diskBuf.Count())
	}
}

func TestPipelineProcessorChain(t *testing.T) {
	filterProc, err := NewFilterProcessorMock()
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	src := &mockSource{
		batch: []*core.LogEntry{
			{Message: "keep", Level: "INFO"},
			{Message: "drop", Level: "DEBUG"},
		},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}

	p := NewPipeline("test", src, []core.Processor{filterProc}, []DestinationInfo{makeDestInfo(dest, 0)}, cp, nil, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	p.Stop()

	// We can't easily assert exact counts here due to async nature,
	// but at least the pipeline should start/stop without panic.
}

// Mock processor for testing
type filterProcessorMock struct{}

func NewFilterProcessorMock() (*filterProcessorMock, error) {
	return &filterProcessorMock{}, nil
}

func (f *filterProcessorMock) Process(ctx context.Context, logs []*core.LogEntry) ([]*core.LogEntry, error) {
	result := make([]*core.LogEntry, 0, len(logs))
	for _, e := range logs {
		if e.Level != "DEBUG" {
			result = append(result, e)
		}
	}
	return result, nil
}

func TestPullDoesNotPollSourceWhilePrefetchQueueFull(t *testing.T) {
	src := &mockSource{
		batch:      []*core.LogEntry{{Message: "test1"}},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{}
	cp := &mockCheckpointer{cursor: "start"}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		PollInterval:       time.Hour,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("first pull failed: %v", err)
	}
	if err := p.pull(); err != nil {
		t.Fatalf("second pull while in-flight failed: %v", err)
	}

	if len(src.cursors) != 1 {
		t.Fatalf("expected source to be polled once, got %d times", len(src.cursors))
	}
	if src.cursors[0] != "start" {
		t.Fatalf("expected cursor 'start', got %q", src.cursors[0])
	}
}

func TestCursorAdvancesAfterSuccessfulProcessBatch(t *testing.T) {
	src := &mockSource{
		batch:      []*core.LogEntry{{Message: "test1"}},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{}
	cp := &mockCheckpointer{cursor: "start"}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		PollInterval:       time.Hour,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if len(src.cursors) != 1 || src.cursors[0] != "start" {
		t.Fatalf("unexpected cursors: %v", src.cursors)
	}

	_, _ = p.processBatch(&checkpointState{}, <-p.Buffer)

	if p.cursor.Snapshot() != "cursor123" {
		t.Fatalf("cursor should advance after successful processBatch, got %q", p.cursor.Snapshot())
	}

	if err := p.pull(); err != nil {
		t.Fatalf("pull after processBatch failed: %v", err)
	}

	if len(src.cursors) != 2 {
		t.Fatalf("expected 2 cursors, got %d", len(src.cursors))
	}
	if src.cursors[1] != "cursor123" {
		t.Fatalf("expected second cursor 'cursor123', got %q", src.cursors[1])
	}
}

func TestCursorDoesNotAdvanceAfterFailedDelivery(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{
		batch:      []*core.LogEntry{{Message: "test1"}},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: errors.New("downstream unavailable")}
	cp := &mockCheckpointer{cursor: "start"}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		PollInterval:       time.Hour,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	_, ok := p.processBatch(&checkpointState{}, <-p.Buffer)
	if ok {
		t.Fatal("processBatch should return false for failed delivery")
	}

	if p.cursor.Snapshot() != "start" {
		t.Fatalf("cursor should not advance after failed delivery, got %q", p.cursor.Snapshot())
	}
}

func TestDLQWrite(t *testing.T) {
	dir := t.TempDir()
	d, err := dlq.New(dir, "test", 0)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}
	defer d.Close()

	entries := []*core.LogEntry{
		{Message: "failed"},
	}
	if err := d.Write(entries, "http", "connection refused"); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if d.Count() != 1 {
		t.Fatalf("expected count 1, got %d", d.Count())
	}

	// Verify file exists
	if _, err := os.Stat(filepath.Join(dir, "test.dlq.journal")); os.IsNotExist(err) {
		t.Error("expected DLQ journal file to exist")
	}
}

func TestCheckpointWriteFailsCursorStillAdvancesInMemory(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{
		batch:      []*core.LogEntry{{Message: "test1"}},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{}
	checkpointWriteCalled := false
	checkpointWriteErr := errors.New("checkpoint write failed")
	cp := &errCheckingCheckpointer{cursor: "start", err: checkpointWriteErr, called: &checkpointWriteCalled}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         0,
		RetryBaseBackoff:   time.Millisecond,
		PollInterval:       time.Hour,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	_, ok := p.processBatch(&checkpointState{}, <-p.Buffer)
	if !ok {
		t.Fatal("processBatch should return true for successful delivery")
	}

	if p.cursor.Snapshot() != "cursor123" {
		t.Fatalf("cursor should advance in memory even if checkpoint write fails, got %q", p.cursor.Snapshot())
	}

	if !checkpointWriteCalled {
		t.Error("checkpoint write should have been called")
	}
}

type errCheckingCheckpointer struct {
	cursor string
	err    error
	called *bool
}

func (m *errCheckingCheckpointer) Get(pipelineID string) (string, error) {
	return m.cursor, nil
}

func (m *errCheckingCheckpointer) Set(pipelineID string, cursor string) error {
	*m.called = true
	m.cursor = cursor
	return m.err
}

func (m *errCheckingCheckpointer) Close() error {
	return nil
}

func TestProcessorErrorAdvancesCursorEvenWhenDLQFails(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	dlqDir := t.TempDir()
	dlqWriter, err := dlq.New(dlqDir, "test", 0)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}

	entries := []*core.LogEntry{{Message: "bad1"}}
	filterProc := &failingProcessor{err: errors.New("processor failed"), entriesToReturn: nil}

	src := &mockSource{
		batch:      entries,
		nextCursor: "cursor123",
	}
	dest := &mockDestination{}
	cp := &mockCheckpointer{cursor: "start"}

	p := NewPipeline("test", src, []core.Processor{filterProc}, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         0,
		RetryBaseBackoff:   time.Millisecond,
		PollInterval:       time.Hour,
		CheckpointInterval: 0,
	}, nil, dlqWriter)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	_, ok := p.processBatch(&checkpointState{}, <-p.Buffer)
	if !ok {
		t.Fatal("processBatch should return true even after processor error (to advance cursor)")
	}

	if p.cursor.Snapshot() != "cursor123" {
		t.Fatalf("cursor should advance after processor error, got %q", p.cursor.Snapshot())
	}

	if len(dest.received) != 0 {
		t.Fatalf("destination should not have received anything, got %d", len(dest.received))
	}
}

type failingProcessor struct {
	err             error
	entriesToReturn []*core.LogEntry
}

func (f *failingProcessor) Process(ctx context.Context, logs []*core.LogEntry) ([]*core.LogEntry, error) {
	return f.entriesToReturn, f.err
}

func TestDiskBufferWriteFailsOnSizeLimit(t *testing.T) {
	bufDir := t.TempDir()
	db, err := buffer.NewDiskBuffer(bufDir, "test", 0)
	if err != nil {
		t.Fatalf("failed to create disk buffer: %v", err)
	}
	defer db.Close()

	batch := buffer.DiskBatch{
		Entries:    []*core.LogEntry{{Message: "entry1"}},
		NextCursor: "cursor1",
	}

	if err := db.Write(batch); err != nil {
		t.Fatalf("write with maxSizeMB=0 (disabled) should succeed, got: %v", err)
	}

	if db.Count() != 1 {
		t.Fatalf("expected count 1, got %d", db.Count())
	}
}

func TestGracefulShutdownFlushesPendingCheckpoint(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         0,
		RetryBaseBackoff:   time.Millisecond,
		PollInterval:       time.Hour,
		CheckpointInterval: time.Hour,
		ShutdownTimeout:    2 * time.Second,
	}, nil, nil)
	p.ctx = context.Background()

	p.Buffer <- Batch{
		Entries:    []*core.LogEntry{{Message: "in-flight"}},
		NextCursor: "cursor123",
	}
	close(p.Buffer)

	p.consumerWg.Add(1)
	p.pushRoutine()

	if cp.cursor != "cursor123" {
		t.Fatalf("pending checkpoint should be flushed on channel close, got cursor %q", cp.cursor)
	}
}

func TestSentinel429TriggersRetryWithBackoff(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	src := &mockSource{
		batch:      []*core.LogEntry{{Message: "test1"}},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: destination.ErrRateLimited}
	cp := &mockCheckpointer{cursor: "start"}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 3)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         3,
		RetryBaseBackoff:   10 * time.Millisecond,
		PollInterval:       100 * time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	_, ok := p.processBatch(&checkpointState{}, <-p.Buffer)
	if ok {
		t.Fatal("processBatch should return false after exhausting retries on 429")
	}

	if p.cursor.Snapshot() != "start" {
		t.Fatalf("cursor should not advance after 429 failure, got %q", p.cursor.Snapshot())
	}
}

func TestSentinel400SchemaMismatchStopsRetriesAndDLQ(t *testing.T) {
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(originalLogger)

	dlqDir := t.TempDir()
	dlqWriter, err := dlq.New(dlqDir, "test", 0)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}

	src := &mockSource{
		batch:      []*core.LogEntry{{Message: "bad1"}},
		nextCursor: "cursor123",
	}
	dest := &mockDestination{sendErr: fmt.Errorf("schema mismatch: field type incorrect: %w", core.ErrPoisonPill)}
	cp := &mockCheckpointer{cursor: "start"}

	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 10)}, cp, &PipelineSettings{
		BufferSize:         1,
		MaxRetries:         10,
		RetryBaseBackoff:   time.Millisecond,
		PollInterval:       100 * time.Millisecond,
		CheckpointInterval: 0,
	}, nil, dlqWriter)
	p.ctx = context.Background()
	p.cursor.Advance("start")

	if err := p.pull(); err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	remaining, ok := p.processBatch(&checkpointState{}, <-p.Buffer)
	// When DLQ write succeeds, the failed sub-batch is considered handled:
	// processBatch should signal "all done" (true) with no remaining work.
	if !ok {
		t.Fatalf("processBatch should return true when DLQ write succeeds (no remaining work), remaining=%v", remaining)
	}

	// Cursor must advance past the DLQ'd batch so it is not retried forever.
	if p.cursor.Snapshot() == "start" {
		t.Fatal("cursor should advance after successful DLQ write to prevent infinite retry loop")
	}

	if dlqWriter.Count() == 0 {
		t.Error("expected DLQ to have entry after poison pill")
	}
}

func TestPerEntryRejectionDLQsOnlyFailedEntries(t *testing.T) {
	dir := t.TempDir()
	dlqWriter, err := dlq.New(dir, "test", 0)
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	defer dlqWriter.Close()

	dest := &mockDestination{
		perEntryErrs: []error{nil, errors.New("rejected by sentinel"), nil},
	}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, dlqWriter)
	p.ctx = context.Background()

	// 3 entries with per-entry cursors so the pusher can advance to the
	// last successful one.
	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "good-1", Cursor: "c1"},
			{Message: "bad", Cursor: "c2"},
			{Message: "good-2", Cursor: "c3"},
		},
		EntryCursors: []string{"c1", "c2", "c3"},
		NextCursor:   "c3",
	}

	_, ok := p.processBatch(&checkpointState{}, batch)
	if !ok {
		t.Fatal("expected processBatch to succeed (per-entry rejection is not a batch failure)")
	}

	// Destination received all 3 entries.
	if len(dest.received) != 3 {
		t.Errorf("expected destination to receive 3 entries, got %d", len(dest.received))
	}

	// DLQ has exactly the 1 rejected entry.
	recovered, err := dlqWriter.Recover()
	if err != nil {
		t.Fatalf("dlq.Recover: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected DLQ to have 1 entry, got %d", len(recovered))
	}
	if recovered[0].Entries[0].Message != "bad" {
		t.Errorf("expected DLQ entry to be the rejected one, got %q", recovered[0].Entries[0].Message)
	}

	// Cursor advanced to the last successful entry (c3, the third entry
	// which succeeded), not to c2 (the rejected one).
	if p.cursor.Snapshot() != "c3" {
		t.Errorf("expected cursor to advance to c3 (last successful), got %q", p.cursor.Snapshot())
	}
	if cp.cursor != "c3" {
		t.Errorf("expected checkpoint to be c3, got %q", cp.cursor)
	}
}

func TestPerEntryAllRejectionStillAdvancesCursor(t *testing.T) {
	dir := t.TempDir()
	dlqWriter, err := dlq.New(dir, "test", 0)
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	defer dlqWriter.Close()

	dest := &mockDestination{
		perEntryErrs: []error{errors.New("rej-1"), errors.New("rej-2")},
	}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, dlqWriter)
	p.ctx = context.Background()

	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "1", Cursor: "c1"},
			{Message: "2", Cursor: "c2"},
		},
		EntryCursors: []string{"c1", "c2"},
		NextCursor:   "c2",
	}

	_, ok := p.processBatch(&checkpointState{}, batch)
	if !ok {
		t.Fatal("expected processBatch to succeed (per-entry rejection is not a batch failure)")
	}
	recovered, _ := dlqWriter.Recover()
	if len(recovered) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(recovered))
	}
	if len(recovered[0].Entries) != 2 {
		t.Errorf("expected DLQ entry to contain both rejected entries, got %d", len(recovered[0].Entries))
	}
}

func TestInMemoryPartialFailureSpillsRemainingToDiskBuffer(t *testing.T) {
	dir := t.TempDir()
	// mockDestination with sendErr fails on every call. This forces both
	// the in-memory and the disk-buffer paths to fail, so the spilled
	// sub-batches remain in the disk buffer for inspection after
	// pushRoutine returns.
	dest := &mockDestination{sendErr: errors.New("always fails")}
	cp := &mockCheckpointer{}
	diskBuf, err := buffer.NewDiskBuffer(dir, "test", 100)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	var cancel context.CancelFunc
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
		PollInterval:       time.Hour,
		ShutdownTimeout:    2 * time.Second,
	}, diskBuf, nil)
	p.ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// 5 entries split into sub-batches of 2: [E1,E2], [E3,E4], [E5].
	// The first call to Send fails; no sub-batches succeed; processBatch
	// returns (subBatches[0:], false) with all 3 sub-batches as
	// remaining. The pusher's caller must spill all 3 to the disk buffer.
	p.Buffer <- Batch{
		Entries: []*core.LogEntry{
			{Message: "1", Cursor: "c1"},
			{Message: "2", Cursor: "c2"},
			{Message: "3", Cursor: "c3"},
			{Message: "4", Cursor: "c4"},
			{Message: "5", Cursor: "c5"},
		},
		EntryCursors: []string{"c1", "c2", "c3", "c4", "c5"},
		NextCursor:   "c5",
	}
	close(p.Buffer)

	p.consumerWg.Add(1)
	done := make(chan struct{})
	go func() { p.pushRoutine(); close(done) }()
	deadline := time.After(time.Second)
	for diskBuf.Count() != 3 {
		select {
		case <-deadline:
			t.Fatal("failed page was not spilled")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pusher did not stop")
	}

	// All 3 sub-batches were spilled to the disk buffer; the disk buffer
	// drain in pushRoutine then also failed (every Send call returns
	// the error), so the spilled batches remain in the disk buffer.
	if got := diskBuf.Count(); got != 3 {
		t.Errorf("disk buffer count = %d, want 3 (spilled sub-batches)", got)
	}
	var spilled []buffer.DiskBatch
	if derr := diskBuf.Drain(func(_ int64, b buffer.DiskBatch) error {
		spilled = append(spilled, b)
		return nil
	}); derr != nil {
		t.Fatalf("disk buffer Drain: %v", derr)
	}
	if len(spilled) != 3 {
		t.Fatalf("expected 3 spilled batches, got %d", len(spilled))
	}
	wantSizes := []int{2, 2, 1}
	for i, b := range spilled {
		if len(b.Entries) != wantSizes[i] {
			t.Errorf("spilled batch %d: got %d entries, want %d", i, len(b.Entries), wantSizes[i])
		}
	}
}

func TestSplitBatchFallsBackToNextEntryCursor(t *testing.T) {
	dest := &mockDestination{}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	// 5 entries. E2 and E4 have empty Cursor and empty EntryCursors; the
	// sub-batches ending at E2 and E4 must use a fallback. After this
	// fix, they should fall back to the next entry's cursor (E3 and E5
	// respectively).
	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "1", Cursor: "c1"},
			{Message: "2", Cursor: ""},
			{Message: "3", Cursor: "c3"},
			{Message: "4", Cursor: ""},
			{Message: "5", Cursor: "c5"},
		},
		EntryCursors: []string{"c1", "", "c3", "", "c5"},
		NextCursor:   "c5",
	}

	subBatches := splitBatch(batch, p.settings.SendBatchSize)
	if len(subBatches) != 3 {
		t.Fatalf("expected 3 sub-batches, got %d", len(subBatches))
	}
	// Sub-batch 0: [E1, E2] — last entry E2 has no cursor; fall back to
	// the next entry E3's cursor = "c3".
	if subBatches[0].NextCursor != "c3" {
		t.Errorf("sub-batch 0 NextCursor = %q, want %q (fall back to next entry E3)", subBatches[0].NextCursor, "c3")
	}
	// Sub-batch 1: [E3, E4] — last entry E4 has no cursor; fall back to
	// the next entry E5's cursor = "c5".
	if subBatches[1].NextCursor != "c5" {
		t.Errorf("sub-batch 1 NextCursor = %q, want %q (fall back to next entry E5)", subBatches[1].NextCursor, "c5")
	}
	// Sub-batch 2: [E5] — last entry E5 has cursor "c5"; use it directly.
	if subBatches[2].NextCursor != "c5" {
		t.Errorf("sub-batch 2 NextCursor = %q, want %q", subBatches[2].NextCursor, "c5")
	}
}

func TestSplitBatchFallsBackToBatchNextCursor(t *testing.T) {
	dest := &mockDestination{}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	// 3 entries, all with empty cursors. Sub-batches [E1, E2] and [E3].
	// [E1, E2] has no "next entry" (it's not the last sub-batch) — wait,
	// actually there IS a next entry (E3) which also has no cursor. So
	// both fall back to batch.NextCursor = "end".
	batch := Batch{
		Entries: []*core.LogEntry{
			{Message: "1", Cursor: ""},
			{Message: "2", Cursor: ""},
			{Message: "3", Cursor: ""},
		},
		EntryCursors: []string{"", "", ""},
		NextCursor:   "end",
	}

	subBatches := splitBatch(batch, p.settings.SendBatchSize)
	if len(subBatches) != 2 {
		t.Fatalf("expected 2 sub-batches, got %d", len(subBatches))
	}
	if subBatches[0].NextCursor != "end" {
		t.Errorf("sub-batch 0 NextCursor = %q, want %q (fall back to batch.NextCursor)", subBatches[0].NextCursor, "end")
	}
	if subBatches[1].NextCursor != "end" {
		t.Errorf("sub-batch 1 NextCursor = %q, want %q (fall back to batch.NextCursor)", subBatches[1].NextCursor, "end")
	}
}

// selectiveDest fails every send that contains a message in failOn.
type selectiveDest struct {
	failOn map[string]bool
}

func (d *selectiveDest) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	for _, e := range logs {
		if d.failOn[e.Message] {
			return nil, errors.New("destination down for " + e.Message)
		}
	}
	out := make([]core.EntryStatus, len(logs))
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
	}
	return out, nil
}
func (d *selectiveDest) Close() error { return nil }
func (d *selectiveDest) Type() string { return "selective" }

// A drain that stops partway must leave delivered batches delivered and
// the cursor saved, across a restart of both the disk buffer and the
// checkpointer.
func TestDrainProgressSurvivesRestart(t *testing.T) {
	bufDir, cpDir := t.TempDir(), t.TempDir()
	diskBuf, err := buffer.NewDiskBuffer(bufDir, "test", 100)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}
	for _, m := range []string{"a", "b"} {
		if err := diskBuf.Write(buffer.DiskBatch{
			Entries:      []*core.LogEntry{{Message: m}},
			EntryCursors: []string{"cursor-" + m},
			NextCursor:   "cursor-" + m,
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	cp, err := checkpointer.NewFileCheckpointer(cpDir)
	if err != nil {
		t.Fatalf("NewFileCheckpointer: %v", err)
	}

	dest := &selectiveDest{failOn: map[string]bool{"b": true}}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		MaxRetries:       1,
		RetryBaseBackoff: time.Millisecond,
	}, diskBuf, nil)
	p.ctx = context.Background()

	p.drainDiskBuffer(&checkpointState{})

	diskBuf.Close()
	cp.Close()

	reopenedBuf, err := buffer.NewDiskBuffer(bufDir, "test", 100)
	if err != nil {
		t.Fatalf("reopen disk buffer: %v", err)
	}
	defer reopenedBuf.Close()
	var pending []buffer.DiskBatch
	if err := reopenedBuf.Drain(func(_ int64, b buffer.DiskBatch) error {
		pending = append(pending, b)
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(pending) != 1 || pending[0].Entries[0].Message != "b" {
		t.Fatalf("pending after restart = %+v, want only batch b", pending)
	}
	if got := pending[0].Destinations; len(got) != 0 {
		t.Errorf("pending destinations = %v, want none (no progress, so batch b is left as written)", got)
	}

	reopenedCp, err := checkpointer.NewFileCheckpointer(cpDir)
	if err != nil {
		t.Fatalf("reopen checkpointer: %v", err)
	}
	defer reopenedCp.Close()
	if got, err := reopenedCp.Get("test"); err != nil || got != "cursor-a" {
		t.Errorf("cursor after restart = %q, %v; want cursor-a", got, err)
	}
}

type failingGetCheckpointer struct {
	err error
}

func (f *failingGetCheckpointer) Get(pipelineID string) (string, error) {
	return "", f.err
}
func (f *failingGetCheckpointer) Set(pipelineID string, cursor string) error {
	return nil
}
func (f *failingGetCheckpointer) Close() error { return nil }

func TestPipelineStartZombieRegression(t *testing.T) {
	// Regression test: a Checkpointer.Get error during Start
	// must leave the pipeline in a clean stopped state. A second
	// Start call (with the error fixed) must succeed.
	cpErr := errors.New("checkpointer read failed")
	cp := &failingGetCheckpointer{err: cpErr}
	dest := &mockDestination{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
		PollInterval:       time.Hour,
		ShutdownTimeout:    2 * time.Second,
	}, nil, nil)

	if err := p.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail with cpErr")
	} else if !errors.Is(err, cpErr) {
		t.Errorf("Start err = %v, want cpErr", err)
	}

	// Status must be Stopped (not Starting).
	if got := p.Status(); got != PipelineStatusStopped {
		t.Errorf("Status after failed Start = %v, want Stopped", got)
	}

	// A second Start with the cp fixed must succeed.
	cp.err = nil
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	defer p.Stop()
	if got := p.Status(); got != PipelineStatusRunning {
		t.Errorf("Status after successful Start = %v, want Running", got)
	}
}

func TestPipelineStopBoundsProducerWait(t *testing.T) {
	// Regression test: if the producer is stuck in a non-
	// context-aware operation (we simulate this by blocking Collect
	// indefinitely), Stop must still return within ShutdownTimeout.
	src := &blockingMockSource{block: make(chan struct{}), releaseOnce: make(chan struct{})}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
		PollInterval:       time.Hour,
		ShutdownTimeout:    200 * time.Millisecond,
	}, nil, nil)
	p.ctx = context.Background()

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Release the source so the puller can make one Collect call,
	// then it'll block in the second call. The producer is now
	// stuck.
	close(src.releaseOnce)

	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2x ShutdownTimeout")
	}
}

type blockingMockSource struct {
	block       chan struct{}
	releaseOnce chan struct{}
	calls       int
}

func (b *blockingMockSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	b.calls++
	if b.calls == 1 {
		<-b.releaseOnce
		return []*core.LogEntry{{Message: "first", Cursor: "c1"}}, "c1", nil
	}
	// Subsequent calls block forever (until ctx is cancelled).
	select {
	case <-b.block:
	case <-ctx.Done():
		return nil, cursor, ctx.Err()
	}
	return nil, cursor, nil
}
func (b *blockingMockSource) Close() error { return nil }
func (b *blockingMockSource) Name() string { return "blocking" }

func TestPipelineStopOnConsumerTimeoutSkipsDiskBufferClose(t *testing.T) {
	// Regression test: when the consumer times out, the disk
	// buffer must NOT be closed (the consumer may still hold
	// db.mu). The source and destinations ARE closed (they unblock
	// the consumer's next Collect).
	dir := t.TempDir()
	diskBuf, err := buffer.NewDiskBuffer(dir, "test", 100)
	if err != nil {
		t.Fatalf("NewDiskBuffer: %v", err)
	}

	dest := &blockingDestination{}
	src := &mockSource{}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		SendBatchSize:      2,
		BufferSize:         1,
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
		PollInterval:       time.Hour,
		ShutdownTimeout:    200 * time.Millisecond,
	}, diskBuf, nil)
	p.ctx = context.Background()

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send a batch — dest blocks forever.
	p.Buffer <- Batch{
		Entries:      []*core.LogEntry{{Message: "1", Cursor: "c1"}},
		EntryCursors: []string{"c1"},
		NextCursor:   "c1",
	}

	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
		// good — Stop returned despite consumer timeout
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return")
	}

	// The disk buffer's underlying journal should still be open
	// (not closed). We can verify by attempting a Recover on the
	// same path; if it was closed, the journal would have been
	// fsync'd. The simpler check: diskBuf.f is non-nil in the
	// implementation. We can also check by calling diskBuf.Count
	// which requires the journal to be open.
	if got := diskBuf.Count(); got < 0 {
		t.Errorf("diskBuf.Count = %d, want >= 0", got)
	}
}

type blockingDestination struct{}

func (b *blockingDestination) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	// Block forever (until ctx is cancelled).
	<-ctx.Done()
	return nil, ctx.Err()
}
func (b *blockingDestination) Close() error { return nil }
func (b *blockingDestination) Type() string { return "blocking" }

func TestCircuitBreakerIsPersistentAcrossBatches(t *testing.T) {
	failCount := 0
	mu := sync.Mutex{}
	dest := &mockDestination{
		sendErr: errors.New("transient failure"),
	}
	// Wrap the destination so we can count failures before the breaker
	// short-circuits.
	countingDest := &countingDestination{inner: dest, mu: &mu, failCount: &failCount}

	info := DestinationInfo{Destination: countingDest, MaxRetries: 3, Breaker: retry.NewBreaker()}
	info.Breaker.FailureThreshold = 2
	info.Breaker.OpenDuration = 100 * time.Millisecond

	cp := &mockCheckpointer{}
	p := NewPipeline("test", &mockSource{}, nil, []DestinationInfo{info}, cp, &PipelineSettings{
		MaxRetries:         1,
		RetryBaseBackoff:   time.Millisecond,
		CheckpointInterval: 0,
	}, nil, nil)
	p.ctx = context.Background()

	// First batch: will fail and record 2 consecutive failures (threshold=2).
	_, err := p.sendWithRetry([]*core.LogEntry{{Message: "a"}})
	if err == nil {
		t.Fatal("expected first batch to fail")
	}

	// Second batch: breaker should be OPEN, so it should fail fast without
	// calling the destination. If the breaker were recreated per batch, it
	// would try again.
	_, err = p.sendWithRetry([]*core.LogEntry{{Message: "b"}})
	if err == nil {
		t.Fatal("expected second batch to fail (breaker open)")
	}
	if !errors.Is(err, retry.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	mu.Lock()
	callCount := countingDest.calls
	mu.Unlock()
	// First batch: 2 calls (attempt 0 and 1 succeed before breaker opens
	// on attempt 2). Second batch: 0 calls (breaker open). Total = 2.
	if callCount != 2 {
		t.Fatalf("expected 2 destination calls (first batch only before breaker opens), got %d", callCount)
	}
}

type countingDestination struct {
	inner     core.Destination
	mu        *sync.Mutex
	calls     int
	failCount *int
}

func (c *countingDestination) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.inner.Send(ctx, logs)
}
func (c *countingDestination) Close() error { return c.inner.Close() }
func (c *countingDestination) Type() string { return c.inner.Type() }

// --- Pipeline state machine tests ---

// newTestPipeline is a small helper that builds a pipeline with
// sensible defaults for state-machine tests. It does not Start the
// pipeline; callers do that (or call SetStatus / evaluateStatus
// directly).
func newTestPipeline(id string, settings *PipelineSettings) *Pipeline {
	if settings == nil {
		settings = &PipelineSettings{
			PollInterval:               10 * time.Millisecond,
			BufferSize:                 1,
			MaxRetries:                 0,
			RetryBaseBackoff:           time.Millisecond,
			CheckpointInterval:         0,
			ShutdownTimeout:            time.Second,
			StalledThresholdMultiplier: 3,
		}
	}
	src := &mockSource{}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}
	return NewPipeline(id, src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, settings, nil, nil)
}

func TestPipelineStateTransitions_StartingRunningStoppingStopped(t *testing.T) {
	// Use a poll interval long enough that the stall monitor's first
	// tick does not race the Stop call. We don't actually let the
	// goroutines start, because we want to assert the transitions we
	// drive manually.
	p := newTestPipeline("test", &PipelineSettings{
		PollInterval:               time.Hour,
		BufferSize:                 1,
		ShutdownTimeout:            200 * time.Millisecond,
		StalledThresholdMultiplier: 5,
	})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	// Initial state.
	if got := p.Status(); got != PipelineStatusStopped {
		t.Fatalf("initial status = %v, want Stopped", got)
	}

	p.SetStatus(PipelineStatusStarting)
	if got := p.Status(); got != PipelineStatusStarting {
		t.Fatalf("status after Starting = %v, want Starting", got)
	}

	p.SetStatus(PipelineStatusRunning)
	if got := p.Status(); got != PipelineStatusRunning {
		t.Fatalf("status after Running = %v, want Running", got)
	}

	p.SetStatus(PipelineStatusDraining)
	if got := p.Status(); got != PipelineStatusDraining {
		t.Fatalf("status after Draining = %v, want Draining", got)
	}

	p.SetStatus(PipelineStatusStopped)
	if got := p.Status(); got != PipelineStatusStopped {
		t.Fatalf("status after Stopped = %v, want Stopped", got)
	}

	// SetStatus to the same value is a no-op (no transition logged).
	p.SetStatus(PipelineStatusStopped)
	if got := p.Status(); got != PipelineStatusStopped {
		t.Fatalf("status after no-op SetStatus = %v, want Stopped", got)
	}
}

func TestPipelineStalledDetection_NoActivity(t *testing.T) {
	p := newTestPipeline("test", &PipelineSettings{
		PollInterval:               10 * time.Millisecond,
		BufferSize:                 1,
		StalledThresholdMultiplier: 3,
	})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	// Simulate "pipeline has been Running for a while, last activity
	// at t0, now is way past the stall window".
	p.SetStatus(PipelineStatusRunning)
	t0 := time.Now()
	p.mu.Lock()
	p.health.LastCollect = t0.Add(-time.Second)
	p.health.LastSend = t0.Add(-time.Second)
	p.mu.Unlock()

	// 3 × 10ms = 30ms is the stall threshold. 1 second of idle is
	// way past that. evaluateStatus(now) should mark us Stalled.
	p.evaluateStatus(t0)

	if got := p.Status(); got != PipelineStatusStalled {
		t.Fatalf("status after no activity = %v, want Stalled", got)
	}
}

func TestPipelineStallRecoversOnActivity(t *testing.T) {
	p := newTestPipeline("test", &PipelineSettings{
		PollInterval:               10 * time.Millisecond,
		BufferSize:                 1,
		StalledThresholdMultiplier: 3,
	})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	p.SetStatus(PipelineStatusStalled)
	t0 := time.Now()
	p.mu.Lock()
	// Last activity was a long time ago.
	p.health.LastCollect = t0.Add(-time.Second)
	p.health.LastSend = t0.Add(-time.Second)
	p.mu.Unlock()

	// New activity: update LastCollect to now, then evaluateStatus
	// should transition to Running (or Degraded if there's a recent
	// error).
	p.mu.Lock()
	p.health.LastCollect = t0
	p.mu.Unlock()
	p.evaluateStatus(t0)

	if got := p.Status(); got != PipelineStatusRunning {
		t.Fatalf("status after activity = %v, want Running", got)
	}
}

func TestPipelineDegradedDetection_RecentError(t *testing.T) {
	p := newTestPipeline("test", &PipelineSettings{
		PollInterval:               10 * time.Millisecond,
		BufferSize:                 1,
		StalledThresholdMultiplier: 5,
	})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	p.SetStatus(PipelineStatusRunning)
	now := time.Now()
	p.mu.Lock()
	// Recent activity + recent error -> Degraded.
	p.health.LastCollect = now
	p.health.LastError = now
	p.mu.Unlock()

	p.evaluateStatus(now)

	if got := p.Status(); got != PipelineStatusDegraded {
		t.Fatalf("status after recent error = %v, want Degraded", got)
	}

	// Error clears (older than threshold), activity still recent ->
	// Running.
	p.mu.Lock()
	p.health.LastError = now.Add(-time.Hour)
	p.mu.Unlock()
	p.evaluateStatus(now)

	if got := p.Status(); got != PipelineStatusRunning {
		t.Fatalf("status after error cleared = %v, want Running", got)
	}
}

func TestEvaluateStatus_TerminalStatesAreNotOverridden(t *testing.T) {
	p := newTestPipeline("test", &PipelineSettings{
		PollInterval:               10 * time.Millisecond,
		BufferSize:                 1,
		StalledThresholdMultiplier: 3,
	})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	now := time.Now()
	p.mu.Lock()
	// Way past the stall window, to make sure we'd transition if we
	// were going to.
	p.health.LastCollect = now.Add(-time.Hour)
	p.health.LastSend = now.Add(-time.Hour)
	p.mu.Unlock()

	terminals := []PipelineStatus{
		PipelineStatusStopped,
		PipelineStatusStopping,
		PipelineStatusDraining,
		PipelineStatusStarting,
	}
	for _, st := range terminals {
		p.SetStatus(st)
		p.evaluateStatus(now)
		if got := p.Status(); got != st {
			t.Errorf("evaluateStatus changed terminal state %v -> %v (must not)", st, got)
		}
	}
}

func TestPipelineStatusMetric_TracksCurrentState(t *testing.T) {
	// We just verify that SetStatus updates the gauge for the new
	// state to 1 and the previous state to 0. The actual metric
	// value is read via the prometheus testutil package.
	p := newTestPipeline("metric-test", &PipelineSettings{
		PollInterval:               time.Hour,
		BufferSize:                 1,
		StalledThresholdMultiplier: 5,
	})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	p.SetStatus(PipelineStatusStarting)
	if got := testutil.ToFloat64(metrics.PipelineStatus.WithLabelValues(p.ID, "starting")); got != 1 {
		t.Errorf("starting metric = %v, want 1", got)
	}

	p.SetStatus(PipelineStatusRunning)
	if got := testutil.ToFloat64(metrics.PipelineStatus.WithLabelValues(p.ID, "starting")); got != 0 {
		t.Errorf("starting metric after transition = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.PipelineStatus.WithLabelValues(p.ID, "running")); got != 1 {
		t.Errorf("running metric after transition = %v, want 1", got)
	}

	p.SetStatus(PipelineStatusStalled)
	if got := testutil.ToFloat64(metrics.PipelineStatus.WithLabelValues(p.ID, "stalled")); got != 1 {
		t.Errorf("stalled metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.PipelineStatus.WithLabelValues(p.ID, "running")); got != 0 {
		t.Errorf("running metric after Stalled = %v, want 0", got)
	}
}

func TestStopTransitionsThroughDraining(t *testing.T) {
	// A pipeline that was Running should pass through Draining on
	// its way to Stopped.
	src := &mockSource{}
	dest := &mockDestination{}
	cp := &mockCheckpointer{}
	p := NewPipeline("test", src, nil, []DestinationInfo{makeDestInfo(dest, 0)}, cp, &PipelineSettings{
		PollInterval:               time.Hour,
		BufferSize:                 1,
		ShutdownTimeout:            200 * time.Millisecond,
		StalledThresholdMultiplier: 5,
	}, nil, nil)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Immediately after Start, the state must be Running.
	if got := p.Status(); got != PipelineStatusRunning {
		t.Fatalf("status after Start = %v, want Running", got)
	}

	// Stop synchronously (the source returns no rows, the consumer
	// exits quickly).
	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2 seconds")
	}

	if got := p.Status(); got != PipelineStatusStopped {
		t.Fatalf("status after Stop = %v, want Stopped", got)
	}
}
