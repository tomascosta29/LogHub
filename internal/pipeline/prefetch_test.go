package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"loghub/internal/buffer"
	"loghub/internal/checkpointer"
	"loghub/internal/core"
)

type pagedSource struct {
	total     int
	delay     time.Duration
	collected atomic.Int32
}

func (s *pagedSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	i := 0
	if cursor != "" {
		var err error
		i, err = strconv.Atoi(cursor)
		if err != nil {
			return nil, "", err
		}
	}
	if i >= s.total {
		return nil, cursor, nil
	}
	if err := delayContext(ctx, s.delay); err != nil {
		return nil, cursor, err
	}
	next := strconv.Itoa(i + 1)
	s.collected.Add(1)
	return []*core.LogEntry{{Message: next, Cursor: next}}, next, nil
}
func (*pagedSource) Name() string { return "paged" }
func (*pagedSource) Close() error { return nil }

type functionDestination struct {
	send func(context.Context, []*core.LogEntry) error
}

func (d *functionDestination) Send(ctx context.Context, entries []*core.LogEntry) ([]core.EntryStatus, error) {
	if err := d.send(ctx, entries); err != nil {
		return nil, err
	}
	statuses := make([]core.EntryStatus, len(entries))
	for i := range statuses {
		statuses[i].Index = i
	}
	return statuses, nil
}
func (*functionDestination) Type() string { return "function" }
func (*functionDestination) Close() error { return nil }

func delayContext(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func awaitCondition(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal(message)
		case <-time.After(time.Millisecond):
		}
	}
}

func prefetchSettings() *PipelineSettings {
	return &PipelineSettings{PollInterval: time.Millisecond, BufferSize: 10,
		MaxRetries: 1, RetryBaseBackoff: time.Millisecond, ShutdownTimeout: 20 * time.Millisecond,
		CircuitBreakerFailureThreshold: 10000}
}

func TestPrefetchOverlapsDeliveryAndBoundsReads(t *testing.T) {
	src := &pagedSource{total: 4}
	gate := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(gate) })
	started := make(chan struct{})
	dest := &functionDestination{send: func(ctx context.Context, entries []*core.LogEntry) error {
		if entries[0].Message == "1" {
			close(started)
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	cp := &mockCheckpointer{}
	p := NewPipeline("overlap", src, nil, []DestinationInfo{{Destination: dest}}, cp, prefetchSettings(), nil, nil)
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	awaitCondition(t, func() bool { return src.collected.Load() == 2 }, "next page was not prefetched during delivery")
	time.Sleep(10 * time.Millisecond)
	if got := src.collected.Load(); got != 2 {
		t.Fatalf("prefetch read %d pages; want 2", got)
	}
	if got := p.cursor.Snapshot(); got != "" {
		t.Fatalf("checkpoint advanced before acknowledgement: %s", got)
	}
	release.Do(func() { close(gate) })
	awaitCondition(t, func() bool { return p.cursor.Snapshot() == "4" }, "prefetched pages were not delivered")
}

func TestPrefetchFailureDoesNotAdvancePastGap(t *testing.T) {
	for _, mode := range []string{"memory", "disk", "disk-write-failure"} {
		t.Run(mode, func(t *testing.T) {
			src := &pagedSource{total: 4}
			var recovered atomic.Bool
			var mu sync.Mutex
			var accepted []string
			var peerAccepted []string
			first := make(chan struct{})
			var once sync.Once
			gate := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(gate) })
			dest := &functionDestination{send: func(ctx context.Context, entries []*core.LogEntry) error {
				id := entries[0].Message
				if id == "1" && !recovered.Load() {
					once.Do(func() { close(first) })
					select {
					case <-gate:
					case <-ctx.Done():
						return ctx.Err()
					}
					return errors.New("temporary outage")
				}
				mu.Lock()
				accepted = append(accepted, id)
				mu.Unlock()
				return nil
			}}
			var db *buffer.DiskBuffer
			peer := &functionDestination{send: func(_ context.Context, entries []*core.LogEntry) error {
				mu.Lock()
				defer mu.Unlock()
				peerAccepted = append(peerAccepted, entries[0].Message)
				return nil
			}}
			if mode != "memory" {
				var err error
				db, err = buffer.NewDiskBuffer(t.TempDir(), "gap", 0)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "disk-write-failure" {
					db.Close()
				}
			}
			p := NewPipeline("gap", src, nil, []DestinationInfo{{Destination: peer}, {Destination: dest}}, &mockCheckpointer{}, prefetchSettings(), db, nil)
			if err := p.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer p.Stop()
			select {
			case <-first:
			case <-time.After(time.Second):
				t.Fatal("first send did not start")
			}
			awaitCondition(t, func() bool { return src.collected.Load() == 2 }, "second page not prefetched")
			release.Do(func() { close(gate) })
			awaitCondition(t, func() bool { _, blocked := p.flight.Snapshot(); return blocked }, "failure did not block later pages")
			time.Sleep(10 * time.Millisecond)
			if got := p.cursor.Snapshot(); got != "" {
				t.Fatalf("cursor crossed failed page: %s", got)
			}
			mu.Lock()
			n := len(accepted)
			mu.Unlock()
			if n != 0 {
				t.Fatal("later page delivered before failed page")
			}
			recovered.Store(true)
			awaitCondition(t, func() bool { return p.cursor.Snapshot() == "4" }, "pipeline did not recover")
			p.Stop()
			mu.Lock()
			defer mu.Unlock()
			if fmt.Sprint(accepted) != "[1 2 3 4]" {
				t.Fatalf("delivery order: %v", accepted)
			}
			if fmt.Sprint(peerAccepted) != "[1 2 3 4]" {
				t.Fatalf("healthy destination received duplicates or reordered pages: %v", peerAccepted)
			}
		})
	}
}

func TestPrefetchShutdownAndRestart(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprint(disk), func(t *testing.T) {
			dir := t.TempDir()
			cp, err := checkpointer.NewFileCheckpointer(dir + "/checkpoints")
			if err != nil {
				t.Fatal(err)
			}
			defer cp.Close()
			var db *buffer.DiskBuffer
			if disk {
				db, err = buffer.NewDiskBuffer(dir, "resume", 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			src := &pagedSource{total: 4}
			dest := &functionDestination{send: func(ctx context.Context, _ []*core.LogEntry) error { <-ctx.Done(); return ctx.Err() }}
			p := NewPipeline("resume", src, nil, []DestinationInfo{{Destination: dest}}, cp, prefetchSettings(), db, nil)
			if err := p.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer p.Stop()
			awaitCondition(t, func() bool {
				return src.collected.Load() == 2 && len(p.Buffer) == 1
			}, "second page was not queued")
			p.Stop()
			if got := p.cursor.Snapshot(); got != "" {
				t.Fatalf("shutdown acknowledged undelivered data: %s", got)
			}
			if err := cp.Close(); err != nil {
				t.Fatal(err)
			}
			cp, err = checkpointer.NewFileCheckpointer(dir + "/checkpoints")
			if err != nil {
				t.Fatal(err)
			}
			defer cp.Close()
			if disk {
				db, err = buffer.NewDiskBuffer(dir, "resume", 0)
				if err != nil {
					t.Fatal(err)
				}
				if db.Count() != 2 {
					t.Fatalf("got %d durable pages, want 2", db.Count())
				}
			}
			var mu sync.Mutex
			var accepted []string
			dest = &functionDestination{send: func(_ context.Context, entries []*core.LogEntry) error {
				mu.Lock()
				defer mu.Unlock()
				accepted = append(accepted, entries[0].Message)
				return nil
			}}
			src = &pagedSource{total: 4}
			p = NewPipeline("resume", src, nil, []DestinationInfo{{Destination: dest}}, cp, prefetchSettings(), db, nil)
			if err := p.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer p.Stop()
			awaitCondition(t, func() bool { return p.cursor.Snapshot() == "4" }, "restart did not deliver all pages")
			p.Stop()
			mu.Lock()
			defer mu.Unlock()
			if fmt.Sprint(accepted) != "[1 2 3 4]" {
				t.Fatalf("restart delivery: %v", accepted)
			}
			wantReads := int32(4)
			if disk {
				wantReads = 2
			}
			if got := src.collected.Load(); got != wantReads {
				t.Fatalf("collected %d pages after restart, want %d", got, wantReads)
			}
		})
	}
}

// BenchmarkPipelineOverlap models equally expensive source and destination I/O.
// It measures the architectural overlap, not a production throughput forecast.
func BenchmarkPipelineOverlap(b *testing.B) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(previous)
	const pages = 30
	for i := 0; i < b.N; i++ {
		src := &pagedSource{total: pages, delay: time.Millisecond}
		done := make(chan struct{})
		var sends int
		dest := &functionDestination{send: func(ctx context.Context, _ []*core.LogEntry) error {
			if err := delayContext(ctx, time.Millisecond); err != nil {
				return err
			}
			sends++
			if sends == pages {
				close(done)
			}
			return nil
		}}
		settings := prefetchSettings()
		settings.PollInterval = 100 * time.Microsecond
		settings.ShutdownTimeout = time.Second
		p := NewPipeline("bench-overlap", src, nil, []DestinationInfo{{Destination: dest}}, &mockCheckpointer{}, settings, nil, nil)
		if err := p.Start(context.Background()); err != nil {
			b.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			p.Stop()
			b.Fatal("benchmark timed out")
		}
		p.Stop()
	}
	b.ReportMetric(float64(pages)*float64(b.N)/b.Elapsed().Seconds(), "pages/s")
}
