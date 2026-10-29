package destination

import (
	"context"
	"errors"
	"testing"
	"time"

	"loghub/internal/core"
)

func dummyEntries(n int) []*core.LogEntry {
	logs := make([]*core.LogEntry, n)
	for i := range logs {
		logs[i] = &core.LogEntry{Message: "m"}
	}
	return logs
}

func TestDummyDropRate(t *testing.T) {
	for _, tt := range []struct {
		rate     float64
		rejected int
	}{{0, 0}, {1, 20}} {
		d, err := NewDummy(DummyConfig{DropRate: tt.rate})
		if err != nil {
			t.Fatal(err)
		}
		statuses, err := d.Send(context.Background(), dummyEntries(20))
		if err != nil {
			t.Fatal(err)
		}
		rejected := 0
		for _, s := range statuses {
			if errors.Is(s.Error, errDummyDrop) {
				rejected++
			}
		}
		if rejected != tt.rejected {
			t.Errorf("drop_rate %v rejected %d of 20, want %d", tt.rate, rejected, tt.rejected)
		}
		if count, _ := d.Stats(); count != int64(20-tt.rejected) {
			t.Errorf("drop_rate %v counted %d received, want %d", tt.rate, count, 20-tt.rejected)
		}
	}
}

func TestDummyRejectsDropRateOutOfRange(t *testing.T) {
	for _, rate := range []float64{-0.1, 1.5} {
		if _, err := NewDummy(DummyConfig{DropRate: rate}); err == nil {
			t.Errorf("NewDummy accepted drop_rate %v", rate)
		}
	}
}

func TestDummyLatencyHonorsContext(t *testing.T) {
	d, err := NewDummy(DummyConfig{Latency: "1h"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := d.Send(ctx, dummyEntries(1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Send took %v after the context ended", elapsed)
	}
}
