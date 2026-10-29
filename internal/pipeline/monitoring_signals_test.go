package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"loghub/internal/core"
	"loghub/internal/metrics"
)

func collectErrors(id string) float64 {
	return testutil.ToFloat64(metrics.CollectErrors.WithLabelValues(id, "mock-source"))
}

// A failed source read is counted in loghub_collect_errors_total, which
// the LogHubCollectionErrors alert watches.
func TestCollectErrorIsCounted(t *testing.T) {
	id := fmt.Sprintf("collect-errors-%d", time.Now().UnixNano())
	src := &mockSource{err: errors.New("database unreachable")}
	p := NewPipeline(id, src, nil, []DestinationInfo{makeDestInfo(&mockDestination{}, 0)}, &mockCheckpointer{}, &PipelineSettings{
		BufferSize:   1,
		PollInterval: time.Second,
	}, nil, nil)
	if got := collectErrors(id); got != 0 {
		t.Fatalf("collect_errors_total before any read = %v, want the series published at 0", got)
	}
	p.ctx = context.Background()

	if err := p.pull(); err == nil {
		t.Fatal("pull returned nil for a failing source")
	}
	if got := collectErrors(id); got != 1 {
		t.Errorf("collect_errors_total = %v, want 1", got)
	}

	// A read cancelled because LogHub is stopping is not a source failure.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.ctx = ctx
	src.err = context.Canceled
	_ = p.pull()
	if got := collectErrors(id); got != 1 {
		t.Errorf("collect_errors_total after a cancelled read = %v, want it unchanged at 1", got)
	}
}

// Entries a destination rejects individually mark the pipeline degraded
// and set LastError, like a batch-level failure.
func TestPerEntryRejectionDegradesPipeline(t *testing.T) {
	for _, withDLQ := range []bool{true, false} {
		dest := &mockDestination{perEntryErrs: []error{nil, errors.New("schema mismatch"), nil}}
		var p *Pipeline
		if withDLQ {
			p, _ = makePipelineWithDests(t, []core.Destination{dest})
		} else {
			p = NewPipeline("test", &mockSource{}, nil, []DestinationInfo{makeDestInfo(dest, 0)}, &mockCheckpointer{}, &PipelineSettings{MaxRetries: 1, RetryBaseBackoff: time.Millisecond}, nil, nil)
			p.ctx = context.Background()
		}
		p.settings.PollInterval = time.Second
		p.SetStatus(PipelineStatusRunning)

		if _, ok := p.processBatch(&checkpointState{}, Batch{
			Entries:      []*core.LogEntry{{Message: "a", Cursor: "c1"}, {Message: "b", Cursor: "c2"}, {Message: "c", Cursor: "c3"}},
			EntryCursors: []string{"c1", "c2", "c3"},
			NextCursor:   "c3",
		}); !ok {
			t.Fatalf("withDLQ=%v: processBatch not handled", withDLQ)
		}

		h := p.Health()
		if h.Status != PipelineStatusDegraded {
			t.Errorf("withDLQ=%v: status = %s, want degraded", withDLQ, h.Status)
		}
		if h.LastError.IsZero() || !strings.Contains(h.ErrorMsg, "rejected 1 of 3 entries") || !strings.Contains(h.ErrorMsg, "schema mismatch") {
			t.Errorf("withDLQ=%v: LastError=%v ErrorMsg=%q, want the rejection recorded", withDLQ, h.LastError, h.ErrorMsg)
		}
	}
}
