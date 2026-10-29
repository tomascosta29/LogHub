package pipeline

import (
	"log/slog"
	"time"

	"loghub/internal/core"
	"loghub/internal/metrics"
)

func (p *Pipeline) pullRoutine() {
	defer p.producerWg.Done()

	ticker := time.NewTicker(p.settings.PollInterval)
	defer ticker.Stop()
	slog.Debug("Pipeline poller started", "pipeline_id", p.ID, "poll_interval", p.settings.PollInterval)

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			slog.Debug("Polling source", "pipeline_id", p.ID)
			if err := p.pull(); err != nil {
				slog.Error("Pipeline pull error", "pipeline_id", p.ID, "error", err)
			}
		}
	}
}

func (p *Pipeline) pull() error {
	start := time.Now()
	// Admit at most one page ahead of the page being delivered. Recovery
	// blocks new reads; the pusher must finish the older page first.
	n, blocked := p.flight.Snapshot()
	if blocked || n >= 2 || (n > 0 && cap(p.Buffer) == 0) {
		return nil
	}
	if cap(p.Buffer) > 0 && len(p.Buffer) == cap(p.Buffer) {
		return nil
	}
	if p.hasPendingDiskBatches() {
		return nil
	}
	if n == 0 {
		// Also picks up cursor progress from a recovered disk backlog.
		p.readCursor = p.cursor.Snapshot()
	}
	cursor := p.readCursor
	slog.Debug("Collecting from source", "pipeline_id", p.ID, "cursor", cursor)

	entries, nextCursor, err := p.Source.Collect(p.ctx, cursor)
	if err != nil {
		p.mu.Lock()
		p.health.LastError = time.Now()
		p.health.ErrorMsg = err.Error()
		p.mu.Unlock()
		p.evaluateStatus(time.Now())
		return err
	}

	metrics.CollectDuration.WithLabelValues(p.ID).Observe(time.Since(start).Seconds())
	p.mu.Lock()
	p.health.LastCollect = time.Now()
	p.mu.Unlock()
	p.evaluateStatus(time.Now())

	if len(entries) == 0 {
		slog.Debug("No entries collected", "pipeline_id", p.ID, "cursor", cursor)
		return nil
	}
	slog.Debug("Collected entries from source", "pipeline_id", p.ID, "entries", len(entries), "next_cursor", nextCursor)

	batch := Batch{
		Entries:      entries,
		EntryCursors: entryCursors(entries),
		NextCursor:   nextCursor,
	}

	metrics.EntriesCollected.WithLabelValues(p.ID, p.Source.Name()).Add(float64(len(entries)))
	metrics.BatchesCollected.WithLabelValues(p.ID).Inc()
	metrics.BatchSize.WithLabelValues(p.ID).Observe(float64(len(entries)))

	// Mark the batch in flight before handing it over: the pusher may
	// finish it (and call markDone) before the send below even returns.
	p.markInFlight()

	select {
	case p.Buffer <- batch:
		p.readCursor = nextCursor
		slog.Debug("Queued batch in memory buffer", "pipeline_id", p.ID, "entries", len(entries), "buffer_depth", len(p.Buffer))
		return nil
	case <-p.ctx.Done():
		p.markDone()
		return p.ctx.Err()
	}
}

func (p *Pipeline) markInFlight() {
	p.flight.Inc()
}

func entryCursors(entries []*core.LogEntry) []string {
	cursors := make([]string, len(entries))
	for i, entry := range entries {
		if entry != nil {
			cursors[i] = entry.Cursor
		}
	}
	return cursors
}
