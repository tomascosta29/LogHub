package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"loghub/internal/buffer"
	"loghub/internal/core"
	"loghub/internal/metrics"
	"loghub/internal/retry"
)

// Reasons reported on the loghub_entries_dropped_total metric.
const (
	dropProcessorRejected   = "processor_rejected"
	dropDestinationRejected = "destination_rejected"
)

type checkpointState struct {
	mu             sync.Mutex
	lastCheckpoint time.Time
	pendingCursor  string
}

// destResult is the outcome of sending one sub-batch to one destination.
type destResult struct {
	dest     *DestinationInfo
	statuses []core.EntryStatus
	err      error
}

func (p *Pipeline) pushRoutine() {
	defer p.consumerWg.Done()
	cpState := &checkpointState{}
	defer p.flushCheckpoint(cpState)
	slog.Debug("Pipeline pusher started", "pipeline_id", p.ID)

	var pending []Batch
	for {
		_, blocked := p.flight.Snapshot()
		if p.shuttingDown() || (blocked && p.ctx.Err() != nil) {
			// Later prefetched pages must remain behind the failed prefix.
			// Stop closes Buffer before cancelling the delivery context.
			for batch := range p.Buffer {
				pending = append(pending, batch)
			}
			if len(pending) > 0 {
				if left := p.spillRemaining(pending); len(left) > 0 {
					slog.Warn("Undelivered pages will be replayed from the source checkpoint", "pipeline_id", p.ID, "pages", len(left), "cursor", p.cursor.Snapshot())
				}
			}
			return
		}
		if p.hasPendingDiskBatches() {
			p.drainDiskBuffer(cpState)
			if p.hasPendingDiskBatches() {
				p.flight.Block()
				p.waitForRetry()
				continue
			}
		}

		var batch Batch
		if len(pending) > 0 {
			batch, pending = pending[0], pending[1:]
			p.markInFlight()
		} else {
			p.flight.Unblock()
			var ok bool
			batch, ok = <-p.Buffer
			if !ok {
				return
			}
		}
		remaining, ok := p.processBatch(cpState, batch)
		if !ok && len(remaining) > 0 {
			pending = append(remaining, pending...)
			pending = p.spillRemaining(pending)
			p.waitForRetry()
		}
	}
}

// spillRemaining appends only a contiguous prefix. A failed write keeps that
// page and its successors in memory; advancing past them would lose data.
// Without a disk buffer, shutdown leaves the acknowledged source cursor intact
// so those pages are read again on restart.
func (p *Pipeline) spillRemaining(pending []Batch) []Batch {
	if p.diskBuffer == nil {
		return pending
	}
	for i, b := range pending {
		if err := p.diskBuffer.Write(buffer.DiskBatch{
			Entries: b.Entries, EntryCursors: b.EntryCursors,
			NextCursor: b.NextCursor, Destinations: b.Destinations,
		}); err != nil {
			p.recordError(err)
			slog.Error("Failed to persist pending page; retaining source checkpoint", "pipeline_id", p.ID, "error", err)
			return pending[i:]
		}
	}
	return nil
}

func (p *Pipeline) waitForRetry() {
	timer := time.NewTimer(p.drainRetryInterval())
	defer timer.Stop()
	select {
	case <-p.pushContext().Done():
	case <-p.ctx.Done():
	case <-timer.C:
	}
}

func (p *Pipeline) hasPendingDiskBatches() bool {
	return p.diskBuffer != nil && p.diskBuffer.Count() > 0
}

// drainRetryInterval is how long the pusher waits before retrying a disk
// buffer drain that stopped early.
func (p *Pipeline) drainRetryInterval() time.Duration {
	if p.settings.PollInterval > 0 {
		return p.settings.PollInterval
	}
	return time.Second
}

// drainDiskBuffer streams pending batches from the disk buffer and
// processes each via processBatch. A delivered batch is marked delivered
// straight away; a partly delivered one is replaced by what is left, and
// the drain stops so a failing destination is not hammered. Either way,
// a later drain (or a restart) only re-delivers what is still pending.
func (p *Pipeline) drainDiskBuffer(cpState *checkpointState) {
	err := p.diskBuffer.Drain(func(id int64, b buffer.DiskBatch) error {
		p.markInFlight()
		remaining, ok := p.processBatch(cpState, Batch{
			Entries:      b.Entries,
			EntryCursors: b.EntryCursors,
			NextCursor:   b.NextCursor,
			Destinations: b.Destinations,
		})
		if ok {
			return p.diskBuffer.MarkDelivered(id)
		}
		// Rewrite the batch only if part of it was delivered; otherwise a
		// long outage would append a copy of it on every retry.
		if p.madeProgress(b, remaining) {
			if err := p.diskBuffer.Replace(id, toDiskBatches(remaining)); err != nil {
				return err
			}
		}
		return buffer.ErrDrainStop
	})
	if err != nil {
		if errors.Is(err, buffer.ErrDrainStop) {
			slog.Warn("Disk buffer drain stopped mid-stream; will retry the remaining batches", "pipeline_id", p.ID, "pending_batches", p.diskBuffer.Count())
		} else {
			slog.Error("Failed to drain disk buffer", "pipeline_id", p.ID, "error", err)
		}
		return
	}
	if err := p.diskBuffer.Compact(); err != nil {
		slog.Warn("Failed to compact disk buffer", "pipeline_id", p.ID, "error", err)
	}
}

// madeProgress reports whether processBatch delivered any part of orig,
// given the sub-batches it returned as still needing delivery: either
// fewer entries remain, or the first of them needs fewer destinations.
func (p *Pipeline) madeProgress(orig buffer.DiskBatch, remaining []Batch) bool {
	left := 0
	for _, b := range remaining {
		left += len(b.Entries)
	}
	if left < len(orig.Entries) || len(remaining) == 0 {
		return true
	}
	return len(p.targetDestinations(remaining[0].Destinations)) < len(p.targetDestinations(orig.Destinations))
}

func toDiskBatches(batches []Batch) []buffer.DiskBatch {
	out := make([]buffer.DiskBatch, len(batches))
	for i, b := range batches {
		out[i] = buffer.DiskBatch{
			Entries:      b.Entries,
			EntryCursors: b.EntryCursors,
			NextCursor:   b.NextCursor,
			Destinations: b.Destinations,
		}
	}
	return out
}

// processBatch runs the processors over each sub-batch and delivers it
// to the destinations that still need it. It returns (nil, true) when
// the whole batch was handled. Otherwise it returns false together with
// the sub-batches that still need delivery, which the caller spills to
// the disk buffer.
func (p *Pipeline) processBatch(cpState *checkpointState, batch Batch) (remaining []Batch, handled bool) {
	defer func() {
		if !handled && len(remaining) > 0 {
			p.flight.Block()
		}
		p.markDone()
	}()
	metrics.BufferUtilization.WithLabelValues(p.ID).Set(float64(len(p.Buffer)))

	subBatches := splitBatch(batch, p.settings.SendBatchSize)
	slog.Debug("Processing batch", "pipeline_id", p.ID, "entries", len(batch.Entries), "sub_batches", len(subBatches), "next_cursor", batch.NextCursor)
	for i, sub := range subBatches {
		retryBatch, stop := p.deliverSubBatch(cpState, sub, i, len(subBatches))
		if retryBatch != nil {
			p.flushCheckpoint(cpState)
			return append([]Batch{*retryBatch}, subBatches[i+1:]...), false
		}
		if stop {
			p.flushCheckpoint(cpState)
			if i+1 < len(subBatches) {
				return subBatches[i+1:], false
			}
			return nil, true
		}
	}
	return nil, true
}

// deliverSubBatch processes and sends one sub-batch.
//
// When some destination still needs the sub-batch (its circuit breaker
// is open, or its failure could not be dead-lettered), the cursor is not
// advanced and the sub-batch is returned, narrowed to those destinations,
// so destinations that already accepted it are not sent it again.
//
// Otherwise the cursor advances past the sub-batch. stop is true when a
// destination failed at the batch level and was dead-lettered; the
// caller then spills the remaining sub-batches instead of sending them
// straight away to a destination that is failing.
func (p *Pipeline) deliverSubBatch(cpState *checkpointState, sub Batch, idx, total int) (retryBatch *Batch, stop bool) {
	entries, err := p.runProcessors(sub.Entries)
	if err != nil {
		return &sub, false
	}
	if len(entries) == 0 {
		slog.Debug("All entries filtered out after processing", "pipeline_id", p.ID, "sub_batch", idx+1, "sub_batches", total, "next_cursor", sub.NextCursor)
		p.commitCursor(cpState, sub.NextCursor)
		return nil, false
	}

	targets := p.targetDestinations(sub.Destinations)
	slog.Debug("Sending sub-batch", "pipeline_id", p.ID, "sub_batch", idx+1, "sub_batches", total, "entries", len(entries), "destinations", len(targets), "next_cursor", sub.NextCursor)

	var pending []string
	delivered := false
	for _, r := range p.sendToDestinations(entries, targets) {
		key := r.dest.key
		destType := r.dest.Destination.Type()
		switch {
		case r.err != nil && p.shuttingDown():
			// The send was abandoned because LogHub is shutting down,
			// not because the destination failed: keep the sub-batch.
			slog.Warn("Send interrupted by shutdown; keeping sub-batch for retry", "pipeline_id", p.ID, "destination", key, "entries", len(entries))
			pending = append(pending, key)

		case errors.Is(r.err, retry.ErrCircuitOpen):
			// Load-shed, not lost: the destination may recover, so
			// keep the sub-batch for it rather than dead-lettering.
			slog.Warn("Circuit breaker open: keeping sub-batch for retry", "pipeline_id", p.ID, "destination", key, "sub_batch", idx+1, "sub_batches", total, "entries", len(entries))
			p.recordError(r.err)
			pending = append(pending, key)

		case r.err != nil:
			slog.Error("Failed to send batch after retries", "pipeline_id", p.ID, "destination", key, "error", r.err)
			p.recordError(r.err)
			metrics.EntriesFailed.WithLabelValues(p.ID, destType).Add(float64(len(entries)))
			if p.dlq == nil {
				pending = append(pending, key)
				continue
			}
			if dlqErr := p.dlq.Write(entries, key, r.err.Error()); dlqErr != nil {
				slog.Error("Failed to write failed batch to DLQ, entries will be retried from disk", "pipeline_id", p.ID, "destination", key, "error", dlqErr)
				pending = append(pending, key)
				continue
			}
			slog.Warn("Batch written to DLQ after send failure", "pipeline_id", p.ID, "destination", key, "entries", len(entries))
			stop = true

		default:
			delivered = true
			if !p.handleEntryStatuses(r, entries) {
				pending = append(pending, key)
			}
		}
	}

	if delivered {
		metrics.BatchesSent.WithLabelValues(p.ID).Inc()
		p.recordSend()
	}
	if len(pending) > 0 {
		retryBatch = &Batch{
			Entries:      sub.Entries,
			EntryCursors: sub.EntryCursors,
			NextCursor:   sub.NextCursor,
			Destinations: pending,
		}
		return retryBatch, false
	}

	// Every target either accepted the entries or has its failures in
	// the DLQ, so nothing in this sub-batch needs to be read again.
	p.commitCursor(cpState, sub.NextCursor)
	return nil, stop
}

// handleEntryStatuses records the per-entry outcome of a successful send
// and dead-letters the entries the destination rejected. It returns
// false if the rejected entries could not be written to a configured DLQ,
// in which case the destination must be sent the sub-batch again.
func (p *Pipeline) handleEntryStatuses(r destResult, entries []*core.LogEntry) bool {
	destType := r.dest.Destination.Type()
	var rejected []*core.LogEntry
	var firstErr error
	for i, st := range r.statuses {
		if i >= len(entries) {
			break
		}
		if st.Error != nil {
			rejected = append(rejected, entries[i])
			if firstErr == nil {
				firstErr = st.Error
			}
		}
	}
	accepted := len(entries) - len(rejected)
	metrics.EntriesSent.WithLabelValues(p.ID, destType).Add(float64(accepted))
	if len(rejected) == 0 {
		return true
	}
	metrics.EntriesFailed.WithLabelValues(p.ID, destType).Add(float64(len(rejected)))
	if err := p.deadLetter(rejected, r.dest.key, "destination rejected entry: "+firstErr.Error(), dropDestinationRejected); err != nil {
		slog.Error("Failed to write rejected entries to DLQ, will retry the sub-batch", "pipeline_id", p.ID, "destination", r.dest.key, "error", err)
		return false
	}
	slog.Warn("Per-entry rejections handled", "pipeline_id", p.ID, "destination", r.dest.key, "rejected", len(rejected), "accepted", accepted)
	return true
}

// runProcessors applies the processor chain to entries. When a processor
// fails on the batch, each entry is retried on its own so only the
// entries that fail individually are dead-lettered. It returns an error
// only when those entries could not be written to a configured DLQ.
func (p *Pipeline) runProcessors(entries []*core.LogEntry) ([]*core.LogEntry, error) {
	for _, proc := range p.Processors {
		out, err := proc.Process(p.pushContext(), entries)
		if err == nil {
			entries = out
			continue
		}
		procName := fmt.Sprintf("%T", proc)
		slog.Warn("Processor error on batch, attempting per-entry isolation", "pipeline_id", p.ID, "processor", procName, "error", err)
		p.recordError(err)

		var kept, rejected []*core.LogEntry
		for _, entry := range entries {
			single, entryErr := proc.Process(p.pushContext(), []*core.LogEntry{entry})
			if entryErr != nil {
				slog.Warn("Processor rejected single entry", "pipeline_id", p.ID, "processor", procName, "error", entryErr)
				rejected = append(rejected, entry)
				continue
			}
			kept = append(kept, single...)
		}
		if len(rejected) > 0 {
			if dlqErr := p.deadLetter(rejected, "processor", err.Error(), dropProcessorRejected); dlqErr != nil {
				slog.Error("Failed to write per-entry processor failures to DLQ, returning for retry", "pipeline_id", p.ID, "error", dlqErr)
				return nil, dlqErr
			}
		}
		entries = kept
		if len(entries) == 0 {
			return nil, nil
		}
	}
	return entries, nil
}

// deadLetter writes entries to the DLQ under the given destination label.
// Without a DLQ the entries are dropped, which is logged and counted
// under dropReason. It returns an error only when a configured DLQ fails.
func (p *Pipeline) deadLetter(entries []*core.LogEntry, destination, errMsg, dropReason string) error {
	if p.dlq == nil {
		slog.Error("No DLQ configured; dropping entries", "pipeline_id", p.ID, "destination", destination, "entries", len(entries), "reason", dropReason, "error", errMsg)
		p.countDropped(dropReason, len(entries))
		return nil
	}
	if err := p.dlq.Write(entries, destination, errMsg); err != nil {
		return err
	}
	slog.Warn("Entries written to DLQ", "pipeline_id", p.ID, "destination", destination, "entries", len(entries))
	return nil
}

func (p *Pipeline) countDropped(reason string, n int) {
	metrics.EntriesDropped.WithLabelValues(p.ID, reason).Add(float64(n))
}

// recordError stores err as the pipeline's most recent error and
// re-evaluates the pipeline status.
func (p *Pipeline) recordError(err error) {
	now := time.Now()
	p.mu.Lock()
	p.health.LastError = now
	p.health.ErrorMsg = err.Error()
	p.mu.Unlock()
	p.evaluateStatus(now)
}

// recordSend stores the time of the most recent successful send and
// re-evaluates the pipeline status.
func (p *Pipeline) recordSend() {
	now := time.Now()
	p.mu.Lock()
	p.health.LastSend = now
	p.mu.Unlock()
	p.evaluateStatus(now)
}

// commitCursor checkpoints cursor and advances the in-memory cursor.
func (p *Pipeline) commitCursor(cpState *checkpointState, cursor string) {
	p.saveCheckpoint(cpState, cursor)
	p.cursor.Advance(cursor)
}

func (p *Pipeline) saveCheckpoint(cpState *checkpointState, cursor string) {
	cpState.mu.Lock()
	cpState.pendingCursor = cursor
	shouldSave := p.settings.CheckpointInterval == 0 || time.Since(cpState.lastCheckpoint) >= p.settings.CheckpointInterval
	if shouldSave {
		cpState.lastCheckpoint = time.Now()
	}
	cpState.mu.Unlock()

	if shouldSave {
		if err := p.Checkpointer.Set(p.ID, cursor); err != nil {
			slog.Error("Failed to save checkpoint", "pipeline_id", p.ID, "error", err)
		} else {
			cpState.mu.Lock()
			if cpState.pendingCursor == cursor {
				cpState.pendingCursor = ""
			}
			cpState.mu.Unlock()
			metrics.CheckpointsSaved.WithLabelValues(p.ID).Inc()
			slog.Info("Checkpoint saved", "pipeline_id", p.ID, "cursor", cursor)
		}
	}
}

func (p *Pipeline) flushCheckpoint(cpState *checkpointState) {
	cpState.mu.Lock()
	cursor := cpState.pendingCursor
	cpState.mu.Unlock()
	if cursor == "" {
		return
	}
	if err := p.Checkpointer.Set(p.ID, cursor); err != nil {
		slog.Error("Failed to flush checkpoint", "pipeline_id", p.ID, "error", err)
	} else {
		cpState.mu.Lock()
		if cpState.pendingCursor == cursor {
			cpState.pendingCursor = ""
		}
		cpState.mu.Unlock()
		metrics.CheckpointsSaved.WithLabelValues(p.ID).Inc()
		slog.Info("Checkpoint flushed", "pipeline_id", p.ID, "cursor", cursor)
	}
}

func (p *Pipeline) markDone() {
	p.flight.Dec()
}

func splitBatch(batch Batch, sendBatchSize int) []Batch {
	if sendBatchSize <= 0 || len(batch.Entries) <= sendBatchSize {
		return []Batch{batch}
	}

	subBatches := make([]Batch, 0, (len(batch.Entries)+sendBatchSize-1)/sendBatchSize)
	for start := 0; start < len(batch.Entries); start += sendBatchSize {
		end := start + sendBatchSize
		if end > len(batch.Entries) {
			end = len(batch.Entries)
		}
		var entryCursors []string
		if len(batch.EntryCursors) >= end {
			entryCursors = batch.EntryCursors[start:end]
		}
		// Choose a NextCursor for this sub-batch. The cursor must be unique
		// per sub-batch; otherwise the cursor advance would skip work or
		// duplicate it. Try in order:
		//   1. The cursor of the last entry in this sub-batch.
		//   2. The cursor of the first entry of the next sub-batch (which
		//      is the entry immediately after this sub-batch).
		//   3. The batch's overall NextCursor (used for the last sub-batch
		//      and as a final fallback for any sub-batch).
		//   4. Empty + warning: a data quality issue at the source.
		nextCursor := cursorAt(batch, end-1)
		if nextCursor == "" && end < len(batch.Entries) {
			nextCursor = cursorAt(batch, end)
		}
		if nextCursor == "" {
			nextCursor = batch.NextCursor
		}
		if nextCursor == "" {
			slog.Warn("splitBatch: cannot determine NextCursor for sub-batch; data loss risk on restart", "sub_batch_start", start, "sub_batch_end", end, "pipeline_id_implicit", true)
		}
		subBatches = append(subBatches, Batch{
			Entries:      batch.Entries[start:end],
			EntryCursors: entryCursors,
			NextCursor:   nextCursor,
			Destinations: batch.Destinations,
		})
	}
	return subBatches
}

func cursorAt(batch Batch, idx int) string {
	if idx >= 0 && idx < len(batch.EntryCursors) && batch.EntryCursors[idx] != "" {
		return batch.EntryCursors[idx]
	}
	if idx >= 0 && idx < len(batch.Entries) && batch.Entries[idx] != nil && batch.Entries[idx].Cursor != "" {
		return batch.Entries[idx].Cursor
	}
	return ""
}

// targetDestinations returns the destinations named by keys, or every
// destination when keys is empty. A key that matches no configured
// destination (the configuration changed while the batch sat in the
// disk buffer) falls back to every destination: delivering twice is
// safer than not delivering.
func (p *Pipeline) targetDestinations(keys []string) []*DestinationInfo {
	all := make([]*DestinationInfo, len(p.Destinations))
	for i := range p.Destinations {
		all[i] = &p.Destinations[i]
	}
	if len(keys) == 0 {
		return all
	}
	byKey := make(map[string]*DestinationInfo, len(all))
	for _, d := range all {
		byKey[d.key] = d
	}
	targets := make([]*DestinationInfo, 0, len(keys))
	for _, k := range keys {
		d, ok := byKey[k]
		if !ok {
			slog.Warn("Batch names an unknown destination; sending to all destinations", "pipeline_id", p.ID, "destination", k)
			return all
		}
		targets = append(targets, d)
	}
	return targets
}

// sendToDestinations sends entries to each target destination in
// parallel and returns one result per target, in target order. Each
// destination has its own retry budget and circuit breaker, so one dead
// destination cannot hold up the others for its full retry budget.
//
// Per-entry errors (e.g. Sentinel returning 400) are not retried; they
// are reported in the result's statuses. Batch-level errors (network,
// 5xx, timeout) are retried with exponential backoff. While a
// destination's breaker is open the retrier aborts immediately and the
// result's error is retry.ErrCircuitOpen; there is no point sleeping the
// back-off when the destination is known to be down.
func (p *Pipeline) sendToDestinations(entries []*core.LogEntry, targets []*DestinationInfo) []destResult {
	results := make([]destResult, len(targets))
	if len(entries) == 0 {
		for i, dest := range targets {
			results[i] = destResult{dest: dest}
		}
		return results
	}
	var wg sync.WaitGroup
	for i, dest := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses, err := p.sendToDestination(dest, entries)
			results[i] = destResult{dest: dest, statuses: statuses, err: err}
		}()
	}
	wg.Wait()
	return results
}

// sendToDestination sends entries to one destination, retrying
// batch-level failures within the destination's retry budget.
func (p *Pipeline) sendToDestination(dest *DestinationInfo, entries []*core.LogEntry) ([]core.EntryStatus, error) {
	maxRetries := p.settings.MaxRetries
	if dest.MaxRetries > 0 {
		maxRetries = dest.MaxRetries
	}
	retrier := retry.Retrier{MaxRetries: maxRetries, BaseBackoff: p.settings.RetryBaseBackoff}
	destType := dest.Destination.Type()

	var lastErr error
	var statuses []core.EntryStatus
	err := retrier.Do(p.pushContext(), func() error {
		// The breaker is checked on every attempt, so a probe failure
		// that re-opens the circuit stops the remaining retries.
		if allowErr := dest.Breaker.Allow(); allowErr != nil {
			lastErr = allowErr
			return retry.ErrFatal
		}
		start := time.Now()
		slog.Debug("Sending batch to destination", "pipeline_id", p.ID, "destination", dest.key, "entries", len(entries), "max_retries", maxRetries)
		s, sendErr := dest.Destination.Send(p.pushContext(), entries)
		metrics.SendDuration.WithLabelValues(p.ID).Observe(time.Since(start).Seconds())
		if sendErr != nil {
			lastErr = sendErr
			if errors.Is(sendErr, core.ErrPoisonPill) {
				// A deterministic rejection, not an outage: don't
				// retry and don't count it against the breaker.
				return retry.ErrFatal
			}
			metrics.Retries.WithLabelValues(p.ID, destType).Inc()
			dest.Breaker.RecordFailure()
			return sendErr
		}
		dest.Breaker.RecordSuccess()
		statuses = s
		return nil
	})
	if err != nil {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, errors.Join(ErrDestinationUnreachable, err)
	}
	return statuses, nil
}
