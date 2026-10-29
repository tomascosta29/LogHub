# Failure Modes and Recovery

This document describes expected behavior for production failure scenarios in LogHub. Understanding these behaviors helps operators know when data loss, duplication, or manual intervention may occur.

## Delivery Guarantees

LogHub provides **at-least-once delivery** semantics. With cursor-based progress tracking, a batch is only checkpointed after successful processing and delivery. This means:

- **Potential duplication**: Batches delivered but not yet checkpointed may be replayed after a crash.
- **No data loss**: As long as checkpoints are durable, no source records are skipped.
- **Cursor correctness**: One source page may be prefetched, but acknowledgements never advance past an older failed page.

## Failure Scenarios

### 1. MSSQL Source Temporarily Unavailable

**Scenario**: The MSSQL database becomes unreachable (network partition, server overload, maintenance window).

**Expected Behavior**:
- `Source.Collect()` returns an error, which is logged at error level.
- The pull routine logs the error and waits for the next poll interval.
- The cursor is **not** advanced.
- On the next poll interval, the pipeline retries from the same cursor.

**Data Loss or Duplication**: Neither. The batch was never collected, so no data is lost or duplicated.

**Operator Action Required**: None. The pipeline automatically retries once MSSQL is available. Monitor `pipeline_pull_errors_total` metric for sustained collection failures.

---

### 2. Key Vault Temporarily Unavailable

**Scenario**: Azure Key Vault is unreachable during secret resolution (with `strict_resolution=true`).

**Expected Behavior**:
- A secret that has already resolved keeps its last value: running pipelines are unaffected, and a reload (SIGHUP or `reload_on_secret_change`) rebuilds them with the last values. Each failed lookup is logged and counted in `loghub_secret_refresh_errors_total`.
- A secret that has never resolved (at startup, or newly added on reload):
  - If `global.strict_resolution: true`: resolution fails. Startup fails; a reload fails and the current pipelines keep running.
  - If `global.strict_resolution: false` (default): the reference `${provider:name}` is left as-is, which may cause downstream authentication failures. The failure is not cached: the next startup or reload tries again.

**Data Loss or Duplication**: No data loss or duplication due to Key Vault unavailability itself. However, if `strict_resolution: false` and a required secret is unresolved, authentication will fail with an unclear error.

**Operator Action Required**:
- Already-running LogHub: none; restore Key Vault access. If a secret was rotated during the outage, send SIGHUP afterwards.
- Startup with `strict_resolution: true`: wait for Key Vault to become available, then restart.
- Startup with `strict_resolution: false`: fix the secret reference or Key Vault permissions, then restart.

**Configuration Note**: For production, set `global.strict_resolution: true` to fail fast on missing secrets.

---

### 3. Managed Identity Token Acquisition Fails

**Scenario**: The Managed Identity credential cannot acquire a token (identity not configured, network issue, Azure AD unavailable).

**Expected Behavior** (`sentinelDestination.Send` in `internal/plugins/destination/sentinel.go`):
- `tokenProvider.GetToken()` returns an error.
- The Sentinel destination returns `fmt.Errorf("sentinel token acquisition failed: %w", err)`.
- The pipeline retries with exponential backoff (`sendToDestination` in `internal/pipeline/pusher.go`).
- If all retries are exhausted, the batch is written to the DLQ (if configured), and the cursor advances past it. Without a DLQ, or if the DLQ write fails, the batch is kept for retry and the cursor is **not** advanced.

**Data Loss or Duplication**: No data loss. If DLQ is configured and all retries fail, data is preserved in DLQ. Potential duplication if retries succeed after the token is eventually acquired.

**Operator Action Required**: Verify the managed identity has the `Monitoring Metrics Publisher` role on the DCR. Check Azure AD service health. If the issue persists, data in DLQ can be replayed after recovery.

---

### 4. Sentinel Returns 429 (Rate Limited)

**Scenario**: Azure Monitor Logs Ingestion API returns HTTP 429 Too Many Requests.

**Expected Behavior** (`HTTP.do` in `internal/plugins/destination/http.go`, which the Sentinel destination uses):
- `ErrRateLimited` is returned.
- `sendToDestination` retries with exponential backoff: `BaseBackoff * 2^attempt`.
- The cursor is **not** advanced until delivery succeeds.

**Data Loss or Duplication**: No data loss. No duplication (unless operator manually replays after fixing the rate limit).

**Operator Action Required**: Check if LogHub's ingestion rate exceeds Azure Monitor limits. Consider reducing `poll_interval` or batch size. If rate limits are sustained, Azure subscription limits may need adjustment.

---

### 5. Sentinel Returns 5xx (Server Error)

**Scenario**: Azure Monitor Logs Ingestion API returns a server-side error (5xx).

**Expected Behavior** (`HTTP.do` in `internal/plugins/destination/http.go`):
- `ErrServerError` is returned.
- The retry mechanism retries with exponential backoff.
- The cursor is **not** advanced until delivery succeeds.

**Data Loss or Duplication**: No data loss. If DLQ is configured and all retries fail, data is preserved in DLQ.

**Operator Action Required**: Check Azure Monitor service health. Once service is restored, the pipeline automatically resumes. DLQ entries can be replayed if needed.

---

### 6. Sentinel Returns 400 Due to Schema Mismatch

**Scenario**: The DCR stream schema does not match the data being sent (field type mismatch, missing required field, etc.).

**Expected Behavior** (`HTTP.do` in `internal/plugins/destination/http.go`):
- `ErrBadRequest` and a `PoisonPill` error are returned.
- `sendToDestination` sees the `ErrPoisonPill` and stops retrying immediately (`retry.ErrFatal`); the rejection does not count against the circuit breaker.
- The batch is written to DLQ (if configured) for that destination only (for example `"sentinel#0"`), with an error containing `"bad request"`. Other destinations in the pipeline are not affected.
- Once the DLQ write succeeds the cursor advances. If the DLQ write fails, the batch is kept and retried, and the cursor is not advanced.
- Without a DLQ, the rejected entries are dropped: the cursor advances, the drop is logged, and it is counted in `loghub_entries_dropped_total{reason="destination_rejected"}`. A rejected batch is not retried, because the destination would reject it again and block the pipeline.
- Either way the pipeline is marked `degraded`, with the rejection in `/health`.

**Data Loss or Duplication**: No data loss with a DLQ (preserved in DLQ). Without a DLQ, the rejected entries are lost. No duplication.

**Operator Action Required**: Inspect the DLQ file to identify the malformed batch. Check the DCR stream definition and ensure source columns match. Fix the data transformation or DCR schema. Replay DLQ entries after fixing.

**DLQ Entry Inspection**:
```bash
loghub dlq export --config <config> --pipeline <pipeline_id> | jq
```

---

### 7. Disk Buffer Reaches Max Size

**Scenario**: The disk spillover buffer (`disk_buffer_max_size_mb`) reaches its configured limit.

**Expected Behavior**:
- `DiskBuffer.Write()` returns `disk buffer size limit exceeded`.
- The pusher retains the unwritten page and its successors in bounded memory.
- It drains any earlier disk pages and retries the retained prefix before delivering later pages. New collection is paused during recovery.
- On shutdown, pages that could not be persisted remain behind the durable source checkpoint for replay on restart.

**Data Loss or Duplication**: The pipeline does not acknowledge past the failed page. Recovery requires the source to retain uncheckpointed records; a crash can replay already delivered records.

**Operator Action Required**: 
1. Increase `disk_buffer_max_size_mb` if disk space allows.
2. Investigate why Sentinel/destination is slow (backpressure).
3. To recover without restart: the pipeline will automatically resume once a drain completes and space is freed.

---

### 8. DLQ Write Fails

**Scenario**: Writing a failed batch to the Dead Letter Queue fails (disk full, permissions, etc.).

**Expected Behavior** (`runProcessors`, `deliverSubBatch` and `handleEntryStatuses` in `internal/pipeline/pusher.go`):

| Error Type | DLQ Write Fails | Cursor Advanced? | Data Lost? |
|------------|-----------------|------------------|------------|
| Processor error (per-entry) | Yes | No | No (sub-batch retried) |
| Destination failure after retries | Yes | No | No (kept for that destination and retried) |
| Destination rejected individual entries | Yes | No | No (sub-batch retried for that destination) |

In every case the error is logged, and the sub-batch is kept and retried (in the disk buffer, if configured). The cursor does not move past entries that are neither delivered nor dead-lettered.

Without a DLQ configured at all, entries a processor or destination rejects individually are dropped, which is logged and counted in `loghub_entries_dropped_total`. A destination that fails after its retries keeps the sub-batch and retries it.

**Data Loss or Duplication**:
- A failed DLQ write loses no data, but the sub-batch is retried until the DLQ is writable again, which holds up the pipeline.
- Destinations that already accepted the sub-batch are not sent it again.

**Operator Action Required**:
- Ensure the DLQ directory has adequate disk space and correct permissions.
- Monitor `loghub_dlq_entries_total` metric for DLQ growth.
- Alert on `loghub_entries_dropped_total` if any pipeline runs without a DLQ.

---

### 9. Checkpoint Write Fails

**Scenario**: Writing the cursor checkpoint to persistent storage fails.

**Expected Behavior** (`saveCheckpoint` in `internal/pipeline/pusher.go`):
- `Checkpointer.Set()` returns an error.
- The error is logged at error level.
- The cursor **is** still advanced in memory, and the failed cursor is kept as pending; it is written again with the next checkpoint and at shutdown.
- On the next poll, the source will use the in-memory cursor (not the failed checkpoint).

**Data Loss or Duplication**: **Potential duplication**. If the process crashes after the cursor advances but before the checkpoint is durable, the same batch may be delivered twice on restart (the checkpoint still holds the old cursor).

**Operator Action Required**:
- Watch for "Failed to save checkpoint" log messages, or for `loghub_checkpoints_saved_total` no longer increasing while entries are sent.
- If checkpoint failures are sustained, check disk health and checkpoint directory permissions.
- After a crash, check for duplicate deliveries in Sentinel (use a unique identifier field to detect duplicates).

**Checkpoint Implementation**: The file checkpointer writes atomically using `os.WriteFile` to a temp file followed by `os.Rename`, with sync before rename.

---

### 10. Process Crashes After Delivery But Before Checkpoint

**Scenario**: The pipeline delivers a batch to Sentinel, but crashes before the checkpoint is saved.

**Expected Behavior**:
- The batch was delivered to Sentinel (confirmed via 200 response).
- The in-memory cursor was updated to the next position.
- The checkpoint write did not occur.
- On restart, the pipeline loads the last durable checkpoint (before the delivery).
- The same batch is re-collected and re-delivered.

**Data Loss or Duplication**: **Potential duplication**. The batch is delivered twice (at-least-once semantics).

**Operator Action Required**:
- Use a deduplication strategy in Sentinel (e.g., a unique `_itemId` field based on cursor + batch sequence).
- Monitor for duplicate entries using a test query with unique identifiers.
- This is an expected trade-off of at-least-once delivery and does not require operator action unless duplicates are unacceptable.

---

### 11. Process Receives SIGTERM During Delivery

**Scenario**: The container/process receives SIGTERM (e.g., `kubectl delete pod`, `docker stop`).

**Expected Behavior**:
1. Collection stops; a source read interrupted before enqueue is recovered from the durable checkpoint.
2. Delivery drains queued pages in order while the shutdown timeout allows it.
3. An older failure blocks later delivery. Undelivered pages are persisted in order when possible; otherwise the durable source checkpoint stays behind them for replay.
4. Pending acknowledged progress is flushed before exit. Timed-out sends are kept for retry rather than dead-lettered as destination failures.

**Data Loss or Duplication**: At-least-once behavior applies. Successful sends without a durable checkpoint may be duplicated after restart; unpersisted pages require source retention.

**Operator Action Required**: None. The pipeline implements graceful shutdown. Ensure `shutdown_timeout` is long enough for in-flight batches to complete (default: 30s).

**Configuration Note**: Set `global.shutdown_timeout` to accommodate maximum batch delivery time under load.

---

## DLQ Recovery Procedures

The Dead Letter Queue stores failed batches in `<dlq_dir>/<pipeline_id>.dlq.journal`, with rotated archives next to it. Use `loghub dlq` to read them; see the runbook's Dead Letter Queue section.

### Inspecting DLQ Entries

```bash
# Records and entries per pipeline and destination
loghub dlq list --config /config/loghub.prod.yaml

# Every record as JSON lines
loghub dlq export --config /config/loghub.prod.yaml --pipeline pipeline-1 | jq
```

### Replaying DLQ Entries

LogHub does not replay the DLQ itself. To replay:

1. Export the records with `loghub dlq export --archives`.
2. Copy the export to a staging location.
3. Transform entries if needed to fix the root cause.
4. Use a temporary pipeline or script to re-ingest the entries.
5. Alternatively, modify the source query to re-collect the failed records if the source still has them.

---

## Disk Buffer Recovery Procedures

The disk buffer stores pending batches in `<disk_buffer_dir>/<pipeline_id>.buffer.journal`. The `loghub_disk_buffer_batches` gauge shows how many are waiting.

### Recovery After Crash

If the process crashes with data in the disk buffer:

1. On restart, the pending batches are recovered from the journal: every batch without a delivered marker.
2. The puller waits while batches are pending; the pusher delivers them in order and marks each one delivered as soon as it is handled.
3. A batch that was only partly delivered before the crash was replaced by what was left in the same transaction, so only that remainder is delivered again.

**No data loss**: a batch stays pending until it is delivered or written to the DLQ. A batch delivered just before the crash, but not yet marked, is delivered again (at-least-once).

### Upgrading from v0.1.0

Batches in a v0.1.0 `<pipeline_id>.buffer` or `<pipeline_id>.buffer.drain` file are imported into the journal on first start, and the old files are renamed with a `.migrated` suffix. The same happens to `<pipeline_id>.dlq` and `<pipeline_id>.cursor`.

---

## Summary Table

| Scenario | Data Lost? | Duplicated? | Operator Action |
|----------|------------|-------------|-----------------|
| MSSQL unavailable | No | No | Auto-retry |
| Key Vault unavailable (strict) | No | No | Restart after KV recovery |
| Key Vault unavailable (non-strict) | Risk | No | Fix secret references |
| MI token acquisition fails | No | Risk | Check identity permissions |
| Sentinel 429 | No | No | Check rate limits |
| Sentinel 5xx | No | No | Check Azure status |
| Sentinel 400 (schema) | No with a DLQ; **Yes** without | No | Fix DCR schema, replay DLQ |
| Disk buffer full | No | No | Increase size, investigate backpressure |
| DLQ write fails (dest error) | No | No | Check DLQ disk space |
| DLQ write fails (proc error) | No | No | Check DLQ disk space |
| Checkpoint write fails | No | **Risk** | Check checkpoint disk |
| Crash after delivery | No | **Risk** | Deduplicate in Sentinel |
| SIGTERM during delivery | No | No | None (graceful) |

---

## Test Coverage

The following failure scenarios have automated tests:

- `TestPipelineRetryOnFailure` - Transient destination errors with retry
- `TestPipelinePoisonPillDropsBatch` - Poison pill (schema mismatch) stops retries
- `TestPipelineDLQ` - Failed batches written to DLQ
- `TestPipelineDiskBuffer` - Disk buffer spillover and drain
- `TestPullDoesNotAdvanceCursorBeforeDelivery` - Cursor not advanced on failure
- `TestCursorDoesNotAdvanceAfterFailedDelivery` - Cursor stability after dest failure
- `TestDrainProgressSurvivesRestart` - Partial drain keeps delivered batches delivered and the cursor saved across a restart
- `TestDiskBufferReplacementKeepsDeliveryOrder` - A partly delivered batch keeps its place in delivery order
- `TestUpgradeFromV010State` - Starting on a v0.1.0 data directory resumes from the old cursor and delivers the old disk buffer
- `TestDLQWrite` - DLQ write persistence
- `TestPushRoutineFlushesPendingCheckpointOnShutdown` - Graceful shutdown checkpoint flush

High-risk scenarios that may benefit from additional tests:
- Checkpoint write failure followed by crash (potential duplication)
- DLQ write failure on processor error (data loss scenario)
- SIGTERM during in-flight delivery (graceful shutdown)
