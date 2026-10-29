# Azure Sentinel Production Guide

This document describes the schema contract and operational requirements for sending MSSQL audit logs to Microsoft Sentinel via the Azure Monitor Logs Ingestion API.

## Table and Stream Naming

| Component | Format | Example |
|-----------|--------|---------|
| Custom table name | `{Name}_CL` | `MSSQLAudit_CL` |
| Stream name | `Custom-{TableName}` | `Custom-MSSQLAudit_CL` |
| DCR endpoint | `/dataCollectionRules/{DCRId}/streams/{StreamName}` | `/dataCollectionRules/abc123/streams/Custom-MSSQLAudit_CL` |

The `_CL` suffix marks custom tables in Log Analytics. The stream name must match the DCR stream declaration exactly, including case.

## TimeGenerated Field Handling

`TimeGenerated` is the primary timestamp field in Sentinel. It determines when events appear in the timeline and is used for retention and querying.

| Requirement | Implementation |
|-------------|----------------|
| Source field | LogHub `LogEntry.Timestamp`, normally populated from MSSQL `event_time` |
| Time zone | Must be UTC (ISO 8601/RFC 3339 format) |
| Transformation | Applied in-flight before transmission |
| Format | `2026-05-06T12:00:01Z` |

The production config uses the `timestamp` processor to parse MSSQL `event_time`
into `LogEntry.Timestamp`. The Sentinel destination then writes
`TimeGenerated` from that timestamp in UTC RFC 3339 format.

## Required Transformation Rules

The following transformations are required when sending MSSQL audit logs to Sentinel:

1. **Timestamp mapping**: `event_time` → `TimeGenerated`
2. **UTC enforcement**: `TimeGenerated` must be RFC 3339 format with `Z` suffix
3. **Stream decoration**: Each payload must be an array of records matching the DCR stream schema
4. **Flattened record body**: LogHub sends `LogEntry.Fields` as top-level JSON fields and adds `TimeGenerated`; it does not send nested `fields`, `message`, `level`, or `source` properties

## Sample Payload Structure

```json
[
  {
    "TimeGenerated": "2026-05-06T12:00:01Z",
    "id": "000001",
    "event_time": "2026-05-06T12:00:01Z",
    "user_name": "user-000001",
    "action": "login"
  },
  {
    "TimeGenerated": "2026-05-06T12:00:02Z",
    "id": "000002",
    "event_time": "2026-05-06T12:00:02Z",
    "user_name": "user-000002",
    "action": "denied"
  }
]
```

### Field Mapping: MSSQL Source to Sentinel

| MSSQL Column | Sentinel Field | Type | Notes |
|--------------|----------------|------|-------|
| `id` | `id` | string | Unique event identifier |
| `event_time` | `TimeGenerated` | datetime | UTC, maps to Sentinel's TimeGenerated |
| `event_time` | `event_time` | datetime | Retained for reference |
| `user_name` | `user_name` | string | Actor who performed the action |
| `action` | `action` | string | login, denied, logout, etc. |

## DCR Stream Declaration

The DCR must declare a stream matching the table schema. Example sanitized DCR stream configuration:

```json
{
  "streams": [
    "Custom-MSSQLAudit_CL"
  ],
  "schema": {
    "columns": [
      { "name": "TimeGenerated", "type": "datetime" },
      { "name": "id", "type": "string" },
      { "name": "event_time", "type": "datetime" },
      { "name": "user_name", "type": "string" },
      { "name": "action", "type": "string" }
    ]
  }
}
```

## KQL Canary Query

Use the following KQL query to validate that test records have arrived in Sentinel:

```kusto
MSSQLAudit_CL
| where event_time >= ago(5m)
| where id contains "<unique-test-run-id>"
| order by TimeGenerated desc
| take 10
```

For a canary test run with a unique identifier:

```kusto
let test_run_id = "canary-2026-05-06-001";
MSSQLAudit_CL
| where event_time >= ago(10m)
| where id contains test_run_id
| order by TimeGenerated desc
```

## Scale and Throughput

### Sentinel Ingestion Limits

| Limit | Value |
|-------|-------|
| Max payload size per request | 1 MB |
| Max payload size per minute | 30 MB |
| Recommended API version | 2023-01-01 |
| Endpoint | `{DCE}/dataCollectionRules/{DCRId}/streams/{StreamName}?api-version=2023-01-01` |

Sentinel's Azure Monitor Logs Ingestion API enforces a 30 MB/minute ingestion quota per DCR. Payloads exceeding 1 MB are rejected with HTTP 413. For typical MSSQL audit events (~200-500 bytes each), the 1 MB limit allows approximately 2,000-5,000 records per request.

### Expected Throughput

| Batch Size | Records/Request | Est. Throughput (100ms latency) |
|------------|-----------------|-------------------------------|
| 100 | 100 | ~1,000 records/sec |
| 200 | 200 | ~2,000 records/sec |
| 500 | 500 | ~5,000 records/sec (approaching 1MB limit) |

The source's `fetch_size` (rows per page, default 100) and `runtime.send_batch_size` (records per request) set these sizes. The default page of 100 rows balances MSSQL query cost, cursor pagination overhead, and Sentinel payload limits.

### Batch Size Justification

The default `fetch_size` of 100 is chosen because:

1. **Payload size**: 100 records at ~200-500 bytes each = 20-50 KB, well within the 1 MB limit
2. **MSSQL query cost**: `TOP (100)` per poll interval limits row set size and memory pressure
3. **Cursor accuracy**: Smaller batches reduce the window where duplicate timestamps at boundary conditions could cause missed records
4. **Pipeline memory**: a pipeline holds at most two pages in memory (one being delivered, one read ahead); 2 × 100 rows × ~1KB ≈ 200 KB per pipeline. `buffer_size`, the page queue's capacity in pages, does not change this.

For high-volume sources (>10,000 events/sec), increasing `fetch_size` to 200-500 is safe if average record size remains under 1 KB.

### Poll Interval Recommendations

| Environment | Recommended Poll Interval | Notes |
|-------------|---------------------------|-------|
| Low volume (<100 events/sec) | 5-10 seconds | Minimizes MSSQL query frequency |
| Medium volume (100-1,000 events/sec) | 1-5 seconds | Balance between latency and load |
| High volume (>1,000 events/sec) | 500ms-1 second | Tune `source.fetch_size` and `runtime.send_batch_size` together |

Poll interval is configured via `pipeline.runtime.poll_interval` in the config file. The pipeline pages through the source with its cursor. It reads at most one page ahead of delivery, and the checkpoint only advances past pages that were delivered (see `docs/performance.md`).

### MSSQL Index Requirements

Cursor queries require a composite index on `(timestamp_column, cursor_column)` for efficient pagination:

```sql
-- Required index for cursor-based pagination
CREATE NONCLUSTERED INDEX IX_Audit_Timestamp_Id
ON dbo.AuditLog (event_time ASC, id ASC)
INCLUDE (action, user_name, session_id);

-- Single-column index fallback (less efficient for tie-breaking)
CREATE NONCLUSTERED INDEX IX_Audit_Timestamp
ON dbo.AuditLog (event_time ASC);
```

The MSSQL source generates queries like (`internal/plugins/source/mssql.go`):

```sql
SELECT TOP (@fetch_size) * FROM @table
WHERE event_time > @ts_cursor
  OR (event_time = @ts_cursor AND id > @tie_cursor)
ORDER BY event_time ASC, id ASC
```

Without proper indexes, MSSQL performs table/scanned index scans, increasing query cost and memory usage.

### Destination Retry Behavior

The HTTP destination (`internal/plugins/destination/http.go`) handles failures as follows:

| HTTP Status | Error Type | Retry Behavior |
|-------------|------------|----------------|
| 429 Too Many Requests | `ErrRateLimited` | Retried with exponential backoff |
| 5xx Server Errors | `ErrServerError` | Retried with exponential backoff |
| 400 Bad Request | `ErrBadRequest` | Treated as poison pill (no retry) |
| Timeout | `ErrTimeout` | Retried with exponential backoff |

Retry configuration (`internal/retry/retry.go`):
- Exponential backoff: `BaseBackoff * 2^attempt` (e.g., 100ms, 200ms, 400ms...)
- Max retries: configurable per pipeline (default from `PipelineSettings.MaxRetries`)
- Cursor advancement: only after Sentinel accepts the batch, or after the failed entries are written to the DLQ (`deliverSubBatch` in `internal/pipeline/pusher.go`)

### Disk Buffer Behavior When Sentinel is Slow

When the Sentinel destination is slow or unavailable:

1. **Bounded prefetch**: The puller can fetch one page ahead of delivery, using a separate read cursor. Queue saturation stops collection.
2. **Ordered recovery**: A failed page is kept for its pending destinations, on disk when configured or in bounded memory if persistence is unavailable. Later pages wait behind it.
3. **Checkpoint safety**: Only the ordered acknowledged prefix advances the checkpoint. Pending disk pages drain before later queued pages or new source reads.
4. **Shutdown**: Undelivered pages are persisted in order when possible. Otherwise the durable source cursor stays behind them for replay; the source must retain the records.

### 429-Specific Behavior

Sentinel may return HTTP 429 when the DCR's ingestion quota is exceeded. `sendToDestination` (`internal/pipeline/pusher.go`) retries it like any other transient error, with exponential backoff (`retry.Retrier`), and counts each retry in `loghub_retries_total`. Repeated failures also count toward the destination's circuit breaker.

For production, configure `pipeline.retry.base_backoff` of at least 1 second to avoid hammering Sentinel during rate limiting windows.

## MSSQL Cursor Contract

LogHub uses cursor-based pagination to collect MSSQL records without skipping or duplicating. The cursor encodes `(timestamp_column, cursor_column)` as a JSON object.

### Requirements for Cursor Columns

| Requirement | Description |
|-------------|-------------|
| `timestamp_column` monotonicity | Values must increase over time. Multiple records may share the same timestamp (e.g., batch inserts). |
| `cursor_column` uniqueness | Must be unique and stable within the scope of a given timestamp. Primary key or auto-increment ID is ideal. |
| `cursor_column` stability | Values must never change for a given record after insertion. |
| Query ordering | `ORDER BY timestamp_column ASC, cursor_column ASC` |

### Cursor Query Logic

With `cursor_column` set, LogHub generates:
```sql
SELECT TOP (fetch_size) * FROM table
WHERE timestamp_column > @ts_cursor
   OR (timestamp_column = @ts_cursor AND cursor_column > @tie_cursor)
ORDER BY timestamp_column ASC, cursor_column ASC
```

The dual-field comparison ensures:
- Records with duplicate timestamps are ordered by `cursor_column`
- The tie-breaker prevents skipping records that share a timestamp with the cursor position

`source.fetch_size` controls how many rows each poll reads from MSSQL. `runtime.send_batch_size` controls how many rows each destination request carries after polling.

### Cursor Comparison

The cursor is serialized as JSON when a tie-breaker is needed:
```json
{"timestamp":"2026-05-06T12:00:00Z","tie":"000123"}
```

Without a tie-breaker, only the timestamp is used as the cursor.

### Null Timestamp Handling

Null `timestamp_column` values are replaced with the current time at collection. This is a known limitation: such records are not filtered out at the SQL level and may be reprocessed if the query is re-run before the substituted timestamp passes. Production tables should use a `NOT NULL` constraint on `timestamp_column`.

### Clock and Timezone Assumptions

- `timestamp_column` must contain UTC values
- Timestamps are compared as-is without timezone conversion
- MSSQL `DATETIME2` with UTC time is recommended
- Local-time timestamps must be converted to UTC before ingestion

### Late-Arriving Records

Records that arrive with an older `timestamp_column` value than previously collected are a known limitation:

- The cursor-based query cannot revisit earlier timestamps
- Such records will not be re-collected after the cursor advances
- If late-arriving records are possible, consider using a window-based collection strategy or accept data loss for out-of-order inserts

### Required Index

The production MSSQL table must have an index supporting the cursor query pattern:

```sql
CREATE INDEX ix_cursor ON dbo.MSSQLAudit (event_time ASC, id ASC)
```

The index must cover both `timestamp_column` and `cursor_column` in that order to support:
1. Efficient seek on `timestamp_column > @cursor`
2. Efficient tie-breaking with `cursor_column > @tie_cursor`

### Duplicate Timestamp Test Results

The cursor contract is validated by `TestMockMSSQLToMockSentinelLargePagedNoDuplicates` in `tests/mssql_sentinel/mssql_sentinel_test.go`. This test verifies that 2500 records with timestamps grouped in 25-record batches (25 records share each timestamp) are delivered to Sentinel without duplicates or gaps.

## Acceptance Criteria

- [ ] Custom table exists in Log Analytics with `_CL` suffix
- [ ] DCR stream name matches `Custom-{TableName}` format
- [ ] All timestamps in `TimeGenerated` are UTC
- [ ] Stream schema in DCR includes all emitted fields
- [ ] KQL canary query returns expected records within 5 minutes
- [ ] Production MSSQL table has an index supporting `(event_time, id)` cursor query
- [ ] `timestamp_column` is NOT NULL with UTC values
- [ ] `cursor_column` is unique and stable
- [ ] Late-arriving record behavior is documented and accepted
- [ ] MSSQL cursor column index exists for production query volume
- [ ] `fetch_size` and `poll_interval` tuned to expected event rate
- [ ] Retry configuration tested with 429 responses
