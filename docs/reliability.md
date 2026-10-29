# Reliability and Delivery Guarantees

LogHub provides at-least-once delivery semantics with cursor-based progress tracking. This document describes the guarantees operators can rely on and the behaviors that ensure correctness.

## Cursor-Based Source Polling

The puller owns a read cursor separate from the acknowledged cursor. It may
collect one page ahead of delivery: at most two source pages are being
collected, queued, or delivered. `buffer_size`, the capacity of the in-memory
page queue in pages, must be at least 1, and a larger value does not increase
this window.

The pusher processes pages and their sub-batches in order. Only that ordered
acknowledged prefix can advance a checkpoint. An older failed page blocks later
delivery and new reads until it is delivered or durably dead-lettered. A read
already in progress may finish and queue its page behind the failure.

SQL cursors only move forward, by timestamp (and a tie-breaker for equal
timestamps). A row committed with a timestamp older than the cursor (a
long-running transaction, a backfill, or a writer whose clock is behind) is
never read. Use a timestamp the database assigns at commit time, or keep
writers' clocks in sync, where that matters.

## File Source

The `file` source follows one path. Its cursor is the byte offset just past the last complete line, plus the file's device and inode on Linux and macOS.

- **Partial lines.** A final line without a newline is held back until the newline arrives.
- **Rotation by rename.** When a new file appears at the path, the old file is read to its end first. A file that is rotated in and out between two polls is never opened, and its lines are lost. Rotation must be slower than `poll_interval`.
- **Truncation (copytruncate).** The source starts again from the beginning when the file shrinks below the offset, or when the content before the offset has changed: the byte before it is no longer a newline, or the file's first bytes differ. Lines written between the last poll and the truncation are lost; that is inherent to copytruncate.
- **Encoding.** Lines are treated as UTF-8. Invalid byte sequences are replaced with U+FFFD, because the JSON that destinations receive cannot carry them.

## Checkpoint Semantics

Checkpoints are saved **only after** successful processing and delivery. `processBatch` in `pusher.go` handles each sub-batch in this sequence:

1. Apply processors. If a processor fails on the batch, each entry is retried on its own and only the entries that still fail are written to the DLQ.
2. If no entries are left, save the checkpoint and advance the cursor.
3. Send to every destination that still needs the sub-batch, in parallel, each with its own retries and circuit breaker.
4. If every destination accepted the entries or has its failures in the DLQ, save the checkpoint and advance the cursor.
5. Otherwise do **not** advance the cursor; keep the sub-batch, narrowed to the destinations that still need it, in the disk buffer (or bounded memory when buffering is unavailable) for retry.

## Failed Destination Sends

Each destination is tracked separately, so a failure at one destination never causes a resend to destinations that already accepted the entries:

- **Circuit breaker open**: the sub-batch is kept in the disk buffer for that destination only; it is not written to the DLQ.
- **Retries exhausted**: the sub-batch is written to the DLQ for that destination and the cursor advances. Without a DLQ, it is kept in the disk buffer for that destination and the cursor stays put.
- While the disk buffer holds batches, the puller does not poll the source, so the same records are not collected twice. The pusher retries the drain every poll interval.
- Without a disk buffer, or if writing it fails, the pusher retains the failed prefix in memory and retries it before later pages. On shutdown, unpersisted pages remain behind the durable source checkpoint and are read again on restart. The source must retain those records until recovery.

## Dead Letter Queue (DLQ)

When DLQ is enabled, failed batches are persisted for later inspection and replay:

- **Processor failures**: the rejected entries are written with destination `"processor"`, and the cursor **is** advanced. This prevents infinite retry loops on malformed data.
- **Destination failures and per-entry rejections**: the entries are written with the destination's key, `<type>#<position>` (for example `sentinel#0` for the first destination), so a replay only needs to target that destination. The cursor advances once the DLQ write succeeds.
- Without a DLQ, rejected entries are dropped and counted on `loghub_entries_dropped_total`.
- DLQ entries include the pipeline ID, timestamp, destination, error message, and the entries.

## Disk Buffer Drain and Commit

When a delivery failure or interrupted shutdown leaves undelivered pages:

1. If disk buffering is enabled, the undelivered pages are written to its journal in order. The acknowledged cursor is **not** advanced; it travels with each page. Queue saturation applies backpressure to collection rather than spilling speculative pages.
2. While the disk buffer holds batches, the puller does not poll the source, so the same records are not collected twice.
3. The pusher drains the batches in the order they were written. Each one is marked delivered in the journal as soon as it is handled.
4. If only part of a batch was delivered (one destination is down, or a later sub-batch failed), the batch is replaced, in one journal transaction, by what is left. The replacement keeps the original's place in the order. The drain then stops and is retried every poll interval.
5. On restart, the pending batches are recovered from the same journal, so only undelivered batches are delivered again.

## Crash and Restart Behavior

LogHub provides at-least-once delivery. After a crash or restart:

- The checkpointer stores the cursor after successful delivery.
- **Potential duplicate delivery**: Batches that were committed to the destination but not yet checkpointed may be replayed on restart.
- The file checkpointer appends to each pipeline's journal and fsyncs a commit barrier. Operations on one pipeline serialize with each other; unrelated pipelines can write, recover or compact concurrently. Closing the checkpointer waits for active operations and rejects new operations after closure.
- The read cursor is never persisted as an acknowledgement. On restart, saved disk-buffer pages drain first; collection then resumes from the acknowledged cursor.

## State File Integrity

Checkpoints, the disk buffer and the DLQ are journals: append-only files in which a commit marker, written with an fsync, closes each transaction.

- **Crash leftovers.** After a crash a journal can end in a torn record or zero fill past its last commit. That tail was never committed; it is removed when the journal is opened.
- **Corruption.** An unreadable record before the last commit means the file was damaged after it was written. LogHub refuses to start and names the file, rather than treating it as empty: that would restart a cursor from the beginning or discard a backlog. See the runbook's "Corrupt state file" section.
- **Ownership.** A checkpoint records the pipeline it belongs to. A checkpoint copied from another pipeline, or kept across a rename, is refused.
- **Access.** New state files are created `0600` and new state directories `0700`. A state path that is a symlink is refused. The checkpoint directory is locked as well as its `.lock` file, so deleting the lock file does not let a second instance in.

## MSSQL Duplicate Timestamp Handling

MSSQL sources may produce rows with identical timestamp values. To ensure correct cursor advancement and prevent data loss or duplication, configure `cursor_column` in addition to `timestamp_column`:

```yaml
source:
  type: "mssql"
  mssql:
    table: "SystemLogs"
    timestamp_column: "CreatedDate"
    cursor_column: "Id"  # required for unique ordering
```

Without `cursor_column`, identical timestamps can cause the cursor to stall or skip rows. The same applies to SQLite sources.

## SQLite Timestamp Comparison

SQLite has no date type, so the SQLite source compares the timestamp column with the cursor as time, using `julianday()`. Text in any SQLite date format works, with a space or a `T`, any number of fractional digits, and an optional `Z` or `±HH:MM` offset. Times without a zone are read as UTC unless `source_timezone` is set. Values `julianday()` cannot parse, such as integer epoch seconds, fall back to a plain comparison. In 1.0.0 and earlier, the comparison was textual, so rows written as `YYYY-MM-DD HH:MM:SS` were skipped when the cursor was `...T...Z`.

- `julianday()` resolves milliseconds. Without `cursor_column`, each poll re-reads the cursor's millisecond and drops rows already delivered, comparing them at 100 ns precision. Rows that differ by less than 100 ns are treated as identical.
- Keep one format per column. If a column mixes formats, rows within one millisecond can be ordered incorrectly.
- An index on the plain column is not used. On large tables, add an expression index such as `CREATE INDEX events_time ON events(julianday(event_time))`, or keep `fetch_size` and the table small. Custom `query` sources are unchanged: their `WHERE` clause is used as written.

## Azure Monitor / Sentinel Managed Identity Token Acquisition

LogHub supports Azure Monitor Logs Ingestion API authentication using Managed Identity. This enables production Sentinel ingestion without static bearer tokens.

### Destination Configuration

The `sentinel` (or `azure_monitor`) destination authenticates with a Managed Identity by default:

```yaml
destination:
  type: "sentinel"
  sentinel:
    dcr: "<DCR Immutable ID>"
    stream: "<Stream Name>"
    dce: "<Data Collection Endpoint>"
    timeout: "30s"                     # optional
    client_id: "<User-Assigned MI Client ID>"  # optional, omit for system-assigned MI
```

Outside Azure (for example, end-to-end tests from a workstation) a Managed Identity is not available. Use a service principal instead, with its secret taken from a secret provider:

```yaml
destination:
  type: "sentinel"
  sentinel:
    dcr: "<DCR Immutable ID>"
    stream: "<Stream Name>"
    dce: "<Data Collection Endpoint>"
    auth:
      method: "ad-service-principal"
      tenant_id: "<tenant-id>"
      client_id: "<app-id>"
      client_secret: "${env:SENTINEL_CLIENT_SECRET}"
```

`auth.method` is `ad-managed-identity` (the default; `auth.client_id` selects a user-assigned identity) or `ad-service-principal`, the same names the MSSQL source uses. Set `client_id` either at the top level or inside `auth`, not both.

### Token Acquisition Behavior

- **Scope**: `https://monitor.azure.com/.default` (Azure Monitor Logs Ingestion API)
- **Credential type**:
  - `azidentity.ManagedIdentityCredential`: system-assigned when no `client_id` is set, user-assigned when it is
  - `azidentity.ClientSecretCredential` for `auth.method: ad-service-principal`
- **Token caching**: Handled automatically by `azidentity`; tokens are cached and refreshed as needed
- **Token refresh**: The `GetToken` call returns a cached token if valid; Azure SDK handles refresh automatically

### Token Provider Design

```go
// internal/plugins/destination/sentinel.go

type tokenProvider struct {
    cred   azcore.TokenCredential
}

func (tp *tokenProvider) GetToken(ctx context.Context) (string, error) {
    azToken, err := tp.cred.GetToken(ctx, policy.TokenRequestOptions{
        Scopes: []string{azureMonitorScope},
    })
    if err != nil {
        return "", fmt.Errorf("token acquisition failed: %w", err)
    }
    return azToken.Token, nil
}
```

### Error Handling

Token acquisition failures surface as send failures:

```go
token, err := s.tokenProvider.GetToken(ctx)
if err != nil {
    slog.Warn("Sentinel token acquisition failed", "error", err)
    return fmt.Errorf("sentinel token acquisition failed: %w", err)
}
```

This ensures:
- Failed token acquisition triggers retry behavior via the pipeline's standard retry mechanism
- No access tokens are logged (only the error is logged)
- The cursor is not advanced until the batch is successfully sent

### DCR Endpoint Format

LogHub constructs the Azure Monitor Logs Ingestion API URL as:

```
{DCE}/dataCollectionRules/{DCR Immutable ID}/streams/{Stream Name}?api-version=2023-01-01
```

`DCE` is required. Use the Logs Ingestion endpoint from the Data Collection
Endpoint associated with the DCR, for example
`https://<dce-name>.<region>.ingest.monitor.azure.com`.

### Security Notes

- Access tokens are never logged
- Static bearer tokens are not required for production Sentinel ingestion
- Managed Identity permissions on the DCR should be `Monitoring Metrics Publisher`

## MSSQL Authentication Modes

LogHub supports four authentication methods for MSSQL sources. The three Microsoft Entra methods share their names and behaviour with the Sentinel destination's `auth` block.

| Method | Use Case | Required `auth` properties |
|--------|----------|----------------------------|
| `sql` | On-prem or dev SQL | `user`, `password` |
| `ad-managed-identity` | Azure SQL from an Azure host (recommended) | none; `client_id` selects a user-assigned identity |
| `ad-service-principal` | Azure SQL from outside Azure (CI, workstation tests) | `tenant_id`, `client_id`, `client_secret` |
| `ad-password` | Azure SQL with an Entra user (deprecated by Microsoft; cannot satisfy MFA) | `user`, `password`, `client_id` (the app the user signs in through); `tenant_id` optional |

For `sql`, the user and password are sent in the connection string; they are URL-escaped, so they may contain `;`, `=`, `@` or spaces. For the Entra methods, LogHub gets an access token for `https://database.windows.net/.default` from the configured identity and hands it to the driver for each new connection; no credential appears in the connection string.

```yaml
source:
  type: "mssql"
  mssql:
    server: "your-sql-server.database.windows.net"   # also host:port, host,port or host\instance
    database: "your-database"
    auth:
      method: "ad-managed-identity"
      # client_id: "<user-assigned-identity-client-id>"
    table: "AuditLogs"
    timestamp_column: "CreatedAt"
```

The database needs a user for the identity (managed identity or service principal), created by a Microsoft Entra admin of the server:

```sql
CREATE USER [<identity-or-app-name>] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [<identity-or-app-name>];
```

### Least Privilege Database Permissions

The MSSQL user for LogHub should have only `SELECT` on the required tables:

```sql
-- Create a dedicated reader user
CREATE USER [loghub-reader] FROM EXTERNAL PROVIDER; -- for MI auth
-- OR for SQL auth:
-- CREATE LOGIN [loghub-reader] WITH PASSWORD = '...';
-- CREATE USER [loghub-reader] FOR LOGIN [loghub-reader];

-- Grant minimal required permissions
GRANT SELECT ON SCHEMA::dbo TO [loghub-reader];
-- Or for specific tables:
-- GRANT SELECT ON [dbo].[AuditLogs] TO [loghub-reader];
```

### Production Auth Mode Selection

- **Azure SQL from an Azure host**: Use `ad-managed-identity`, system-assigned or user-assigned. No secrets required.
- **Azure SQL from outside Azure**: Use `ad-service-principal`, with the client secret in Key Vault or an environment secret.
- **On-prem SQL**: Use `sql` auth with credentials in Key Vault.
