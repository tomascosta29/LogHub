# LogHub Production Runbook

This runbook provides operational procedures for running LogHub in production. For deployment configuration details, see [deployment.md](deployment.md). For failure modes and recovery, see [failure-modes.md](failure-modes.md). For ingestion validation, see [canary-test-plan.md](canary-test-plan.md).

---

## Table of Contents

1. [Deployment](#deployment)
2. [Managed Identity Configuration](#managed-identity-configuration)
3. [Azure Permissions](#azure-permissions)
4. [Validating Ingestion](#validating-ingestion)
5. [Checkpoint Inspection](#checkpoint-inspection)
6. [Dead Letter Queue](#dead-letter-queue)
7. [Disk Buffer Recovery](#disk-buffer-recovery)
8. [Secret Rotation](#secret-rotation)
9. [Rollback Procedure](#rollback-procedure)

---

## Deployment

See [deployment.md](deployment.md) for full deployment instructions.

### Quick Start

```bash
# Run LogHub with production config
./loghub --config /config/loghub.prod.yaml

# Container deployment
docker run \
  -v /path/to/config:/config \
  -v /data:/data \
  loghub:latest
```

### Required Directories

| Directory | Purpose |
|----------|---------|
| `/data/checkpoints` | Cursor position tracking |
| `/data/disk-buffer/{pipeline_id}` | Memory buffer overflow |
| `/data/dlq/{pipeline_id}` | Failed batch storage |

---

## Managed Identity Configuration

LogHub uses Azure Managed Identity for authentication to Azure services.

### MSSQL Access (ad-managed-identity)

Configure the MSSQL source to use Active Directory Managed Identity:

```yaml
pipelines:
  - id: mssql-audit-to-sentinel
    runtime:
      send_batch_size: 100
    source:
      type: mssql
      mssql:
        server: "<sql-server>.database.windows.net"
        database: "<db>"
        table: "AuditLog"
        timestamp_column: "event_time"
        cursor_column: "id"
        fetch_size: 100
        auth:
          method: "ad-managed-identity"
```

### Sentinel Ingestion (Managed Identity)

Configure the Sentinel destination with Managed Identity authentication:

```yaml
pipelines:
  - id: mssql-audit-to-sentinel
    destinations:
      - type: sentinel
        sentinel:
          dce: "https://<dce-name>.<region>.ingest.monitor.azure.com"
          dcr: "<dcr-immutable-id>"
          stream: "Custom-MSSQLAudit_CL"
          timeout: "30s"
          # client_id: "<user-assigned-managed-identity-client-id>"
```

Where no Managed Identity exists (a workstation, CI outside Azure), use a service principal with the "Monitoring Metrics Publisher" role on the DCR:

```yaml
        sentinel:
          # dce, dcr, stream as above
          auth:
            method: "ad-service-principal"
            tenant_id: "<tenant-id>"
            client_id: "<app-id>"
            client_secret: "${env:SENTINEL_CLIENT_SECRET}"
```

### KeyVault Access

LogHub accesses KeyVault for secrets using Managed Identity:

```yaml
secrets:
  keyvault-prod:
    type: keyvault
    vault_url: "https://<keyvault-name>.vault.azure.net/"
    tenant_id: "<tenant-id>"  # Optional; see below
```

Without `client_id`, LogHub authenticates with `DefaultAzureCredential`, and `tenant_id` sets the tenant it uses (for the environment, workload identity and developer credentials it tries). With `client_id`, LogHub uses that user-assigned managed identity, which always authenticates in its own tenant; `tenant_id` is then ignored, with a warning.

---

## Azure Permissions

### Grant KeyVault Access (RBAC)

The Managed Identity requires the `Key Vault Secrets User` role for secret retrieval.

```bash
# Get the Managed Identity principal ID
MI_OBJECT_ID=$(az ad sp list \
  --filter "clientId eq '<mi-client-id>'" \
  --query '[0].id' -o tsv)

# Grant Key Vault Secrets User role
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee-object-id $MI_OBJECT_ID \
  --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<keyvault-name>
```

Alternatively, via Azure Portal: KeyVault → Access Control (IAM) → Role Assignments → Add role assignment.

### Grant DCR Ingestion Permission

The Managed Identity requires `Monitoring Metrics Publisher` role on the Data Collection Rule.

```bash
# Get the MI principal ID
MI_OBJECT_ID=$(az ad sp list \
  --filter "clientId eq '<mi-client-id>'" \
  --query '[0].id' -o tsv)

# Assign Monitoring Metrics Publisher on the DCR
az role assignment create \
  --role "Monitoring Metrics Publisher" \
  --assignee-object-id $MI_OBJECT_ID \
  --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Insights/dataCollectionRules/<dcr-name>
```

### Grant SQL Access

Create a user in the database mapped to the Managed Identity and grant SELECT on the required table.

```sql
-- Create user for system-assigned Managed Identity
CREATE USER [<identity-name>] FROM EXTERNAL PROVIDER;

-- Grant SELECT on specific table
GRANT SELECT ON SCHEMA::dbo TO [<identity-name>];

-- For user-assigned Managed Identity, use the client ID to find the SP
-- The user name format is: https://identity.azure.net/<tenant-id>/<mi-client-id>
```

---

## Validating Ingestion

### KQL Validation Query

After starting LogHub, validate that records are arriving in Sentinel:

```kusto
MSSQLAudit_CL
| where event_time >= ago(5m)
| where id contains "<unique-test-run-id>"
| order by TimeGenerated desc
| take 10
```

For detailed canary testing procedures, see [canary-test-plan.md](canary-test-plan.md).

### Expected Behavior

| Check | Pass Criteria |
|-------|----------------|
| Record count | Matches source records |
| TimeGenerated format | UTC ISO 8601 with `Z` suffix |
| Field mapping | Matches source columns exactly |
| Latency | Records appear within 5 minutes |

---

## Checkpoint Inspection

Checkpoints track the cursor position for each pipeline. Each pipeline's cursor is kept in a journal; every save appends a line, and the journal is compacted to the latest cursor every 1000 saves.

### Checkpoint Directory Structure

```
<checkpoint_dir>/
  .lock                          # held by the running LogHub process
  <pipeline_id>.journal          # cursor journal (the last committed entry is current)
  <pipeline_id>.cursor.migrated  # v0.1.0 cursor file, if one was imported
```

### Inspect Checkpoint

```bash
# Print the current cursor (journal payloads are base64-encoded)
tail -n 2 /data/checkpoints/my-pipeline.journal | jq -r 'select(.data) | .data | @base64d'

# Example output with an MSSQL tie-breaker cursor:
v1:{"timestamp":"2026-05-07T10:30:00Z","tie":"000123"}
```

### Checkpoint Fields

| Field | Description |
|-------|-------------|
| `timestamp` | Last processed `timestamp_column` value |
| `tie` | Last processed `cursor_column` value (for tie-breaking) |

Without a `cursor_column`, the cursor is the bare timestamp: `v1:2026-05-07T10:30:00Z`.

### Invalid Checkpoint Cursor

If a SQL source logs `checkpoint cursor "...": invalid cursor`, the pipeline stops collecting rather than guess where to resume. A guess would re-read, and re-send, the whole table. `/health` shows the pipeline as `degraded` with that error. Causes:

- **`unsupported version`**: the checkpoint was written by a newer LogHub, for example after a rollback. Roll forward again, or reset the cursor.
- **anything else**: the checkpoint is corrupt.

To reset, stop LogHub and move `<checkpoint_dir>/<pipeline_id>.journal` aside. On the next start the source begins from the start of the table, or from `max_lookback` if set. To resume from a known point instead, set `max_lookback` for one start.

---

## Dead Letter Queue

Entries are written to the DLQ when a destination rejects them, when a destination still fails after all retries, or when a processor rejects them.

### DLQ Files

```
<dlq_dir>/
  <pipeline_id>.dlq.journal                  # current queue
  <pipeline_id>.dlq.journal.<ts>.archive     # rotated at durability.dlq.max_size_mb
  <pipeline_id>.dlq.migrated                 # v0.1.0 file, already imported
```

`<dlq_dir>` is `durability.dlq.dir`, or `<global.data_dir>/dlq/<pipeline_id>`. The journal files store each record base64-encoded, so read them with `loghub dlq` rather than `cat`. The command only reads the files and is safe while LogHub is running.

### Inspect DLQ Entries

```bash
# Records and entries per pipeline and destination
loghub dlq list --config /config/loghub.prod.yaml

# Every record as one JSON object per line, oldest first
loghub dlq export --config /config/loghub.prod.yaml --pipeline my-pipeline | jq

# Include rotated archives
loghub dlq export --config /config/loghub.prod.yaml --pipeline my-pipeline --archives > dlq.jsonl
```

### DLQ Entry Format

```json
{
  "timestamp": "2026-05-07T10:30:00Z",
  "pipeline_id": "my-pipeline",
  "entries": [...],
  "error": "bad request: schema mismatch",
  "destination": "sentinel#0"
}
```

`destination` is `"processor"` for entries a processor rejected, or the key of the destination that failed or rejected them: its type and its position in the pipeline's `destinations` list (`sentinel#0` is the first destination). Replay those entries to that destination only; the pipeline's other destinations already have them.

### Replay Procedure

LogHub does not replay the DLQ itself. To replay:

1. **Export the records** (the files stay in place):
   ```bash
   loghub dlq export --config /config/loghub.prod.yaml --pipeline my-pipeline --archives > dlq.jsonl
   ```

2. **Identify and fix the root cause** (e.g., fix DCR schema, correct data transformation).

3. **Re-ingest the failed entries** to the destination named in each record, using one of:
   - A script that reads `dlq.jsonl` and POSTs each record's `entries` to the ingestion API
   - Modify the source query to re-collect the failed records (using a timestamp range)

4. **Verify replay** using the KQL validation query.

5. **Clear the replayed records** by stopping LogHub and moving the DLQ files away:
   ```bash
   kill -SIGTERM <pid>
   mkdir -p /data/dlq-replayed && mv /data/dlq/my-pipeline/my-pipeline.dlq.journal* /data/dlq-replayed/
   ```

---

## Disk Buffer Recovery

When the memory buffer is full, or a destination needs a batch retried, batches are kept in the disk buffer.

### Disk Buffer Files

```
<disk_buffer_dir>/
  <pipeline_id>.buffer.journal       # pending batches and delivered markers
  <pipeline_id>.buffer[.drain].migrated   # v0.1.0 files, already imported
```

Each batch is marked delivered as soon as it is handled, so a drain that stops (or a restart) only re-delivers what is still pending. The file is emptied once every batch has been delivered. While it holds pending batches, the pipeline does not poll its source; the `loghub_disk_buffer_batches` gauge shows how many are waiting.

### Recover from Disk Buffer Growth

**Option 1: Increase disk buffer size**

If disk space allows, increase the pipeline's `durability.disk_buffer.max_size_mb`:

```yaml
pipelines:
  - id: my-pipeline
    durability:
      disk_buffer:
        enabled: true
        max_size_mb: 5120  # 5GB
```

**Option 2: Investigate destination backpressure**

Disk buffer growth usually indicates Sentinel/destination is slow. Check:
- Sentinel ingestion metrics in Azure Portal
- `loghub_retries_total` metric for retry spikes
- `loghub_circuit_breaker_state` for an open breaker
- Network connectivity to Azure Monitor endpoints

**Option 3: Wait for automatic drain**

The pipeline retries the drain every poll interval and resumes polling the source once the disk buffer is empty. No operator action required if backpressure is temporary.

---

## Secret Rotation

For secrets stored in KeyVault, use the standard Azure rotation procedure.

LogHub reads secrets when it builds pipelines, at startup and on each reload (SIGHUP), and keeps using those values until the next build. Every `global.secret_refresh_interval` (default 5m) it looks the cached secrets up again:

- If a value changed, LogHub logs `Secret value changed` with the secret reference (never the value). With `global.reload_on_secret_change: true` it then reloads by itself, which stops and rebuilds every pipeline as SIGHUP does.
- If a lookup fails (e.g. Key Vault is unreachable), the last resolved value stays in use. The failure is logged and counted in `loghub_secret_refresh_errors_total`, so a Key Vault outage does not break a running LogHub or a reload.

### KeyVault Secret Rotation

1. **Update the secret in KeyVault**:
   ```bash
   az keyvault secret set \
     --vault-name <keyvault-name> \
     --name <secret-name> \
     --value "<new-secret-value>"
   ```

2. **Apply it.** Keep the old secret valid until this step has finished.
   - With `reload_on_secret_change: true`, wait up to `secret_refresh_interval`; the log shows `Secret value changed; reloading pipelines`, then `Config reloaded`.
   - Otherwise, reload now. A reload looks every secret up again, so it does not wait for the refresh interval:
     ```bash
     kill -HUP <pid>
     # Kubernetes: kubectl exec deploy/loghub -- kill -HUP 1
     ```
     A restart also works.

3. **Verify** that the reload logged `Config reloaded` and that ingestion resumed. A `Some secrets could not be looked up` warning means the reload used the old value; fix Key Vault access and reload again.

### Managed Identity Rotation

User-assigned Managed Identity client secrets require rotation before expiry:

1. **Create new credential**:
   ```bash
   az ad app credential create \
     --id <app-id> \
     --append \
     --display-name "LogHub MI Key $(date +%Y-%m-%d)" \
     --end-date "<new-expiry-date>"
   ```

2. **Update KeyVault secret** with the new client secret (if stored).

3. **Update LogHub config** with the new secret reference or restart if using KeyVault.

4. **Verify** the pipeline is running with the new credential.

---

## Rollback Procedure

### Container Image Rollback

```bash
# Rollback to previous image tag
kubectl set image deployment/loghub loghub=<registry>/loghub:<previous-tag>

# Or for Docker
docker run <registry>/loghub:<previous-tag>
```

### Configuration Rollback

```bash
# Restore previous config
cp /config/loghub.prod.yaml.backup /config/loghub.prod.yaml

# Restart with previous config
kill -SIGTERM <pid>
./loghub --config /config/loghub.prod.yaml
```

### Checkpoint Safety on Rollback

Rollback does not affect checkpoints. The cursor position is preserved in the checkpoint directory and is forward-only. After rollback:
- The same cursor position is used
- No data is lost or duplicated
- In-flight batches at the time of shutdown are replayed (at-least-once semantics)

---

## Quick Reference

| Operation | Command |
|-----------|---------|
| Start pipeline | `./loghub --config /config/loghub.prod.yaml` |
| Stop pipeline | `kill -SIGTERM <pid>` |
| View checkpoint | `tail -n 2 /data/checkpoints/<pipeline_id>.journal \| jq -r 'select(.data) \| .data \| @base64d'` |
| View DLQ | `loghub dlq list --config /config/loghub.prod.yaml` |
| Export DLQ | `loghub dlq export --config /config/loghub.prod.yaml --pipeline <pipeline_id> \| jq` |
| Print version | `loghub --version` |
| Check disk buffer | `ls -lh /data/disk-buffer/` |
| View metrics | `curl http://localhost:9090/metrics` |
| Health check | `curl http://localhost:9090/health` |
