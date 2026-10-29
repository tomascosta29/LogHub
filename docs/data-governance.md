# Data Governance: MSSQL to Sentinel

This document governs what data from MSSQL audit logs is sent to Microsoft Sentinel, ensuring compliance, cost control, and security.

## Approved Field List

The following fields are approved for ingestion into the Sentinel custom table `MSSQLAudit_CL`:

| MSSQL Column | Sentinel Field | Type | Required | Notes |
|--------------|----------------|------|----------|-------|
| `id` | `id` | string | Yes | Unique event identifier; used for deduplication |
| `event_time` | `TimeGenerated` | datetime | Yes | UTC timestamp; maps to Sentinel's primary timeline field |
| `event_time` | `event_time` | datetime | Yes | Retained for reference queries |
| `user_name` | `user_name` | string | Yes | Actor who performed the action |
| `action` | `action` | string | Yes | Action type: login, denied, logout, etc. |

**Total: 5 fields**

## Selecting Columns

Without a `columns` setting, the SQL sources read every column (`SELECT *`). That sends Sentinel all of them, including:

- Columns not needed for security analytics
- High-cardinality columns that increase ingestion volume
- Potentially sensitive or PII fields that were never meant for Sentinel
- Noisy fields that add storage cost without security value

In production, list the approved fields in `columns`. The source then selects only those:

```yaml
pipelines:
  - id: mssql-audit-to-sentinel
    source:
      type: mssql
      mssql:
        table: AuditLog
        timestamp_column: event_time
        cursor_column: id
        columns:
          - id
          - event_time
          - user_name
          - action
```

Column and table names must be plain identifiers (letters, digits and `_`). For a table in another schema, or for a join or computed column, use a custom `query` instead of `table`.

## Required Fix

The MSSQL source must be updated to select only the approved fields explicitly:

```sql
SELECT TOP (100) id, event_time, user_name, action
FROM dbo.AuditLog
WHERE event_time > @cursor
ORDER BY event_time ASC
```

This requires adding a configurable column list to the MSSQL source plugin, defaulting to the approved set.

## PII / Security-Sensitive Field Analysis

| Field | Sensitivity | Allowed in Sentinel | Rationale |
|-------|-------------|---------------------|-----------|
| `id` | Low | Yes | Anonymous event identifier; no PII |
| `event_time` | None | Yes | Timestamp; required for timeline |
| `user_name` | Medium | Yes | May be a username or app identity; Sentinel is a security analytics platform where this is necessary |
| `action` | Low | Yes | Action classification; no PII |

**Fields to EXCLUDE unless explicitly approved:**

- `session_id` - high cardinality, often unnecessary for analytics
- `ip_address` - PII in many jurisdictions; requires legal review before inclusion
- `email`, `phone`, `address` - explicit PII; must not be sent to Sentinel without legal approval
- `password_hash`, `salt`, `token` - security credentials; must never leave the database
- `ssn`, `credit_card`, `dob` - regulated PII; prohibited

If `user_name` contains UPN format (e.g., `user@domain.com`), Microsoft Entra user IDs should be preferred over display names to avoid storing email addresses in Sentinel.

## Retention Policy

| Setting | Value | Notes |
|---------|-------|-------|
| Sentinel custom table retention | 30-90 days | Default is 30 days; configurable up to 90 days in Log Analytics |
| Recommended retention | 30 days | Balances cost and security analytics needs |
| Archive approach | Export to Azure Storage or Data Lake for long-term retention | Not included in Sentinel costs |

**Cost impact**: Each extra field per record adds ~20-50 bytes. Over 1M records/day, one extra string field (~30 bytes) adds ~30MB/day or ~900MB/month in ingestion volume.

## Ingestion Volume and Cost Estimates

### Record Size

| Field | Estimated Size |
|-------|---------------|
| `TimeGenerated` | 24 bytes (ISO 8601 UTC) |
| `id` | 8-16 bytes (string) |
| `event_time` | 24 bytes (ISO 8601 UTC) |
| `user_name` | 16-64 bytes |
| `action` | 8-16 bytes |
| **Total per record** | **~80-140 bytes** |

### Volume Estimates (5 approved fields only)

| Events/day | Raw bytes/day | GB/month (est.) | Sentinel cost/month |
|------------|---------------|-----------------|---------------------|
| 10,000 | 1.4 MB | 0.04 GB | ~$2.60 |
| 100,000 | 14 MB | 0.4 GB | ~$26 |
| 1,000,000 | 140 MB | 4.2 GB | ~$273 |
| 10,000,000 | 1.4 GB | 42 GB | ~$2,730 |

*Based on Azure Sentinel ~$65/GB ingestion (Log Analytics ingestion + Sentinel add-on).*

### Impact of Extra Fields

Adding just 2 unneeded string columns (~40 bytes/record):

| Events/day | Extra GB/month | Extra cost/month |
|------------|-----------------|-------------------|
| 1,000,000 | +1.2 GB | +$78 |
| 10,000,000 | +12 GB | +$780 |

## Field Filtering / Transformations

### Current Behavior

The pipeline passes all fields from MSSQL through to Sentinel without filtering.

### Required Transformations

1. **Timestamp mapping**: `event_time` → `TimeGenerated` (already documented in `production-sentinel.md`)
2. **UTC enforcement**: `TimeGenerated` must be RFC 3339 format with `Z` suffix
3. **Field allowlist**: only approved fields are transmitted (`columns`, see above)

## References

- `docs/production-sentinel.md` - Sentinel field mapping and schema
- `internal/plugins/source/mssql.go` (`BuildQuery`) - Query builder; projects `columns` when set
- `tests/mssql_sentinel/mssql_sentinel_test.go` - Test suite for MSSQL-Sentinel pipeline
- Azure Sentinel pricing: https://azure.microsoft.com/en-us/pricing/details/azure-sentinel/
- Log Analytics pricing: https://azure.microsoft.com/en-us/pricing/details/monitor/
