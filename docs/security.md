# Security Review - LogHub MSSQL to Azure Sentinel

## Overview

This document outlines the security posture review for production deployment of LogHub, covering least-privilege access, secret handling, and data exposure controls.

## Least Privilege Access Matrix

### Azure Key Vault

| Identity | Permission | Scope | Justification |
|----------|------------|-------|---------------|
| LogHub Managed Identity | `Get` (keyvault Secrets User role) | Only required secrets | Access only secrets that LogHub needs to resolve |
| No list permission required | - | Vault-level | LogHub only retrieves specific named secrets |

**RBAC Configuration:**
```bash
# Grant Key Vault Secrets User role to LogHub Managed Identity
az role assignment create \
  --assignee <managed-identity-client-id> \
  --role "Key Vault Secrets User" \
  --scope "/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault-name>"
```

### Data Collection Rule (DCR) Ingestion

| Identity | Permission | Scope | Justification |
|----------|------------|-------|---------------|
| LogHub Managed Identity | `Monitoring Metrics Publisher` | Specific DCR only | Least privilege for Sentinel ingestion |

**RBAC Configuration:**
```bash
# Grant Monitoring Metrics Publisher on specific DCR
az role assignment create \
  --assignee <managed-identity-client-id> \
  --role "Monitoring Metrics Publisher" \
  --scope "/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Insights/dataCollectionRules/<dcr-name>"
```

### MSSQL Database

| Identity | Permission | Scope | Justification |
|----------|------------|-------|---------------|
| LogHub Managed Identity | `SELECT` | Specific tables/views only | Read-only access to audit data |

**SQL Configuration:**
```sql
-- Create user for Managed Identity
CREATE USER [<managed-identity-name>] FROM EXTERNAL PROVIDER;

-- Grant SELECT on specific table only
GRANT SELECT ON [AuditDB].[dbo].[AuditEvents] TO [<managed-identity-name>];

-- For custom queries, create a view with only needed columns
CREATE VIEW dbo.LogHubAuditView AS
SELECT event_time, id, event_type, actor, action, status
FROM AuditEvents
WHERE <filter conditions>;
```

## Secret Handling

### No Secrets In

| Location | Status | Evidence |
|----------|--------|----------|
| Config files | Compliant | All examples use placeholders (`${provider:secret}` or `your-*.database.windows.net`) |
| Logs | Compliant | No secret values logged; errors only contain error context |
| Metrics | Compliant | Prometheus metrics do not expose secrets |
| Panic output | Compliant | No sensitive data in panic messages |
| Tests | Compliant | Test fixtures use placeholder values (e.g., `"password"`, `"secret-token"`) |

### Secret Resolution Behavior

- **Strict Mode** (`strict_resolution: true`): Unresolved secret references cause fatal startup failure
- **Non-Strict Mode** (default): Unresolved references pass through as-is (can hide misconfiguration)

**Recommendation**: Use `strict_resolution: true` in production to fail fast on missing secrets.

### Key Vault Secret Names

Use descriptive, restricted secret names to limit exposure:
- `${keyvault-prod:mssql-password}` - MSSQL auth password (if using SQL auth)
- `${keyvault-prod:sp-client-secret}` - Service principal secret (if using SP auth)

## HTTP Destination Authorization Headers

**Status**: VERIFIED - Authorization headers are NOT logged

Review of `internal/plugins/destination/http.go`:
- Headers from config are set on the HTTP request (lines 99-101)
- Response body is read with a 1024-byte limit for error handling (line 112)
- On read failure, only the error is logged: `slog.Warn("Failed to read response body", "error", err)` (line 114)
- **The Authorization header value is never logged**

Similarly, `internal/plugins/destination/sentinel.go`:
- Token acquisition failures log only the error message (line 145)
- The bearer token itself is never exposed in logs

## TLS Enforcement

### Azure Endpoints

All Azure endpoints use HTTPS (TLS 1.2+ enforced by Azure SDK):

| Endpoint | TLS Required | Verification |
|----------|-------------|--------------|
| Azure Key Vault | TLS 1.2+ | Azure SDK enforces minimum TLS 1.2 |
| Azure Monitor Ingestion API | TLS 1.2+ | Azure SDK enforces minimum TLS 1.2 |
| Azure SQL Database / SQL Server | TLS 1.2+ | Encryption is always on (`encrypt` is forced to `true`). The server certificate is validated unless `trust_server_certificate: true`, which logs a warning |

**Note**: The MSSQL source uses Microsoft's maintained SQL Server driver, `github.com/microsoft/go-mssqldb` (it replaced the archived `denisenkom/go-mssqldb` in 2026-10). For production, leave `trust_server_certificate` unset (`false`) so the certificate is validated; set it only for test servers with self-signed certificates.

## Outbound Network Restriction Recommendations

Restrict outbound traffic to only required endpoints:

| Destination | Port | Purpose |
|-------------|------|---------|
| MSSQL Server | 1433 (TCP) | Database queries |
| Key Vault | 443 (HTTPS) | Secret resolution |
| Azure Monitor Ingestion | 443 (HTTPS) | Sentinel log ingestion |
| DCE (if private link) | 443 (HTTPS) | Private link ingestion |

### Network Security Group (NSG) Rules

```bash
# Allow outbound to MSSQL
az network nsg rule create \
  --resource-group <rg> \
  --nsg-name <nsg-name> \
  -n allow-mssql-outbound \
  --priority 100 \
  --destination-address-prefix <sql-server-ip> \
  --destination-port 1433 \
  --direction Outbound \
  --access Allow

# Allow outbound to Azure Monitor (all regions via service tag)
az network nsg rule create \
  --resource-group <rg> \
  --nsg-name <nsg-name> \
  -n allow-azure-monitor-outbound \
  --priority 110 \
  --destination-address-prefix AzureMonitor \
  --destination-port 443 \
  --direction Outbound \
  --access Allow

# Allow outbound to Key Vault
az network nsg rule create \
  --resource-group <rg> \
  --nsg-name <nsg-name> \
  -n allow-keyvault-outbound \
  --priority 120 \
  --destination-address-prefix <keyvault-ip-or-tag> \
  --destination-port 443 \
  --direction Outbound \
  --access Allow
```

## Sensitive MSSQL Field Filtering

Remove or mask sensitive fields at the source, before they leave the database. LogHub's processors cannot remove or mask individual fields: the `filter` processor keeps or drops whole records.

### Recommended Field Filtering

| Field Type | Action | Implementation |
|------------|--------|----------------|
| Passwords | EXCLUDE | Leave out of `columns` or the query |
| Credit card numbers | MASK | Tokenize or hash in the query |
| SSN / National IDs | MASK | Partial masking in the query |
| API keys | EXCLUDE | Leave out of `columns` or the query |
| PII (names, emails) | REVIEW | Confirm allowed in Sentinel; apply retention policy |

### Column Allowlist

List the approved columns in the source's `columns` setting. Only those are selected; see [data-governance.md](data-governance.md).

```yaml
source:
  type: "mssql"
  mssql:
    table: "AuditEvents"
    timestamp_column: "event_time"
    columns: ["event_time", "id", "event_type", "actor_user", "action", "status"]
```

To mask rather than drop a field, select a masked expression in a custom query, as below.

### Custom Query for Field Restriction

```sql
-- Instead of SELECT *, enumerate only needed columns
SELECT
    event_time,
    id,
    event_type,
    actor_user,
    action,
    target_resource,
    status,
    ip_address
FROM AuditEvents
WHERE event_time > @cursor
ORDER BY event_time ASC
```

## Security Checklist

### Pre-Deployment

- [ ] Managed Identity created with no unnecessary permissions
- [ ] Key Vault Secrets User role granted only for required secrets
- [ ] DCR Monitoring Metrics Publisher role scoped to specific DCR only
- [ ] MSSQL user has SELECT on specific tables/views only
- [ ] `strict_resolution: true` configured in production
- [ ] No real credentials in config files or code
- [ ] TLS 1.2+ enforced for all connections
- [ ] Outbound network restricted to required endpoints
- [ ] Sensitive MSSQL fields filtered or masked before ingestion

### Configuration Review

- [ ] Config examples use placeholder values only
- [ ] Secret references use `${provider:name}` syntax
- [ ] Production config loaded and validated
- [ ] Checkpoint, disk buffer and DLQ directories on persistent storage, owned by the LogHub user. LogHub creates new state files `0600` and directories `0700`; files created by earlier releases keep their mode, so `chmod` them once

### Monitoring

- [ ] Logs reviewed for any secret exposure
- [ ] Metrics reviewed for any sensitive data
- [ ] Alert on failed secret resolution (strict mode)
- [ ] Alert on authentication/authorization failures

## References

- [Azure Key Vault Security](https://learn.microsoft.com/en-us/azure/key-vault/general/security-features)
- [Azure Monitor Logs Ingestion API](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/custom-logs-overview)
- [Managed Identity for Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-azure-ad-user-assigned-managed-identity)
- [RBAC for Data Collection Rules](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-rule-overview)
