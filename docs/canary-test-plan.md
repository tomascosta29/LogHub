# LogHub Azure Canary Test Plan

This document defines the procedure for validating LogHub MSSQL-to-Sentinel ingestion using real Azure resources.

## Objective

Run an end-to-end canary test to confirm that LogHub can:
1. Connect to an MSSQL test source
2. Authenticate to Azure using Managed Identity
3. Send records to a Sentinel-connected Log Analytics workspace via the Azure Monitor Logs Ingestion API
4. Have those records appear in the target custom table with correct schema

---

## Prerequisites

The following Azure resources and access must be available before running this canary:

| Requirement | Description | How to Verify |
|-------------|-------------|---------------|
| Azure Subscription | Active subscription where non-production resources reside | `az account show` |
| Log Analytics Workspace | Non-production workspace connected to Sentinel | Azure Portal > Sentinel > Workspaces |
| Sentinel Enabled | Microsoft Sentinel solution enabled on the workspace | Azure Portal > Sentinel > Configuration |
| Custom Table | `MSSQLAudit_CL` (or as named in your DCR) created in the workspace | `az monitor log-analytics workspace table list -g <rg> -w <workspace>` |
| Data Collection Rule (DCR) | DCR with stream declaration matching the custom table | Azure Portal > Monitor > Data Collection Rules |
| Data Collection Endpoint (DCE) | DCE if required by the DCR (private link or newer DCRs) | DCR Overview in Azure Portal |
| Managed Identity | System or user-assigned MI assigned to the LogHub deployment | Azure Portal > Managed Identity |
| MI Permission on DCR | `Monitoring Metrics Publisher` role assigned to the MI on the DCR | DCR Access Control (IAM) |
| MI Permission on Workspace | Log Analytics Contributor or custom role for DCR ingestion | Workspace Access Control (IAM) |
| MSSQL Test Server | Azure SQL Database or SQL Managed Instance for testing | Azure Portal > SQL servers |
| MI Permission on SQL | MI has `db_datareader` on the test database | SQL Server > Azure AD admin |
| Network Access | LogHub can reach MSSQL, Key Vault, and Azure Monitor endpoints | NSG/Firewall rules |

### Azure CLI Verification Commands

```bash
# Verify subscription access
az account show

# List Log Analytics workspaces
az monitor log-analytics workspace list -g <resource-group>

# Verify Sentinel is enabled (should show as "Sentinel" solution)
az monitor log-analytics solution list -g <resource-group>

# List custom tables in the workspace
az monitor log-analytics workspace table list -g <resource-group> -w <workspace-name>

# List DCRs
az monitor data-collection rule list -g <resource-group>

# Verify MI permissions on DCR
az role assignment list --assignee <mi-client-id> --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Insights/dataCollectionRules/<dcr>
```

---

## Low-Cost Test Plan

The whole canary can run for pennies if everything lives in one resource group that is deleted afterwards. Run most tests from a workstation with a service principal (Phase A, no compute cost), then do one short run on an Azure VM to test managed identities (Phase B). Prices change; check the [Azure pricing calculator](https://azure.microsoft.com/pricing/calculator/).

| Resource | Choice | Cost |
|---|---|---|
| Log Analytics workspace, custom table, DCE, DCR | Pay-as-you-go; the first 5 GB/month of ingestion are free | $0 for a canary's few MB |
| Microsoft Sentinel | Not needed to test ingestion; new workspaces get a 31-day trial if you want it | $0 |
| Azure SQL Database | Free offer (serverless, auto-pauses) | $0 |
| VM for Phase B | `Standard_B1s`, for an hour or two | cents |

Avoid: Azure Container Registry (copy the binary instead), Defender for SQL / Defender for Cloud prompts, provisioned SQL tiers, Log Analytics commitment tiers.

### Shared Setup

```bash
RG=loghub-canary; LOC=westeurope; WS=loghub-canary-ws
az group create -n $RG -l $LOC
az monitor log-analytics workspace create -g $RG -n $WS -l $LOC
az monitor data-collection endpoint create -g $RG -n loghub-canary-dce -l $LOC --public-network-access Enabled
```

Then create the custom table and the DCR as in Steps 2 and 3, in `$RG`. Note the DCE's logs ingestion URL and the DCR's immutable ID; the config needs both. Since LogHub sends every field as a string, declare `event_time` as `string` in the DCR stream and convert it in the transform: `"transformKql": "source | extend event_time = todatetime(event_time)"`.

Practical notes from the first run:

- **Regions:** newer subscriptions can be refused in busy regions ("The selected region is currently not accepting new customers"), separately per resource type. The first run used North Europe for the workspace, DCE and DCR, and Sweden Central for SQL; resources need not share a region.
- **CLI extensions:** `az monitor data-collection` installs the `monitor-control-service` extension, which fails if `az` was installed without `pip` (e.g. with `uv`). Creating the DCE and DCR with `az rest --method put` against `Microsoft.Insights/dataCollectionEndpoints` and `dataCollectionRules` (api-version `2023-03-11`) needs no extension.
- **Querying:** `az rest --resource https://api.loganalytics.io --url https://api.loganalytics.io/v1/workspaces/<customerId>/query` runs KQL without the `log-analytics` extension.

Azure SQL with the free offer, plus a firewall rule for your workstation:

```bash
SQL=loghub-canary-sql-$RANDOM
az sql server create -g $RG -n $SQL -l $LOC --admin-user loghubadmin --admin-password '<strong-password>'
az sql db create -g $RG -s $SQL -n loghubdb -e GeneralPurpose -f Gen5 -c 2 \
  --compute-model Serverless --use-free-limit --free-limit-exhaustion-behavior AutoPause
az sql server firewall-rule create -g $RG -s $SQL -n workstation \
  --start-ip-address <your-public-ip> --end-ip-address <your-public-ip>
```

Create the test table from Step 5 with one more column, `details NVARCHAR(256) NOT NULL` holding a small JSON object (e.g. `{"src":"canary"}`), for scenario A5.

### Phase A: Workstation E2E with a Service Principal

Create a service principal and let it publish to the DCR:

```bash
az ad sp create-for-rbac -n loghub-canary-sp   # note appId, password, tenant
az role assignment create --assignee <appId> --role "Monitoring Metrics Publisher" \
  --scope $(az monitor data-collection rule show -g $RG -n <dcr-name> --query id -o tsv)
```

Role assignments can take a few minutes to apply. Use the Step 6 config with these changes:

```yaml
secrets:
  env:
    type: env
pipelines:
  - id: mssql-canary-to-sentinel
    # ... runtime, retry, durability as in Step 6, with local directories
    source:
      type: mssql
      mssql:
        # ... as in Step 6
        auth:
          method: "sql"
          user: "loghubadmin"
          password: "${env:SQL_PASSWORD}"
    processors:
      - type: json_parse        # for A5: a row whose details is not JSON is rejected on its own
        json_parse:
          source_field: "details"
          on_error: "poison"
    destinations:
      - type: sentinel
        sentinel:
          # dce, dcr, stream as in Step 6
          auth:
            method: "ad-service-principal"
            tenant_id: "<tenant>"
            client_id: "<appId>"
            client_secret: "${env:SENTINEL_CLIENT_SECRET}"
```

```bash
export SQL_PASSWORD='<strong-password>' SENTINEL_CLIENT_SECRET='<password from create-for-rbac>'
make build && ./loghub --config loghub-canary.yaml
```

Run these scenarios. Use a fresh `run_id` prefix for each, and allow 5–15 minutes for rows to show up in Log Analytics. Check duplicates and gaps with:

```kusto
MSSQLAudit_CL
| where id startswith "<run_id>"
| summarize rows = count(), distinct_ids = count_distinct(id)
```

Use `count_distinct()`, not `dcount()`: `dcount()` is an estimate and can report more distinct IDs than rows. Run one LogHub at a time against the table, or a second pipeline will collect the same rows and the counts will show duplicates that LogHub did not cause.

| # | Scenario | Steps | Pass criteria |
|---|---|---|---|
| A1 | Baseline | Insert 10 rows (Step 7) | `rows == distinct_ids == 10`; checks in "What to Verify" pass |
| A2 | Graceful restart | Insert 2000 rows; after the first batches arrive, `kill -TERM` LogHub and start it again | `rows == distinct_ids == 2000` |
| A3 | Hard kill | As A2 but `kill -9` | `distinct_ids == 2000`; `rows - distinct_ids` is at most one batch (at-least-once) |
| A4 | Destination outage | Start LogHub with `dce` set to an unreachable address (e.g. `https://127.0.0.1:9`), insert 500 rows, wait for `loghub_circuit_breaker_state` to show 1 (open), then restart with the real `dce`. (Alternatively, add `127.0.0.1 <dce-host>` to `/etc/hosts`; that needs root.) | The first one or two batches are in the DLQ (`loghub dlq list` shows them under `sentinel#0`); the rest wait in the disk buffer (`loghub_disk_buffer_batches` > 0), the source is not polled meanwhile, and they arrive in order after recovery. Every row is in the table or the DLQ |
| A5 | Malformed row | Insert a row whose `details` is not JSON, between good rows | The row is in the DLQ under `processor`; the rows around it arrive |
| A6 | No `cursor_column` | Stop the main pipeline. Run a copy of the config without `cursor_column`, with a fresh checkpoint directory and `max_lookback: "10s"` (so it does not read the whole table), insert 100 rows, let it poll for a few minutes | `rows == distinct_ids == 100` (rows are not re-sent on each poll) |
| A7 | v0.1.0 upgrade (optional) | Start on a copy of a v0.1.0 data directory | Resumes from the old cursor; old files renamed `*.migrated` |
| A8 | SQL via Entra (recommended before Phase B) | Give the SQL server a Microsoft Entra admin (`az sql server ad-admin create`); connected as that admin with an Entra token (`az account get-access-token --resource https://database.windows.net/`; a SQL-auth login cannot do this), run `CREATE USER [loghub-canary-sp] FROM EXTERNAL PROVIDER; ALTER ROLE db_datareader ADD MEMBER [loghub-canary-sp];`; switch the source to `auth: {method: "ad-service-principal", tenant_id, client_id, client_secret}` | A1 passes. This uses the same token path as the managed-identity run, so problems show up here before the VM |

### Phase B: Managed Identities on One VM

```bash
az identity create -g $RG -n loghub-canary-uami
az vm create -g $RG -n loghub-canary-vm --image Ubuntu2404 --size Standard_B1s \
  --assign-identity '[system]' loghub-canary-uami --admin-username azureuser --generate-ssh-keys
DCR_ID=$(az monitor data-collection rule show -g $RG -n <dcr-name> --query id -o tsv)
for P in $(az vm show -g $RG -n loghub-canary-vm --query identity.principalId -o tsv) \
         $(az identity show -g $RG -n loghub-canary-uami --query principalId -o tsv); do
  az role assignment create --assignee-object-id $P --assignee-principal-type ServicePrincipal \
    --role "Monitoring Metrics Publisher" --scope $DCR_ID
done
az sql server firewall-rule create -g $RG -s $SQL -n vm \
  --start-ip-address <vm-public-ip> --end-ip-address <vm-public-ip>
```

For the MSSQL source to use `ad-managed-identity`, give the server a Microsoft Entra admin, then in the database run `CREATE USER [loghub-canary-vm] FROM EXTERNAL PROVIDER; ALTER ROLE db_datareader ADD MEMBER [loghub-canary-vm];` and the same for `[loghub-canary-uami]`.

The workstation's glibc may be newer than the VM's, so copy a static binary:

```bash
make build-static VERSION=canary
scp loghub loghub-canary.yaml azureuser@<vm-public-ip>:
```

| # | Scenario | Config | Pass criteria |
|---|---|---|---|
| B1 | System-assigned MI | Step 6 config as is (`auth.method: ad-managed-identity`, no `client_id`) | A1 passes |
| B2 | User-assigned MI | Add `client_id: <uami-client-id>` to the destination, and `client_id` to the source's `auth` | A1 passes |

#### Alternative: Azure Container Instances

New subscriptions often cannot create a small VM: the B-series sizes may be capacity-restricted in the region, and the families that are available may have a quota of 0 (`QuotaExceeded ... Current Limit: 0`). Container Instances support both kinds of managed identity, bill per second, and need no VM quota; production also runs in containers. The first run used them:

1. Upload the static binary (`make build-static`), the two configs, the host's CA bundle (`/etc/ssl/certs/ca-certificates.crt`) and a start script to an Azure Files share. Configs for managed identities hold no secrets.
2. The start script waits for a flag file, then runs `SSL_CERT_FILE=/mnt/loghub/ca-certificates.crt /mnt/loghub/loghub-bin --config /mnt/loghub/<b1|b2>.yaml`. The stock `ubuntu:24.04` image has no CA certificates, hence the bundle.
3. Create one container group per scenario, 0.5 vCPU and 0.5 GB, from `docker.io/library/ubuntu:24.04`, with the share mounted and `--assign-identity '[system]'` (B1) or `--assign-identity <uami-resource-id>` (B2). No registry is needed.
4. Grant the identities as above. The system-assigned identity's database user is named after its container group.
5. Create the flag file. The first SQL login fails with `Client with IP address '<ip>' is not allowed`; add a firewall rule for that single IP rather than "Allow Azure services".
6. Run one container at a time, or both pipelines collect the same rows.

If `az storage file upload` fails with `ParentNotFound`, upload with the Azure Files REST API (`PUT` with `x-ms-type: file`, then `PUT ?comp=range` in 4 MiB ranges) using a share SAS.

### Teardown

```bash
az group delete -n $RG --yes --no-wait
az ad sp delete --id <appId>   # service principals live in the tenant, not the resource group
```

Remove the `/etc/hosts` line if it is still there, and confirm no Sentinel trial or Defender plan is left enabled.

## Canary Test Flow

### Step 1: Identify or Create Non-Production Resources

Use an existing non-production workspace or create one:

```bash
# Option: Create a new non-production workspace (if needed)
az monitor log-analytics workspace create \
  -g <nonprod-resource-group> \
  -n <nonprod-workspace> \
  --location eastus
```

Ensure Sentinel is connected to this workspace via the Sentinel portal.

### Step 2: Create the Custom Table

Create the custom table before ingestion. It must exist or the Logs Ingestion API will return 400.

```bash
az monitor log-analytics workspace table create \
  -g <resource-group> \
  -w <workspace-name> \
  -n MSSQLAudit_CL \
  --type CustomLog \
  --schema '{"columns":[{"name":"TimeGenerated","type":"datetime"},{"name":"id","type":"string"},{"name":"event_time","type":"datetime"},{"name":"user_name","type":"string"},{"name":"action","type":"string"}]}'
```

> **Note:** Table creation via CLI may have limitations. The table can also be created via Azure Portal or ARM template.

### Step 3: Create or Identify the DCR and Stream

The DCR must:
- Declare a stream matching the table name: `Custom-MSSQLAudit_CL`
- Have a transformation or accept all declared fields
- Target the Log Analytics workspace

If creating a new DCR:

```bash
az monitor data-collection rule create \
  -g <resource-group> \
  -n <dcr-name> \
  --location eastus \
  --rule-file dcr-config.json
```

Example `dcr-config.json`:

```json
{
  "properties": {
    "dataCollectionEndpointId": "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Insights/dataCollectionEndpoints/<dce>",
    "streamDeclarations": {
      "Custom-MSSQLAudit_CL": {
        "columns": [
          { "name": "TimeGenerated", "type": "datetime" },
          { "name": "id", "type": "string" },
          { "name": "event_time", "type": "datetime" },
          { "name": "user_name", "type": "string" },
          { "name": "action", "type": "string" }
        ]
      }
    },
    "destinations": {
      "logAnalytics": [
        {
          "workspaceResourceId": "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<workspace>",
          "name": "MSSQLAudit-Dest"
        }
      ]
    },
    "dataFlows": [
      {
        "streams": ["Custom-MSSQLAudit_CL"],
        "destinations": ["MSSQLAudit-Dest"]
      }
    ]
  }
}
```

### Step 4: Assign LogHub Managed Identity Permissions

Assign `Monitoring Metrics Publisher` role to the MI on the DCR:

```bash
# Get the MI principal ID (objectId)
MI_OBJECT_ID=$(az ad sp list --filter "clientId eq '<mi-client-id>'" --query '[0].id' -o tsv)

# Assign Monitoring Metrics Publisher on the DCR
az role assignment create \
  --role "Monitoring Metrics Publisher" \
  --assignee-object-id $MI_OBJECT_ID \
  --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Insights/dataCollectionRules/<dcr>
```

Alternatively, use the DCR's Access Control (IAM) in the Azure Portal.

### Step 5: Prepare the Test MSSQL Source

Create a small test table in a non-production MSSQL database:

```sql
CREATE TABLE LogHubCanaryTest (
    id NVARCHAR(64) PRIMARY KEY,
    event_time DATETIME2 NOT NULL,
    user_name NVARCHAR(128),
    action NVARCHAR(64)
);

INSERT INTO LogHubCanaryTest (id, event_time, user_name, action)
VALUES
    ('canary-2026-05-07-001', GETUTCDATE(), 'test-user-001', 'login'),
    ('canary-2026-05-07-002', GETUTCDATE(), 'test-user-002', 'denied');
```

Alternatively, use the test fixture in `tests/mssql_sentinel/` if available.

### Step 6: Configure and Run LogHub

Create a canary config file (e.g., `loghub-canary.yaml`):

```yaml
global:
  checkpoints_dir: /var/lib/loghub/checkpoints

pipelines:
  - id: mssql-canary-to-sentinel
    runtime:
      poll_interval: 10s
      buffer_size: 1000
      send_batch_size: 100
      shutdown_timeout: 30s
    retry:
      max_attempts: 3
      base_backoff: 2s
    durability:
      disk_buffer:
        enabled: true
        dir: /var/lib/loghub/buffer/mssql-canary-to-sentinel
      dlq:
        enabled: true
        dir: /var/lib/loghub/dlq/mssql-canary-to-sentinel
    source:
      type: mssql
      mssql:
        server: "<sql-server>.database.windows.net"
        database: "<test-db>"
        table: "LogHubCanaryTest"
        timestamp_column: "event_time"
        cursor_column: "id"
        fetch_size: 100
        auth:
          method: "ad-managed-identity"
    destinations:
      - type: sentinel
        sentinel:
          dce: "https://<dce-name>.<region>.ingest.monitor.azure.com"
          dcr: "<dcr-immutable-id>"
          stream: "Custom-MSSQLAudit_CL"
          timeout: "30s"
```

Run LogHub:

```bash
./loghub --config loghub-canary.yaml
```

### Step 7: Generate Unique test_run_id Records

Insert records with a unique identifier that can be queried:

```sql
DECLARE @run_id NVARCHAR(64) = 'canary-' + FORMAT(GETUTCDATE(), 'yyyy-MM-dd-HHmmss');
INSERT INTO LogHubCanaryTest (id, event_time, user_name, action)
VALUES
    (@run_id + '-001', GETUTCDATE(), 'canary-user-001', 'login'),
    (@run_id + '-002', GETUTCDATE(), 'canary-user-002', 'logout');
```

Record the `@run_id` value (e.g., `canary-2026-05-07-120000`).

---

## KQL Validation Query

After waiting ~2-5 minutes for ingestion, query Sentinel using the unique test run ID:

```kusto
let test_run_id = "canary-2026-05-07-120000";
MSSQLAudit_CL
| where id startswith test_run_id
| order by TimeGenerated desc
```

Or directly filter by the unique ID segment:

```kusto
MSSQLAudit_CL
| where id contains "canary-2026-05-07-120000"
| order by TimeGenerated desc
```

---

## Expected Result Format

A successful query should return records like:

| TimeGenerated | id | event_time | user_name | action |
|---------------|-----|------------|-----------|--------|
| 2026-05-07T12:00:01.000Z | canary-2026-05-07-120000-001 | 2026-05-07T12:00:01.000Z | canary-user-001 | login |
| 2026-05-07T12:00:02.000Z | canary-2026-05-07-120000-002 | 2026-05-07T12:00:02.000Z | canary-user-002 | logout |

---

## What to Verify

| Check | How to Verify | Pass Criteria |
|-------|---------------|---------------|
| Record count | Count rows returned by KQL | Matches number of inserted records |
| TimeGenerated format | Inspect the TimeGenerated field | UTC ISO 8601 / RFC 3339 with `Z` suffix |
| Field mapping | Check each column in KQL result | Matches source MSSQL columns exactly |
| Timestamp accuracy | Compare TimeGenerated to event_time | TimeGenerated matches event_time (UTC) |
| No duplicate records | Query by unique id | Each id appears exactly once |
| Latency | Compare event_time to query time | Records appear within 5 minutes |
| DCR transformation | Verify if transformation was applied | Fields are correctly mapped per DCR |

---

## Recording Results

Record each run so it can be compared with the next one:

| Field | Value |
|-------|-------|
| Date | |
| LogHub version | |
| Workspace | |
| Table | |
| DCR | |
| DCR Immutable ID | |
| Managed Identity | |
| Records Sent | |
| Records Received | |
| Validation Query | |
| Result | PASS / FAIL |

For each scenario, record Pass or Fail with notes: row counts, distinct IDs, DLQ contents, and anything unexpected. If a scenario cannot be run (for example, missing access to a resource), record what is missing and why.

---

## References

- [Azure Monitor Logs Ingestion API](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/custom-logs-overview)
- [Logs Ingestion API Tutorial](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-api)
- [DCR Overview](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-rule-overview)
- [Managed Identity for Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-azure-ad-user-assigned-managed-identity)
- `docs/production-sentinel.md` - Schema contract and field mapping reference
