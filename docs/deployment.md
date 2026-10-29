# LogHub Deployment Guide

## Runtime Targets

LogHub is a Go binary that can be deployed on any platform supporting containerized or VM-based workloads.

| Platform | Managed Identity | Notes |
|----------|-----------------|-------|
| Container (Docker/Podman) | N/A - use workload identity | Requires token mounting |
| VM (Azure VM/VMSS) | System-assigned / User-assigned | Use Azure Identity SDK |
| Azure Container Apps | System-assigned / User-assigned | Built-in workload identity |
| Azure Kubernetes Service (AKS) | Via aad-pod-identity or workload identity | Requires pod identity configuration |
| Azure App Service | System-assigned | Built-in managed identity |

## Network Access Requirements

Configure firewall rules or network security groups to allow:

| Endpoint | Port | Purpose |
|----------|------|---------|
| MSSQL (e.g., `*.database.windows.net`) | 1433 | Database source/destination |
| Key Vault (e.g., `*.vault.azure.net`) | 443 | Secrets retrieval |
| Azure Monitor Ingestion | 443 | `dc.services.visualstudio.com` or DCE private endpoint |

## Data Directories and Permissions

LogHub requires persistent storage for:

| Directory | Default Path | Purpose |
|-----------|--------------|---------|
| Checkpoints | `./data/checkpoints` | Cursor position tracking |
| Disk Buffer | `./data/disk-buffer/{pipeline_id}` | Overflow when memory buffer is full |
| Dead Letter Queue | `./data/dlq/{pipeline_id}` | Failed message storage |

**File Permissions:** The process user must have read/write access to these directories. For container deployments, mount persistent volumes (PVCs or Azure Files) to these paths.

## Entry Point and Flags

```bash
loghub [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-config` | `loghub.yaml` | Path to configuration file |
| `-metrics` | `:9090` | Address for Prometheus metrics server when `global.metrics_enabled: true` |

## Environment Variables

Environment variables are not used for configuration; all settings come from the YAML config file specified via `-config`.

## Health and Metrics Endpoints

Metrics and health endpoints are disabled unless `global.metrics_enabled: true` is set in the YAML config.

| Endpoint | Port | Description |
|----------|------|-------------|
| `/health` | 9090 (or `-metrics` value) | Returns 200 OK if orchestrator is running, 503 if stopped |
| `/metrics` | 9090 (or `-metrics` value) | Prometheus metrics |

## Graceful Shutdown Behavior

LogHub handles `SIGTERM` and `SIGINT` signals for graceful shutdown:

1. Receives shutdown signal
2. Stops all pipelines in parallel. For each one:
   1. Cancels the pipeline context (stops new ingestion)
   2. Waits for the producer, then for in-flight batches to be delivered. One `shutdown_timeout` (default: 30s) covers both waits.
   3. If the timeout expires, abandons the in-flight sends and waits up to 5s more while the interrupted batches are kept for retry (in the disk buffer, if configured), not dead-lettered
   4. Closes source, destination, disk buffer, and DLQ connections
3. Exits once every pipeline has stopped, or at the shutdown deadline: the longest `shutdown_timeout` + 5s drain grace + 5s for closing connections (40s with the defaults). Each pipeline that timed out, failed to close something, or was still stopping at the deadline is logged as an error naming the pipeline.

Set the container's termination grace period above that deadline, or the kubelet kills LogHub mid-drain; `k8s/deployment.yaml` uses 45s. A batch interrupted by a kill is re-read from the source on restart (at-least-once), not lost.

The `shutdown_timeout` can be configured globally or per-pipeline:

```yaml
global:
  shutdown_timeout: 30s
```

## Resource Recommendations

HTTP and Sentinel destinations share connection pools when their transport
settings match. Set `global.http_max_idle_conns_per_host` to change the idle
connection limit (default 100; omitted or zero uses the default, negative values
are rejected). This also sets the total idle connection limit for that pool;
it does not limit active requests. Pools expire idle connections after 90 seconds
and bound TLS handshakes to 10 seconds. Closing one destination keeps the pool
available to the others; the last close releases its idle connections.

Responses retain at most 1 KB for diagnostics and drain up to another 64 KB to
allow connection reuse. Larger responses are closed without an unbounded drain.
See [performance testing](performance.md) for local checks and measurements.

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| Memory | 512Mi | 1Gi+ |
| CPU | 0.5 cores | 1-2 cores |

Memory requirements scale with `buffer_size` and disk buffer usage. The default `buffer_size` is 100 batches in memory.

## Configuration Example

```yaml
global:
  checkpoints_dir: /data/checkpoints
  shutdown_timeout: 30s
  metrics_enabled: true

pipelines:
  - id: my-pipeline
    runtime:
      buffer_size: 100
      poll_interval: 1s
      shutdown_timeout: 30s
    retry:
      max_attempts: 4
      base_backoff: 1s
    durability:
      disk_buffer:
        enabled: true
        dir: /data/disk-buffer/my-pipeline
      dlq:
        enabled: true
        dir: /data/dlq/my-pipeline
    source:
      type: mssql
      mssql:
        server: "<sql-server>.database.windows.net"
        database: "<db>"
        table: "AuditLog"
        timestamp_column: "event_time"
        cursor_column: "id"
        auth:
          method: "ad-managed-identity"
    destinations:
      - type: sentinel
        sentinel:
          dce: "https://<dce-name>.<region>.ingest.monitor.azure.com"
          dcr: "<dcr-immutable-id>"
          stream: "Custom-MSSQLAudit_CL"
```

## Container Deployment Notes

- Run as non-root user with read/write access to mounted volumes
- Set `readOnlyRootFilesystem: false` if using disk buffer or DLQ
- Configure `terminationGracePeriodSeconds` above `shutdown_timeout` + 10s (45s for the 30s default); see Graceful Shutdown Behavior
- Liveness/readiness probes can use `/health` endpoint

## Volume Mounts for Kubernetes

```yaml
volumeMounts:
  - name: loghub-data
    mountPath: /data
    subPath: loghub
  - name: loghub-config
    mountPath: /config
volumes:
  - name: loghub-data
    persistentVolumeClaim:
      claimName: loghub-data-pvc
  - name: loghub-config
    configMap:
      name: loghub-config
```

## Monitoring and Alerting

See [docs/monitoring.md](monitoring.md) for detailed Prometheus metrics, alerting rules, Grafana dashboard examples, and failure-mode testing guidance.
