# LogHub

LogHub is a Go log ingestion engine built around reliable, cursor-based pipelines. Its production target is MSSQL or Azure SQL audit data delivered to Microsoft Sentinel through the Azure Monitor Logs Ingestion API with Managed Identity authentication.

## What It Does

- Polls sources with durable cursor checkpoints.
- Processes batches through configurable processors.
- Sends records to one or more destinations with retry/backoff behavior.
- Supports disk spillover and dead-letter queues for operational recovery.
- Resolves secrets from environment variables or Azure Key Vault.
- Can expose Prometheus metrics and a health endpoint when enabled.

## Production Path

The primary production path is:

```text
MSSQL / Azure SQL -> LogHub -> Azure Monitor Logs Ingestion API -> Microsoft Sentinel
```

For Sentinel, LogHub sends flattened JSON records made from `LogEntry.Fields` and adds `TimeGenerated` from the entry timestamp in UTC RFC 3339 format. The DCR stream schema must match those top-level fields.

## Quick Start

```bash
go test ./...
go build -o loghub ./cmd/loghub
./loghub --config loghub.yaml.example
```

The production example is in `loghub.prod.yaml.example`. It is sanitized and uses placeholders for Azure resources.

## Minimal Pipeline Example

```yaml
global:
  checkpoints_dir: "./data/checkpoints"
  strict_resolution: true
  metrics_enabled: true

pipelines:
  - id: "mssql-audit-to-sentinel"
    runtime:
      poll_interval: "5s"
      send_batch_size: 100
      buffer_size: 100
      checkpoint_interval: "30s"
      shutdown_timeout: "60s"
    retry:
      max_attempts: 4
      base_backoff: "1s"
    source:
      type: "mssql"
      mssql:
        server: "example.database.windows.net"
        database: "AuditDB"
        table: "AuditLog"
        timestamp_column: "event_time"
        cursor_column: "id"
        fetch_size: 100
        max_lookback: "5m"
        source_timezone: "Europe/Berlin"
        auth:
          method: "ad-managed-identity"
    processors:
      - type: "timestamp"
        timestamp:
          source_field: "event_time"
          source_layout: "2006-01-02T15:04:05Z07:00"
          target_field: "timestamp"
    destinations:
      - type: "sentinel"
        max_retries: 4
        sentinel:
          dce: "https://your-dce.eastus.ingest.monitor.azure.com"
          dcr: "00000000-0000-0000-0000-000000000000"
          stream: "Custom-MSSQLAudit_CL"
          timeout: "30s"
```

## Plugins

Sources:

- `file`
- `sqlite`
- `mssql`

Destinations:

- `sentinel` / `azure_monitor`
- `http`
- `local_jsonl`
- `dummy`

Processors:

- `filter`
- `json_parse`
- `timestamp`

Secrets:

- `env`
- `keyvault`

## Reliability Model

LogHub is designed for at-least-once delivery. A batch is checkpointed only after processing and destination delivery succeed. Checkpoints, the disk buffer and the DLQ are each stored in a per-pipeline journal file; use `loghub dlq list` / `loghub dlq export` to inspect the DLQ. Duplicate delivery is still possible after crashes or checkpoint failures, so Sentinel-side queries should use stable IDs where deduplication matters.

## Production Readiness

Before production, run the real Azure canary in `docs/canary-test-plan.md` and confirm:

- Managed Identity has SQL, DCR, and Key Vault permissions.
- The DCR stream schema matches the flattened payload.
- Checkpoint resume, Sentinel 429/5xx retry, DLQ, disk buffer, and rollback paths are exercised.

## Documentation

| Topic | Page |
|---|---|
| Sentinel schema contract and field mapping | [docs/production-sentinel.md](docs/production-sentinel.md) |
| Delivery guarantees, cursors, duplicates | [docs/reliability.md](docs/reliability.md) |
| Deployment, shutdown, Kubernetes | [docs/deployment.md](docs/deployment.md) |
| Operations: checkpoints, DLQ, secret rotation, rollback | [docs/runbook.md](docs/runbook.md) |
| Metrics and alerts | [docs/monitoring.md](docs/monitoring.md) |
| Failure modes and recovery | [docs/failure-modes.md](docs/failure-modes.md) |
| Throughput tuning | [docs/performance.md](docs/performance.md) |
| Security hardening | [docs/security.md](docs/security.md) |
| Data governance | [docs/data-governance.md](docs/data-governance.md) |
| Azure canary test | [docs/canary-test-plan.md](docs/canary-test-plan.md) |
| Releasing | [docs/releasing.md](docs/releasing.md) |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup, the checks CI runs, and the project guidelines. Report security issues as described in [SECURITY.md](SECURITY.md).

```bash
make test
make race
docker build -t loghub:latest .
```

## License

LogHub is licensed under the [Apache License 2.0](LICENSE).

## Sponsors

[![CRIF GmbH](https://www.crif.de/assets/img/logo-crif.svg)](https://careers.crif.com/search/?optionsFacetsDD_country=DE)
