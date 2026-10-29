# Monitoring and Alerting Guide

## Prometheus Metrics

LogHub exposes metrics at the `/metrics` endpoint (default: `:9090`). The following metrics are available:

### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `loghub_entries_collected_total` | `pipeline_id`, `source_name` | Total log entries collected from sources |
| `loghub_entries_sent_total` | `pipeline_id`, `destination_type` | Total log entries sent to destinations |
| `loghub_entries_failed_total` | `pipeline_id`, `destination_type` | Total log entries that failed to send |
| `loghub_collect_errors_total` | `pipeline_id`, `source_name` | Failed source reads (the source returned an error, e.g. database or file unreachable). Reads cancelled by shutdown are not counted. Published at 0 when the pipeline is built |
| `loghub_batches_collected_total` | `pipeline_id` | Total batches collected from sources |
| `loghub_batches_sent_total` | `pipeline_id` | Total batches sent to destinations |
| `loghub_retries_total` | `pipeline_id`, `destination_type` | Total retry attempts |
| `loghub_checkpoints_saved_total` | `pipeline_id` | Total checkpoints saved |
| `loghub_checkpoint_errors_total` | `pipeline_id` | Checkpoint writes that failed. Delivery continues and the pipeline goes `degraded`; until a checkpoint succeeds, a restart re-sends what was delivered since the last saved one |
| `loghub_dlq_entries_total` | `pipeline_id` | Total entries written to dead letter queue |
| `loghub_entries_dropped_total` | `pipeline_id`, `reason` | Entries rejected with no DLQ configured. `reason` is `processor_rejected` or `destination_rejected`. Older builds also emitted `no_disk_buffer` and `spill_failed`; pending delivery is now retained for ordered retry instead of dropped |
| `loghub_circuit_breaker_tripped_total` | `pipeline_id`, `destination_type` | Number of times the per-destination circuit breaker tripped open (or re-opened from half-open) |
| `loghub_secret_refresh_total` | `provider` | Secret lookups by the secret cache (first use, reloads, background refresh) |
| `loghub_secret_refresh_errors_total` | `provider` | Failed secret lookups; the last resolved value stays in use |

### Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `loghub_pipeline_running` | `pipeline_id` | Whether pipeline is running (1) or stopped (0) |
| `loghub_pipeline_status` | `pipeline_id`, `status` | Current pipeline state (1 for the active state per pipeline, 0 for the others). Status values: `stopped`, `starting`, `running`, `degraded`, `stalled`, `draining`, `stopping`. |
| `loghub_poll_interval_seconds` | `pipeline_id` | Configured poll interval |
| `loghub_buffer_utilization` | `pipeline_id` | Number of pending batches in memory buffer |
| `loghub_disk_buffer_batches` | `pipeline_id` | Number of batches currently stored in disk buffer |
| `loghub_circuit_breaker_state` | `pipeline_id`, `destination_type` | Current per-destination circuit breaker state (0=closed, 1=open, 2=half-open) |

### Histograms

| Metric | Labels | Description |
|--------|--------|-------------|
| `loghub_batch_size` | `pipeline_id` | Size of batches collected from the source before destination splitting |
| `loghub_collect_duration_seconds` | `pipeline_id` | Time spent collecting from sources |
| `loghub_send_duration_seconds` | `pipeline_id` | Time spent sending to destinations |

## Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'loghub'
    static_configs:
      - targets: ['loghub:9090']
    metrics_path: /metrics
    scrape_interval: 15s
    scrape_timeout: 10s
```

For Kubernetes deployments:

```yaml
---
apiVersion: v1
kind: Service
metadata:
  name: loghub-metrics
  labels:
    app: loghub
spec:
  ports:
    - name: metrics
      port: 9090
      targetPort: 9090
  selector:
    app: loghub
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: loghub
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: loghub
  endpoints:
    - port: metrics
      interval: 15s
```

## Alerting Rules

Below are recommended Prometheus alerting rules for LogHub production monitoring.

### Pipeline Health Alerts

```yaml
groups:
  - name: loghub.pipeline
    interval: 30s
    rules:
      - alert: LogHubPipelineNotRunning
        expr: loghub_pipeline_running == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} is not running"
          description: "Pipeline {{ $labels.pipeline_id }} has been stopped for more than 1 minute."

      - alert: LogHubPipelineStaleCheckpoint
        expr: |
          (
            loghub_entries_collected_total{pipeline_id=~".+"}
            - loghub_entries_collected_total{pipeline_id=~".+"} offset 5m
          ) == 0
          and
          (
            loghub_checkpoints_saved_total{pipeline_id=~".+"}
            - loghub_checkpoints_saved_total{pipeline_id=~".+"} offset 5m
          ) == 0
          and loghub_pipeline_running == 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} has stale checkpoint"
          description: "Pipeline {{ $labels.pipeline_id }} is running but no entries have been collected and no checkpoints saved in the last 5 minutes."

      - alert: LogHubCheckpointSaveFailures
        expr: |
          increase(loghub_checkpoint_errors_total[5m]) > 0
        labels:
          severity: critical
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} checkpoint failures"
          description: "Pipeline {{ $labels.pipeline_id }} failed to save checkpoints in the last 5 minutes. Delivery continues, but a restart will re-send everything since the last saved checkpoint. Check the checkpoint directory's disk space and permissions; /health shows the error."
```

### Collection Alerts

```yaml
      - alert: LogHubCollectionErrors
        expr: increase(loghub_collect_errors_total[5m]) > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} cannot read its source"
          description: "Pipeline {{ $labels.pipeline_id }} failed to read source {{ $labels.source_name }} in the last 5 minutes. Check the source's availability and credentials; the pipeline's ErrorMsg in /health has the last error."
```

### Send and Retry Alerts

```yaml
      - alert: LogHubSendFailures
        expr: rate(loghub_entries_failed_total[5m]) > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} send failures"
          description: "Pipeline {{ $labels.pipeline_id }} has send failures to {{ $labels.destination_type }}."

      - alert: LogHubRetrySpike
        expr: |
          rate(loghub_retries_total[5m])
          > 10 * rate(loghub_entries_sent_total[5m])
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} retry spike"
          description: "Pipeline {{ $labels.pipeline_id }} retries are exceeding 10x the send rate, indicating destination issues."

      - alert: LogHubHighRetryRate
        expr: |
          rate(loghub_retries_total[5m])
          / (rate(loghub_entries_sent_total[5m]) + 0.001) > 0.5
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} high retry rate"
          description: "More than 50% of sends are requiring retries for pipeline {{ $labels.pipeline_id }}."

      - alert: LogHubCircuitBreakerOpen
        expr: loghub_circuit_breaker_state > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "LogHub destination {{ $labels.destination_type }} circuit breaker open"
          description: "Circuit breaker for pipeline {{ $labels.pipeline_id }} destination {{ $labels.destination_type }} is open (or half-open). The destination is being shed to the disk buffer; investigate the destination's health."

      - alert: LogHubCircuitBreakerTripping
        expr: increase(loghub_circuit_breaker_tripped_total[15m]) > 3
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "LogHub destination {{ $labels.destination_type }} circuit breaker flapping"
          description: "Circuit breaker for pipeline {{ $labels.pipeline_id }} destination {{ $labels.destination_type }} has tripped more than 3 times in the last 15 minutes. The destination is unstable."

      - alert: LogHubSecretRefreshFailing
        expr: increase(loghub_secret_refresh_errors_total[30m]) > 0 and increase(loghub_secret_refresh_total[30m]) == increase(loghub_secret_refresh_errors_total[30m])
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "LogHub cannot look up secrets from {{ $labels.provider }}"
          description: "Every secret lookup from provider {{ $labels.provider }} failed for 30 minutes. LogHub keeps using the last values, but a rotated secret will not be picked up, and a restart would fail in strict mode. Check Key Vault access."
```

### DLQ and Disk Buffer Alerts

```yaml
      - alert: LogHubDLQGrow
        expr: |
          increase(loghub_dlq_entries_total[15m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} DLQ growth"
          description: "Pipeline {{ $labels.pipeline_id }} has written {{ $value }} entries to DLQ in the last 15 minutes."

      - alert: LogHubDLQCritical
        expr: |
          increase(loghub_dlq_entries_total[1h]) > 100
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} DLQ critical"
          description: "Pipeline {{ $labels.pipeline_id }} has written more than 100 entries to DLQ in the last hour."

      - alert: LogHubEntriesDropped
        expr: |
          increase(loghub_entries_dropped_total[5m]) > 0
        labels:
          severity: critical
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} is dropping entries"
          description: "Pipeline {{ $labels.pipeline_id }} dropped {{ $value }} entries ({{ $labels.reason }}) in the last 5 minutes. Configure a DLQ and disk buffer to keep them."

      - alert: LogHubDiskBufferGrowth
        expr: |
          loghub_disk_buffer_batches > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} using disk buffer"
          description: "Pipeline {{ $labels.pipeline_id }} has {{ $value }} batches in disk buffer. Memory buffer may be full."

      - alert: LogHubDiskBufferCritical
        expr: |
          loghub_disk_buffer_batches > 1000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "LogHub pipeline {{ $labels.pipeline_id }} disk buffer critical"
          description: "Pipeline {{ $labels.pipeline_id }} has {{ $value }} batches in disk buffer. Risk of disk space exhaustion."
```

## AlertManager Configuration

```yaml
route:
  group_by: ['pipeline_id', 'alertname']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'loghub-ops'
  routes:
    - match:
        severity: critical
      receiver: 'loghub-critical'
      continue: true
    - match:
        severity: warning
      receiver: 'loghub-warning'

receivers:
  - name: 'loghub-critical'
    slack_configs:
      - channel: '#loghub-critical'
        send_resolved: true
    pagerduty_configs:
      - service_key: '<pagerduty-service-key>'
        severity: critical
  - name: 'loghub-warning'
    slack_configs:
      - channel: '#loghub-ops'
        send_resolved: true
  - name: 'loghub-ops'
    slack_configs:
      - channel: '#loghub-ops'
        send_resolved: true
```

## Grafana Dashboard

Example Grafana dashboard panels for LogHub:

### Pipeline Overview (Single Stat Panels)
- Pipelines Running: `count(loghub_pipeline_running == 1)`
- Total Entries Sent (5m rate): `rate(loghub_entries_sent_total[5m])`
- Total Entries Failed (5m rate): `rate(loghub_entries_failed_total[5m])`
- DLQ Entries (15m increase): `increase(loghub_dlq_entries_total[15m])`

### Throughput (Graph Panel)
```
# Sent rate by pipeline
rate(loghub_entries_sent_total[5m])

# Failed rate by pipeline
rate(loghub_entries_failed_total[5m])
```

### Retry Rate (Graph Panel)
```
# Retry to send ratio
rate(loghub_retries_total[5m]) / (rate(loghub_entries_sent_total[5m]) + 0.001)
```

### Buffer Utilization (Graph Panel)
```
# Memory buffer utilization
loghub_buffer_utilization

# Disk buffer batches
loghub_disk_buffer_batches
```

### Collection/Send Duration (Heatmap)
```
loghub_collect_duration_seconds_bucket
loghub_send_duration_seconds_bucket
```

## Failure Mode Tests and Alert Behavior

This section describes how alerts should fire under various failure conditions.

| Failure Scenario | Expected Alert | Metric Behavior |
|------------------|----------------|-----------------|
| Pipeline crashes or is stopped | `LogHubPipelineNotRunning` | `loghub_pipeline_running` → 0 |
| MSSQL source unavailable | `LogHubCollectionErrors` | `loghub_collect_errors_total` increases; the pipeline goes `degraded`, then `stalled` (`/health` 503) |
| Sentinel/destination unavailable | `LogHubSendFailures`, `LogHubRetrySpike` | `loghub_entries_failed_total` increases, `loghub_retries_total` spikes |
| Destination rejects individual entries (e.g. HTTP 400) | `LogHubDLQGrow` with a DLQ, `LogHubEntriesDropped` without one | `loghub_entries_failed_total` increases with no retries; the pipeline goes `degraded` with the rejection in `ErrorMsg` |
| Destination returning 429/5xx | `LogHubRetrySpike`, `LogHubHighRetryRate` | `loghub_retries_total` >> `loghub_entries_sent_total` |
| Malformed data causing processor errors | `LogHubDLQGrow` | `loghub_dlq_entries_total` increases |
| Failed delivery retained on disk | `LogHubDiskBufferGrowth` | `loghub_disk_buffer_batches` > 0 |
| Prolonged destination outage | `LogHubDiskBufferCritical`, `LogHubDLQCritical` | `loghub_disk_buffer_batches` > 1000, DLQ entries > 100/hour |
| Checkpoint write fails (disk full/permissions) | `LogHubCheckpointSaveFailures` | `loghub_checkpoint_errors_total` increases, `loghub_checkpoints_saved_total` stops; the pipeline is `degraded` with the error in `/health` |
| Source returns no new data | `LogHubPipelineStaleCheckpoint` | No change in `loghub_entries_collected_total` and `loghub_checkpoints_saved_total` for 5m+ |

## Log Format Requirements

LogHub logs use structured logging (slog) with the following conventions:

### Required Log Fields

All pipeline operations MUST include `pipeline_id` for correlation:

```
pipeline_id=<id>
```

### Error Context

Errors should be logged with contextual information:

```
slog.Error("Failed to send batch after retries", "pipeline_id", p.ID, "error", err)
slog.Error("Failed to save checkpoint", "pipeline_id", p.ID, "error", err)
```

### Prohibited in Logs

The following MUST NOT appear in logs:
- Access tokens or bearer tokens
- Connection strings (except in startup errors before secrets resolution)
- Passwords or secrets from Key Vault
- Full request/response bodies for HTTP destinations
- SQL query parameters containing sensitive data

Example safe log:
```
slog.Warn("Sentinel token acquisition failed", "error", err)  // No token value logged
```

Example unsafe (avoid):
```
slog.Error("HTTP request failed", "url", url, "body", fullBody)  // body may contain secrets
```

## Health Check Endpoint

The `/health` endpoint returns:
- `200 OK` when the orchestrator is running and no pipeline is stalled or stale.
- `503 Service Unavailable` when the orchestrator is not running, any pipeline is in the `stalled` state, or (when `health_staleness_threshold` is set) a running or degraded pipeline has not collected or sent within that threshold.

The JSON body reports `status` (`up`/`down`), `running`, `uptime`, an optional `reason` for a 503, and per-pipeline `ID`, `Status`, `StartedAt`, `LastCollect`, `LastSend`, `LastError`, and `ErrorMsg` fields. `ErrorMsg` is the latest error while the pipeline is `degraded` or `stalled`, and is cleared once it is back to `running`; `LastError` keeps the time. Until a pipeline's first successful read or send, stall and staleness are measured from `StartedAt`.

A `degraded` pipeline still returns 200. Degraded covers errors a restart does not fix: a destination rejecting entries, a full checkpoint disk. Returning 503 for them would make a liveness probe restart LogHub in a loop. `/health` therefore answers "is LogHub alive and moving data?". Data loss and durability problems are alerted on from metrics: `LogHubEntriesDropped` (`loghub_entries_dropped_total`) and `LogHubCheckpointSaveFailures` (`loghub_checkpoint_errors_total`). For life-critical pipelines, page on both.

This can be used for liveness/readiness probes in Kubernetes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Production Metrics Checklist

- [ ] Prometheus is configured to scrape LogHub `/metrics` endpoint
- [ ] AlertManager is configured with receivers for critical and warning alerts
- [ ] Alert rules cover: pipeline not running, collection/send failures, retry spikes, DLQ growth, disk buffer growth, checkpoint failures
- [ ] Grafana dashboard exists with key panels: pipeline status, throughput, retry rate, buffer utilization, DLQ size
- [ ] On-call rotation is configured for critical alert routing
- [ ] Runbook documents how to respond to each alert type
- [ ] Health check endpoint is wired to liveness/readiness probes
- [ ] Logs are forwarded to a centralized logging system with `pipeline_id` as a searchable field
