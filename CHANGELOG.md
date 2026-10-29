# Changelog

All notable changes to LogHub are recorded here, newest first, grouped into the sections described in [docs/releasing.md](docs/releasing.md). LogHub uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.1.0] - 2026-10-29

First public release, under the Apache License 2.0.

### Security

- **Maintained MSSQL driver.** The MSSQL source moved from the archived `github.com/denisenkom/go-mssqldb` v0.12.3 to Microsoft's maintained fork, `github.com/microsoft/go-mssqldb` v1.11.2. No config change: the connection string, TLS behavior and Entra token path are the same. The Azure SDK modules move up with it (`azcore` 1.23.1, `azidentity` 1.14.1).
- **Supported toolchain.** `go.mod` requires Go 1.26 (was 1.23, which no longer gets security fixes). The image builds with Go 1.27 on Alpine 3.24 and runs on Alpine 3.24 (was 1.23 / Alpine 3.19).
- **Dependency updates** fix 8 vulnerabilities reachable from LogHub's code (HTTP/2 transport, IDNA and text normalization): `golang.org/x/net` 0.62.0, `golang.org/x/text` 0.43.0, `golang.org/x/crypto` 0.58.0, `golang.org/x/sys` 0.49.0.

### Added

- **Secret rotation without restart.** Cached secrets are looked up again every `global.secret_refresh_interval` (default `5m`; `"0"` disables). A change is logged, and applied on the next SIGHUP, or straight away with `global.reload_on_secret_change: true` (default `false`). Metrics: `loghub_secret_refresh_total`, `loghub_secret_refresh_errors_total`.
- CI (`.github/workflows/ci.yml`): fmt, vet, tests, race and build on Go 1.26 and 1.27, `govulncheck`, an image build, and live MSSQL source tests against a SQL Server container.
- `make vulncheck`, now a required release check.

### Fixed

- **`max_open_conns` takes effect on SQL sources.** It passed validation but was ignored, so the pool was always one connection. A negative value is now rejected.
- **Secret lookups no longer cache failures.** A failed lookup serves the last resolved value instead of failing or, in non-strict mode, caching the raw `${provider:name}` for 5 minutes. SIGHUP looks every secret up again, so it no longer reuses a value cached before the rotation.
- **Shutdown is bounded by one deadline.** A pipeline's `shutdown_timeout` now covers the producer wait and the drain together (it applied to each, so a stop could take twice as long). All pipelines stop in parallel, and shutdown ends by the longest `shutdown_timeout` + 10s even if one pipeline hangs. Errors are logged per pipeline. `k8s/deployment.yaml` sets `terminationGracePeriodSeconds: 45`; raise it if you raise `shutdown_timeout`.
- **SQLite sources no longer skip rows because of the timestamp format.** The timestamp column is compared with the cursor as time (`julianday`), not text, so `YYYY-MM-DD HH:MM:SS`, `...T...Z` and different fractional digits order correctly. Rows sharing a millisecond are not skipped. Integer and unparseable timestamp columns keep the old comparison. An index on the plain timestamp column is no longer used; see "SQLite Timestamp Comparison" in `docs/reliability.md`.

### Deprecated

- **`wait_for_change`** on SQL sources never had an effect. It still loads, with a warning; remove it from your config. Sources are polled every `runtime.poll_interval`.

## [1.0.0] - 2026-10-28

### Breaking Changes

- **Flat per-pipeline settings are removed.** Move each one into the nested section below; a config that still uses one fails to load with an error naming the line and the replacement. Global settings (`global.poll_interval`, …) are unchanged.

  | v0.1.0 key (under a pipeline) | Use instead |
  |---|---|
  | `poll_interval` | `runtime.poll_interval` |
  | `send_batch_size` | `runtime.send_batch_size` |
  | `buffer_size` | `runtime.buffer_size` |
  | `checkpoint_interval` | `runtime.checkpoint_interval` |
  | `shutdown_timeout` | `runtime.shutdown_timeout` |
  | `max_retries` | `retry.max_attempts` |
  | `retry_base_backoff` | `retry.base_backoff` |
  | `dlq_enabled` | `durability.dlq.enabled` |
  | `dlq_dir` | `durability.dlq.dir` |
  | `disk_buffer_enabled` | `durability.disk_buffer.enabled` |
  | `disk_buffer_dir` | `durability.disk_buffer.dir` |
  | `disk_buffer_max_size_mb` | `durability.disk_buffer.max_size_mb` |

- **DLQ records name the destination that failed.** `destination` is now `<type>#<position>`, e.g. `sentinel#0` for the first destination in the pipeline, or `processor` for processor rejections. The old values `"all"` and `"per-entry-rejected"` are no longer written. Tooling that filters on `destination` must be updated.
- **The DLQ is stored as a journal**, `<dlq_dir>/<pipeline_id>.dlq.journal`, not JSONL. Read it with `loghub dlq export` instead of `cat … | jq`.
- **Plugin API:** `core.Source.GetName()` is renamed to `Name()`. Out-of-tree source plugins must rename the method.
- **MSSQL `ad-password` now requires `auth.client_id`**, the application the user signs in through; the driver cannot authenticate without it. The method never worked before (see Fixed), and it is deprecated by Microsoft because it cannot satisfy MFA.

### Config Changes

- New counter `loghub_entries_dropped_total{pipeline_id, reason}` counts entries lost because no DLQ or disk buffer could hold them; `docs/monitoring.md` adds the `LogHubEntriesDropped` alert.

### Migration Changes

State from v0.1.0 is imported automatically on first start, and each old file is renamed with a `.migrated` suffix:

- `<checkpoints_dir>/<pipeline_id>.cursor` → the checkpoint journal.
- `<disk_buffer_dir>/<pipeline_id>.buffer` and `.buffer.drain` → the disk buffer journal. Batches waiting there are delivered on start. Unreleased builds from `main` ignored these files, so upgrading with them lost those batches.
- `<dlq_dir>/<pipeline_id>.dlq` → the DLQ journal, keeping each record's timestamp.

Unreleased builds from `main` also wrote a shared `<checkpoints_dir>/<pipeline_id>.pipeline.journal`. Its latest cursor, undelivered disk-buffer batches and DLQ records are imported the same way. If it holds batches or DLQ records while the disk buffer or DLQ is now disabled, the pipeline refuses to start and says so, so nothing is dropped silently.

An interrupted import runs again on the next start; batches imported twice are delivered twice (at-least-once).

### New Dependencies

- None.

### Fixed

- **SQL rows with sub-second timestamps were re-sent on every poll.** The cursor was cut to whole seconds, so `event_time > cursor` kept matching the last second's rows. `TimeGenerated` was truncated the same way.
- **A graceful stop (SIGTERM) dead-lettered the batch in flight.** It is now delivered; if `shutdown_timeout` runs out, the batch is kept in the disk buffer for the next start.
- **MSSQL Microsoft Entra authentication works.** `ad-managed-identity`, `ad-service-principal` and `ad-password` passed ADO.NET-style connection-string keywords that the driver ignores, so they always tried to log in with no credentials. LogHub now gets the access token itself and hands it to the driver. User-assigned managed identities work through `auth.client_id`.
- **MSSQL passwords containing `;`, `=` or `{` broke the connection string.** It is now URL-escaped.
- **SQL sources without `cursor_column` re-sent the whole table on every poll.** The timestamp-only cursor (`v1:<timestamp>`) was read back as empty.
- **A corrupt or unknown-version SQL cursor now stops collection with an error** that says how to recover, instead of silently re-reading the table. See the runbook's "Invalid Checkpoint Cursor".
- **Cursor survives restarts.** Unreleased builds could lose the cursor after any DLQ write, disk spill or journal compaction, and then re-read the source from the start.
- **Disk buffer state.**
  - The pending count is recovered from the journal that is drained, so spilled batches are delivered after a restart.
  - Each batch is marked delivered as soon as it is handled, so a drain that stops partway only re-delivers what is left. A partly delivered batch keeps its place in delivery order.
  - While batches are pending, the source is not polled, so they are not collected twice.
- **Multiple destinations.** A failure at one destination is retried, shed or dead-lettered for that destination alone; the others are not sent the batch again.
- **Puller in-flight count.** The puller could stop polling for good when the pusher finished a batch before the puller had marked it in flight.
- **Journal internals.**
  - Compaction keeps a multi-entry transaction intact.
  - A failed rewrite no longer reuses entry IDs or panics.
  - The checkpoint journal is compacted every 1000 saves instead of growing forever.
- **Other fixes.**
  - A failed DLQ rotation no longer leaves the queue unwritable.
  - The plugin registries are safe for concurrent `Register` calls.

### Added

- **Service principal auth for the Sentinel destination**, for running outside Azure (e.g. end-to-end tests from a workstation): `auth: {method: "ad-service-principal", tenant_id, client_id, client_secret}`. Managed identity stays the default.
- `make build-static` builds a fully static binary that runs on any x86-64 Linux (e.g. to copy to a test VM).
- `loghub dlq list` and `loghub dlq export [--pipeline <id>] [--archives]` read the DLQ, safely while LogHub runs.
- `loghub --version`. Set the version with `make build VERSION=…` or `docker build --build-arg VERSION=…`.
- `make build`, `make fmt-check` and `make vet`; the last two are required release checks.

### Known Gaps

- SQLite sources compare timestamps as text; store them in RFC 3339 form (`2006-01-02T15:04:05Z`) or rows can be skipped.
- No DLQ replay command yet.
