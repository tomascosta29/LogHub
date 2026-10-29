# Workstation E2E and performance checks

Run local end-to-end tests without Azure credentials:

```sh
go test ./tests/... -count=1 -v
```

`TestDesktopBinaryE2E` builds and launches the actual CLI, delivers 1,000 file
records to a local HTTP receiver, retries one HTTP 429, checks `/health` and
`/metrics`, and stops with SIGTERM. It restarts against the same checkpoint,
appends another record, and verifies all 1,001 IDs arrive without duplicates.
The other tests cover SQLite delivery, v0.1.0 state migration, and mocked
MSSQL/Sentinel contracts. These local tests do not validate live Azure auth or
ingestion; use `canary-test-plan.md` for that.

## HTTP benchmark

```sh
go test ./internal/plugins/destination -run '^$' \
  -bench BenchmarkHTTPSendRaw -benchmem -count=3
```

Measured on 2026-10-28, Linux amd64, Ryzen 9 9900X, Go 1.27.3, sequential
loopback HTTP/1 requests. Medians of three runs before and after transport
tuning:

| Response | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| Empty | 15,168 | 15,167 | 8,459 | 8,419 | 86 | 86 |
| 8 KB | 25,027 | 23,748 | 11,828 | 10,975 | 109 | 105 |

The 8 KB response case uses about 5% less time and 7% less allocation memory.
These are microbenchmark results, not a prediction of Azure throughput. The
workstation CLI test delivered its initial 1,000 records in about 22 ms including
startup and the injected retry; this is a smoke measurement, not a sustained
load test. Run benchmarks separately from builds and race tests to reduce noise.

HTTP and Sentinel destinations now share reference-counted connection pools.
One destination closing does not evict another's idle connections. The last
owner closes the pool. Small responses are consumed to EOF; larger responses
retain a 1 KB diagnostic prefix and drain up to another 64 KB, bounded by the
client timeout. This follows the [Go HTTP connection reuse contract](https://pkg.go.dev/net/http#Client.Do).
Regression tests check reuse after 200, 400, 429 and 500 responses, pool sharing,
concurrent creation/close, and HTTP/Sentinel pool configuration.

## Pipeline tuning

The puller can fetch one source page ahead of ordered delivery. Separate read
and acknowledged cursors preserve checkpoint safety; a failed page blocks later
delivery until recovery. When source and delivery are fast relative to the
poll interval, the approximate ceiling is `fetch_size / poll_interval` records
per second. The production example's 100 rows every 10 seconds therefore gives
roughly 10 records/second. To drain backlogs faster, increase source `fetch_size`
or reduce `runtime.poll_interval`, checking database load and destination limits.
`runtime.send_batch_size` splits a source page into requests; increasing it alone
does not increase source polling throughput. A larger memory buffer also does
not increase the one-page prefetch window.

SQL sources (`sqlite`, `mssql`) open one database connection by default.
`max_open_conns` sets the pool size (idle connections too). One goroutine reads
each source, so a larger pool does not speed up polling. It only helps when
other queries share the pool. `wait_for_change` is deprecated and has no effect:
sources are always polled every `runtime.poll_interval`. It still loads, with a
warning, so remove it from your config.

Checkpoint writes include a journal fsync. Inspect collection/send latency
metrics and profile storage time before changing `runtime.checkpoint_interval`:
less frequent durable checkpoints can increase replay after a crash. Keep
stable event IDs when evaluating throughput and duplicate delivery.

## Architectural overlap benchmark

```sh
go test ./internal/pipeline -run '^$' -bench BenchmarkPipelineOverlap -count=3 -benchtime=1s
```

The benchmark delivers 30 pages with 1 ms simulated source I/O and 1 ms simulated
send I/O per page, a 100 µs poll interval, and an in-memory checkpointer. On the
same workstation, median time fell from 74.66 ms to 37.75 ms (402 to 795 pages/s,
about 1.98× throughput) after bounded prefetch. The baseline used the pre-change
pipeline from commit `bb4c786` in an isolated checkout with the identical benchmark.
This demonstrates stage overlap under balanced I/O; workloads dominated by one
stage, production polling delays, or durable storage will see different gains.

Checkpoint isolation also removes the shared mutex from filesystem I/O:
initialization, saves and compaction on unrelated pipelines can proceed
independently. A deterministic concurrency test blocks one pipeline's critical
section and verifies another can initialize, save and read, while Close waits
for the blocked operation. No disk-speed improvement is claimed from that test.
