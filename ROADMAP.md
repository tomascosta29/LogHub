# Roadmap

Planned work, roughly in priority order. Nothing here is a commitment to a release date. Items with a GitHub issue link there for discussion; to work on one without an issue, open one first.

## Delivery

- **HTTP 413 handling.** A destination that rejects a request as too large gets the same batch again on every retry. Split the batch and resend the halves, down to single entries, before treating it as failed.
- **Authorization failures.** HTTP 401 and 403 are retried like an outage, then kept or dead-lettered. Consider opening the circuit breaker on them instead, so a credential problem pauses delivery without filling the DLQ.
- **`loghub dlq replay`** to send dead-lettered entries again once the cause is fixed ([#1](https://github.com/tomascosta29/LogHub/issues/1)).
- **Exit non-zero when shutdown reports errors**, so orchestrators can tell a clean stop from a lossy one ([#5](https://github.com/tomascosta29/LogHub/issues/5)).

## Sources

- **Change notification for SQL sources** instead of fixed-interval polling ([#3](https://github.com/tomascosta29/LogHub/issues/3)).
- **Configurable SQL connection lifetime** ([#2](https://github.com/tomascosta29/LogHub/issues/2)).
- **Syslog source** (TCP/UDP, RFC 5424 and 3164).
- **File identity on Windows.** File cursors record the file's device and inode on Linux and macOS, so a rotation while LogHub is stopped is detected on restart. On Windows they hold only the offset.

## Processors

- **Field masking.** No processor removes, masks or hashes individual fields; today this has to be done in the source query (see [docs/security.md](docs/security.md)). A `redact` processor would cover file sources and fields produced by `json_parse`.

## Extensibility

- **Runtime plugin loading** ([#4](https://github.com/tomascosta29/LogHub/issues/4)).

## Testing

- **Fault injection for durable state:** a checkpoint write or fsync that fails, a full disk (ENOSPC) and a permission error (EACCES) for the checkpoint journal, disk buffer and DLQ.
- **Long-running checks in CI:** a nightly soak of at least 30 minutes, longer fuzzing runs over the config loader, cursor and timestamp parsers, and the system tests against a race-instrumented binary.
- **Key Vault coverage:** a mocked Key Vault test for the secret provider, and `tenant_id` checked in the Azure canary ([docs/canary-test-plan.md](docs/canary-test-plan.md)).
