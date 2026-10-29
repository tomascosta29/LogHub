# Contributing to LogHub

Thanks for your interest in LogHub. Bug reports, fixes and improvements are welcome.

## Reporting Issues

Open a GitHub issue with the LogHub version (`loghub --version`), the relevant part of your config (with secrets removed), what you expected, and what happened, including log output. For security vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Development Setup

You need:

- Go 1.26 or newer.
- A C compiler (`gcc` or `clang`). The SQLite source uses cgo.
- Docker, optionally, to build the image and to run the live SQL Server tests.

```bash
make build          # builds ./loghub
./loghub --config loghub.yaml.example --validate
```

## Checks

CI (`.github/workflows/ci.yml`) runs these on every pull request, and they must pass before a change is merged. Run them locally first:

```bash
make fmt-check      # gofmt; fix with gofmt -w <file>
make vet
go test ./... -count=1
make race
make vulncheck      # govulncheck: fails on vulnerabilities reachable from LogHub's code
```

CI also builds the Docker image and runs the live SQL Server tests. To run those locally, start a SQL Server container and point the tests at it:

```bash
docker run -d -e ACCEPT_EULA=Y -e 'MSSQL_SA_PASSWORD=<password>' -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
LOGHUB_MSSQL_TEST_SERVER=localhost:1433 LOGHUB_MSSQL_TEST_PASSWORD='<password>' \
  go test ./internal/plugins/source -run Live -count=1
```

Without `LOGHUB_MSSQL_TEST_SERVER`, these tests are skipped. The Azure paths (Managed Identity, Sentinel ingestion) are covered by mocks in the test suite. To check them against real Azure resources, follow [docs/canary-test-plan.md](docs/canary-test-plan.md).

## Project Layout

| Path | Contents |
|---|---|
| `cmd/loghub/` | Entry point |
| `internal/bootstrap/` | Wiring: config, secrets, pipelines, reload |
| `internal/config/` | Config parsing and validation |
| `internal/pipeline/` | Puller, pusher, pipeline lifecycle |
| `internal/orchestrator/` | Running pipelines, signals, shutdown |
| `internal/plugins/source/` | `file`, `sqlite`, `mssql` |
| `internal/plugins/destination/` | `sentinel`, `http`, `local_jsonl`, `dummy` |
| `internal/plugins/processor/` | `filter`, `json_parse`, `timestamp` |
| `internal/plugins/secrets/` | `env`, `keyvault` |
| `internal/journal/`, `internal/checkpointer/`, `internal/buffer/`, `internal/dlq/` | Durable state: journal, checkpoints, disk buffer, dead-letter queue |
| `internal/schemas/` | Plugin property schemas |
| `tests/` | End-to-end tests |
| `docs/` | Operator documentation |
| `k8s/` | Kubernetes manifests |

## Guidelines

- **Tests.** A feature comes with tests, and a bug fix with a test that fails without it. For concurrency changes, run `make race`.
- **Compatibility.** Within a major version, existing configs must keep loading: new settings are optional, with defaults. Checkpoint, disk-buffer and DLQ files written by earlier releases must still be read. If a format has to change, the new code reads the old one.
- **Plugin properties.** A new source, destination or processor property must be added to the schema in `internal/schemas/schemas.go` and to its test copy in `internal/config/schemas_test.go`. Unknown properties fail config validation.
- **Import boundaries.** `internal/config` and `internal/plugins/*` must not import each other. Shared interfaces live in `internal/core`.
- **Secrets.** Never log secret values, tokens or `Authorization` headers, including in error messages.
- **Style.** Follow the surrounding code. Document exported identifiers, and explain non-obvious behavior in comments that make sense without outside context.
- **Docs.** If you change behavior operators can see (config, metrics, logs, shutdown, recovery), update the relevant page in `docs/`, and add an entry under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md).

## Pull Requests

- Keep each pull request to one change, and describe what it changes and why.
- Write commit messages in the [Conventional Commits](https://www.conventionalcommits.org/) style used in the history, e.g. `fix(source): compare SQLite timestamps as time`.
- Reference the GitHub issue a change resolves (`Fixes #123`).

Releases are described in [docs/releasing.md](docs/releasing.md).

## License

LogHub is licensed under the [Apache License 2.0](LICENSE). By contributing, you agree that your contributions are licensed under the same terms.
