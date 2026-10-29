# Releasing LogHub

## Before a Release

1. CI is green on `main`. The checks are listed in [CONTRIBUTING.md](../CONTRIBUTING.md#checks).
2. Validate the production example. It must load with the release build:

   ```bash
   go run ./cmd/loghub --config loghub.prod.yaml.example --validate
   ```

3. For a release that changes the MSSQL source, the Sentinel destination or Azure authentication, run the Azure canary in [canary-test-plan.md](canary-test-plan.md).

## Versioning

LogHub uses [Semantic Versioning](https://semver.org/). A change that makes an existing config fail to load, or that changes a persisted format without reading the old one, requires a major version.

## Release Notes

Move the `## [Unreleased]` entries in [CHANGELOG.md](../CHANGELOG.md) under a new `## [X.Y.Z] - YYYY-MM-DD` heading. Use these sections, leaving out any that are empty:

| Section | Contents |
|---|---|
| Breaking Changes | Changes that need operator action, with the replacement |
| Security | Security fixes, including dependency and toolchain updates |
| Added | New features and settings |
| Changed | Behavior changes that need no action |
| Deprecated | Settings or behavior to be removed later, and what to use instead |
| Fixed | Bug fixes, described by their effect on operators |
| Config Changes | New, renamed or removed config keys, with defaults |
| Migration Changes | Checkpoint, disk-buffer, DLQ or schema format changes, and how upgrades handle them |
| New Dependencies | New external dependencies |

## Building

The version is set at build time:

```bash
make build VERSION=X.Y.Z
docker build --build-arg VERSION=X.Y.Z -t loghub:X.Y.Z .
```

`loghub --version` prints it.

## Tagging

Tag the release commit on `main` and push the tag:

```bash
git tag -a vX.Y.Z -m "LogHub X.Y.Z"
git push origin vX.Y.Z
```

Then create a GitHub release from the tag, with the changelog section as its notes.
