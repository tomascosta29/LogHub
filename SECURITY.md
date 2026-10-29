# Security Policy

## Reporting a Vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Report them privately through GitHub: on the repository's **Security** tab, choose **Report a vulnerability**. Include the affected version (`loghub --version`), a description of the issue and its impact, and steps to reproduce it if you have them.

You will get an acknowledgement within a week. Once a fix is available, it is released, and the advisory is published with credit to the reporter unless you prefer otherwise.

## Supported Versions

Security fixes go into the latest release.

## Scope

In scope: LogHub's own code, its default configuration, the Dockerfile, and the Kubernetes manifests in `k8s/`. Vulnerabilities in dependencies are in scope when LogHub's code reaches them; CI runs `govulncheck` to find those.

For hardening guidance (Managed Identity, Key Vault, TLS, network restrictions), see [docs/security.md](docs/security.md).
