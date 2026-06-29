# Security Release Policy

## Scope

This policy covers automated dependency maintenance, container validation, image
publication, and rollback preparation for NextSpyke. It does not cover production
deployment automation.

Supported release train today:

- default branch release train via `latest`
- immutable build tags via `sha-<commit>-r<run>`
- exact Git tag releases when a `v*` Git tag is pushed

Because the project version is still `0.x`, floating major/minor compatibility tags
such as `0` or `0.1` are intentionally not published yet.

## Dependency bot and duplicate-bot policy

- Renovate owns dependency-update PRs, Docker digest updates, and GitHub Action SHA updates.
- GitHub Dependabot Alerts should remain enabled as the alert data source.
- Dependabot version-update PRs and Dependabot security-update PRs should remain disabled
  to avoid duplicate automation in the same ecosystem.

## Automerge matrix

| Update class | Handling |
|---|---|
| Known-vulnerable dependency with a fixed version | Automerge after required checks pass |
| Docker base-image digest update | Automerge after required checks pass |
| Stable Poetry patch update, current version not `0.x` | Automerge after required checks pass and 14-day release age |
| Stable Poetry minor update, current version not `0.x` | Automerge after required checks pass and 14-day release age |
| Any dependency currently in `0.x` | PR only, no automerge |
| Major update | PR only, no automerge |
| GitHub Action SHA update | Automerge after required checks pass |
| Lock-file maintenance | Automerge after required checks pass |

## Required CI checks

Required branch-protection checks for `main`:

- `CI / quality-and-container-gate`
- `Dependency Review / dependency-review`

`CodeQL / analyze` should be enabled and monitored, but should only become required
after it has run reliably in this repository.

## Locked dependency policy

- `poetry.lock` is committed and treated as the source of truth.
- CI validates lock consistency with `poetry check --lock`.
- CI installs locked dependencies with `poetry sync --extras dev`.
- Production image installs runtime dependencies only with `poetry sync --only main --no-root`.
- The Poetry CLI version is explicitly pinned in CI and Docker build stages.

## Container build and OS package policy

- Docker base images are pinned as tag plus digest.
- Runtime image uses a multi-stage build and carries only the application source,
  schema, and locked runtime dependencies.
- The image runs as UID/GID `10001`.
- No unbounded `apt-get upgrade` is used.
- No extra OS packages are installed today because the runtime does not require them.

## Scan policy

Release candidates are checked with Trivy for:

- image vulnerabilities
- repository secret exposure
- Dockerfile and configuration misconfigurations

Gate behavior:

- fail on fixable High/Critical image vulnerabilities
- always produce a non-blocking full High/Critical image report including unfixed items
- fail on High/Critical secret or misconfiguration findings in repository scanning

Approved scanner exceptions must name the CVE, owner, reason, and expiry. There are
currently no approved exceptions in the repository.

## Tag semantics

Current publication policy:

- `latest`
  latest successfully validated image built from protected `main`
- `sha-<commit>-r<run>`
  immutable forensic build tag for every published workflow run
- `v*`
  immutable Git tag release image when the workflow is triggered by a matching Git tag

Images should be consumed by digest or immutable build tag when exact reproducibility
is required.

## Weekly rebuild schedule

- scheduled rebuild: Mondays at `03:23 UTC`
- scheduled rebuilds rebuild, smoke test, scan, and republish only after the same
  release gates pass

## SBOM and provenance policy

- published images are built with SBOM generation enabled
- published images are built with provenance attestations enabled
- publication occurs only after the candidate image passes smoke and scan gates

## Notification behavior

- failed CI, dependency updates, and release runs remain visible in GitHub Actions
- Renovate dependency dashboard remains enabled
- no extra webhook notification is configured by default in this repository

## What this policy does not guarantee

This policy improves reproducibility and maintenance discipline. It does not guarantee:

- absence of unknown vulnerabilities
- immediate upstream fixes for third-party issues
- host kernel or container runtime patching
- safe production deployment behavior
- automatic rollback of running user workloads

## Ownership and review

- owner: repository maintainers
- review cadence: whenever CI/CD policy or registry behavior changes, and at least
  every 90 days
