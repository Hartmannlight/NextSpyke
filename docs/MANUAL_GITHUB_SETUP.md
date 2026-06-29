# Manual GitHub Setup

These steps must be completed by the repository owner in GitHub. They are not
safe to assume from local repository changes alone.

## 1. Install and verify Renovate

1. Install the Renovate GitHub App for this repository.
2. Confirm the onboarding PR appears and that `renovate.json` is active after merge.
3. Keep Dependabot Alerts enabled.
4. Disable Dependabot version-update PRs.
5. Disable Dependabot security-update PRs for the same ecosystems if Renovate
   vulnerability alerts are enabled successfully.

## 2. Configure branch protection for `main`

Create a ruleset or branch protection rule with:

- pull request required before merge
- required status checks
- branch must be up to date before merge
- force pushes blocked
- branch deletion blocked

Mark these checks as required after their first successful run:

- `CI / quality-and-container-gate`
- `Dependency Review / dependency-review`

`CodeQL / analyze` should only become required after it has been observed running
reliably in this repository.

Verification step:

1. create a temporary branch with a deliberately failing test
2. open a PR
3. verify GitHub refuses to merge it
4. remove the temporary branch afterward

## 3. Enable auto-merge

In repository settings, enable GitHub auto-merge.

Then verify that a passing Renovate PR merges only after the required checks pass.
Do not rely on a green label without branch rules.

## 4. Lock down GitHub Actions

In repository Actions settings:

- set the default `GITHUB_TOKEN` permission to read-only if the repository setting allows it
- restrict allowed actions or reusable workflows according to your trust policy
- keep forked PR workflows read-only and secret-free
- do not allow untrusted pull requests onto privileged self-hosted runners

## 5. Enable code security features

Enable where the repository plan allows it:

- dependency graph
- Dependabot Alerts
- secret scanning
- push protection
- code scanning / CodeQL
- dependency review

## 6. Verify GHCR package behavior

1. Confirm the workflow can publish with `GITHUB_TOKEN`.
2. Decide package visibility intentionally.
3. Configure retention so that immutable release/build tags needed for rollback are kept.
4. Test pulling:
   - `ghcr.io/hartmannlight/nextspyke:latest`
   - one immutable build tag
   - one digest reference

## 7. Secrets

No extra long-lived publish secret is required when GHCR publication through
`GITHUB_TOKEN` works.

Only add secrets if you later configure optional notifications or a private registry.
If a legacy PAT exists for publishing, rotate and remove it.

## 8. Host maintenance outside the image

Patch management is not finished when a new image is published. For every Docker host:

- enable unattended OS security updates
- monitor failed upgrades and required reboots
- keep Docker or the container runtime patched
- back up persistent Postgres data
- document how operators pull and recreate containers from immutable image references
