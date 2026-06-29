# Rollback

## Identify the previous known-good image

1. Open the last successful `Container Release / build-scan-and-publish` run in GitHub Actions.
2. Read the job summary for:
   - immutable build tag
   - published image digest
   - source commit
3. If the newest release is bad, choose the previous successful immutable tag or digest.

Prefer one of:

- `ghcr.io/hartmannlight/nextspyke:sha-<commit>-r<run>`
- `ghcr.io/hartmannlight/nextspyke@sha256:<digest>`

Do not roll back by guessing which old `latest` value was previously live.

## Roll back with an immutable tag or digest

Example with an immutable digest:

```yaml
services:
  app:
    image: ghcr.io/hartmannlight/nextspyke@sha256:<known-good-digest>
```

Example with an immutable build tag:

```yaml
services:
  app:
    image: ghcr.io/hartmannlight/nextspyke:sha-<commit>-r<run>
```

Then recreate only the application container:

```bash
docker compose pull app
docker compose up -d app
```

## Do not delete registry tags during rollback

- keep the bad image for forensics
- move consumers to the last known-good immutable reference
- if necessary, publish a corrected new image that later becomes the next `latest`

## Stop a bad floating-tag rollout

1. Change deployments to an immutable digest or immutable build tag.
2. If a workflow is still running, cancel it in GitHub Actions.
3. If a bad image already moved `latest`, publish a corrected image rather than rewriting
   an immutable tag.

## Pause Renovate automerge safely

Use one of:

- disable repository auto-merge temporarily in GitHub settings
- disable or pause the Renovate app for the repository
- set the branch rules so required checks or up-to-date requirements block merges

Do not bypass required checks just to unblock a security update.

## Rerun after a fix

1. fix the code, test, or scanner issue in a normal pull request
2. rerun `CI / quality-and-container-gate`
3. merge only after required checks pass
4. rerun the release workflow through `workflow_dispatch` if a manual republish is needed

## Record a security exception

If an unfixed vulnerability must be tolerated temporarily:

1. record the CVE
2. record the owner
3. record the reason and mitigation
4. record an expiry date
5. link the decision in the relevant PR or issue

Do not add broad scanner suppression without this record.
