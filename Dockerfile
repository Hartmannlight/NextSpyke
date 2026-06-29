FROM python:3.13-slim-bookworm@sha256:fcbd8dfc2605ba7c2eca646846c5e892b2931e41f6227985154a596f26ab8ed7 AS builder

ARG POETRY_VERSION=2.1.3

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true

WORKDIR /app

RUN python -m pip install --no-cache-dir "poetry==${POETRY_VERSION}"

COPY pyproject.toml poetry.lock /app/
RUN poetry sync --only main --no-root --no-interaction

FROM python:3.13-slim-bookworm@sha256:fcbd8dfc2605ba7c2eca646846c5e892b2931e41f6227985154a596f26ab8ed7 AS runtime

ARG VERSION=0.1.0
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown
ARG SOURCE_URL=https://github.com/Hartmannlight/Nextspyke
ARG LICENSE=MIT

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/app/.venv/bin:$PATH \
    PYTHONPATH=/app/src

WORKDIR /app

RUN groupadd --gid 10001 nextspyke \
    && useradd --uid 10001 --gid nextspyke --no-create-home nextspyke

COPY --from=builder /app/.venv /app/.venv
COPY --chown=nextspyke:nextspyke schema.sql /app/schema.sql
COPY --chown=nextspyke:nextspyke src /app/src

LABEL org.opencontainers.image.title="NextSpyke" \
      org.opencontainers.image.description="Collect and store Nextbike live data for movement and availability analysis." \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="${LICENSE}"

USER 10001:10001

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["python", "-m", "nextspyke.app", "health"]

CMD ["python", "-m", "nextspyke.app"]
