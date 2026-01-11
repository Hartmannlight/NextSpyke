import time
from threading import Thread
from urllib.error import URLError

import psycopg
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from nextspyke.config import AppConfig

APP_UP = Gauge("app_up", "1 while running, 0 once shutdown begins")
APP_BUILD_INFO = Gauge(
    "app_build_info", "Build info", ["version", "commit", "env", "service"]
)
APP_ITERATIONS_TOTAL = Counter("app_iterations_total", "Number of ingest iterations")
APP_LAST_ITERATION_TS = Gauge(
    "app_last_iteration_timestamp_seconds",
    "Unix timestamp when the last iteration finished",
)
APP_ITERATION_FAILURES_TOTAL = Counter(
    "app_iteration_failures_total", "Number of failed ingest iterations"
)
APP_ITERATION_FAILURE_REASONS_TOTAL = Counter(
    "app_iteration_failure_reasons_total",
    "Failed ingest iterations by reason",
    ["reason"],
)
APP_ITERATION_DURATION = Histogram(
    "app_iteration_duration_seconds", "Ingest iteration duration seconds"
)

_metrics_started = False


def init_metrics(config: AppConfig) -> None:
    APP_BUILD_INFO.labels(
        version=config.version,
        commit=config.commit,
        env=config.env,
        service=config.service,
    ).set(1)
    APP_UP.set(1)


def start_metrics_server(config: AppConfig) -> None:
    global _metrics_started
    if not config.metrics_enabled or _metrics_started:
        return

    def _run() -> None:
        start_http_server(config.metrics_port)

    Thread(target=_run, daemon=True).start()
    _metrics_started = True


def mark_iteration_success(duration_s: float) -> None:
    APP_ITERATIONS_TOTAL.inc()
    APP_ITERATION_DURATION.observe(duration_s)
    APP_LAST_ITERATION_TS.set(time.time())


def mark_iteration_failure(duration_s: float, reason: str) -> None:
    APP_ITERATIONS_TOTAL.inc()
    APP_ITERATION_FAILURES_TOTAL.inc()
    APP_ITERATION_FAILURE_REASONS_TOTAL.labels(reason=reason).inc()
    APP_ITERATION_DURATION.observe(duration_s)
    APP_LAST_ITERATION_TS.set(time.time())


def mark_shutdown() -> None:
    APP_UP.set(0)


def classify_failure_reason(exc: BaseException) -> str:
    if isinstance(exc, psycopg.Error):
        return "db"
    if isinstance(exc, URLError):
        return "http"
    return "unknown"
