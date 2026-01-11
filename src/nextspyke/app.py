import signal
import sys
import time

import psycopg

from nextspyke.config import env_bool, load_config
from nextspyke.db import build_dsn, init_db
from nextspyke.health import health_check
from nextspyke.ingest import ingest_once
from nextspyke.logging import log_event, utc_now, iso_ts
from nextspyke.metrics import (
    classify_failure_reason,
    init_metrics,
    mark_iteration_failure,
    mark_iteration_success,
    mark_shutdown,
    start_metrics_server,
)

_shutdown_requested = False
_shutdown_reason = "signal"


def _handle_signal(signum, _frame) -> None:
    global _shutdown_requested
    global _shutdown_reason
    _shutdown_requested = True
    try:
        _shutdown_reason = signal.Signals(signum).name
    except ValueError:
        _shutdown_reason = f"signal_{signum}"


def main() -> None:
    config = load_config()
    if len(sys.argv) > 1 and sys.argv[1] == "health":
        raise SystemExit(health_check(config))

    run_once = env_bool("RUN_ONCE", False)
    init_metrics(config)
    start_metrics_server(config)

    log_event(
        "info",
        "app.lifecycle",
        "Service startup began",
        event="startup_begin",
        config=config,
    )

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn = psycopg.connect(build_dsn())
    init_db(conn)

    log_event(
        "info",
        "app.lifecycle",
        "Service startup succeeded",
        event="startup_success",
        config=config,
    )

    shutdown_started_at = None
    try:
        while True:
            if _shutdown_requested:
                shutdown_started_at = utc_now()
                mark_shutdown()
                log_event(
                    "info",
                    "app.lifecycle",
                    "Shutting down",
                    event="shutting_down",
                    config=config,
                    extra={"reason": _shutdown_reason},
                )
                break
            iteration_started = utc_now()
            try:
                result = ingest_once(conn, config)
                duration_s = (utc_now() - iteration_started).total_seconds()
                mark_iteration_success(duration_s)
                log_event(
                    "info",
                    "app.ingest",
                    "Ingest completed",
                    event="ingest_success",
                    config=config,
                    extra={
                        "snapshot_id": result["snapshot_id"],
                        "fetched_at": iso_ts(result["fetched_at"]),
                        "cities": result["cities"],
                        "places": result["places"],
                        "bikes": result["bikes"],
                        "movements": result["movements"],
                        "duration_ms": int(duration_s * 1000),
                    },
                )
            except Exception as exc:
                duration_s = (utc_now() - iteration_started).total_seconds()
                mark_iteration_failure(duration_s, classify_failure_reason(exc))
                log_event(
                    "error",
                    "app.ingest",
                    "Ingest failed",
                    event="ingest_failed",
                    config=config,
                    extra={
                        "duration_ms": int(duration_s * 1000),
                    },
                    exc=exc,
                )
            if run_once:
                break
            time.sleep(config.poll_interval)
    except Exception as exc:
        log_event(
            "error",
            "app.lifecycle",
            "Application crashed",
            event="crashed",
            config=config,
            exc=exc,
        )
        raise
    finally:
        mark_shutdown()
        conn.close()
        if shutdown_started_at:
            log_event(
                "info",
                "app.lifecycle",
                "Shutdown complete",
                event="shutdown_complete",
                config=config,
                extra={
                    "shutdown_duration_ms": int(
                        (utc_now() - shutdown_started_at).total_seconds() * 1000
                    )
                },
            )


if __name__ == "__main__":
    main()
