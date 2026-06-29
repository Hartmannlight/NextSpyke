import json

import psycopg

from nextspyke.config import AppConfig
from nextspyke.db import build_dsn
from nextspyke.logging import RUN_ID, iso_ts, utc_now


def health_check(config: AppConfig) -> int:
    checks = []
    start = utc_now()
    checks.append(
        {
            "name": "self",
            "status": "ok",
            "latency_ms": int((utc_now() - start).total_seconds() * 1000),
        }
    )

    db_start = utc_now()
    db_status = "ok"
    snapshot_check = None
    try:
        with psycopg.connect(build_dsn(), connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                snapshot_start = utc_now()
                max_age_seconds = max(config.poll_interval * 3, 180)
                try:
                    cur.execute(
                        """
                        SELECT fetched_at
                        FROM snapshot
                        WHERE domain = %s
                        ORDER BY fetched_at DESC
                        LIMIT 1
                        """,
                        (config.domain,),
                    )
                    row = cur.fetchone()
                    latest_snapshot = row[0] if row else None
                    age_seconds = (
                        int((utc_now() - latest_snapshot).total_seconds())
                        if latest_snapshot
                        else None
                    )
                    snapshot_status = (
                        "ok"
                        if age_seconds is not None and age_seconds <= max_age_seconds
                        else "fail"
                    )
                except Exception:
                    age_seconds = None
                    snapshot_status = "fail"
                snapshot_check = {
                    "name": "snapshot_freshness",
                    "status": snapshot_status,
                    "latency_ms": int((utc_now() - snapshot_start).total_seconds() * 1000),
                    "age_seconds": age_seconds,
                    "max_age_seconds": max_age_seconds,
                }
    except Exception:
        db_status = "fail"
    checks.append(
        {
            "name": "db",
            "status": db_status,
            "latency_ms": int((utc_now() - db_start).total_seconds() * 1000),
        }
    )
    if snapshot_check:
        checks.append(snapshot_check)

    status = "ok" if all(c["status"] == "ok" for c in checks) else "fail"
    payload = {
        "status": status,
        "service": config.service,
        "env": config.env,
        "version": config.version,
        "commit": config.commit,
        "run_id": RUN_ID,
        "config_hash": config.config_hash,
        "timestamp": iso_ts(utc_now()),
        "checks": checks,
    }
    print(json.dumps(payload, separators=(",", ":")))
    return 0 if status == "ok" else 1
