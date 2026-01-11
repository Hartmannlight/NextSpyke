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
    try:
        with psycopg.connect(build_dsn(), connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except Exception:
        db_status = "fail"
    checks.append(
        {
            "name": "db",
            "status": db_status,
            "latency_ms": int((utc_now() - db_start).total_seconds() * 1000),
        }
    )

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
