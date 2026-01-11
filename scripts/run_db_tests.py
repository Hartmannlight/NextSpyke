import os
import subprocess
import sys
import time

import psycopg

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke import db


def detect_compose_command() -> list[str]:
    try:
        subprocess.run(
            ["docker", "compose", "version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return ["docker", "compose"]
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    try:
        subprocess.run(
            ["docker-compose", "version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return ["docker-compose"]
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        raise RuntimeError("Docker Compose is not available") from exc


def wait_for_db(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with psycopg.connect(db.build_dsn(), connect_timeout=3) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Database did not become ready in time.")


def main() -> int:
    compose_cmd = detect_compose_command()
    env = os.environ.copy()
    env.setdefault("PGHOST", "localhost")
    env.setdefault("PGPORT", "5432")
    env.setdefault("PGDATABASE", "nextspyke")
    env.setdefault("PGUSER", "nextspyke")
    env.setdefault("PGPASSWORD", "nextspyke")
    env["RUN_DB_TESTS"] = "true"

    subprocess.run(compose_cmd + ["up", "-d", "db"], check=True)
    try:
        wait_for_db()
        subprocess.run(
            [sys.executable, "-m", "unittest", "tests.test_db_integration"],
            check=True,
            env=env,
        )
    finally:
        if env.get("KEEP_DB", "").strip().lower() not in {"1", "true", "yes", "on"}:
            subprocess.run(compose_cmd + ["stop", "db"], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
