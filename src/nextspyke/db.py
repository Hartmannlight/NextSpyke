from datetime import datetime, timezone
from pathlib import Path
import os

import psycopg
from psycopg import sql


def build_dsn() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "nextspyke")
    user = os.getenv("PGUSER", "nextspyke")
    password = os.getenv("PGPASSWORD", "nextspyke")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def load_schema_sql() -> str:
    candidates = []
    env_path = os.getenv("SCHEMA_PATH")
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path(__file__).resolve().parents[2] / "schema.sql")
    candidates.append(Path("/app/schema.sql"))
    for path in candidates:
        if path.is_file():
            return path.read_text(encoding="utf-8")
    raise FileNotFoundError("schema.sql not found")


def init_db(conn: psycopg.Connection) -> None:
    schema_sql = load_schema_sql()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()


def month_bounds(ts: datetime) -> tuple[datetime, datetime]:
    start = datetime(ts.year, ts.month, 1, tzinfo=timezone.utc)
    if ts.month == 12:
        end = datetime(ts.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(ts.year, ts.month + 1, 1, tzinfo=timezone.utc)
    return start, end


def ensure_month_partition(cur: psycopg.Cursor, table: str, ts: datetime) -> None:
    start, end = month_bounds(ts)
    suffix = f"{start.year}{start.month:02d}"
    partition = f"{table}_{suffix}"
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {partition}
            PARTITION OF {table}
            FOR VALUES FROM ({start}) TO ({end})
            """
        ).format(
            partition=sql.Identifier(partition),
            table=sql.Identifier(table),
            start=sql.Literal(start),
            end=sql.Literal(end),
        )
    )


def ensure_partitions(cur: psycopg.Cursor, ts: datetime) -> None:
    for table in ("snapshot", "city_status", "place_status", "bike_status"):
        ensure_month_partition(cur, table, ts)
