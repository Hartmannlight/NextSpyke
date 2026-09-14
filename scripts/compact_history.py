"""Compact one completed UTC day. Defaults to a rollback-only dry run."""

import argparse
import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nextspyke.db import build_dsn


def compact_day(conn, day: date, domain: str, *, apply=False, purge_raw=False):
    if day >= datetime.now(timezone.utc).date():
        raise ValueError("Only completed UTC days can be compacted")
    start = datetime.combine(day, time(), timezone.utc)
    end = start + timedelta(days=1)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SET LOCAL lock_timeout = '10s'")
            # Coordinate with the collector and other cleanup processes.
            cur.execute("SELECT pg_advisory_xact_lock(20260914, 1)")
            script = Path(__file__).with_suffix(".sql").read_text(encoding="utf-8")
            # Psycopg extended queries cannot prepare multiple parameterized commands.
            statements = script.split(";")
            cur.execute(statements[0], (start, end, domain))
            cur.execute(statements[1])
            cur.execute("CREATE INDEX ON compact_polls (fetched_at)")
            cur.execute("ANALYZE compact_polls")
            cur.execute(statements[2], (start, end))
            cur.execute("ANALYZE compact_source")
            cur.execute(statements[3])
            cur.execute("ANALYZE compact_runs")
            cur.execute("SELECT count(*) FROM compact_source")
            before = cur.fetchone()[0]
            cur.execute(
                """
                SELECT count(*) FROM bike_status h
                JOIN compact_polls p USING (snapshot_id, fetched_at)
                WHERE h.fetched_at >= %s AND h.fetched_at < %s
            """,
                (start, end),
            )
            if cur.fetchone()[0] != before:
                raise RuntimeError("Invalid or cross-day interval endpoints; rolling back")
            cur.execute("SELECT count(*) FROM compact_runs")
            after = cur.fetchone()[0]
            # Compare exact reconstructed observations, including all state fields.
            cur.execute("""
                WITH expanded AS (
                    SELECT p.snapshot_id, p.fetched_at, r.bike_number, r.place_uid,
                           r.active, r.state, r.pedelec_battery, r.battery_pack_pct,
                           r.battery_range_km, ST_AsEWKB(r.geom) AS geom
                    FROM compact_runs r JOIN compact_polls p
                      ON p.fetched_at BETWEEN r.fetched_at AND r.last_seen_at
                ), original AS (
                    SELECT p.snapshot_id, p.fetched_at, r.bike_number, r.place_uid,
                           r.active, r.state, r.pedelec_battery, r.battery_pack_pct,
                           r.battery_range_km, ST_AsEWKB(r.geom) AS geom
                    FROM compact_source r JOIN compact_polls p
                      ON p.fetched_at BETWEEN r.fetched_at AND COALESCE(r.last_seen_at, r.fetched_at)
                ), differences AS (
                    (SELECT * FROM expanded EXCEPT ALL SELECT * FROM original)
                    UNION ALL
                    (SELECT * FROM original EXCEPT ALL SELECT * FROM expanded)
                ) SELECT count(*) FROM differences
            """)
            if cur.fetchone()[0]:
                raise RuntimeError("Compaction changed observations; rolling back")
            cur.execute("""
                SELECT count(*), COALESCE(sum(pg_column_size(s.raw_json)), 0)::bigint
                FROM snapshot s JOIN compact_polls p USING (snapshot_id, fetched_at)
                WHERE s.raw_json IS NOT NULL
            """)
            raw_count, raw_bytes = cur.fetchone()
            if apply:
                cur.execute(
                    """
                    DELETE FROM bike_status h USING compact_source old
                    WHERE h.snapshot_id = old.snapshot_id AND h.fetched_at = old.fetched_at
                      AND h.bike_number = old.bike_number
                      AND h.fetched_at >= %s AND h.fetched_at < %s
                """,
                    (start, end),
                )
                cur.execute("""
                    INSERT INTO bike_status (
                        snapshot_id, fetched_at, bike_number, place_uid, active, state,
                        pedelec_battery, battery_pack_pct, battery_range_km, geom,
                        last_seen_at, last_snapshot_id
                    ) SELECT * FROM compact_runs
                """)
                # A latest-state row may still point into this day after collector downtime.
                cur.execute("""
                    UPDATE bike_last_status l SET history_snapshot_id = r.snapshot_id,
                        history_fetched_at = r.fetched_at
                    FROM compact_runs r WHERE l.bike_number = r.bike_number
                      AND l.snapshot_id = r.last_snapshot_id AND l.fetched_at = r.last_seen_at
                """)
                if purge_raw:
                    cur.execute(
                        """
                        UPDATE snapshot s SET raw_json = NULL FROM compact_polls p
                        WHERE s.snapshot_id = p.snapshot_id AND s.fetched_at = p.fetched_at
                          AND s.fetched_at >= %s AND s.fetched_at < %s AND s.raw_json IS NOT NULL
                    """,
                        (start, end),
                    )
        result = dict(
            day=str(day),
            domain=domain,
            before=before,
            after=after,
            removed=before - after,
            raw_rows=raw_count,
            raw_column_bytes=raw_bytes,
            applied=apply,
            raw_purged=apply and purge_raw,
        )
        if apply:
            conn.commit()
        else:
            conn.rollback()
        return result
    except BaseException:
        conn.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--domain", default="fg")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--purge-raw", action="store_true")
    args = parser.parse_args()
    with psycopg.connect(build_dsn()) as conn:
        print(
            json.dumps(
                compact_day(conn, args.day, args.domain, apply=args.apply, purge_raw=args.purge_raw)
            )
        )


if __name__ == "__main__":
    main()
