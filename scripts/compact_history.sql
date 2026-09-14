-- Bound parameters: UTC day start, next UTC day start, domain.
CREATE TEMP TABLE compact_polls ON COMMIT DROP AS
SELECT snapshot_id, fetched_at,
       row_number() OVER (ORDER BY fetched_at, snapshot_id) AS ordinal
FROM snapshot WHERE fetched_at >= %s AND fetched_at < %s AND domain = %s;
CREATE UNIQUE INDEX ON compact_polls (snapshot_id, fetched_at);

CREATE TEMP TABLE compact_source ON COMMIT DROP AS
SELECT h.*, first_poll.ordinal AS first_ordinal, last_poll.ordinal AS last_ordinal,
       jsonb_build_array(h.place_uid, h.active, h.state, h.pedelec_battery,
                         h.battery_pack_pct, h.battery_range_km,
                         encode(ST_AsEWKB(h.geom), 'hex')) AS payload
FROM bike_status h
JOIN compact_polls first_poll USING (snapshot_id, fetched_at)
JOIN compact_polls last_poll
  ON last_poll.snapshot_id = COALESCE(h.last_snapshot_id, h.snapshot_id)
 AND last_poll.fetched_at = COALESCE(h.last_seen_at, h.fetched_at)
WHERE h.fetched_at >= %s AND h.fetched_at < %s;

CREATE TEMP TABLE compact_runs ON COMMIT DROP AS
WITH boundaries AS (
    SELECT *, CASE WHEN
        lag(payload) OVER w IS NOT DISTINCT FROM payload
        AND lag(last_ordinal) OVER w + 1 = first_ordinal
        THEN 0 ELSE 1 END AS new_run
    FROM compact_source
    WINDOW w AS (PARTITION BY bike_number ORDER BY fetched_at, snapshot_id)
), numbered AS (
    SELECT *, sum(new_run) OVER (
        PARTITION BY bike_number ORDER BY fetched_at, snapshot_id
    ) AS run_number FROM boundaries
), endpoints AS (
    SELECT *,
        last_value(COALESCE(last_seen_at, fetched_at)) OVER w AS run_end,
        last_value(COALESCE(last_snapshot_id, snapshot_id)) OVER w AS run_end_id
    FROM numbered
    WINDOW w AS (PARTITION BY bike_number, run_number ORDER BY fetched_at, snapshot_id
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT DISTINCT ON (bike_number, run_number)
       snapshot_id, fetched_at, bike_number, place_uid, active, state,
       pedelec_battery, battery_pack_pct, battery_range_km, geom,
       run_end AS last_seen_at, run_end_id AS last_snapshot_id
FROM endpoints ORDER BY bike_number, run_number, fetched_at, snapshot_id;
