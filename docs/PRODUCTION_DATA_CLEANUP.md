# Production data cleanup

This cleanup removes legacy free-floating pseudo-stations and movements below the
60 metre threshold. Bike and movement geometries remain available. Run it only
after the fixed collector has been deployed.

## Preconditions

1. Create and verify a database backup.
2. Stop every NextSpyke writer. Grafana may remain online.
3. Ensure enough free disk space for PostgreSQL WAL and updated table pages.
4. Connect as a role allowed to update and delete the NextSpyke tables.

The statements are deliberately idempotent. Run them one by one in the listed
order rather than wrapping the entire cleanup in one large transaction.

## Dry run

```sql
SELECT COUNT(*) AS legacy_places
FROM place
WHERE spot IS NOT TRUE;

SELECT COUNT(*) AS legacy_place_status_rows
FROM place_status ps
JOIN place p ON p.place_uid = ps.place_uid
WHERE p.spot IS NOT TRUE;

SELECT COUNT(*) AS bike_status_references_to_clear
FROM bike_status bs
JOIN place p ON p.place_uid = bs.place_uid
WHERE p.spot IS NOT TRUE;

SELECT COUNT(*) AS movement_start_references_to_clear
FROM bike_movement bm
JOIN place p ON p.place_uid = bm.start_place_uid
WHERE p.spot IS NOT TRUE;

SELECT COUNT(*) AS movement_end_references_to_clear
FROM bike_movement bm
JOIN place p ON p.place_uid = bm.end_place_uid
WHERE p.spot IS NOT TRUE;

SELECT COUNT(*) AS movements_below_threshold
FROM bike_movement
WHERE distance_m IS NULL OR distance_m < 60;
```

The 2026-07-10 production export contained 80,516 legacy places and 60,495
movements below 60 metres. Recheck the current counts before continuing.

## Cleanup

Use an unlimited statement timeout for the maintenance session, but fail quickly
if an unexpected lock is held:

```sql
SET statement_timeout = 0;
SET lock_timeout = '10s';
```

Remove movements that do not satisfy the tracking threshold:

```sql
DELETE FROM bike_movement
WHERE distance_m IS NULL OR distance_m < 60;
```

Detach retained movement and bike-history geometries from pseudo-stations. This
does not remove `start_geom`, `end_geom`, or `bike_status.geom`:

```sql
UPDATE bike_movement bm
SET start_place_uid = NULL
FROM place p
WHERE p.place_uid = bm.start_place_uid
  AND p.spot IS NOT TRUE;

UPDATE bike_movement bm
SET end_place_uid = NULL
FROM place p
WHERE p.place_uid = bm.end_place_uid
  AND p.spot IS NOT TRUE;

UPDATE bike_status bs
SET place_uid = NULL
FROM place p
WHERE p.place_uid = bs.place_uid
  AND p.spot IS NOT TRUE;

UPDATE bike_last_status bls
SET place_uid = NULL
FROM place p
WHERE p.place_uid = bls.place_uid
  AND p.spot IS NOT TRUE;
```

Delete availability snapshots belonging to pseudo-stations, then delete the
pseudo-stations themselves:

```sql
DELETE FROM place_status ps
USING place p
WHERE p.place_uid = ps.place_uid
  AND p.spot IS NOT TRUE;

DELETE FROM place
WHERE spot IS NOT TRUE;
```

Refresh planner statistics. Plain `VACUUM` makes the freed space reusable inside
the database files but does not normally shrink the files on disk:

```sql
VACUUM (ANALYZE) bike_movement;
VACUUM (ANALYZE) bike_status;
VACUUM (ANALYZE) place_status;
VACUUM (ANALYZE) place;
```

Do not run `VACUUM FULL` without a separate maintenance plan. It requires
exclusive table locks and temporary disk space.

## Verification

Every query must return zero:

```sql
SELECT COUNT(*) FROM place WHERE spot IS NOT TRUE;
SELECT COUNT(*) FROM bike_movement WHERE distance_m IS NULL OR distance_m < 60;

SELECT COUNT(*)
FROM bike_status bs
JOIN place p ON p.place_uid = bs.place_uid
WHERE p.spot IS NOT TRUE;

SELECT COUNT(*)
FROM place_status ps
JOIN place p ON p.place_uid = ps.place_uid
WHERE p.spot IS NOT TRUE;

SELECT COUNT(*)
FROM bike_movement bm
JOIN place p
  ON p.place_uid = bm.start_place_uid
  OR p.place_uid = bm.end_place_uid
WHERE p.spot IS NOT TRUE;
```

Restart the collector and confirm that one poll completes successfully. The
official station count should remain about 119 for the supplied production data,
while `place` must no longer grow with free-floating bike coordinates.
