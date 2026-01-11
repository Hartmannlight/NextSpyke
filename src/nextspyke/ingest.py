import json
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg
from psycopg.types.json import Json

from nextspyke.config import AppConfig
from nextspyke.db import ensure_partitions
from nextspyke.logging import log_event, utc_now

LIVE_BASE_URL = "https://maps.nextbike.net/maps/nextbike-live.json"
ZONE_BASE_URL = "https://zone-service.nextbikecloud.net/v1/zones/city/{city_id}"
FLEXZONE_URL = "https://api.nextbike.net/reservation/geojson/flexzone_{domain}.json"
GBFS_ROOT_URL = "https://gbfs.nextbike.net/maps/gbfs/v2/{system_id}/gbfs.json"

MATERIALIZED_VIEWS = (
    "mv_hotspots_hourly",
    "mv_city_bikes_hourly",
    "mv_routes_top",
    "mv_bike_dwell",
)

_last_mv_refresh_at: datetime | None = None


def fetch_json(url: str, params: dict | None = None) -> dict:
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "NextSpyke/0.1"})
    with urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def maybe_refresh_materialized_views(conn: psycopg.Connection, now: datetime, interval: int) -> None:
    global _last_mv_refresh_at
    if interval <= 0:
        return
    if _last_mv_refresh_at and (now - _last_mv_refresh_at).total_seconds() < interval:
        return
    with conn.cursor() as cur:
        for view in MATERIALIZED_VIEWS:
            cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
    conn.commit()
    _last_mv_refresh_at = now


def upsert_country(cur: psycopg.Cursor, country: dict) -> None:
    cur.execute(
        """
        INSERT INTO country (
            domain, name, country_code, country_name, timezone, currency, hotline,
            email, website, terms, policy, pricing, operator_address, country_calling_code
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (domain) DO UPDATE SET
            name = EXCLUDED.name,
            country_code = EXCLUDED.country_code,
            country_name = EXCLUDED.country_name,
            timezone = EXCLUDED.timezone,
            currency = EXCLUDED.currency,
            hotline = EXCLUDED.hotline,
            email = EXCLUDED.email,
            website = EXCLUDED.website,
            terms = EXCLUDED.terms,
            policy = EXCLUDED.policy,
            pricing = EXCLUDED.pricing,
            operator_address = EXCLUDED.operator_address,
            country_calling_code = EXCLUDED.country_calling_code
        """,
        (
            country.get("domain"),
            country.get("name"),
            country.get("country"),
            country.get("country_name"),
            country.get("timezone"),
            country.get("currency"),
            country.get("hotline"),
            country.get("email"),
            country.get("website"),
            country.get("terms"),
            country.get("policy"),
            country.get("pricing"),
            country.get("system_operator_address"),
            country.get("country_calling_code"),
        ),
    )


def upsert_cities(cur: psycopg.Cursor, cities: list[dict]) -> None:
    for city in cities:
        bounds = city.get("bounds") or {}
        sw = bounds.get("south_west") or {}
        ne = bounds.get("north_east") or {}
        if sw and ne:
            bounds_sql = "ST_MakeEnvelope(%s, %s, %s, %s, 4326)"
            bounds_args = (sw.get("lng"), sw.get("lat"), ne.get("lng"), ne.get("lat"))
        else:
            bounds_sql = "NULL"
            bounds_args = ()
        cur.execute(
            f"""
            INSERT INTO city (
                city_uid, domain, name, alias, lat, lng, zoom, bounds, refresh_rate_ms, website
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, {bounds_sql}, %s, %s)
            ON CONFLICT (city_uid) DO UPDATE SET
                domain = EXCLUDED.domain,
                name = EXCLUDED.name,
                alias = EXCLUDED.alias,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                zoom = EXCLUDED.zoom,
                bounds = EXCLUDED.bounds,
                refresh_rate_ms = EXCLUDED.refresh_rate_ms,
                website = EXCLUDED.website
            """,
            (
                city.get("uid"),
                city.get("domain"),
                city.get("name"),
                city.get("alias"),
                city.get("lat"),
                city.get("lng"),
                city.get("zoom"),
                *bounds_args,
                int(city.get("refresh_rate") or 0) or None,
                city.get("website"),
            ),
        )


def upsert_places(cur: psycopg.Cursor, city_uid: int, places: list[dict]) -> None:
    for place in places:
        cur.execute(
            """
            INSERT INTO place (
                place_uid, city_uid, name, number, spot, terminal_type, lat, lng, geom,
                maintenance, active_place, bike, booked_bikes, bikes, bikes_available_to_rent,
                bike_racks, free_racks, special_racks, free_special_racks, rack_locks,
                place_type, address
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (place_uid) DO UPDATE SET
                city_uid = EXCLUDED.city_uid,
                name = EXCLUDED.name,
                number = EXCLUDED.number,
                spot = EXCLUDED.spot,
                terminal_type = EXCLUDED.terminal_type,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                geom = EXCLUDED.geom,
                maintenance = EXCLUDED.maintenance,
                active_place = EXCLUDED.active_place,
                bike = EXCLUDED.bike,
                booked_bikes = EXCLUDED.booked_bikes,
                bikes = EXCLUDED.bikes,
                bikes_available_to_rent = EXCLUDED.bikes_available_to_rent,
                bike_racks = EXCLUDED.bike_racks,
                free_racks = EXCLUDED.free_racks,
                special_racks = EXCLUDED.special_racks,
                free_special_racks = EXCLUDED.free_special_racks,
                rack_locks = EXCLUDED.rack_locks,
                place_type = EXCLUDED.place_type,
                address = EXCLUDED.address
            """,
            (
                place.get("uid"),
                city_uid,
                place.get("name"),
                place.get("number"),
                place.get("spot"),
                place.get("terminal_type"),
                place.get("lat"),
                place.get("lng"),
                place.get("lng"),
                place.get("lat"),
                place.get("maintenance"),
                place.get("active_place"),
                place.get("bike"),
                place.get("booked_bikes"),
                place.get("bikes"),
                place.get("bikes_available_to_rent"),
                place.get("bike_racks"),
                place.get("free_racks"),
                place.get("special_racks"),
                place.get("free_special_racks"),
                place.get("rack_locks"),
                str(place.get("place_type")) if place.get("place_type") is not None else None,
                place.get("address"),
            ),
        )


def upsert_vehicle_types(cur: psycopg.Cursor, type_ids: set[str]) -> None:
    if not type_ids:
        return
    rows = [(type_id,) for type_id in type_ids]
    cur.executemany(
        "INSERT INTO vehicle_type (vehicle_type_id) VALUES (%s) ON CONFLICT DO NOTHING",
        rows,
    )


def upsert_bikes(cur: psycopg.Cursor, bikes: list[tuple]) -> None:
    if not bikes:
        return
    cur.executemany(
        """
        INSERT INTO bike (
            bike_number, boardcomputer, bike_type_id, electric_lock, lock_types,
            first_seen_at, last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (bike_number) DO UPDATE SET
            boardcomputer = COALESCE(bike.boardcomputer, EXCLUDED.boardcomputer),
            bike_type_id = COALESCE(EXCLUDED.bike_type_id, bike.bike_type_id),
            electric_lock = COALESCE(EXCLUDED.electric_lock, bike.electric_lock),
            lock_types = COALESCE(EXCLUDED.lock_types, bike.lock_types),
            last_seen_at = EXCLUDED.last_seen_at
        """,
        bikes,
    )


def insert_city_status(
    cur: psycopg.Cursor, snapshot_id: int, fetched_at: datetime, city: dict
) -> None:
    cur.execute(
        """
        INSERT INTO city_status (
            snapshot_id, fetched_at, city_uid, booked_bikes, set_point_bikes, available_bikes,
            bike_types
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            snapshot_id,
            fetched_at,
            city.get("uid"),
            city.get("booked_bikes"),
            city.get("set_point_bikes"),
            city.get("available_bikes"),
            Json(city.get("bike_types") or {}),
        ),
    )


def insert_place_status(
    cur: psycopg.Cursor, snapshot_id: int, fetched_at: datetime, places: list[dict]
) -> None:
    rows = []
    for place in places:
        rows.append(
            (
                snapshot_id,
                fetched_at,
                place.get("uid"),
                place.get("booked_bikes"),
                place.get("bikes"),
                place.get("bikes_available_to_rent"),
                place.get("bike_racks"),
                place.get("free_racks"),
                place.get("special_racks"),
                place.get("free_special_racks"),
            )
        )
    if rows:
        cur.executemany(
            """
            INSERT INTO place_status (
                snapshot_id, fetched_at, place_uid, booked_bikes, bikes, bikes_available_to_rent,
                bike_racks, free_racks, special_racks, free_special_racks
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def insert_bike_status(
    cur: psycopg.Cursor, snapshot_id: int, bike_rows: list[tuple]
) -> None:
    if not bike_rows:
        return
    cur.executemany(
        """
        INSERT INTO bike_status (
            snapshot_id, fetched_at, bike_number, place_uid, active, state, pedelec_battery,
            battery_pack_pct, geom
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
        """,
        bike_rows,
    )


def insert_snapshot(cur: psycopg.Cursor, fetched_at: datetime, domain: str, raw_json: dict | None) -> int:
    cur.execute(
        """
        INSERT INTO snapshot (fetched_at, domain, source, raw_json)
        VALUES (%s, %s, %s, %s)
        RETURNING snapshot_id
        """,
        (fetched_at, domain, "nextbike-live", Json(raw_json) if raw_json else None),
    )
    return cur.fetchone()[0]


def record_snapshot_gap(
    cur: psycopg.Cursor, fetched_at: datetime, domain: str, expected_interval_s: int
) -> dict | None:
    cur.execute(
        "SELECT fetched_at FROM snapshot WHERE domain = %s ORDER BY fetched_at DESC LIMIT 1",
        (domain,),
    )
    row = cur.fetchone()
    if not row:
        return None
    prev_fetched_at = row[0]
    gap_seconds = int((fetched_at - prev_fetched_at).total_seconds())
    if expected_interval_s <= 0:
        return None
    threshold = int(expected_interval_s * 1.5)
    if gap_seconds <= threshold:
        return None
    missing_count = max(int(gap_seconds // expected_interval_s) - 1, 0)
    cur.execute(
        """
        INSERT INTO snapshot_gap (
            domain, gap_start, gap_end, gap_seconds, expected_interval_s, missing_count
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            domain,
            prev_fetched_at,
            fetched_at,
            gap_seconds,
            expected_interval_s,
            missing_count,
        ),
    )
    return {
        "gap_start": prev_fetched_at,
        "gap_end": fetched_at,
        "gap_seconds": gap_seconds,
        "expected_interval_s": expected_interval_s,
        "missing_count": missing_count,
    }


def insert_bike_movements(cur: psycopg.Cursor, snapshot_id: int, fetched_at: datetime) -> int:
    cur.execute(
        """
        WITH current AS (
            SELECT bike_number, snapshot_id, fetched_at, place_uid, geom
            FROM bike_status
            WHERE snapshot_id = %s AND fetched_at = %s
        ),
        prev AS (
            SELECT DISTINCT ON (bs.bike_number)
                bs.bike_number,
                bs.snapshot_id,
                bs.fetched_at,
                bs.place_uid,
                bs.geom
            FROM bike_status bs
            JOIN current c ON c.bike_number = bs.bike_number
            WHERE NOT (bs.snapshot_id = %s AND bs.fetched_at = %s)
            ORDER BY bs.bike_number, bs.fetched_at DESC
        )
        INSERT INTO bike_movement (
            bike_number,
            start_snapshot_id,
            start_fetched_at,
            end_snapshot_id,
            end_fetched_at,
            start_place_uid,
            end_place_uid,
            start_geom,
            end_geom,
            distance_m,
            duration_seconds,
            is_station_to_station,
            confidence
        )
        SELECT
            c.bike_number,
            p.snapshot_id,
            p.fetched_at,
            c.snapshot_id,
            c.fetched_at,
            p.place_uid,
            c.place_uid,
            p.geom,
            c.geom,
            ROUND(ST_Distance(p.geom::geography, c.geom::geography))::int,
            GREATEST(EXTRACT(EPOCH FROM c.fetched_at - p.fetched_at)::int, 0),
            CASE WHEN ps.spot IS TRUE AND pe.spot IS TRUE THEN TRUE ELSE FALSE END,
            CASE WHEN ps.spot IS TRUE AND pe.spot IS TRUE THEN 100 ELSE 50 END
        FROM current c
        JOIN prev p ON p.bike_number = c.bike_number
        LEFT JOIN place ps ON ps.place_uid = p.place_uid
        LEFT JOIN place pe ON pe.place_uid = c.place_uid
        WHERE c.place_uid IS NOT NULL
          AND p.place_uid IS NOT NULL
          AND c.place_uid <> p.place_uid
        ON CONFLICT DO NOTHING
        """,
        (snapshot_id, fetched_at, snapshot_id, fetched_at),
    )
    return cur.rowcount or 0


def upsert_zone_features(cur: psycopg.Cursor, city_uid: int, zone_source: str, data: dict) -> None:
    features = data.get("features") or []
    for feature in features:
        properties = feature.get("properties") or {}
        zone_id = feature.get("id") or properties.get("flexzoneId") or properties.get("name")
        if not zone_id:
            continue
        zone_type = properties.get("type") or properties.get("category")
        name = properties.get("name")
        geometry = feature.get("geometry")
        if not geometry:
            continue
        geom_json = json.dumps(geometry)
        cur.execute(
            """
            INSERT INTO zone (zone_id, city_uid, zone_source, zone_type, name, geom, properties)
            VALUES (
                %s, %s, %s, %s, %s,
                ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
                %s
            )
            ON CONFLICT (zone_id) DO UPDATE SET
                city_uid = EXCLUDED.city_uid,
                zone_source = EXCLUDED.zone_source,
                zone_type = EXCLUDED.zone_type,
                name = EXCLUDED.name,
                geom = EXCLUDED.geom,
                properties = EXCLUDED.properties
            """,
            (str(zone_id), city_uid, zone_source, zone_type, name, geom_json, Json(properties)),
        )


def fetch_gbfs_vehicle_types(system_id: str) -> list[dict]:
    root = fetch_json(GBFS_ROOT_URL.format(system_id=system_id))
    feeds = root.get("data", {}).get("en", {}).get("feeds", [])
    for feed in feeds:
        if feed.get("name") == "vehicle_types":
            return fetch_json(feed.get("url")).get("data", {}).get("vehicle_types", [])
    return []


def upsert_vehicle_type_metadata(cur: psycopg.Cursor, vehicle_types: list[dict]) -> None:
    rows = []
    for item in vehicle_types:
        rows.append(
            (
                item.get("vehicle_type_id"),
                item.get("name"),
                item.get("form_factor"),
                item.get("propulsion_type"),
                item.get("max_range_meters"),
            )
        )
    if not rows:
        return
    cur.executemany(
        """
        INSERT INTO vehicle_type (
            vehicle_type_id, name, form_factor, propulsion_type, max_range_meters
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (vehicle_type_id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, vehicle_type.name),
            form_factor = COALESCE(EXCLUDED.form_factor, vehicle_type.form_factor),
            propulsion_type = COALESCE(EXCLUDED.propulsion_type, vehicle_type.propulsion_type),
            max_range_meters = COALESCE(EXCLUDED.max_range_meters, vehicle_type.max_range_meters)
        """,
        rows,
    )


def ingest_once(conn: psycopg.Connection, config: AppConfig) -> dict:
    fetched_at = utc_now()
    live_data = fetch_json(LIVE_BASE_URL, {"domains": config.domain})
    country = (live_data.get("countries") or [None])[0]
    if not country:
        raise RuntimeError("No country data returned from live API")

    cities = country.get("cities") or []
    raw_json = live_data if config.store_raw_json else None

    with conn.cursor() as cur:
        ensure_partitions(cur, fetched_at)
        upsert_country(cur, country)
        upsert_cities(cur, cities)
        gap_info = record_snapshot_gap(cur, fetched_at, config.domain, config.poll_interval)
        if gap_info:
            log_event(
                "warn",
                "ingest",
                "Snapshot gap detected",
                event="snapshot_gap",
                config=config,
                extra=gap_info,
            )
        snapshot_id = insert_snapshot(cur, fetched_at, config.domain, raw_json)

        all_bike_type_ids: set[str] = set()
        all_bikes = []
        bike_status_rows = []
        place_count = 0
        bike_count = 0
        movement_candidates = 0

        for city in cities:
            insert_city_status(cur, snapshot_id, fetched_at, city)
            places = city.get("places") or []
            upsert_places(cur, city.get("uid"), places)
            insert_place_status(cur, snapshot_id, fetched_at, places)
            place_count += len(places)
            for place in places:
                for bike in place.get("bike_list") or []:
                    bike_number = bike.get("number")
                    if not bike_number:
                        continue
                    bike_type_id = (
                        str(bike.get("bike_type")) if bike.get("bike_type") is not None else None
                    )
                    if bike_type_id:
                        all_bike_type_ids.add(bike_type_id)
                    all_bikes.append(
                        (
                            bike_number,
                            bike.get("boardcomputer"),
                            bike_type_id,
                            bike.get("electric_lock"),
                            bike.get("lock_types"),
                            fetched_at,
                            fetched_at,
                        )
                    )
                    battery_pack = bike.get("battery_pack") or {}
                    bike_status_rows.append(
                        (
                            snapshot_id,
                            fetched_at,
                            bike_number,
                            place.get("uid"),
                            bike.get("active"),
                            bike.get("state"),
                            bike.get("pedelec_battery"),
                            battery_pack.get("percentage"),
                            place.get("lng"),
                            place.get("lat"),
                        )
                    )
                    bike_count += 1

        upsert_vehicle_types(cur, all_bike_type_ids)
        upsert_bikes(cur, all_bikes)
        insert_bike_status(cur, snapshot_id, bike_status_rows)
        movement_candidates = insert_bike_movements(cur, snapshot_id, fetched_at)

        if config.fetch_zones and config.city_id:
            zone_data = fetch_json(ZONE_BASE_URL.format(city_id=config.city_id))
            upsert_zone_features(cur, config.city_id, "zone-service", zone_data)
            flex_data = fetch_json(FLEXZONE_URL.format(domain=config.domain))
            upsert_zone_features(cur, config.city_id, "flexzone", flex_data)

        if config.fetch_gbfs:
            vehicle_types = fetch_gbfs_vehicle_types(config.gbfs_system_id)
            upsert_vehicle_type_metadata(cur, vehicle_types)

    conn.commit()
    maybe_refresh_materialized_views(conn, fetched_at, config.refresh_mv_interval)
    return {
        "snapshot_id": snapshot_id,
        "fetched_at": fetched_at,
        "cities": len(cities),
        "places": place_count,
        "bikes": bike_count,
        "movements": movement_candidates,
    }
