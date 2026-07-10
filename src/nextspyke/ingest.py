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


def fetch_json(url: str, params: dict | None = None) -> dict:
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "NextSpyke/0.1"})
    with urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def log_optional_failure(config: AppConfig, source: str, exc: BaseException) -> None:
    log_event(
        "warn",
        "ingest",
        "Optional ingest step failed",
        event="optional_ingest_failed",
        config=config,
        extra={"optional_source": source},
        exc=exc,
    )


def is_connection_failure(exc: BaseException) -> bool:
    return isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError))


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


def upsert_cities(cur: psycopg.Cursor, domain: str, cities: list[dict]) -> None:
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
                city_uid, domain, name, alias, lat, lng, zoom, bounds, refresh_rate_ms, website,
                place_types, return_to_official_only
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, {bounds_sql}, %s, %s, %s, %s)
            ON CONFLICT (city_uid) DO UPDATE SET
                domain = EXCLUDED.domain,
                name = EXCLUDED.name,
                alias = EXCLUDED.alias,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                zoom = EXCLUDED.zoom,
                bounds = EXCLUDED.bounds,
                refresh_rate_ms = EXCLUDED.refresh_rate_ms,
                website = EXCLUDED.website,
                place_types = EXCLUDED.place_types,
                return_to_official_only = EXCLUDED.return_to_official_only
            """,
            (
                city.get("uid"),
                city.get("domain") or domain,
                city.get("name"),
                city.get("alias"),
                city.get("lat"),
                city.get("lng"),
                city.get("zoom"),
                *bounds_args,
                int(city.get("refresh_rate") or 0) or None,
                city.get("website"),
                Json(city.get("place_types") or {}),
                city.get("return_to_official_only"),
            ),
        )


def upsert_places(cur: psycopg.Cursor, city_uid: int, places: list[dict]) -> None:
    for place in places:
        if place.get("spot") is not True:
            continue
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
            boardcomputer = COALESCE(EXCLUDED.boardcomputer, bike.boardcomputer),
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
        if place.get("spot") is not True:
            continue
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
                Json(place.get("bike_types") or {}),
            )
        )
    if rows:
        cur.executemany(
            """
            INSERT INTO place_status (
                snapshot_id, fetched_at, place_uid, booked_bikes, bikes, bikes_available_to_rent,
                bike_racks, free_racks, special_racks, free_special_racks, bike_types
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def insert_bike_status(cur: psycopg.Cursor, snapshot_id: int, bike_rows: list[tuple]) -> None:
    if not bike_rows:
        return
    cur.executemany(
        """
        INSERT INTO bike_status (
            snapshot_id, fetched_at, bike_number, place_uid, active, state, pedelec_battery,
            battery_pack_pct, battery_range_km, geom
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
        """,
        bike_rows,
    )


def insert_snapshot(
    cur: psycopg.Cursor, fetched_at: datetime, domain: str, raw_json: dict | None
) -> int:
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


def insert_bike_movements(
    cur: psycopg.Cursor,
    snapshot_id: int,
    fetched_at: datetime,
    min_distance_m: float,
) -> int:
    cur.execute(
        """
        WITH current AS (
            SELECT bike_number, snapshot_id, fetched_at, place_uid, geom
            FROM bike_status
            WHERE snapshot_id = %s AND fetched_at = %s
        ),
        pairs AS (
            SELECT
                c.bike_number,
                p.snapshot_id AS start_snapshot_id,
                p.fetched_at AS start_fetched_at,
                c.snapshot_id AS end_snapshot_id,
                c.fetched_at AS end_fetched_at,
                p.place_uid AS start_place_uid,
                c.place_uid AS end_place_uid,
                p.geom AS start_geom,
                c.geom AS end_geom,
                ROUND(ST_Distance(p.geom::geography, c.geom::geography))::int AS distance_m,
                ps.spot AS start_spot,
                pe.spot AS end_spot
            FROM current c
            JOIN bike_last_status p ON p.bike_number = c.bike_number
            LEFT JOIN place ps ON ps.place_uid = p.place_uid
            LEFT JOIN place pe ON pe.place_uid = c.place_uid
            WHERE c.geom IS NOT NULL
              AND p.geom IS NOT NULL
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
            confidence,
            movement_reason
        )
        SELECT
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
            GREATEST(EXTRACT(EPOCH FROM end_fetched_at - start_fetched_at)::int, 0),
            start_spot IS TRUE AND end_spot IS TRUE,
            CASE
                WHEN start_spot IS TRUE AND end_spot IS TRUE THEN 100
                WHEN start_place_uid <> end_place_uid THEN 75
                ELSE 60
            END,
            CASE
                WHEN start_spot IS TRUE
                 AND end_spot IS TRUE
                 AND start_place_uid IS DISTINCT FROM end_place_uid
                THEN 'station_change'
                ELSE 'coordinate_change'
            END
        FROM pairs
        WHERE distance_m >= %s
        ON CONFLICT DO NOTHING
        """,
        (snapshot_id, fetched_at, min_distance_m),
    )
    return cur.rowcount or 0


def update_bike_last_status(
    cur: psycopg.Cursor,
    snapshot_id: int,
    fetched_at: datetime,
) -> None:
    cur.execute(
        """
        INSERT INTO bike_last_status (
            bike_number, snapshot_id, fetched_at, place_uid, geom, active, state,
            pedelec_battery, battery_pack_pct, battery_range_km
        )
        SELECT
            bike_number, snapshot_id, fetched_at, place_uid, geom, active, state,
            pedelec_battery, battery_pack_pct, battery_range_km
        FROM bike_status
        WHERE snapshot_id = %s AND fetched_at = %s
        ON CONFLICT (bike_number) DO UPDATE SET
            snapshot_id = EXCLUDED.snapshot_id,
            fetched_at = EXCLUDED.fetched_at,
            place_uid = EXCLUDED.place_uid,
            geom = EXCLUDED.geom,
            active = EXCLUDED.active,
            state = EXCLUDED.state,
            pedelec_battery = EXCLUDED.pedelec_battery,
            battery_pack_pct = EXCLUDED.battery_pack_pct,
            battery_range_km = EXCLUDED.battery_range_km
        WHERE bike_last_status.fetched_at < EXCLUDED.fetched_at
        """,
        (snapshot_id, fetched_at),
    )


def backfill_bike_movements(cur: psycopg.Cursor, min_distance_m: float) -> int:
    cur.execute(
        """
        WITH ordered AS (
            SELECT
                bs.bike_number,
                bs.snapshot_id AS end_snapshot_id,
                bs.fetched_at AS end_fetched_at,
                bs.place_uid AS end_place_uid,
                bs.geom AS end_geom,
                LAG(bs.snapshot_id) OVER sighting AS start_snapshot_id,
                LAG(bs.fetched_at) OVER sighting AS start_fetched_at,
                LAG(bs.place_uid) OVER sighting AS start_place_uid,
                LAG(bs.geom) OVER sighting AS start_geom
            FROM bike_status bs
            WINDOW sighting AS (
                PARTITION BY bs.bike_number
                ORDER BY bs.fetched_at, bs.snapshot_id
            )
        ),
        pairs AS (
            SELECT
                o.*,
                ROUND(
                    ST_Distance(o.start_geom::geography, o.end_geom::geography)
                )::int AS distance_m,
                ps.spot AS start_spot,
                pe.spot AS end_spot
            FROM ordered o
            LEFT JOIN place ps ON ps.place_uid = o.start_place_uid
            LEFT JOIN place pe ON pe.place_uid = o.end_place_uid
            WHERE o.start_snapshot_id IS NOT NULL
              AND o.start_geom IS NOT NULL
              AND o.end_geom IS NOT NULL
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
            confidence,
            movement_reason
        )
        SELECT
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
            GREATEST(EXTRACT(EPOCH FROM end_fetched_at - start_fetched_at)::int, 0),
            start_spot IS TRUE AND end_spot IS TRUE,
            CASE
                WHEN start_spot IS TRUE AND end_spot IS TRUE THEN 100
                WHEN start_place_uid <> end_place_uid THEN 75
                ELSE 60
            END,
            CASE
                WHEN start_spot IS TRUE
                 AND end_spot IS TRUE
                 AND start_place_uid IS DISTINCT FROM end_place_uid
                THEN 'station_change'
                ELSE 'coordinate_change'
            END
        FROM pairs
        WHERE distance_m >= %s
        ON CONFLICT DO NOTHING
        """,
        (min_distance_m,),
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
                item.get("_description"),
                item.get("rider_capacity"),
                item.get("vehicle_image"),
            )
        )
    if not rows:
        return
    cur.executemany(
        """
        INSERT INTO vehicle_type (
            vehicle_type_id, name, form_factor, propulsion_type, max_range_meters,
            description, rider_capacity, vehicle_image
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vehicle_type_id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, vehicle_type.name),
            form_factor = COALESCE(EXCLUDED.form_factor, vehicle_type.form_factor),
            propulsion_type = COALESCE(EXCLUDED.propulsion_type, vehicle_type.propulsion_type),
            max_range_meters = COALESCE(EXCLUDED.max_range_meters, vehicle_type.max_range_meters),
            description = COALESCE(EXCLUDED.description, vehicle_type.description),
            rider_capacity = COALESCE(EXCLUDED.rider_capacity, vehicle_type.rider_capacity),
            vehicle_image = COALESCE(EXCLUDED.vehicle_image, vehicle_type.vehicle_image)
        """,
        rows,
    )


def refresh_zone_metadata(conn: psycopg.Connection, config: AppConfig) -> None:
    if not config.fetch_zones or not config.city_id:
        return
    sources = (
        (
            "zone-service",
            ZONE_BASE_URL.format(city_id=config.city_id),
        ),
        (
            "flexzone",
            FLEXZONE_URL.format(domain=config.domain),
        ),
    )
    for source, url in sources:
        try:
            data = fetch_json(url)
            with conn.transaction():
                with conn.cursor() as cur:
                    upsert_zone_features(cur, config.city_id, source, data)
        except Exception as exc:
            log_optional_failure(config, source, exc)
            if is_connection_failure(exc):
                raise


def refresh_vehicle_type_metadata(conn: psycopg.Connection, config: AppConfig) -> None:
    if not config.fetch_gbfs:
        return
    try:
        vehicle_types = fetch_gbfs_vehicle_types(config.gbfs_system_id)
        with conn.transaction():
            with conn.cursor() as cur:
                upsert_vehicle_type_metadata(cur, vehicle_types)
    except Exception as exc:
        log_optional_failure(config, "gbfs_vehicle_types", exc)
        if is_connection_failure(exc):
            raise


def ingest_once(conn: psycopg.Connection, config: AppConfig) -> dict:
    fetched_at = utc_now()
    live_data = fetch_json(LIVE_BASE_URL, {"domains": config.domain})
    country = (live_data.get("countries") or [None])[0]
    if not country:
        raise RuntimeError("No country data returned from live API")

    cities = country.get("cities") or []
    raw_json = live_data if config.store_raw_json else None

    with conn.transaction():
        with conn.cursor() as cur:
            ensure_partitions(cur, fetched_at)
            upsert_country(cur, country)
            upsert_cities(cur, country.get("domain") or config.domain, cities)
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
                stations = [place for place in places if place.get("spot") is True]
                upsert_places(cur, city.get("uid"), stations)
                insert_place_status(cur, snapshot_id, fetched_at, stations)
                place_count += len(stations)
                for place in places:
                    for bike in place.get("bike_list") or []:
                        bike_number = bike.get("number")
                        if not bike_number:
                            continue
                        bike_type_id = (
                            str(bike.get("bike_type"))
                            if bike.get("bike_type") is not None
                            else None
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
                                place.get("uid") if place.get("spot") is True else None,
                                bike.get("active"),
                                bike.get("state"),
                                bike.get("pedelec_battery"),
                                battery_pack.get("percentage"),
                                battery_pack.get("estimated_range_km"),
                                place.get("lng"),
                                place.get("lat"),
                            )
                        )
                        bike_count += 1

            upsert_vehicle_types(cur, all_bike_type_ids)
            upsert_bikes(cur, all_bikes)
            insert_bike_status(cur, snapshot_id, bike_status_rows)
            movement_candidates = insert_bike_movements(
                cur,
                snapshot_id,
                fetched_at,
                config.movement_min_distance_m,
            )
            update_bike_last_status(cur, snapshot_id, fetched_at)

    refresh_zone_metadata(conn, config)
    refresh_vehicle_type_metadata(conn, config)
    return {
        "snapshot_id": snapshot_id,
        "fetched_at": fetched_at,
        "cities": len(cities),
        "places": place_count,
        "bikes": bike_count,
        "movements": movement_candidates,
    }
