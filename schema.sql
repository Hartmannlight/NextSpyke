CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS country (
  domain TEXT PRIMARY KEY,
  name TEXT,
  country_code TEXT,
  country_name TEXT,
  timezone TEXT,
  currency TEXT,
  hotline TEXT,
  email TEXT,
  website TEXT,
  terms TEXT,
  policy TEXT,
  pricing TEXT,
  operator_address TEXT,
  country_calling_code TEXT
);

CREATE TABLE IF NOT EXISTS city (
  city_uid INTEGER PRIMARY KEY,
  domain TEXT REFERENCES country(domain),
  name TEXT,
  alias TEXT,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  zoom INTEGER,
  bounds GEOMETRY(Polygon, 4326),
  refresh_rate_ms INTEGER,
  website TEXT,
  place_types JSONB,
  return_to_official_only BOOLEAN
);

CREATE TABLE IF NOT EXISTS place (
  place_uid INTEGER PRIMARY KEY,
  city_uid INTEGER REFERENCES city(city_uid),
  name TEXT,
  number INTEGER,
  spot BOOLEAN,
  terminal_type TEXT,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  geom GEOMETRY(Point, 4326),
  maintenance BOOLEAN,
  active_place INTEGER,
  bike BOOLEAN,
  booked_bikes INTEGER,
  bikes INTEGER,
  bikes_available_to_rent INTEGER,
  bike_racks INTEGER,
  free_racks INTEGER,
  special_racks INTEGER,
  free_special_racks INTEGER,
  rack_locks BOOLEAN,
  place_type TEXT,
  address TEXT
);

CREATE TABLE IF NOT EXISTS vehicle_type (
  vehicle_type_id TEXT PRIMARY KEY,
  name TEXT,
  form_factor TEXT,
  propulsion_type TEXT,
  max_range_meters INTEGER,
  description TEXT,
  rider_capacity INTEGER,
  vehicle_image TEXT
);

CREATE TABLE IF NOT EXISTS bike (
  bike_number TEXT PRIMARY KEY,
  boardcomputer BIGINT,
  bike_type_id TEXT REFERENCES vehicle_type(vehicle_type_id),
  electric_lock BOOLEAN,
  lock_types TEXT[],
  first_seen_at TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS zone (
  zone_id TEXT PRIMARY KEY,
  city_uid INTEGER REFERENCES city(city_uid),
  zone_source TEXT,
  zone_type TEXT,
  name TEXT,
  geom GEOMETRY(Geometry, 4326),
  properties JSONB
);

CREATE TABLE IF NOT EXISTS snapshot (
  snapshot_id BIGSERIAL,
  fetched_at TIMESTAMPTZ NOT NULL,
  domain TEXT NOT NULL,
  source TEXT NOT NULL,
  raw_json JSONB,
  PRIMARY KEY (snapshot_id, fetched_at)
) PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS snapshot_default PARTITION OF snapshot DEFAULT;

CREATE TABLE IF NOT EXISTS snapshot_gap (
  gap_id BIGSERIAL PRIMARY KEY,
  domain TEXT NOT NULL,
  gap_start TIMESTAMPTZ NOT NULL,
  gap_end TIMESTAMPTZ NOT NULL,
  gap_seconds INTEGER NOT NULL,
  expected_interval_s INTEGER NOT NULL,
  missing_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS city_status (
  snapshot_id BIGINT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  city_uid INTEGER REFERENCES city(city_uid),
  booked_bikes INTEGER,
  set_point_bikes INTEGER,
  available_bikes INTEGER,
  bike_types JSONB,
  PRIMARY KEY (snapshot_id, city_uid, fetched_at),
  FOREIGN KEY (snapshot_id, fetched_at) REFERENCES snapshot(snapshot_id, fetched_at)
) PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS city_status_default PARTITION OF city_status DEFAULT;

CREATE TABLE IF NOT EXISTS place_status (
  snapshot_id BIGINT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  place_uid INTEGER REFERENCES place(place_uid),
  booked_bikes INTEGER,
  bikes INTEGER,
  bikes_available_to_rent INTEGER,
  bike_racks INTEGER,
  free_racks INTEGER,
  special_racks INTEGER,
  free_special_racks INTEGER,
  bike_types JSONB,
  PRIMARY KEY (snapshot_id, place_uid, fetched_at),
  FOREIGN KEY (snapshot_id, fetched_at) REFERENCES snapshot(snapshot_id, fetched_at)
) PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS place_status_default PARTITION OF place_status DEFAULT;

CREATE TABLE IF NOT EXISTS bike_status (
  snapshot_id BIGINT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  bike_number TEXT REFERENCES bike(bike_number),
  place_uid INTEGER REFERENCES place(place_uid),
  active BOOLEAN,
  state TEXT,
  pedelec_battery INTEGER,
  battery_pack_pct INTEGER,
  battery_range_km DOUBLE PRECISION,
  geom GEOMETRY(Point, 4326),
  PRIMARY KEY (snapshot_id, bike_number, fetched_at),
  FOREIGN KEY (snapshot_id, fetched_at) REFERENCES snapshot(snapshot_id, fetched_at)
) PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS bike_status_default PARTITION OF bike_status DEFAULT;

CREATE TABLE IF NOT EXISTS bike_last_status (
  bike_number TEXT PRIMARY KEY REFERENCES bike(bike_number),
  snapshot_id BIGINT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  place_uid INTEGER REFERENCES place(place_uid),
  geom GEOMETRY(Point, 4326),
  active BOOLEAN,
  state TEXT,
  pedelec_battery INTEGER,
  battery_pack_pct INTEGER,
  battery_range_km DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS bike_movement (
  movement_id BIGSERIAL PRIMARY KEY,
  bike_number TEXT REFERENCES bike(bike_number),
  start_snapshot_id BIGINT NOT NULL,
  start_fetched_at TIMESTAMPTZ NOT NULL,
  end_snapshot_id BIGINT NOT NULL,
  end_fetched_at TIMESTAMPTZ NOT NULL,
  start_place_uid INTEGER REFERENCES place(place_uid),
  end_place_uid INTEGER REFERENCES place(place_uid),
  start_geom GEOMETRY(Point, 4326),
  end_geom GEOMETRY(Point, 4326),
  distance_m INTEGER,
  duration_seconds INTEGER,
  is_station_to_station BOOLEAN,
  confidence SMALLINT,
  movement_reason TEXT NOT NULL DEFAULT 'place_change',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE city
  ADD COLUMN IF NOT EXISTS place_types JSONB,
  ADD COLUMN IF NOT EXISTS return_to_official_only BOOLEAN;

ALTER TABLE vehicle_type
  ADD COLUMN IF NOT EXISTS description TEXT,
  ADD COLUMN IF NOT EXISTS rider_capacity INTEGER,
  ADD COLUMN IF NOT EXISTS vehicle_image TEXT;

ALTER TABLE place_status
  ADD COLUMN IF NOT EXISTS bike_types JSONB;

ALTER TABLE bike_status
  ADD COLUMN IF NOT EXISTS battery_range_km DOUBLE PRECISION;

ALTER TABLE bike_movement
  ADD COLUMN IF NOT EXISTS movement_reason TEXT NOT NULL DEFAULT 'place_change';

CREATE INDEX IF NOT EXISTS idx_snapshot_fetched_at ON snapshot (fetched_at);
CREATE INDEX IF NOT EXISTS idx_snapshot_domain_fetched_at
  ON snapshot (domain, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshot_gap_end ON snapshot_gap (gap_end);
CREATE INDEX IF NOT EXISTS idx_snapshot_gap_domain_end ON snapshot_gap (domain, gap_end);
CREATE INDEX IF NOT EXISTS idx_bike_status_bike_time ON bike_status (bike_number, fetched_at);
CREATE INDEX IF NOT EXISTS idx_place_status_place_time ON place_status (place_uid, fetched_at);
CREATE INDEX IF NOT EXISTS idx_city_status_city_time ON city_status (city_uid, fetched_at);
CREATE INDEX IF NOT EXISTS idx_place_geom ON place USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bike_status_geom ON bike_status USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_zone_geom ON zone USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bike_movement_bike_time ON bike_movement (bike_number, end_fetched_at);
CREATE INDEX IF NOT EXISTS idx_bike_movement_time ON bike_movement (end_fetched_at);
CREATE INDEX IF NOT EXISTS idx_bike_movement_places ON bike_movement (start_place_uid, end_place_uid);
CREATE INDEX IF NOT EXISTS idx_bike_movement_end_geom ON bike_movement USING GIST (end_geom);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bike_movement_unique
  ON bike_movement (bike_number, start_snapshot_id, end_snapshot_id);

-- These full-history rollups made ingest latency grow with database size. Dashboards now
-- aggregate bounded time windows directly from bike_movement and bike_last_status.
DROP MATERIALIZED VIEW IF EXISTS mv_hotspots_hourly;
DROP MATERIALIZED VIEW IF EXISTS mv_city_bikes_hourly;
DROP MATERIALIZED VIEW IF EXISTS mv_routes_top;
DROP MATERIALIZED VIEW IF EXISTS mv_bike_dwell;
