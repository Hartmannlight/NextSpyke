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
  website TEXT
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
  max_range_meters INTEGER
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
  geom GEOMETRY(Point, 4326),
  PRIMARY KEY (snapshot_id, bike_number, fetched_at),
  FOREIGN KEY (snapshot_id, fetched_at) REFERENCES snapshot(snapshot_id, fetched_at)
) PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS bike_status_default PARTITION OF bike_status DEFAULT;

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
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshot_fetched_at ON snapshot (fetched_at);
CREATE INDEX IF NOT EXISTS idx_snapshot_gap_end ON snapshot_gap (gap_end);
CREATE INDEX IF NOT EXISTS idx_snapshot_gap_domain_end ON snapshot_gap (domain, gap_end);
CREATE INDEX IF NOT EXISTS idx_bike_status_bike_time ON bike_status (bike_number, fetched_at);
CREATE INDEX IF NOT EXISTS idx_place_status_place_time ON place_status (place_uid, fetched_at);
CREATE INDEX IF NOT EXISTS idx_city_status_city_time ON city_status (city_uid, fetched_at);
CREATE INDEX IF NOT EXISTS idx_place_geom ON place USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bike_status_geom ON bike_status USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_zone_geom ON zone USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bike_movement_bike_time ON bike_movement (bike_number, end_fetched_at);
CREATE INDEX IF NOT EXISTS idx_bike_movement_places ON bike_movement (start_place_uid, end_place_uid);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bike_movement_unique
  ON bike_movement (bike_number, start_snapshot_id, end_snapshot_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hotspots_hourly AS
SELECT
  date_trunc('hour', bs.fetched_at) AS hour,
  p.place_uid,
  p.name AS place_name,
  p.city_uid,
  COUNT(DISTINCT bs.bike_number) AS bikes_seen
FROM bike_status bs
JOIN place p ON p.place_uid = bs.place_uid
GROUP BY 1, 2, 3, 4;

CREATE INDEX IF NOT EXISTS idx_mv_hotspots_hourly ON mv_hotspots_hourly (hour, place_uid);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_city_bikes_hourly AS
SELECT
  date_trunc('hour', bs.fetched_at) AS hour,
  c.city_uid,
  c.name AS city_name,
  COUNT(DISTINCT bs.bike_number) AS bikes_seen
FROM bike_status bs
JOIN place p ON p.place_uid = bs.place_uid
JOIN city c ON c.city_uid = p.city_uid
GROUP BY 1, 2, 3;

CREATE INDEX IF NOT EXISTS idx_mv_city_bikes_hourly ON mv_city_bikes_hourly (hour, city_uid);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_routes_top AS
SELECT
  bm.start_place_uid,
  ps.name AS start_place,
  bm.end_place_uid,
  pe.name AS end_place,
  COUNT(*) AS trips,
  AVG(bm.distance_m)::int AS avg_distance_m,
  AVG(bm.duration_seconds)::int AS avg_duration_s
FROM bike_movement bm
LEFT JOIN place ps ON ps.place_uid = bm.start_place_uid
LEFT JOIN place pe ON pe.place_uid = bm.end_place_uid
GROUP BY 1, 2, 3, 4;

CREATE INDEX IF NOT EXISTS idx_mv_routes_top ON mv_routes_top (trips DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_bike_dwell AS
WITH ordered AS (
  SELECT
    bs.bike_number,
    bs.place_uid,
    bs.fetched_at,
    LAG(bs.place_uid) OVER (PARTITION BY bs.bike_number ORDER BY bs.fetched_at) AS prev_place_uid
  FROM bike_status bs
),
changes AS (
  SELECT
    bike_number,
    MAX(fetched_at) FILTER (WHERE prev_place_uid IS NULL OR prev_place_uid <> place_uid) AS last_change_at
  FROM ordered
  GROUP BY bike_number
),
last_seen AS (
  SELECT DISTINCT ON (bike_number)
    bike_number,
    fetched_at AS last_seen_at,
    place_uid AS last_place_uid
  FROM bike_status
  ORDER BY bike_number, fetched_at DESC
)
SELECT
  l.bike_number,
  l.last_seen_at,
  l.last_place_uid AS place_uid,
  p.name AS place_name,
  EXTRACT(EPOCH FROM (l.last_seen_at - c.last_change_at))::int AS dwell_seconds
FROM last_seen l
JOIN changes c ON c.bike_number = l.bike_number
LEFT JOIN place p ON p.place_uid = l.last_place_uid;

CREATE INDEX IF NOT EXISTS idx_mv_bike_dwell ON mv_bike_dwell (dwell_seconds DESC);
