# NextSpyke

NextSpyke collects live bike availability data from Nextbike and stores it so you
can explore usage patterns, hotspots, and city-level trends over time.

## Quick start

```bash
docker-compose up --build
```

Defaults are set for Karlsruhe (`domain=fg`, `city_id=21`). Change via env vars in
`docker-compose.yml` if needed.

Grafana is available on `http://localhost:3000` (user/pass: `grafana`/`grafana`).
Prometheus is available on `http://localhost:9090`.
Loki is available on `http://localhost:3100`.

## Docker (app + Postgres example)

Use the published image from GHCR and a local Postgres instance:

```yaml
services:
  db:
    image: postgis/postgis:16-3.4
    environment:
      POSTGRES_USER: nextspyke
      POSTGRES_PASSWORD: nextspyke
      POSTGRES_DB: nextspyke
    ports:
      - "5432:5432"
    volumes:
      - nextspyke_pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U nextspyke -d nextspyke"]
      interval: 5s
      timeout: 3s
      retries: 10

  app:
    image: ghcr.io/hartmannlight/nextspyke:latest
    depends_on:
      db:
        condition: service_healthy
    environment:
      PGHOST: db
      PGPORT: 5432
      PGDATABASE: nextspyke
      PGUSER: nextspyke
      PGPASSWORD: nextspyke
      NEXTBIKE_DOMAIN: fg
      NEXTBIKE_CITY_ID: 21
      POLL_INTERVAL_SECONDS: 60
      FETCH_ZONES: "true"
      STORE_RAW_JSON: "true"
      REFRESH_MV_INTERVAL_SECONDS: 300
      METRICS_ENABLED: "true"
      METRICS_PORT: 8000
    ports:
      - "8000:8000"
    restart: unless-stopped

volumes:
  nextspyke_pgdata:
```

## Full stack (app + Postgres + Prometheus + Grafana + Loki)

Use this if you want metrics + logs + dashboards out of the box:

```yaml
services:
  db:
    image: postgis/postgis:16-3.4
    environment:
      POSTGRES_USER: nextspyke
      POSTGRES_PASSWORD: nextspyke
      POSTGRES_DB: nextspyke
    ports:
      - "5432:5432"
    volumes:
      - nextspyke_pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U nextspyke -d nextspyke"]
      interval: 5s
      timeout: 3s
      retries: 10

  app:
    image: ghcr.io/hartmannlight/nextspyke:latest
    depends_on:
      db:
        condition: service_healthy
    environment:
      PGHOST: db
      PGPORT: 5432
      PGDATABASE: nextspyke
      PGUSER: nextspyke
      PGPASSWORD: nextspyke
      NEXTBIKE_DOMAIN: fg
      NEXTBIKE_CITY_ID: 21
      POLL_INTERVAL_SECONDS: 60
      FETCH_ZONES: "true"
      STORE_RAW_JSON: "true"
      REFRESH_MV_INTERVAL_SECONDS: 300
      METRICS_ENABLED: "true"
      METRICS_PORT: 8000
    ports:
      - "8000:8000"
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:v2.51.1
    volumes:
      - ./observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    depends_on:
      app:
        condition: service_healthy
    restart: unless-stopped

  loki:
    image: grafana/loki:2.9.8
    command: -config.file=/etc/loki/loki.yml
    volumes:
      - ./observability/loki/loki.yml:/etc/loki/loki.yml:ro
      - loki_data:/loki
    ports:
      - "3100:3100"
    restart: unless-stopped

  promtail:
    image: grafana/promtail:2.9.8
    command: -config.file=/etc/promtail/promtail.yml
    volumes:
      - ./observability/promtail/promtail.yml:/etc/promtail/promtail.yml:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      loki:
        condition: service_started
    restart: unless-stopped

  grafana:
    image: grafana/grafana:10.4.5
    environment:
      GF_SECURITY_ADMIN_USER: grafana
      GF_SECURITY_ADMIN_PASSWORD: grafana
    volumes:
      - grafana_data:/var/lib/grafana
      - ./observability/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./observability/grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports:
      - "3000:3000"
    depends_on:
      prometheus:
        condition: service_started
      loki:
        condition: service_started
    restart: unless-stopped

volumes:
  nextspyke_pgdata:
  prometheus_data:
  grafana_data:
  loki_data:
```

Grafana is available at `http://localhost:3000` (user/pass: `grafana`/`grafana`).
Prometheus is available at `http://localhost:9090`. Loki at `http://localhost:3100`.

## Using your own Postgres/Grafana/Loki

If you already run your own instances, you can plug NextSpyke into them:

1) Postgres
- Create a database and user (or reuse one) and enable PostGIS.
- Set `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` for the app.

2) Grafana dashboards
- Import the JSON dashboards from `observability/grafana/dashboards/`.
- Add a Postgres data source pointing to your DB.
- Add a Loki data source (optional, for logs).
- Add a Prometheus data source pointing to your metrics endpoint.

3) Metrics + logs
- Prometheus scrape target: `http://<app-host>:8000/metrics`
- Loki: send container logs via Promtail or your preferred log shipper.

## Useful environment variables

- `NEXTBIKE_DOMAIN` (default `fg`)
- `NEXTBIKE_CITY_ID` (default `21`)
- `POLL_INTERVAL_SECONDS` (default `60`)
- `FETCH_ZONES` (default `true`)
- `FETCH_GBFS` (default `true`)
- `STORE_RAW_JSON` (default `true`)
- `REFRESH_MV_INTERVAL_SECONDS` (default `0`, set e.g. `300` to refresh materialized views)
- `SERVICE_NAME` (default `nextspyke`)
- `APP_ENV` (default `dev`)
- `APP_VERSION` (default `0.1.0`)
- `APP_COMMIT` (default `unknown`)
- `METRICS_ENABLED` (default `false`)
- `METRICS_PORT` (default `8000`)

## Health check

```bash
python -m nextspyke.app health
```

## Integration tests (with Docker)

```bash
python scripts/run_db_tests.py
```

Keep the database running after tests:
```bash
set KEEP_DB=true
python scripts/run_db_tests.py
```

## Test coverage

```bash
coverage run -m unittest discover -s tests
coverage report -m
```

Generate an HTML report:
```bash
coverage html
```

## Schema changes and partitions

The time-series tables are partitioned by `fetched_at`. If you already created the
database before this change, drop the volume and recreate:

```bash
docker compose down -v
docker compose up --build
```

## Grafana materialized views

Refresh the precomputed views when needed:

```sql
REFRESH MATERIALIZED VIEW mv_hotspots_hourly;
REFRESH MATERIALIZED VIEW mv_city_bikes_hourly;
REFRESH MATERIALIZED VIEW mv_routes_top;
REFRESH MATERIALIZED VIEW mv_bike_dwell;
```
