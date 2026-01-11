import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    service: str
    env: str
    version: str
    commit: str
    domain: str
    city_id: int | None
    poll_interval: int
    fetch_zones: bool
    fetch_gbfs: bool
    store_raw_json: bool
    refresh_mv_interval: int
    gbfs_system_id: str
    metrics_enabled: bool
    metrics_port: int
    config_source: str
    config_hash: str


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def sanitize_config(config: dict) -> dict:
    redacted = {}
    for key, value in config.items():
        if value is None:
            redacted[key] = None
            continue
        if "PASSWORD" in key or key in {"DATABASE_URL"}:
            redacted[key] = "***"
        else:
            redacted[key] = value
    return redacted


def hash_config(config: dict) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = __import__("hashlib").sha256(payload).hexdigest()
    return f"sha256:{digest}"


def load_config() -> AppConfig:
    service = os.getenv("SERVICE_NAME", "nextspyke")
    env = os.getenv("APP_ENV", "dev")
    version = os.getenv("APP_VERSION", "0.1.0")
    commit = os.getenv("APP_COMMIT", "unknown")
    domain = os.getenv("NEXTBIKE_DOMAIN", "fg")
    city_id_raw = os.getenv("NEXTBIKE_CITY_ID")
    city_id = int(city_id_raw) if city_id_raw else None
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    fetch_zones = env_bool("FETCH_ZONES", True)
    fetch_gbfs = env_bool("FETCH_GBFS", True)
    store_raw_json = env_bool("STORE_RAW_JSON", True)
    refresh_mv_interval = int(os.getenv("REFRESH_MV_INTERVAL_SECONDS", "0"))
    gbfs_system_id = os.getenv("GBFS_SYSTEM_ID", f"nextbike_{domain}")
    metrics_enabled = env_bool("METRICS_ENABLED", False)
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    config_source = "env"

    config_payload = sanitize_config(
        {
            "SERVICE_NAME": service,
            "APP_ENV": env,
            "APP_VERSION": version,
            "APP_COMMIT": commit,
            "NEXTBIKE_DOMAIN": domain,
            "NEXTBIKE_CITY_ID": city_id,
            "POLL_INTERVAL_SECONDS": poll_interval,
            "FETCH_ZONES": fetch_zones,
            "FETCH_GBFS": fetch_gbfs,
            "STORE_RAW_JSON": store_raw_json,
            "REFRESH_MV_INTERVAL_SECONDS": refresh_mv_interval,
            "GBFS_SYSTEM_ID": gbfs_system_id,
            "METRICS_ENABLED": metrics_enabled,
            "METRICS_PORT": metrics_port,
            "PGHOST": os.getenv("PGHOST"),
            "PGPORT": os.getenv("PGPORT"),
            "PGDATABASE": os.getenv("PGDATABASE"),
            "PGUSER": os.getenv("PGUSER"),
            "PGPASSWORD": os.getenv("PGPASSWORD"),
            "DATABASE_URL": os.getenv("DATABASE_URL"),
        }
    )
    config_hash = hash_config(config_payload)

    return AppConfig(
        service=service,
        env=env,
        version=version,
        commit=commit,
        domain=domain,
        city_id=city_id,
        poll_interval=poll_interval,
        fetch_zones=fetch_zones,
        fetch_gbfs=fetch_gbfs,
        store_raw_json=store_raw_json,
        refresh_mv_interval=refresh_mv_interval,
        gbfs_system_id=gbfs_system_id,
        metrics_enabled=metrics_enabled,
        metrics_port=metrics_port,
        config_source=config_source,
        config_hash=config_hash,
    )
