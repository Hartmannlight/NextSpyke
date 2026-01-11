import json
import socket
import sys
import traceback
import uuid
from datetime import datetime, timezone

from nextspyke.config import AppConfig

RUN_ID = str(uuid.uuid4())
INSTANCE_ID = socket.gethostname()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_ts(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def log_event(
    level: str,
    logger: str,
    msg: str,
    *,
    event: str | None = None,
    config: AppConfig | None = None,
    extra: dict | None = None,
    exc: BaseException | None = None,
) -> None:
    record = {
        "ts": iso_ts(utc_now()),
        "level": level,
        "service": (config.service if config else "nextspyke"),
        "logger": logger,
        "instance": INSTANCE_ID,
        "env": (config.env if config else "dev"),
        "run_id": RUN_ID,
        "msg": msg,
    }
    if event:
        record["event"] = event
    if config and event in {
        "startup_begin",
        "startup_success",
        "shutting_down",
        "shutdown_complete",
        "crashed",
    }:
        record.update(
            {
                "version": config.version,
                "commit": config.commit,
                "config_source": config.config_source,
                "config_hash": config.config_hash,
            }
        )
    if extra:
        record.update(extra)
    if level == "error":
        record["error"] = msg
    if exc:
        record["error"] = str(exc) or msg
        record["exception_type"] = exc.__class__.__name__
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        record["stack"] = stack.replace("\n", "\\n")
    sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
    sys.stdout.flush()
