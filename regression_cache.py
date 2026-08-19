import json
import logging
import time
from typing import Any

from fleet_health_cache import _get_redis_client, _read_non_negative_int_env
from regressions import REGRESSION_DAYS

REGRESSION_CACHE_KEY = f"regressions:summary:{REGRESSION_DAYS}"
DEFAULT_REGRESSION_REDIS_TTL_SECONDS = 86400
REQUIRED_SUMMARY_KEYS = {
    "configured",
    "complete",
    "author_metrics",
    "reviewer_metrics",
}


def _is_cacheable_summary(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("days") == REGRESSION_DAYS
        and payload.get("configured") is True
        and REQUIRED_SUMMARY_KEYS <= payload.keys()
        and all(
            isinstance(payload.get(key), list) for key in ("author_metrics", "reviewer_metrics")
        )
    )


def get_cached_regression_summary() -> dict[str, Any] | None:
    client = _get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(REGRESSION_CACHE_KEY)
        payload = (json.loads(raw) if raw else {}).get("payload")
    except Exception:
        logging.exception("Failed to read regression summary from Redis")
        return None
    if not _is_cacheable_summary(payload):
        return None
    return payload


def store_cached_regression_summary(payload: dict[str, Any]) -> bool:
    if not _is_cacheable_summary(payload):
        return False
    client = _get_redis_client()
    if client is None:
        return False
    ttl_seconds = _read_non_negative_int_env(
        "REGRESSION_REDIS_TTL_SECONDS",
        DEFAULT_REGRESSION_REDIS_TTL_SECONDS,
    )
    value = json.dumps(
        {"cached_at_epoch": time.time(), "payload": payload},
        separators=(",", ":"),
    )
    try:
        client.setex(
            REGRESSION_CACHE_KEY,
            ttl_seconds or DEFAULT_REGRESSION_REDIS_TTL_SECONDS,
            value,
        )
    except Exception:
        logging.exception("Failed to write regression summary to Redis")
        return False
    return True


def refresh_regression_summary_cache() -> dict[str, Any] | None:
    from regressions import build_regression_summary

    try:
        summary = build_regression_summary()
    except Exception:
        logging.exception("Failed to refresh regression summary")
        return get_cached_regression_summary()
    store_cached_regression_summary(summary)
    return summary
