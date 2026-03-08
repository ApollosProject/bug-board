import json
import logging
import os
import time
from functools import lru_cache
from typing import Any, cast
from urllib.parse import parse_qsl, urlsplit

try:
    import redis
except ImportError:  # pragma: no cover - optional dependency for cache deployments
    redis = cast(Any, None)


FLEET_HEALTH_CACHE_KEY = "airflow:fleet_health:latest"
DEFAULT_MAX_STALE_SECONDS = 180
DEFAULT_REDIS_TTL_SECONDS = 900
DEFAULT_REDIS_SSL_CERT_REQS = "none"


def should_use_redis_cache() -> bool:
    return bool(_get_redis_url()) and redis is not None


def get_cached_fleet_health() -> tuple[dict[str, Any], int] | None:
    client = _get_redis_client()
    if client is None:
        return None

    try:
        raw = client.get(FLEET_HEALTH_CACHE_KEY)
    except Exception:
        logging.exception("Failed to read airflow fleet health cache from Redis")
        return None

    if not raw:
        return None

    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        logging.warning("Airflow fleet health cache payload is not valid JSON")
        return None

    payload = parsed.get("payload")
    status = parsed.get("status")
    cached_at_epoch = parsed.get("cached_at_epoch")

    if not isinstance(payload, dict) or not isinstance(status, int):
        logging.warning("Airflow fleet health cache payload has invalid shape")
        return None
    if not isinstance(cached_at_epoch, (int, float)):
        logging.warning("Airflow fleet health cache payload missing cached_at_epoch")
        return None

    max_stale_seconds = _read_non_negative_int_env(
        "AIRFLOW_FLEET_HEALTH_MAX_STALE_SECONDS",
        DEFAULT_MAX_STALE_SECONDS,
    )
    if max_stale_seconds > 0 and time.time() - float(cached_at_epoch) > max_stale_seconds:
        return None

    return payload, status


def store_cached_fleet_health(payload: dict[str, Any], status: int) -> bool:
    client = _get_redis_client()
    if client is None:
        return False

    ttl_seconds = _read_non_negative_int_env(
        "AIRFLOW_FLEET_HEALTH_REDIS_TTL_SECONDS",
        DEFAULT_REDIS_TTL_SECONDS,
    )
    record = {
        "cached_at_epoch": time.time(),
        "status": status,
        "payload": payload,
    }
    serialized = json.dumps(record, separators=(",", ":"))

    try:
        if ttl_seconds > 0:
            client.setex(FLEET_HEALTH_CACHE_KEY, ttl_seconds, serialized)
        else:
            client.set(FLEET_HEALTH_CACHE_KEY, serialized)
    except Exception:
        logging.exception("Failed to write airflow fleet health cache to Redis")
        return False

    return True


def refresh_fleet_health_cache() -> tuple[dict[str, Any], int]:
    from airflow_fleet_health import AirflowFleetHealthError, evaluate_fleet_health

    try:
        payload, status = evaluate_fleet_health()
    except AirflowFleetHealthError:
        logging.exception("Airflow fleet health evaluation failed while refreshing Redis")
        payload = {"status": "unknown"}
        status = 503

    store_cached_fleet_health(payload, status)
    return payload, status


def _get_redis_url() -> str:
    return os.getenv("REDIS_URL", "").strip()


@lru_cache(maxsize=1)
def _get_redis_client() -> Any | None:
    redis_url = _get_redis_url()
    if not redis_url:
        return None
    if redis is None:
        logging.warning("REDIS_URL is set but redis package is not installed")
        return None

    try:
        return redis.Redis.from_url(redis_url, **_build_redis_client_kwargs(redis_url))
    except Exception:
        logging.exception("Failed to initialize Redis client for airflow fleet health cache")
        return None


def _build_redis_client_kwargs(redis_url: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "decode_responses": True,
        "socket_connect_timeout": 2,
        "socket_timeout": 2,
    }
    if not redis_url.startswith("rediss://"):
        return kwargs

    query = dict(parse_qsl(urlsplit(redis_url).query, keep_blank_values=True))
    if "ssl_cert_reqs" in query:
        return kwargs

    raw_cert_reqs = os.getenv("REDIS_SSL_CERT_REQS", "").strip().lower()
    if raw_cert_reqs in {"none", "optional", "required"}:
        kwargs["ssl_cert_reqs"] = raw_cert_reqs
        return kwargs
    if raw_cert_reqs:
        logging.warning(
            "Ignoring invalid REDIS_SSL_CERT_REQS=%r; expected one of: none, optional, required",
            raw_cert_reqs,
        )

    kwargs["ssl_cert_reqs"] = DEFAULT_REDIS_SSL_CERT_REQS
    return kwargs


def _read_non_negative_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(0, parsed)
