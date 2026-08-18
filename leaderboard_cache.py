import json
import logging
import time
from typing import Any

from fleet_health_cache import _get_redis_client, _read_non_negative_int_env

LEADERBOARD_CACHE_KEY_PREFIX = "leaderboard:index:"
DEFAULT_LEADERBOARD_DAYS = 30
DEFAULT_REDIS_TTL_SECONDS = 900


def get_cached_leaderboard(days: int) -> dict[str, Any] | None:
    client = _get_redis_client()
    if client is None:
        return None

    try:
        raw = client.get(_cache_key(days))
    except Exception:
        logging.exception("Failed to read leaderboard cache from Redis")
        return None

    if not raw:
        return None

    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        logging.warning("Leaderboard cache payload is not valid JSON")
        return None

    payload = parsed.get("payload")
    if not isinstance(payload, dict):
        logging.warning("Leaderboard cache payload has invalid shape")
        return None
    if not isinstance(payload.get("leaderboard_entries"), list):
        logging.warning("Leaderboard cache payload missing leaderboard_entries")
        return None
    if not isinstance(payload.get("days"), int):
        logging.warning("Leaderboard cache payload missing days")
        return None

    return payload


def store_cached_leaderboard(days: int, payload: dict[str, Any]) -> bool:
    client = _get_redis_client()
    if client is None:
        return False

    ttl_seconds = _read_non_negative_int_env(
        "LEADERBOARD_REDIS_TTL_SECONDS",
        DEFAULT_REDIS_TTL_SECONDS,
    )
    record = {
        "cached_at_epoch": time.time(),
        "payload": payload,
    }
    serialized = json.dumps(record, separators=(",", ":"))

    try:
        if ttl_seconds > 0:
            client.setex(_cache_key(days), ttl_seconds, serialized)
        else:
            client.set(_cache_key(days), serialized)
    except Exception:
        logging.exception("Failed to write leaderboard cache to Redis")
        return False

    return True


def refresh_leaderboard_cache(days: int = DEFAULT_LEADERBOARD_DAYS) -> dict[str, Any]:
    from app import compute_leaderboard_context

    context = compute_leaderboard_context(days)
    store_cached_leaderboard(days, context)
    return context


def _cache_key(days: int) -> str:
    return f"{LEADERBOARD_CACHE_KEY_PREFIX}{days}"
