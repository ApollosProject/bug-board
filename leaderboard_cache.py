import json
import logging
import time
from typing import Any

from fleet_health_cache import (
    DEFAULT_REDIS_TTL_SECONDS,
    _get_redis_client,
    _read_non_negative_int_env,
)

LEADERBOARD_CACHE_KEY_PREFIX = "leaderboard:index:"
DEFAULT_LEADERBOARD_DAYS = 30


def get_cached_leaderboard(days: int) -> dict[str, Any] | None:
    client = _get_redis_client()
    if client is None:
        return None

    try:
        raw = client.get(_cache_key(days))
        parsed = json.loads(raw) if raw else None
    except Exception:
        logging.exception("Failed to read leaderboard cache from Redis")
        return None

    payload = parsed.get("payload") if isinstance(parsed, dict) else None
    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("leaderboard_entries"), list)
        or payload.get("days") != days
    ):
        return None
    return payload


def store_cached_leaderboard(days: int, payload: dict[str, Any]) -> bool:
    client = _get_redis_client()
    if client is None:
        return False

    ttl_seconds = _read_non_negative_int_env(
        "LEADERBOARD_REDIS_TTL_SECONDS", DEFAULT_REDIS_TTL_SECONDS
    )
    serialized = json.dumps(
        {"cached_at_epoch": time.time(), "payload": {**payload, "days": days}},
        separators=(",", ":"),
    )
    try:
        client.setex(_cache_key(days), ttl_seconds or DEFAULT_REDIS_TTL_SECONDS, serialized)
    except Exception:
        logging.exception("Failed to write leaderboard cache to Redis")
        return False
    return True


def refresh_leaderboard_cache(days: int = DEFAULT_LEADERBOARD_DAYS) -> dict[str, Any]:
    from app import compute_leaderboard_context

    try:
        context = compute_leaderboard_context(days)
    except Exception:
        logging.exception("Failed to refresh leaderboard cache")
        return get_cached_leaderboard(days) or {
            "days": days,
            "leaderboard_entries": [],
        }

    store_cached_leaderboard(days, context)
    return context


def _cache_key(days: int) -> str:
    return f"{LEADERBOARD_CACHE_KEY_PREFIX}{days}"
