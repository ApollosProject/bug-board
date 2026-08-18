import json
import logging
import os
import time
from typing import Any

from fleet_health_cache import should_use_redis_cache

LEADERBOARD_CACHE_KEY_PREFIX = "leaderboard:v1:"
DEFAULT_MAX_STALE_SECONDS = 600
DEFAULT_REDIS_TTL_SECONDS = 900
DEFAULT_REFRESH_SECONDS = 60
DEFAULT_LEADERBOARD_DAYS = 30


def leaderboard_cache_key(days: int) -> str:
    return f"{LEADERBOARD_CACHE_KEY_PREFIX}{days}"


def get_cached_leaderboard(days: int) -> dict[str, Any] | None:
    client = _get_redis_client()
    if client is None:
        return None

    try:
        raw = client.get(leaderboard_cache_key(days))
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

    entries = parsed.get("leaderboard_entries")
    cached_at_epoch = parsed.get("cached_at_epoch")
    cached_days = parsed.get("days", days)

    if not isinstance(entries, list) or not isinstance(cached_at_epoch, (int, float)):
        logging.warning("Leaderboard cache payload has invalid shape")
        return None
    if not isinstance(cached_days, int):
        return None

    max_stale_seconds = _read_non_negative_int_env(
        "LEADERBOARD_MAX_STALE_SECONDS",
        DEFAULT_MAX_STALE_SECONDS,
    )
    if max_stale_seconds > 0 and time.time() - float(cached_at_epoch) > max_stale_seconds:
        return None

    return {
        "days": cached_days,
        "leaderboard_entries": entries,
        "cached_at_epoch": float(cached_at_epoch),
    }


def store_cached_leaderboard(days: int, context: dict[str, Any]) -> bool:
    client = _get_redis_client()
    if client is None:
        return False

    ttl_seconds = _read_non_negative_int_env(
        "LEADERBOARD_REDIS_TTL_SECONDS",
        DEFAULT_REDIS_TTL_SECONDS,
    )
    record = {
        "cached_at_epoch": time.time(),
        "days": days,
        "leaderboard_entries": context.get("leaderboard_entries", []),
    }
    serialized = json.dumps(record, separators=(",", ":"))

    try:
        if ttl_seconds > 0:
            client.setex(leaderboard_cache_key(days), ttl_seconds, serialized)
        else:
            client.set(leaderboard_cache_key(days), serialized)
    except Exception:
        logging.exception("Failed to write leaderboard cache to Redis")
        return False

    return True


def refresh_leaderboard_cache(days: int = DEFAULT_LEADERBOARD_DAYS) -> dict[str, Any]:
    from app import compute_leaderboard_context

    context = compute_leaderboard_context(days)
    store_cached_leaderboard(days, context)
    return context


def _get_redis_client() -> Any | None:
    if not should_use_redis_cache():
        return None
    try:
        from fleet_health_cache import _get_redis_client as get_shared_redis_client
    except Exception:
        return None
    try:
        return get_shared_redis_client()
    except Exception:
        logging.exception("Failed to initialize Redis client for leaderboard cache")
        return None


def _read_non_negative_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(0, parsed)
