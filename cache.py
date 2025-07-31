import time
from functools import wraps
from typing import Any, Callable, Tuple


def ttl_cache(ttl_seconds: int) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Simple per-key TTL cache decorator."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        cache: dict[Tuple[Tuple[Any, ...], Tuple[Tuple[str, Any], ...]], Tuple[float, Any]] = {}

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = (args, tuple(sorted(kwargs.items())))
            now = time.monotonic()
            if key in cache:
                expires, value = cache[key]
                if now < expires:
                    return value
                if hasattr(func, "cache_clear"):
                    func.cache_clear()  # type: ignore[attr-defined]
            result = func(*args, **kwargs)
            cache[key] = (now + ttl_seconds, result)
            return result

        def cache_clear() -> None:
            cache.clear()

        wrapper.cache_clear = cache_clear  # type: ignore[attr-defined]
        return wrapper

    return decorator
