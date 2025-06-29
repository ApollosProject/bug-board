from functools import lru_cache
import yaml


@lru_cache(maxsize=1)
def load_config(path="config.yml"):
    """Load configuration data from ``path`` and cache the result."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_platforms():
    """Return platform configuration from the cached config."""
    config = load_config()
    return config.get("platforms", [])
