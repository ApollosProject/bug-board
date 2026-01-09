from functools import lru_cache
import os
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


def get_linear_team_key():
    """Return the Linear team key used for issue/project queries."""
    config = load_config()
    return (
        config.get("linear_team_key")
        or os.getenv("LINEAR_TEAM_KEY")
        or "APO"
    )
