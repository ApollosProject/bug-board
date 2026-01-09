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


def get_github_orgs():
    """Return the GitHub orgs to include when aggregating PR activity."""
    config = load_config()
    orgs = config.get("github_orgs")
    if orgs:
        return orgs
    env_orgs = os.getenv("GITHUB_ORGS")
    if env_orgs:
        return [org.strip() for org in env_orgs.split(",") if org.strip()]
    return ["apollosproject", "differential"]
