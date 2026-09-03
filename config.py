import os
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


def get_linear_team_key():
    """Return the Linear team key used for issue/project queries."""
    config = load_config()
    return config.get("linear_team_key") or os.getenv("LINEAR_TEAM_KEY") or "APO"


def get_linear_team_keys():
    """Return every Linear team key whose issues count toward person metrics.

    ``linear_team_keys`` (a list) extends the primary ``linear_team_key``; the
    primary team is always first. Team-wide pages keep using the primary team.
    """
    config = load_config()
    primary = get_linear_team_key()
    configured = config.get("linear_team_keys") or []
    if isinstance(configured, str):
        configured = [configured]
    keys = [primary]
    for key in configured:
        if isinstance(key, str) and key.strip() and key.strip() not in keys:
            keys.append(key.strip())
    return keys


def get_bug_label_names():
    """Return the Linear label names that mark an issue as a bug."""
    config = load_config()
    configured = config.get("bug_labels") or ["Bug"]
    if isinstance(configured, str):
        configured = [configured]
    names = [name.strip() for name in configured if isinstance(name, str) and name.strip()]
    return names or ["Bug"]


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
