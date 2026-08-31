"""JSON API for Bug Board metrics, authenticated with a static API key.

The dashboard itself is behind GitHub OAuth, which is unusable from a script.
Endpoints declared with :func:`require_api_key` authenticate with the
``BUG_BOARD_API_KEY`` environment variable instead and are exempted from the
OAuth ``before_request`` guard in ``github_oauth`` -- see ``API_KEY_ENDPOINTS``.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from functools import wraps
from hmac import compare_digest
from typing import Any, TypeVar, cast

from dotenv import load_dotenv
from flask import jsonify, request

from person_stats import CARD_METRIC_KEYS, CARD_METRIC_LABELS

load_dotenv()

API_KEY_ENV_VAR = "BUG_BOARD_API_KEY"

# Endpoint names registered by ``require_api_key``. ``github_oauth`` skips its
# session check for these, so an endpoint only escapes OAuth by opting into the
# API key -- an ``/api`` route added without the decorator stays OAuth-only.
API_KEY_ENDPOINTS: set[str] = set()

# Metrics whose displayed value is a day count.
DAY_COUNT_METRIC_KEYS = frozenset({"priority_bug_avg_time_to_fix", "avg_all_time_to_fix"})

View = TypeVar("View", bound=Callable[..., Any])


def _configured_api_key() -> str:
    return os.getenv(API_KEY_ENV_VAR, "").strip()


def _presented_api_key() -> str:
    header = request.headers.get("Authorization", "").strip()
    scheme, _, token = header.partition(" ")
    if scheme.lower() == "bearer" and token.strip():
        return token.strip()
    return request.headers.get("X-API-Key", "").strip()


def require_api_key(view: View) -> View:
    """Reject the request unless it carries the configured API key."""

    @wraps(view)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        configured = _configured_api_key()
        if not configured:
            return jsonify(
                {
                    "error": "api_key_not_configured",
                    "detail": f"Set {API_KEY_ENV_VAR} to enable the JSON API.",
                }
            ), 503
        presented = _presented_api_key()
        if not presented or not compare_digest(
            presented.encode("utf-8"), configured.encode("utf-8")
        ):
            response = jsonify({"error": "unauthorized"})
            response.headers["WWW-Authenticate"] = 'Bearer realm="bug-board"'
            return response, 401
        return view(*args, **kwargs)

    API_KEY_ENDPOINTS.add(view.__name__)
    return cast(View, wrapper)


def _metric_display(key: str, value: Any, context: Mapping[str, Any]) -> str:
    if key == "lead_completed_projects_avg_early_late":
        return str(context.get(key) or "n/a")
    if value is None:
        return "n/a"
    if key in DAY_COUNT_METRIC_KEYS:
        return f"{value}d"
    return str(value)


def _metric_value(key: str, context: Mapping[str, Any]) -> Any:
    if key == "lead_completed_projects_avg_early_late":
        return context.get("lead_completed_projects_avg_early_late_days")
    return context.get(key)


def _metric_entry(key: str, context: Mapping[str, Any]) -> dict[str, Any]:
    value = _metric_value(key, context)
    stdev = (context.get("metric_stdevs") or {}).get(key) or {}
    return {
        "label": CARD_METRIC_LABELS[key],
        "value": value,
        "display": _metric_display(key, value, context),
        # Oriented so a positive z is better than the engineering average,
        # even for metrics where a lower raw value is better.
        "vs_team": (
            {
                "z": stdev.get("z"),
                "label": stdev.get("label"),
                "tone": stdev.get("tone"),
                "eng_avg": stdev.get("eng_avg"),
                "eng_stdev": stdev.get("eng_stdev"),
            }
            if stdev
            else None
        ),
    }


def person_metrics_payload(context: Mapping[str, Any]) -> dict[str, Any]:
    """Serialize a ``/team/<slug>`` page context into the JSON API payload."""
    return {
        "person": {
            "slug": context.get("person_slug"),
            "name": context.get("person_name"),
            "linear_username": context.get("linear_username"),
            "github_username": context.get("github_username"),
        },
        "window": {
            "start": context.get("start"),
            "end": context.get("end"),
            "days": context.get("days"),
            "preset_days": context.get("preset_days"),
            "label": context.get("window_label"),
        },
        "metrics": {key: _metric_entry(key, context) for key in CARD_METRIC_KEYS},
        "regressions": {
            "status": context.get("regression_metrics_status"),
            "authored": context.get("regressions_authored"),
            "authored_rate": context.get("author_regression_rate"),
            "approved": context.get("regressions_approved"),
            "approved_rate": context.get("reviewer_escape_rate"),
        },
        "links": {
            "github_merged_prs": context.get("github_merged_prs_url"),
            **(context.get("issue_metric_urls") or {}),
            **(context.get("project_metric_urls") or {}),
        },
    }
