import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

TERMINAL_STATES = {"success", "failed"}
DAGS_ENDPOINT = "/dags"
STATE_FILE_PATH = "/tmp/airflow_fleet_state.json"

# Intentionally fixed settings to keep this checker simple.
REQUEST_TIMEOUT_SECONDS = 20
DAG_PAGE_SIZE = 200
DAG_QUERY_WORKERS = 30
FAILURE_THRESHOLD_RATIO = 0.35
MIN_EVALUATED_DAGS = 20
TRIGGER_BAD_WINDOWS = 2
RESOLVE_GOOD_WINDOWS = 3
TOP_FAILED_DAGS_LIMIT = 10


class AirflowFleetHealthError(RuntimeError):
    """Raised when the Airflow fleet check cannot complete."""


@dataclass(frozen=True)
class FleetStats:
    checked_at: datetime
    active_dags_total: int
    evaluated_dags: int
    dags_without_runs: int
    non_terminal_dags: int
    failed_dags: list[str]
    failed_runs: int
    failure_ratio: float


@dataclass
class DetectorState:
    status: str = "healthy"
    consec_bad: int = 0
    consec_good: int = 0
    last_transition_at: str | None = None


def evaluate_fleet_health() -> tuple[dict[str, Any], int]:
    """Compute current Airflow fleet health and return (payload, http_status)."""
    base_url = _require_env("AIRFLOW_API_BASE_URL").rstrip("/")
    api_token = _require_env("AIRFLOW_API_TOKEN")
    checked_at = _now_utc()

    session = _build_session(api_token)
    active_dags = _fetch_active_dags(session, base_url)
    latest_state_by_dag = _fetch_latest_states_by_dag(base_url, api_token, active_dags)
    stats = _build_stats(checked_at, active_dags, latest_state_by_dag)

    state = _load_state(STATE_FILE_PATH)
    next_state, transition, insufficient = _advance_state(state, stats)
    _save_state(STATE_FILE_PATH, next_state)

    payload = {
        "mode": "active_dags_latest_run",
        "status": next_state.status,
        "transition": transition,
        "last_transition_at": next_state.last_transition_at,
        "checked_at": stats.checked_at.isoformat(),
        "source": "per_dag_runs_endpoint",
        "active_dags_total": stats.active_dags_total,
        "evaluated_dags": stats.evaluated_dags,
        "dags_without_runs": stats.dags_without_runs,
        "non_terminal_dags": stats.non_terminal_dags,
        "failed_runs": stats.failed_runs,
        "failure_ratio": stats.failure_ratio,
        "threshold_ratio": FAILURE_THRESHOLD_RATIO,
        "min_terminal_runs": MIN_EVALUATED_DAGS,
        "insufficient_volume": insufficient,
        "consecutive_bad_windows": next_state.consec_bad,
        "consecutive_good_windows": next_state.consec_good,
        "trigger_after_bad_windows": TRIGGER_BAD_WINDOWS,
        "resolve_after_good_windows": RESOLVE_GOOD_WINDOWS,
        "top_failed_dags": [
            {"dag_id": dag_id, "state": "failed"}
            for dag_id in stats.failed_dags[:TOP_FAILED_DAGS_LIMIT]
        ],
    }
    http_status = 503 if next_state.status == "degraded" else 200
    return payload, http_status


def _fetch_active_dags(session: requests.Session, base_url: str) -> set[str]:
    dags: set[str] = set()
    offset = 0

    while True:
        payload = _request_json(
            session,
            f"{base_url}{DAGS_ENDPOINT}",
            params={"limit": DAG_PAGE_SIZE, "offset": offset, "only_active": "true"},
        )
        batch = payload.get("dags")
        if not isinstance(batch, list) or not batch:
            break

        for dag in batch:
            if not isinstance(dag, dict):
                continue
            dag_id = dag.get("dag_id")
            if not isinstance(dag_id, str) or not dag_id:
                continue
            if bool(dag.get("is_paused", False)):
                continue
            dags.add(dag_id)

        if not _has_more(batch, payload, offset, DAG_PAGE_SIZE):
            break
        offset += DAG_PAGE_SIZE

    return dags


def _fetch_latest_states_by_dag(
    base_url: str, api_token: str, active_dags: set[str]
) -> dict[str, str]:
    if not active_dags:
        return {}

    latest_state_by_dag: dict[str, str] = {}
    failures = 0
    worker_count = min(DAG_QUERY_WORKERS, len(active_dags))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(_fetch_last_state_for_dag, base_url, api_token, dag_id): dag_id
            for dag_id in active_dags
        }
        for future in as_completed(futures):
            dag_id = futures[future]
            try:
                state = future.result()
            except AirflowFleetHealthError:
                failures += 1
                continue
            if state:
                latest_state_by_dag[dag_id] = state

    if failures:
        raise AirflowFleetHealthError(
            f"Failed to fetch latest runs for {failures} DAG(s)."
        )

    return latest_state_by_dag


def _fetch_last_state_for_dag(base_url: str, api_token: str, dag_id: str) -> str:
    session = _build_session(api_token)
    dag_id_encoded = quote(dag_id, safe="")
    payload = _request_json(
        session,
        f"{base_url}{DAGS_ENDPOINT}/{dag_id_encoded}/dagRuns",
        params={"limit": 1, "offset": 0, "order_by": "-logical_date"},
    )
    dag_runs = _extract_dag_runs(payload)
    if not dag_runs:
        return ""
    return _extract_state(dag_runs[0])


def _build_stats(
    checked_at: datetime, active_dags: set[str], latest_state_by_dag: dict[str, str]
) -> FleetStats:
    evaluated_dags = len(latest_state_by_dag)
    failed_dags = sorted(
        dag_id for dag_id, state in latest_state_by_dag.items() if state == "failed"
    )
    failed_runs = len(failed_dags)
    non_terminal_dags = sum(
        1 for state in latest_state_by_dag.values() if state not in TERMINAL_STATES
    )
    failure_ratio = failed_runs / evaluated_dags if evaluated_dags else 0.0

    return FleetStats(
        checked_at=checked_at,
        active_dags_total=len(active_dags),
        evaluated_dags=evaluated_dags,
        dags_without_runs=max(0, len(active_dags) - evaluated_dags),
        non_terminal_dags=non_terminal_dags,
        failed_dags=failed_dags,
        failed_runs=failed_runs,
        failure_ratio=failure_ratio,
    )


def _advance_state(
    state: DetectorState, stats: FleetStats
) -> tuple[DetectorState, str | None, bool]:
    insufficient = stats.evaluated_dags < MIN_EVALUATED_DAGS
    transition: str | None = None

    if insufficient:
        next_state = DetectorState(
            status=state.status,
            consec_bad=0,
            consec_good=0,
            last_transition_at=state.last_transition_at,
        )
        return next_state, transition, True

    is_bad_window = stats.failure_ratio >= FAILURE_THRESHOLD_RATIO
    if is_bad_window:
        state.consec_bad += 1
        state.consec_good = 0
    else:
        state.consec_good += 1
        state.consec_bad = 0

    if state.status != "degraded" and state.consec_bad >= TRIGGER_BAD_WINDOWS:
        state.status = "degraded"
        state.last_transition_at = stats.checked_at.isoformat()
        transition = "triggered"
    elif state.status == "degraded" and state.consec_good >= RESOLVE_GOOD_WINDOWS:
        state.status = "healthy"
        state.last_transition_at = stats.checked_at.isoformat()
        transition = "resolved"

    return state, transition, False


def _build_session(api_token: str) -> requests.Session:
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {api_token}"
    session.headers["Accept"] = "application/json"
    return session


def _request_json(
    session: requests.Session, url: str, params: dict[str, Any]
) -> dict[str, Any]:
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        raise AirflowFleetHealthError(
            f"Airflow API request failed for {url}: HTTP {status_code}"
        ) from exc
    except requests.RequestException as exc:
        raise AirflowFleetHealthError(f"Airflow API request failed for {url}") from exc

    payload = response.json()
    if not isinstance(payload, dict):
        raise AirflowFleetHealthError(f"Unexpected Airflow API response type for {url}")
    return payload


def _extract_dag_runs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    runs = payload.get("dag_runs")
    if isinstance(runs, list):
        return [run for run in runs if isinstance(run, dict)]
    runs_alt = payload.get("dagRuns")
    if isinstance(runs_alt, list):
        return [run for run in runs_alt if isinstance(run, dict)]
    return []


def _has_more(
    batch: list[dict[str, Any]], payload: dict[str, Any], offset: int, limit: int
) -> bool:
    total_entries = payload.get("total_entries")
    if isinstance(total_entries, int):
        return offset + limit < total_entries
    return len(batch) == limit


def _extract_state(run: dict[str, Any]) -> str:
    state = run.get("state")
    if isinstance(state, str):
        return state.lower()
    return ""


def _load_state(path: str) -> DetectorState:
    try:
        with open(path, "r", encoding="utf-8") as state_file:
            payload = json.load(state_file)
    except FileNotFoundError:
        return DetectorState()
    except (OSError, json.JSONDecodeError):
        logging.exception("Could not load detector state file: %s", path)
        return DetectorState()

    if not isinstance(payload, dict):
        return DetectorState()

    status = payload.get("status")
    if status not in {"healthy", "degraded"}:
        status = "healthy"
    return DetectorState(
        status=status,
        consec_bad=_safe_int(payload.get("consec_bad"), 0),
        consec_good=_safe_int(payload.get("consec_good"), 0),
        last_transition_at=payload.get("last_transition_at"),
    )


def _save_state(path: str, state: DetectorState) -> None:
    try:
        with open(path, "w", encoding="utf-8") as state_file:
            json.dump(
                {
                    "status": state.status,
                    "consec_bad": state.consec_bad,
                    "consec_good": state.consec_good,
                    "last_transition_at": state.last_transition_at,
                },
                state_file,
            )
    except OSError:
        logging.exception("Could not save detector state file: %s", path)


def _safe_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AirflowFleetHealthError(f"{name} is not configured.")
    return value


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)
