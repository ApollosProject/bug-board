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


class AirflowFleetHealthError(RuntimeError):
    """Raised when the Airflow fleet check cannot complete."""


@dataclass(frozen=True)
class FleetConfig:
    base_url: str
    api_token: str | None
    api_username: str | None
    api_password: str | None
    request_timeout_seconds: int
    threshold_ratio: float
    min_terminal_runs: int
    trigger_bad_windows: int
    resolve_good_windows: int
    excluded_dags: set[str]
    state_file_path: str
    dag_runs_page_size: int
    dag_query_workers: int

    @classmethod
    def from_env(cls) -> "FleetConfig":
        base_url = os.getenv("AIRFLOW_API_BASE_URL", "").strip().rstrip("/")
        if not base_url:
            raise AirflowFleetHealthError("AIRFLOW_API_BASE_URL is not configured.")
        return cls(
            base_url=base_url,
            api_token=_optional_env("AIRFLOW_API_TOKEN"),
            api_username=_optional_env("AIRFLOW_API_USERNAME"),
            api_password=_optional_env("AIRFLOW_API_PASSWORD"),
            request_timeout_seconds=_int_env(
                "AIRFLOW_FLEET_REQUEST_TIMEOUT_SECONDS", 20
            ),
            threshold_ratio=_float_env("AIRFLOW_FLEET_FAILURE_THRESHOLD_RATIO", 0.35),
            min_terminal_runs=_int_env("AIRFLOW_FLEET_MIN_TERMINAL_RUNS", 20),
            trigger_bad_windows=_int_env("AIRFLOW_FLEET_TRIGGER_BAD_WINDOWS", 2),
            resolve_good_windows=_int_env("AIRFLOW_FLEET_RESOLVE_GOOD_WINDOWS", 3),
            excluded_dags=_csv_env("AIRFLOW_FLEET_EXCLUDED_DAGS"),
            state_file_path=os.getenv(
                "AIRFLOW_FLEET_STATE_FILE",
                "/tmp/airflow_fleet_state.json",
            ),
            dag_runs_page_size=_int_env("AIRFLOW_FLEET_DAG_RUNS_PAGE_SIZE", 200),
            dag_query_workers=_int_env("AIRFLOW_FLEET_DAG_QUERY_WORKERS", 30),
        )


@dataclass(frozen=True)
class WindowStats:
    checked_at: datetime
    source: str
    active_dags_total: int
    evaluated_dags: int
    dags_without_runs: int
    non_terminal_dags: int
    failed_runs: int
    failure_ratio: float
    top_failed_dags: list[dict[str, Any]]


@dataclass
class DetectorState:
    status: str = "healthy"
    consec_bad: int = 0
    consec_good: int = 0
    last_transition_at: str | None = None


def evaluate_fleet_health() -> tuple[dict[str, Any], int]:
    """Compute current Airflow fleet health and return (payload, http_status)."""
    config = FleetConfig.from_env()
    checked_at = _now_utc()

    stats = _collect_latest_run_stats(config, checked_at)
    state = _load_state(config.state_file_path)
    next_state, transition, insufficient = _advance_state(state, stats, config)
    _save_state(config.state_file_path, next_state)

    payload = {
        "mode": "active_dags_latest_run",
        "status": next_state.status,
        "transition": transition,
        "last_transition_at": next_state.last_transition_at,
        "checked_at": stats.checked_at.isoformat(),
        "source": stats.source,
        "active_dags_total": stats.active_dags_total,
        "evaluated_dags": stats.evaluated_dags,
        "dags_without_runs": stats.dags_without_runs,
        "non_terminal_dags": stats.non_terminal_dags,
        "failed_runs": stats.failed_runs,
        "failure_ratio": stats.failure_ratio,
        "threshold_ratio": config.threshold_ratio,
        "min_terminal_runs": config.min_terminal_runs,
        "insufficient_volume": insufficient,
        "consecutive_bad_windows": next_state.consec_bad,
        "consecutive_good_windows": next_state.consec_good,
        "trigger_after_bad_windows": config.trigger_bad_windows,
        "resolve_after_good_windows": config.resolve_good_windows,
        "top_failed_dags": stats.top_failed_dags,
    }
    http_status = 503 if next_state.status == "degraded" else 200
    return payload, http_status


def _collect_latest_run_stats(config: FleetConfig, checked_at: datetime) -> WindowStats:
    session = _build_session(config)
    active_dags = _fetch_active_dags(session, config)
    latest_state_by_dag = _fetch_latest_states_by_dag(config, active_dags)
    source = "per_dag_runs_endpoint"

    evaluated_dags = len(latest_state_by_dag)
    failed_dags = sorted(
        dag_id
        for dag_id, state in latest_state_by_dag.items()
        if state == "failed"
    )
    failed_runs = len(failed_dags)
    non_terminal_dags = sum(
        1 for state in latest_state_by_dag.values() if state not in TERMINAL_STATES
    )
    failure_ratio = failed_runs / evaluated_dags if evaluated_dags else 0.0
    top_failed_dags = [
        {"dag_id": dag_id, "state": "failed"}
        for dag_id in failed_dags[:10]
    ]

    return WindowStats(
        checked_at=checked_at,
        source=source,
        active_dags_total=len(active_dags),
        evaluated_dags=evaluated_dags,
        dags_without_runs=max(0, len(active_dags) - evaluated_dags),
        non_terminal_dags=non_terminal_dags,
        failed_runs=failed_runs,
        failure_ratio=failure_ratio,
        top_failed_dags=top_failed_dags,
    )


def _fetch_latest_states_by_dag(
    config: FleetConfig, active_dags: set[str]
) -> dict[str, str]:
    latest_state_by_dag: dict[str, str] = {}
    failed_fetches: list[str] = []
    worker_count = max(1, min(config.dag_query_workers, len(active_dags) or 1))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(_fetch_last_state_for_dag, config, dag_id): dag_id
            for dag_id in active_dags
        }
        for future in as_completed(futures):
            dag_id = futures[future]
            try:
                state = future.result()
            except AirflowFleetHealthError:
                failed_fetches.append(dag_id)
                continue
            if state:
                latest_state_by_dag[dag_id] = state

    if failed_fetches:
        raise AirflowFleetHealthError(
            f"Failed to fetch latest runs for {len(failed_fetches)} DAG(s)."
        )
    return latest_state_by_dag


def _fetch_active_dags(
    session: requests.Session, config: FleetConfig
) -> set[str]:
    dags: set[str] = set()
    offset = 0
    limit = config.dag_runs_page_size

    while True:
        payload = _request_json(
            session,
            f"{config.base_url}{DAGS_ENDPOINT}",
            params={"limit": limit, "offset": offset, "only_active": "true"},
            timeout=config.request_timeout_seconds,
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
            is_paused = bool(dag.get("is_paused", False))
            if is_paused or dag_id in config.excluded_dags:
                continue
            dags.add(dag_id)
        if not _has_more(batch, payload, offset, limit):
            break
        offset += limit
    return dags


def _fetch_last_state_for_dag(
    config: FleetConfig, dag_id: str
) -> str:
    session = _build_session(config)
    dag_id_encoded = quote(dag_id, safe="")
    url = f"{config.base_url}{DAGS_ENDPOINT}/{dag_id_encoded}/dagRuns"
    payload = _request_json(
        session,
        url,
        params={"limit": 1, "offset": 0, "order_by": "-logical_date"},
        timeout=config.request_timeout_seconds,
    )
    dag_runs = _extract_dag_runs(payload)
    if not dag_runs:
        return ""
    return _extract_state(dag_runs[0])


def _advance_state(
    state: DetectorState,
    stats: WindowStats,
    config: FleetConfig,
) -> tuple[DetectorState, str | None, bool]:
    insufficient = stats.evaluated_dags < config.min_terminal_runs
    transition: str | None = None

    if insufficient:
        next_state = DetectorState(
            status=state.status,
            consec_bad=0,
            consec_good=0,
            last_transition_at=state.last_transition_at,
        )
        return next_state, transition, True

    is_bad_window = stats.failure_ratio >= config.threshold_ratio
    if is_bad_window:
        state.consec_bad += 1
        state.consec_good = 0
    else:
        state.consec_good += 1
        state.consec_bad = 0

    if state.status != "degraded" and state.consec_bad >= config.trigger_bad_windows:
        state.status = "degraded"
        state.last_transition_at = stats.checked_at.isoformat()
        transition = "triggered"
    elif state.status == "degraded" and state.consec_good >= config.resolve_good_windows:
        state.status = "healthy"
        state.last_transition_at = stats.checked_at.isoformat()
        transition = "resolved"

    return state, transition, False


def _build_session(config: FleetConfig) -> requests.Session:
    session = requests.Session()
    if config.api_token:
        session.headers["Authorization"] = f"Bearer {config.api_token}"
    if config.api_username and config.api_password:
        session.auth = (config.api_username, config.api_password)
    session.headers["Accept"] = "application/json"
    return session


def _request_json(
    session: requests.Session, url: str, params: dict[str, Any], timeout: int
) -> dict[str, Any]:
    try:
        response = session.get(url, params=params, timeout=timeout)
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


def _optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _csv_env(name: str) -> set[str]:
    value = os.getenv(name, "")
    return {item.strip() for item in value.split(",") if item.strip()}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)
