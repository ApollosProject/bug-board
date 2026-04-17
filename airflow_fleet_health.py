import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TypedDict
from urllib.parse import quote

import requests

TERMINAL_STATES = {"success", "failed"}
DAGS_ENDPOINT = "/dags"

# Fixed settings for a simple checker.
REQUEST_TIMEOUT_SECONDS = 20
# Airflow caps /dags responses at 100 items even when a higher limit is requested.
DAG_PAGE_SIZE = 100
DAG_QUERY_WORKERS = 30
DAG_RUN_PAGE_SIZE = 10
FAILURE_THRESHOLD_RATIO = 0.10
MIN_EVALUATED_DAGS = 20
TOP_FAILED_DAGS_LIMIT = 10


class DagRunEvaluation(TypedDict):
    latest_state: str
    latest_terminal_state: str
    dag_run_id: str
    has_runs: bool


class FailedDagEntry(TypedDict):
    dag_id: str
    state: str
    dag_run_id: str


class AirflowFleetHealthError(RuntimeError):
    """Raised when the Airflow fleet check cannot complete."""


@dataclass(frozen=True)
class FleetStats:
    checked_at: datetime
    active_dags_total: int
    evaluated_dags: int
    failed_fetches: int
    dags_without_runs: int
    non_terminal_dags: int
    failed_dags: list[FailedDagEntry]
    failed_runs: int
    failure_ratio: float
    insufficient_volume: bool


def evaluate_fleet_health() -> tuple[dict[str, Any], int]:
    """Compute current Airflow fleet health and return (payload, http_status)."""
    base_url = _require_env("AIRFLOW_API_BASE_URL").rstrip("/")
    api_token = _require_env("AIRFLOW_API_TOKEN")

    session = _build_session(api_token)
    active_dags = _fetch_active_dags(session, base_url)
    latest_run_by_dag, failed_fetches = _fetch_latest_runs_by_dag(base_url, api_token, active_dags)
    if active_dags and not latest_run_by_dag:
        raise AirflowFleetHealthError("Failed to fetch latest runs for all DAGs.")
    stats = _build_stats(active_dags, latest_run_by_dag, failed_fetches)

    is_degraded = not stats.insufficient_volume and stats.failure_ratio >= FAILURE_THRESHOLD_RATIO
    status = "degraded" if is_degraded else "healthy"
    http_status = 503 if is_degraded else 200
    payload = {
        "status": status,
        "checked_at": stats.checked_at.isoformat(),
        "active_dags_total": stats.active_dags_total,
        "evaluated_dags": stats.evaluated_dags,
        "failed_fetches": stats.failed_fetches,
        "dags_without_runs": stats.dags_without_runs,
        "non_terminal_dags": stats.non_terminal_dags,
        "failed_runs": stats.failed_runs,
        "failure_ratio": stats.failure_ratio,
        "threshold_ratio": FAILURE_THRESHOLD_RATIO,
        "failed_dags": stats.failed_dags,
        "top_failed_dags": stats.failed_dags[:TOP_FAILED_DAGS_LIMIT],
    }
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

        batch_size = len(batch)
        for dag in batch:
            if not isinstance(dag, dict):
                continue
            dag_id = dag.get("dag_id")
            if not isinstance(dag_id, str) or not dag_id:
                continue
            if bool(dag.get("is_paused", False)):
                continue
            dags.add(dag_id)

        if not _has_more(batch_size, payload, offset, DAG_PAGE_SIZE):
            break
        offset += batch_size

    return dags


def _fetch_latest_runs_by_dag(
    base_url: str, api_token: str, active_dags: set[str]
) -> tuple[dict[str, DagRunEvaluation], int]:
    if not active_dags:
        return {}, 0

    latest_run_by_dag: dict[str, DagRunEvaluation] = {}
    failures = 0
    worker_count = min(DAG_QUERY_WORKERS, len(active_dags))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(_fetch_last_run_for_dag, base_url, api_token, dag_id): dag_id
            for dag_id in active_dags
        }
        for future in as_completed(futures):
            dag_id = futures[future]
            try:
                latest_run = future.result()
            except AirflowFleetHealthError:
                failures += 1
                continue
            latest_run_by_dag[dag_id] = latest_run

    return latest_run_by_dag, failures


def _fetch_last_run_for_dag(base_url: str, api_token: str, dag_id: str) -> DagRunEvaluation:
    session = _build_session(api_token)
    dag_id_encoded = quote(dag_id, safe="")
    payload = _request_json(
        session,
        f"{base_url}{DAGS_ENDPOINT}/{dag_id_encoded}/dagRuns",
        params={"limit": DAG_RUN_PAGE_SIZE, "offset": 0, "order_by": "-logical_date"},
    )
    dag_runs = _extract_dag_runs(payload)
    if not dag_runs:
        return {
            "latest_state": "",
            "latest_terminal_state": "",
            "dag_run_id": "",
            "has_runs": False,
        }

    latest_state = _extract_state(dag_runs[0])
    latest_terminal_run = next(
        (run for run in dag_runs if _extract_state(run) in TERMINAL_STATES),
        None,
    )
    if latest_terminal_run is None:
        return {
            "latest_state": latest_state,
            "latest_terminal_state": "",
            "dag_run_id": "",
            "has_runs": True,
        }

    return {
        "latest_state": latest_state,
        "latest_terminal_state": _extract_state(latest_terminal_run),
        "dag_run_id": _extract_dag_run_id(latest_terminal_run),
        "has_runs": True,
    }


def _build_stats(
    active_dags: set[str],
    latest_run_by_dag: dict[str, DagRunEvaluation],
    failed_fetches: int,
) -> FleetStats:
    evaluated_dags = sum(
        1 for latest_run in latest_run_by_dag.values() if latest_run["latest_terminal_state"]
    )
    failed_dags: list[FailedDagEntry] = [
        {
            "dag_id": dag_id,
            "state": "failed",
            "dag_run_id": latest_run["dag_run_id"],
        }
        for dag_id, latest_run in sorted(latest_run_by_dag.items())
        if latest_run["latest_terminal_state"] == "failed"
    ]
    failed_runs = len(failed_dags)
    non_terminal_dags = sum(
        1
        for latest_run in latest_run_by_dag.values()
        if latest_run["has_runs"] and latest_run["latest_state"] not in TERMINAL_STATES
    )
    failure_ratio = failed_runs / evaluated_dags if evaluated_dags else 0.0
    insufficient_volume = evaluated_dags < MIN_EVALUATED_DAGS

    return FleetStats(
        checked_at=datetime.now(timezone.utc),
        active_dags_total=len(active_dags),
        evaluated_dags=evaluated_dags,
        failed_fetches=failed_fetches,
        dags_without_runs=sum(1 for latest_run in latest_run_by_dag.values() if not latest_run["has_runs"]),
        non_terminal_dags=non_terminal_dags,
        failed_dags=failed_dags,
        failed_runs=failed_runs,
        failure_ratio=failure_ratio,
        insufficient_volume=insufficient_volume,
    )


def _build_session(api_token: str) -> requests.Session:
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {api_token}"
    session.headers["Accept"] = "application/json"
    return session


def _request_json(session: requests.Session, url: str, params: dict[str, Any]) -> dict[str, Any]:
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

    try:
        payload = response.json()
    except ValueError as exc:
        raise AirflowFleetHealthError(f"Airflow API returned invalid JSON for {url}") from exc
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


def _has_more(batch_size: int, payload: dict[str, Any], offset: int, requested_limit: int) -> bool:
    total_entries = payload.get("total_entries")
    if isinstance(total_entries, int):
        return offset + batch_size < total_entries
    return batch_size == requested_limit


def _extract_state(run: dict[str, Any]) -> str:
    state = run.get("state")
    if isinstance(state, str):
        return state.lower()
    return ""


def _extract_dag_run_id(run: dict[str, Any]) -> str:
    dag_run_id = run.get("dag_run_id")
    if isinstance(dag_run_id, str):
        return dag_run_id

    run_id = run.get("run_id")
    if isinstance(run_id, str):
        return run_id

    return ""


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AirflowFleetHealthError(f"{name} is not configured.")
    return value
