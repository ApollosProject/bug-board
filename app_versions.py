import base64
import binascii
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any

import requests
from packaging.version import InvalidVersion, Version

DEFAULT_BIGQUERY_ANALYTICS_PROJECT_ID = "apollos-project"
DEFAULT_BIGQUERY_ANALYTICS_DATASETS = ("apollos", "apollos_tv", "apollos_roku")
DEFAULT_SEGMENT_TABLES = (
    "identifies",
    "screens",
    "app_became_active",
    "app_became_backgrounded",
    "app_became_inactive",
)
DEFAULT_APP_VERSIONS_LOOKBACK_DAYS = 30
DEFAULT_APP_VERSIONS_LIMIT = 1000
APP_STORE_LOOKUP_URL = "https://itunes.apple.com/lookup"
APP_STORE_LOOKUP_LIMIT = 24
APP_STORE_LOOKUP_TIMEOUT_SECONDS = 5
APP_STORE_LOOKUP_WORKERS = 8
PLATFORMS_GITHUB_REPOSITORY = "ApollosProject/apollos-platforms"
PLATFORMS_GITHUB_API_URL = f"https://api.github.com/repos/{PLATFORMS_GITHUB_REPOSITORY}"
PLATFORM_SOURCE_CONTEXT_CACHE_SECONDS = 300
GITHUB_LOOKUP_TIMEOUT_SECONDS = 5

TIMESTAMP_COLUMN_CANDIDATES = (
    "timestamp",
    "received_at",
    "sent_at",
    "original_timestamp",
    "loaded_at",
)

FIELD_CANDIDATES = {
    "church": ("church", "group_id", "groupId"),
    "apollos_platform": ("apollos_platform", "apollosPlatform", "apollosplatform"),
    "apollos_version": ("apollos_version", "apollosVersion"),
    "app_version": ("app_version", "appVersion"),
    "app_update_id": ("app_update_id", "appUpdateId"),
    "bundle_id": ("bundle_id", "bundleId"),
    "application_name": ("application_name", "applicationName"),
    "source_revision": ("source_revision", "sourceRevision"),
    "source_version": ("source_version", "sourceVersion"),
    "deployment_track": ("deployment_track", "deploymentTrack"),
    "user_id": ("user_id", "userId"),
    "anonymous_id": ("anonymous_id", "anonymousId"),
}

ROKU_ANALYTICS_VERSION_CANDIDATES = ("context_library_version",)
MOBILE_PLATFORMS = {"android", "ios"}
RELEASE_TAG_PLATFORMS = {"amazon", "androidtv", "tv", "tvos"}
STABLE_RELEASE_TAG_PATTERN = re.compile(r"^v\d{4}\.\d{2}\.\d{2}\.\d{2}$")
ALPHA_RELEASE_TAG_PATTERN = re.compile(r"^(v\d{4}\.\d{2}\.\d{2}\.\d{2})-alpha\.\d+$")
INTERNAL_DEPLOYMENT_TRACKS = {"beta", "development", "internal", "preview", "prerelease"}


@dataclass(frozen=True)
class AppVersionsConfig:
    project_id: str
    datasets: tuple[str, ...]
    tables: tuple[str, ...]
    lookback_days: int
    limit: int


class AppVersionsError(RuntimeError):
    pass


def get_app_versions_context() -> dict[str, Any]:
    config = _get_app_versions_config()
    try:
        rows, discovered_tables = fetch_app_versions(config)
    except AppVersionsError as exc:
        logging.warning("App versions query skipped: %s", exc)
        return {
            "status": "unavailable",
            "status_label": "Unavailable",
            "error_message": str(exc),
            "rows": [],
            "lookback_days": config.lookback_days,
            "configured_tables": config.tables,
            "configured_datasets": config.datasets,
        }
    except Exception:
        logging.exception("App versions query failed")
        return {
            "status": "unavailable",
            "status_label": "Unavailable",
            "error_message": "Unable to query BigQuery analytics data.",
            "rows": [],
            "lookback_days": config.lookback_days,
            "configured_tables": config.tables,
            "configured_datasets": config.datasets,
        }

    return {
        "status": "ready",
        "status_label": "Ready",
        "rows": rows,
        "platform_tabs": build_platform_tabs(rows),
        "lookback_days": config.lookback_days,
        "configured_tables": discovered_tables,
        "configured_datasets": config.datasets,
    }


def fetch_app_versions(config: AppVersionsConfig) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    client = _build_bigquery_client(config.project_id)
    schema_by_table = _fetch_segment_schema(client, config)
    query, query_config = _build_app_versions_query(config, schema_by_table)
    results = client.query(query, job_config=query_config).result()
    rows = [_row_to_dict(row) for row in results]
    source_context = _fetch_platform_source_context()
    stable_release_revisions = source_context["stable_release_revisions"]
    latest_rows = _select_latest_observed_versions(rows, stable_release_revisions)
    latest_rows = _enrich_app_store_versions(latest_rows)
    source_context["roku_revision_statuses"] = _fetch_roku_revision_statuses(
        latest_rows,
        source_context.get("roku_target_revision"),
    )
    discovered_tables = tuple(
        f"{dataset}.{table_name}" for dataset, table_name in schema_by_table.keys()
    )
    return (
        _annotate_version_status(latest_rows, source_context)[: config.limit],
        discovered_tables,
    )


def _build_bigquery_client(project_id: str):
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise AppVersionsError(
            "Install google-cloud-bigquery before enabling the app versions page."
        ) from exc
    return bigquery.Client(project=project_id, credentials=_build_bigquery_credentials())


def _build_bigquery_credentials() -> Any:
    encoded_json = os.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64", "").strip()
    if not encoded_json:
        raise AppVersionsError(
            "Set BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64 to enable the app versions page."
        )
    try:
        raw_json = base64.b64decode(encoded_json).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise AppVersionsError("Invalid BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64 value.") from exc

    try:
        service_account_info = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise AppVersionsError("Invalid BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64 value.") from exc

    try:
        from google.oauth2 import service_account
    except ImportError as exc:
        raise AppVersionsError(
            "Install google-auth before using BigQuery service account credentials."
        ) from exc

    return service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def _fetch_segment_schema(
    client: Any,
    config: AppVersionsConfig,
) -> dict[tuple[str, str], dict[str, str]]:
    selects = []
    for dataset in config.datasets:
        schema_table = (
            f"`{_escape_identifier(config.project_id)}."
            f"{_escape_identifier(dataset)}.INFORMATION_SCHEMA.COLUMNS`"
        )
        selects.append(
            f"""
            SELECT
              '{_escape_string_literal(dataset)}' AS dataset_name,
              table_name,
              column_name
            FROM {schema_table}
            WHERE table_name IN UNNEST(@table_names)
            """
        )
    query = " UNION ALL ".join(selects)
    query_config = _query_job_config(
        [
            _array_query_parameter("table_names", "STRING", list(config.tables)),
        ]
    )
    rows = client.query(query, job_config=query_config).result()
    schema_by_table: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        data = _row_to_dict(row)
        dataset_name = data.get("dataset_name")
        table_name = data.get("table_name")
        column_name = data.get("column_name")
        if (
            isinstance(dataset_name, str)
            and isinstance(table_name, str)
            and isinstance(column_name, str)
        ):
            schema_by_table.setdefault((dataset_name, table_name), {})[column_name.lower()] = (
                column_name
            )

    usable_schema = {
        table_key: columns
        for table_key, columns in schema_by_table.items()
        if _choose_column(columns, TIMESTAMP_COLUMN_CANDIDATES)
        and _choose_version_column(table_key[0], columns)[0]
    }
    if not usable_schema:
        raise AppVersionsError(
            "No configured Segment tables expose both a timestamp and supported version column."
        )
    return usable_schema


def _build_app_versions_query(
    config: AppVersionsConfig,
    schema_by_table: dict[tuple[str, str], dict[str, str]],
) -> tuple[str, Any]:
    selects = []
    for (dataset, table_name), columns in schema_by_table.items():
        timestamp_column = _choose_column(columns, TIMESTAMP_COLUMN_CANDIDATES)
        version_column, version_source = _choose_version_column(dataset, columns)
        if not timestamp_column or not version_column:
            continue
        select_fields = [
            f"CAST(`{timestamp_column}` AS TIMESTAMP) AS seen_at",
            f"NULLIF(CAST(`{version_column}` AS STRING), '') AS apollos_version",
            *[
                _string_select_expression(columns, field_name, candidates)
                for field_name, candidates in FIELD_CANDIDATES.items()
                if field_name != "apollos_version"
            ],
            f"'{_escape_string_literal(table_name)}' AS source_table",
            f"'{_escape_string_literal(version_source)}' AS version_source",
        ]
        table_ref = (
            f"`{_escape_identifier(config.project_id)}."
            f"{_escape_identifier(dataset)}."
            f"{_escape_identifier(table_name)}`"
        )
        selects.append(
            f"""
            SELECT
              {", ".join(select_fields)},
              '{_escape_string_literal(dataset)}' AS source_dataset
            FROM {table_ref}
            WHERE `{timestamp_column}` >= TIMESTAMP_SUB(
              CURRENT_TIMESTAMP(),
              INTERVAL @lookback_days DAY
            )
              AND `{version_column}` IS NOT NULL
              AND CAST(`{version_column}` AS STRING) != ''
            """
        )

    if not selects:
        raise AppVersionsError("No configured Segment tables can be queried for app versions.")

    query = f"""
        WITH version_events AS (
          {" UNION ALL ".join(selects)}
        ),
        normalized_events AS (
          SELECT
            seen_at,
            COALESCE(NULLIF(church, ''), 'Unknown church') AS church,
            COALESCE(
              NULLIF(apollos_platform, ''),
              IF(source_dataset = 'apollos_roku', 'roku', NULL),
              IF(source_dataset = 'apollos_tv', 'tv', NULL),
              'unknown'
            ) AS apollos_platform,
            COALESCE(
              NULLIF(application_name, ''),
              IF(source_dataset = 'apollos_roku', 'Roku', NULL),
              'Unknown app'
            ) AS application_name,
            COALESCE(
              NULLIF(bundle_id, ''),
              IF(source_dataset = 'apollos_roku', 'roku', NULL),
              'unknown'
            ) AS bundle_id,
            apollos_version,
            app_version,
            app_update_id,
            source_revision,
            source_version,
            deployment_track,
            source_dataset,
            source_table,
            version_source,
            user_id,
            anonymous_id
          FROM version_events
          WHERE apollos_version IS NOT NULL AND apollos_version != ''
        ),
        filtered_events AS (
          SELECT *
          FROM normalized_events
          WHERE
            source_dataset = 'apollos_roku'
            OR (
              source_dataset = 'apollos_tv'
              AND apollos_platform IN ('amazon', 'androidtv', 'tvos', 'tv')
            )
            OR (
              source_dataset = 'apollos'
              AND apollos_platform NOT IN ('amazon', 'androidtv', 'tvos', 'tv', 'roku')
            )
            OR source_dataset NOT IN ('apollos', 'apollos_tv', 'apollos_roku')
        ),
        app_identity_events AS (
          SELECT
            *,
            IF(
              LOWER(bundle_id) IN ('unknown', 'roku'),
              CONCAT(church, '|', apollos_platform, '|', LOWER(bundle_id)),
              CONCAT(apollos_platform, '|', LOWER(bundle_id))
            ) AS app_identity_key
          FROM filtered_events
        ),
        display_churches AS (
          SELECT
            app_identity_key,
            ARRAY_AGG(
              church
              ORDER BY IF(church = 'Unknown church', 1, 0), church
              LIMIT 1
            )[OFFSET(0)] AS church
          FROM app_identity_events
          GROUP BY app_identity_key
        ),
        version_observations AS (
          SELECT
            events.app_identity_key,
            display_churches.church,
            events.apollos_platform,
            events.application_name,
            events.bundle_id,
            events.apollos_version,
            events.app_version,
            events.app_update_id,
            events.source_revision,
            events.source_version,
            events.deployment_track,
            events.source_dataset,
            events.source_table,
            events.version_source,
            MAX(events.seen_at) AS latest_seen_at,
            COUNT(*) AS version_event_count
          FROM app_identity_events events
          JOIN display_churches
            USING (app_identity_key)
          GROUP BY
            events.app_identity_key,
            display_churches.church,
            events.apollos_platform,
            events.application_name,
            events.bundle_id,
            events.apollos_version,
            events.app_version,
            events.app_update_id,
            events.source_revision,
            events.source_version,
            events.deployment_track,
            events.source_dataset,
            events.source_table,
            events.version_source
        ),
        app_totals AS (
          SELECT
            app_identity_key,
            COUNT(*) AS event_count,
            COUNT(DISTINCT COALESCE(user_id, anonymous_id)) AS user_count
          FROM app_identity_events
          GROUP BY app_identity_key
        )
        SELECT
          observation.church,
          observation.apollos_platform,
          observation.application_name,
          observation.bundle_id,
          observation.apollos_version,
          observation.app_version,
          observation.app_update_id,
          observation.source_revision,
          observation.source_version,
          observation.deployment_track,
          observation.source_dataset,
          observation.source_table,
          observation.version_source,
          observation.latest_seen_at,
          totals.event_count,
          totals.user_count
        FROM version_observations observation
        JOIN app_totals totals
          USING (app_identity_key)
        ORDER BY observation.latest_seen_at DESC
    """
    query_config = _query_job_config(
        [_scalar_query_parameter("lookback_days", "INT64", config.lookback_days)]
    )
    return query, query_config


def _string_select_expression(
    columns: dict[str, str],
    field_name: str,
    candidates: tuple[str, ...],
) -> str:
    column = _choose_column(columns, candidates)
    if not column:
        return f"CAST(NULL AS STRING) AS {field_name}"
    return f"NULLIF(CAST(`{column}` AS STRING), '') AS {field_name}"


def _github_request_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.getenv("GITHUB_TOKEN", "").strip():
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _fetch_platform_source_context() -> dict[str, Any]:
    cache_bucket = int(time.time() // PLATFORM_SOURCE_CONTEXT_CACHE_SECONDS)
    cached = _fetch_platform_source_context_cached(cache_bucket)
    return {
        "stable_release_revisions": dict(cached["stable_release_revisions"]),
        "roku_target_revision": cached.get("roku_target_revision"),
    }


@lru_cache(maxsize=2)
def _fetch_platform_source_context_cached(_cache_bucket: int) -> dict[str, Any]:
    stable_release_revisions: dict[str, str] = {}
    roku_target_revision = None
    headers = _github_request_headers()

    try:
        response = requests.get(
            f"{PLATFORMS_GITHUB_API_URL}/tags",
            params={"per_page": 100},
            headers=headers,
            timeout=GITHUB_LOOKUP_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        tags = response.json()
        if isinstance(tags, list):
            for tag in tags:
                if not isinstance(tag, dict):
                    continue
                name = _string_value(tag.get("name"))
                commit = tag.get("commit") or {}
                revision = _string_value(commit.get("sha")) if isinstance(commit, dict) else None
                if name and revision and STABLE_RELEASE_TAG_PATTERN.fullmatch(name):
                    stable_release_revisions[name] = revision
    except (requests.RequestException, ValueError, TypeError):
        logging.warning("Unable to load Platforms release tags from GitHub", exc_info=True)

    try:
        response = requests.get(
            f"{PLATFORMS_GITHUB_API_URL}/commits",
            params={"sha": "master", "path": "templates/roku", "per_page": "1"},
            headers=headers,
            timeout=GITHUB_LOOKUP_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        commits = response.json()
        if isinstance(commits, list) and commits and isinstance(commits[0], dict):
            roku_target_revision = _string_value(commits[0].get("sha"))
    except (requests.RequestException, ValueError, TypeError):
        logging.warning("Unable to load the latest Roku revision from GitHub", exc_info=True)

    return {
        "stable_release_revisions": stable_release_revisions,
        "roku_target_revision": roku_target_revision,
    }


def _fetch_roku_revision_statuses(
    rows: list[dict[str, Any]],
    target_revision: str | None,
) -> dict[str, str]:
    if not target_revision:
        return {}
    revisions = {
        revision
        for row in rows
        if (_string_value(row.get("apollos_platform")) or "").lower() == "roku"
        and (revision := _string_value(row.get("source_revision")))
    }
    return {
        revision: status
        for revision in revisions
        if (status := _fetch_revision_compare_status(target_revision, revision))
    }


@lru_cache(maxsize=256)
def _fetch_revision_compare_status(target_revision: str, deployed_revision: str) -> str | None:
    if _revisions_match(target_revision, deployed_revision):
        return "identical"
    if not re.fullmatch(r"[0-9a-fA-F]{7,40}", deployed_revision):
        return None
    try:
        response = requests.get(
            f"{PLATFORMS_GITHUB_API_URL}/compare/{target_revision}...{deployed_revision}",
            headers=_github_request_headers(),
            timeout=GITHUB_LOOKUP_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return _string_value(payload.get("status"))
    except (requests.RequestException, ValueError, TypeError):
        logging.warning(
            "Unable to compare Roku revision %s with %s",
            deployed_revision,
            target_revision,
            exc_info=True,
        )
    return None


def _revisions_match(left: str, right: str) -> bool:
    left_normalized = left.strip().lower()
    right_normalized = right.strip().lower()
    return left_normalized.startswith(right_normalized) or right_normalized.startswith(
        left_normalized
    )


def _canonical_release_version(
    row: dict[str, Any],
    stable_release_revisions: dict[str, str],
) -> str | None:
    source_version = _string_value(row.get("source_version"))
    if not source_version:
        return None
    if STABLE_RELEASE_TAG_PATTERN.fullmatch(source_version):
        return source_version
    match = ALPHA_RELEASE_TAG_PATTERN.fullmatch(source_version)
    if not match:
        return None
    stable_version = match.group(1)
    stable_revision = stable_release_revisions.get(stable_version)
    source_revision = _string_value(row.get("source_revision"))
    if not stable_revision or not source_revision:
        return None
    if not _revisions_match(stable_revision, source_revision):
        return None
    return stable_version


def _decorate_observation(
    row: dict[str, Any],
    stable_release_revisions: dict[str, str],
) -> dict[str, Any]:
    updated = dict(row)
    deployment_track = (_string_value(row.get("deployment_track")) or "").lower()
    canonical_source_version = _canonical_release_version(row, stable_release_revisions)
    source_version = (_string_value(row.get("source_version")) or "").lower()

    if deployment_track == "production":
        observation_scope = "production"
    elif deployment_track in INTERNAL_DEPLOYMENT_TRACKS:
        observation_scope = "internal"
    elif canonical_source_version:
        observation_scope = "legacy_production"
    elif ALPHA_RELEASE_TAG_PATTERN.fullmatch(source_version) or source_version == "master":
        observation_scope = "legacy_internal"
    else:
        observation_scope = "legacy_unknown"

    updated["deployment_track"] = deployment_track or None
    updated["observation_scope"] = observation_scope
    updated["canonical_source_version"] = canonical_source_version
    updated["is_production_observation"] = observation_scope in {
        "production",
        "legacy_production",
    }
    updated["is_internal_observation"] = observation_scope in {
        "internal",
        "legacy_internal",
    }
    return updated


def _annotate_version_status(
    rows: list[dict[str, Any]],
    source_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    source_context = source_context or {}
    stable_release_revisions = source_context.get("stable_release_revisions") or {}
    roku_target_revision = _string_value(source_context.get("roku_target_revision"))
    roku_revision_statuses = source_context.get("roku_revision_statuses") or {}
    prepared_rows = [_decorate_observation(row, stable_release_revisions) for row in rows]

    confirmed_runtime_by_platform: dict[str, str] = {}
    fallback_runtime_by_platform: dict[str, str] = {}
    latest_release_by_platform: dict[str, str] = {}

    def record_latest(target: dict[str, str], platform: str, version: str | None) -> None:
        if not version:
            return
        current = target.get(platform)
        if current is None or compare_versions(version, current) > 0:
            target[platform] = version

    for row in prepared_rows:
        platform = (_string_value(row.get("apollos_platform")) or "unknown").lower()
        version = _string_value(row.get("apollos_version"))
        if platform in MOBILE_PLATFORMS or platform == "unknown":
            if row.get("is_production_observation"):
                record_latest(confirmed_runtime_by_platform, platform, version)
            elif not row.get("is_internal_observation"):
                record_latest(fallback_runtime_by_platform, platform, version)
        elif platform in RELEASE_TAG_PLATFORMS and row.get("is_production_observation"):
            record_latest(
                latest_release_by_platform,
                platform,
                _string_value(row.get("canonical_source_version")),
            )

    annotated = []
    for row in prepared_rows:
        updated = dict(row)
        platform = (_string_value(row.get("apollos_platform")) or "unknown").lower()
        version_source = _string_value(row.get("version_source")) or "runtime"
        version = _string_value(row.get("apollos_version"))
        observed_app_version = _string_value(row.get("app_version"))
        latest_app_version = _string_value(row.get("latest_app_version")) or observed_app_version
        latest_app_version_source = (
            _string_value(row.get("latest_app_version_source")) or "observed"
        )
        source_version = _string_value(row.get("source_version"))
        source_revision = _string_value(row.get("source_revision"))
        canonical_source_version = _string_value(row.get("canonical_source_version"))

        updated["latest_app_version"] = latest_app_version
        updated["latest_app_version_source"] = latest_app_version_source
        updated["latest_app_version_source_label"] = format_app_version_source_label(
            latest_app_version_source
        )
        updated["version_source_label"] = format_version_source_label(version_source)

        is_app_version_outdated = False
        if observed_app_version and latest_app_version:
            is_app_version_outdated = compare_versions(observed_app_version, latest_app_version) < 0
        updated["is_app_version_outdated"] = is_app_version_outdated

        is_runtime_outdated = False
        is_source_outdated = False
        is_source_status_tbd = False
        is_outdated = False
        latest_runtime = None
        latest_source_version = None
        source_display = format_source_display(source_version, source_revision)
        freshness_display = version or "TBD"
        version_status_label = "Observed"
        version_status_class = "observed"

        if platform in MOBILE_PLATFORMS or platform == "unknown":
            latest_runtime = confirmed_runtime_by_platform.get(
                platform
            ) or fallback_runtime_by_platform.get(platform)
            if version and latest_runtime:
                is_runtime_outdated = compare_versions(version, latest_runtime) < 0
            if row.get("is_internal_observation"):
                version_status_label = (
                    "Prerelease"
                    if source_version and ALPHA_RELEASE_TAG_PATTERN.fullmatch(source_version)
                    else "Internal"
                )
            elif not version:
                version_status_label = "TBD"
            else:
                is_outdated = is_runtime_outdated
                version_status_label = "Outdated" if is_outdated else "Current"
                version_status_class = "outdated" if is_outdated else "current"
        elif platform in RELEASE_TAG_PLATFORMS:
            latest_source_version = latest_release_by_platform.get(platform)
            source_display = canonical_source_version or source_version or "TBD"
            freshness_display = source_display
            is_source_status_tbd = not source_version and not source_revision
            if row.get("is_internal_observation"):
                version_status_label = (
                    "Prerelease"
                    if source_version and ALPHA_RELEASE_TAG_PATTERN.fullmatch(source_version)
                    else "Internal"
                )
            elif canonical_source_version:
                if latest_source_version:
                    is_source_outdated = (
                        compare_versions(canonical_source_version, latest_source_version) < 0
                    )
                is_outdated = is_source_outdated
                version_status_label = "Outdated" if is_outdated else "Current"
                version_status_class = "outdated" if is_outdated else "current"
            elif is_source_status_tbd:
                version_status_label = "TBD"
            else:
                version_status_label = "Unverified"
        elif platform == "roku":
            source_display = source_revision[:7] if source_revision else "TBD"
            freshness_display = source_display
            is_source_status_tbd = not source_revision
            revision_status = roku_revision_statuses.get(source_revision or "")
            if (
                source_revision
                and roku_target_revision
                and _revisions_match(source_revision, roku_target_revision)
            ):
                revision_status = "identical"
            if not source_revision:
                version_status_label = "TBD"
            elif not roku_target_revision or not revision_status:
                version_status_label = "Observed"
            elif revision_status in {"ahead", "identical"}:
                version_status_label = "Current"
                version_status_class = "current"
            elif revision_status == "behind":
                is_source_outdated = True
                is_outdated = True
                version_status_label = "Outdated"
                version_status_class = "outdated"
            else:
                version_status_label = "Unverified"
            updated["latest_source_revision"] = roku_target_revision

        updated["latest_apollos_version"] = latest_runtime or version
        updated["latest_source_version"] = latest_source_version
        updated["is_runtime_outdated"] = is_runtime_outdated
        updated["is_source_outdated"] = is_source_outdated
        updated["is_source_status_tbd"] = is_source_status_tbd
        updated["is_outdated"] = is_outdated
        updated["source_display"] = source_display
        updated["freshness_display"] = freshness_display
        updated["version_status_label"] = version_status_label
        updated["version_status_class"] = version_status_class
        updated["latest_seen_display"] = format_timestamp(row.get("latest_seen_at"))
        annotated.append(updated)
    annotated.sort(
        key=lambda row: (
            not row.get("is_outdated"),
            str(row.get("apollos_platform") or ""),
            str(row.get("church") or ""),
            str(row.get("application_name") or ""),
        )
    )
    return annotated


def _enrich_app_store_versions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bundle_ids = list(
        dict.fromkeys(
            bundle_id
            for row in rows
            if (bundle_id := _string_value(row.get("bundle_id")))
            and _should_lookup_app_store_version(row)
        )
    )
    lookup_bundle_ids = bundle_ids[:APP_STORE_LOOKUP_LIMIT]
    if len(bundle_ids) > APP_STORE_LOOKUP_LIMIT:
        logging.warning(
            "Skipping App Store lookup for %s iOS bundle IDs over the %s-bundle limit.",
            len(bundle_ids) - APP_STORE_LOOKUP_LIMIT,
            APP_STORE_LOOKUP_LIMIT,
        )
    app_store_versions: dict[str, dict[str, Any]] = {}
    if lookup_bundle_ids:
        try:
            app_store_versions = _fetch_app_store_versions(lookup_bundle_ids)
        except requests.RequestException as exc:
            logging.warning("App Store version lookup failed: %s", exc)
        except ValueError as exc:
            logging.warning("App Store version lookup returned invalid JSON: %s", exc)

    enriched = []
    for row in rows:
        updated = dict(row)
        bundle_id = _string_value(row.get("bundle_id"))
        store_version = (
            app_store_versions.get(bundle_id or "")
            if _should_lookup_app_store_version(row)
            else None
        )
        if store_version and (version := _string_value(store_version.get("version"))):
            updated["latest_app_version"] = version
            updated["latest_app_version_source"] = "app_store"
            updated["latest_app_version_seen_at"] = store_version.get("currentVersionReleaseDate")
            updated["latest_app_name"] = store_version.get("trackName")
        else:
            updated["latest_app_version"] = _string_value(row.get("app_version"))
            updated["latest_app_version_source"] = "observed"
        enriched.append(updated)
    return enriched


def _fetch_app_store_versions(bundle_ids: list[str]) -> dict[str, dict[str, Any]]:
    app_store_versions: dict[str, dict[str, Any]] = {}
    max_workers = min(APP_STORE_LOOKUP_WORKERS, len(bundle_ids))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_by_bundle_id = {
            executor.submit(_fetch_app_store_version, bundle_id): bundle_id
            for bundle_id in bundle_ids
        }
        for future in as_completed(future_by_bundle_id):
            bundle_id = future_by_bundle_id[future]
            try:
                result = future.result()
            except (requests.RequestException, ValueError) as exc:
                logging.warning("App Store version lookup failed for %s: %s", bundle_id, exc)
                continue
            if result:
                app_store_versions[result["bundleId"]] = result
    return app_store_versions


def _fetch_app_store_version(bundle_id: str) -> dict[str, Any] | None:
    response = requests.get(
        APP_STORE_LOOKUP_URL,
        params={
            "bundleId": bundle_id,
            "country": "us",
        },
        timeout=APP_STORE_LOOKUP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    for result in payload.get("results", []):
        if not isinstance(result, dict):
            continue
        fetched_bundle_id = _string_value(result.get("bundleId"))
        version = _string_value(result.get("version"))
        if fetched_bundle_id and version:
            return {**result, "bundleId": fetched_bundle_id}
    return None


def _should_lookup_app_store_version(row: dict[str, Any]) -> bool:
    platform = (_string_value(row.get("apollos_platform")) or "").lower()
    bundle_id = _string_value(row.get("bundle_id")) or ""
    normalized = bundle_id.lower()
    if platform != "ios":
        return False
    return "." in normalized and normalized not in {"unknown", "roku"}


def _select_latest_observed_versions(
    rows: list[dict[str, Any]],
    stable_release_revisions: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    stable_release_revisions = stable_release_revisions or {}
    observations_by_app: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        decorated = _decorate_observation(row, stable_release_revisions)
        observations_by_app.setdefault(_app_identity_key(decorated), []).append(decorated)

    selected = []
    for observations in observations_by_app.values():
        production = [row for row in observations if row["is_production_observation"]]
        non_internal = [row for row in observations if not row["is_internal_observation"]]
        candidates = production or non_internal or observations
        current = candidates[0]
        for candidate in candidates[1:]:
            if _is_newer_observed_version(
                candidate,
                current,
                stable_release_revisions,
            ):
                current = candidate
        selected.append(current)
    return selected


def _app_identity_key(row: dict[str, Any]) -> tuple[str, str, str]:
    church = _string_value(row.get("church")) or "Unknown church"
    platform = _string_value(row.get("apollos_platform")) or "unknown"
    bundle_id = (_string_value(row.get("bundle_id")) or "unknown").lower()
    if bundle_id in {"unknown", "roku"}:
        return church, platform, bundle_id
    return "", platform, bundle_id


def _is_newer_observed_version(
    candidate: dict[str, Any],
    current: dict[str, Any],
    stable_release_revisions: dict[str, str] | None = None,
) -> bool:
    stable_release_revisions = stable_release_revisions or {}
    platform = (_string_value(candidate.get("apollos_platform")) or "unknown").lower()

    if platform in RELEASE_TAG_PLATFORMS:
        candidate_source_version = _string_value(
            candidate.get("canonical_source_version")
        ) or _canonical_release_version(candidate, stable_release_revisions)
        candidate_source_version = candidate_source_version or _string_value(
            candidate.get("source_version")
        )
        current_source_version = _string_value(
            current.get("canonical_source_version")
        ) or _canonical_release_version(current, stable_release_revisions)
        current_source_version = current_source_version or _string_value(
            current.get("source_version")
        )
        if candidate_source_version and current_source_version:
            source_compare = compare_versions(candidate_source_version, current_source_version)
            if source_compare != 0:
                return source_compare > 0
        elif candidate_source_version != current_source_version:
            return bool(candidate_source_version)

    if platform == "roku":
        candidate_source_version = _string_value(candidate.get("source_version"))
        current_source_version = _string_value(current.get("source_version"))
        if candidate_source_version and current_source_version:
            source_compare = compare_versions(candidate_source_version, current_source_version)
            if source_compare != 0:
                return source_compare > 0
        elif candidate_source_version != current_source_version:
            return bool(candidate_source_version)

    if platform not in RELEASE_TAG_PLATFORMS and platform != "roku":
        candidate_version = _string_value(candidate.get("apollos_version"))
        current_version = _string_value(current.get("apollos_version"))
        if candidate_version and current_version:
            version_compare = compare_versions(candidate_version, current_version)
            if version_compare != 0:
                return version_compare > 0
        elif candidate_version != current_version:
            return bool(candidate_version)

    candidate_app_version = _string_value(candidate.get("app_version"))
    current_app_version = _string_value(current.get("app_version"))
    if candidate_app_version and current_app_version:
        app_version_compare = compare_versions(candidate_app_version, current_app_version)
        if app_version_compare != 0:
            return app_version_compare > 0
    elif candidate_app_version != current_app_version:
        return bool(candidate_app_version)

    candidate_church = _string_value(candidate.get("church")) or "Unknown church"
    current_church = _string_value(current.get("church")) or "Unknown church"
    if candidate_church != current_church:
        return current_church == "Unknown church"

    return _timestamp_sort_key(candidate.get("latest_seen_at")) > _timestamp_sort_key(
        current.get("latest_seen_at")
    )


def build_platform_tabs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tabs = []
    rows_by_platform: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        platform = _string_value(row.get("apollos_platform")) or "unknown"
        rows_by_platform.setdefault(platform, []).append(row)

    for platform, platform_rows in sorted(
        rows_by_platform.items(),
        key=lambda item: (
            1 if item[0] == "unknown" else 0,
            item[0],
        ),
    ):
        tabs.append(
            {
                "key": platform,
                "label": format_platform_label(platform),
                "freshness_column_label": format_freshness_column_label(platform),
                "rows": platform_rows,
                "row_count": len(platform_rows),
                "outdated_count": sum(1 for row in platform_rows if row.get("is_outdated")),
            }
        )
    return tabs


def format_freshness_column_label(platform: str) -> str:
    if platform in MOBILE_PLATFORMS:
        return "Expo Runtime"
    if platform in RELEASE_TAG_PLATFORMS:
        return "Release"
    if platform == "roku":
        return "Commit"
    return "Version"


def format_platform_label(platform: str) -> str:
    labels = {
        "amazon": "Amazon",
        "android": "Android",
        "androidtv": "AndroidTV",
        "ios": "iOS",
        "roku": "Roku",
        "tv": "TV",
        "tvos": "tvOS",
        "unknown": "Unknown",
    }
    return labels.get(platform, platform.replace("_", " ").title())


def format_version_source_label(version_source: str) -> str:
    labels = {
        "analytics_library": "Analytics library",
        "runtime": "Runtime",
    }
    return labels.get(version_source, version_source.replace("_", " ").title())


def format_app_version_source_label(version_source: str) -> str:
    labels = {
        "app_store": "App Store",
        "observed": "Observed",
    }
    return labels.get(version_source, version_source.replace("_", " ").title())


def format_source_display(source_version: str | None, source_revision: str | None) -> str:
    if source_version and source_revision:
        return f"{source_version} ({source_revision[:7]})"
    if source_version:
        return source_version
    if source_revision:
        return source_revision[:7]
    return "TBD"


def compare_versions(left: str, right: str) -> int:
    left_key = _version_key(left)
    right_key = _version_key(right)
    if left_key == right_key:
        return 0
    return 1 if left_key > right_key else -1


def _version_key(value: str) -> tuple[Any, ...]:
    normalized = value.strip().removeprefix("v")
    try:
        return (2, Version(normalized))
    except InvalidVersion:
        pass

    parts: list[tuple[int, Any]] = []
    for token in re.findall(r"\d+|[A-Za-z]+", normalized):
        if token.isdigit():
            parts.append((1, int(token)))
        else:
            parts.append((0, token.lower()))
    return (1, tuple(parts), normalized.lower())


def format_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %I:%M %p %Z")
    if isinstance(value, str):
        return value
    return str(value)


def _timestamp_sort_key(value: Any) -> tuple[int, Any]:
    if isinstance(value, datetime):
        return (2, value.timestamp())
    if value is None:
        return (0, "")
    return (1, str(value))


def _get_app_versions_config() -> AppVersionsConfig:
    return AppVersionsConfig(
        project_id=os.getenv(
            "BIGQUERY_ANALYTICS_PROJECT_ID",
            DEFAULT_BIGQUERY_ANALYTICS_PROJECT_ID,
        ).strip(),
        datasets=_get_configured_datasets(),
        tables=_get_configured_tables(),
        lookback_days=_get_positive_int_env(
            "APP_VERSIONS_LOOKBACK_DAYS",
            DEFAULT_APP_VERSIONS_LOOKBACK_DAYS,
        ),
        limit=_get_positive_int_env("APP_VERSIONS_LIMIT", DEFAULT_APP_VERSIONS_LIMIT),
    )


def _get_configured_tables() -> tuple[str, ...]:
    value = os.getenv("BIGQUERY_ANALYTICS_TABLES", "")
    if not value.strip():
        return DEFAULT_SEGMENT_TABLES
    tables = tuple(table.strip() for table in value.split(",") if table.strip())
    return tables or DEFAULT_SEGMENT_TABLES


def _get_configured_datasets() -> tuple[str, ...]:
    value = (
        os.getenv("BIGQUERY_ANALYTICS_DATASETS", "").strip()
        or os.getenv("BIGQUERY_ANALYTICS_DATASET", "").strip()
    )
    if not value:
        return DEFAULT_BIGQUERY_ANALYTICS_DATASETS
    datasets = tuple(dataset.strip() for dataset in value.split(",") if dataset.strip())
    return datasets or DEFAULT_BIGQUERY_ANALYTICS_DATASETS


def _get_positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


def _choose_column(columns: dict[str, str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        column = columns.get(candidate.lower())
        if column:
            return column
    return None


def _choose_version_column(dataset: str, columns: dict[str, str]) -> tuple[str | None, str]:
    runtime_column = _choose_column(columns, FIELD_CANDIDATES["apollos_version"])
    if runtime_column:
        return runtime_column, "runtime"
    if dataset == "apollos_roku":
        analytics_column = _choose_column(columns, ROKU_ANALYTICS_VERSION_CANDIDATES)
        if analytics_column:
            return analytics_column, "analytics_library"
    return None, ""


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "items"):
        return dict(row.items())
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _query_job_config(query_parameters: list[Any]) -> Any:
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(query_parameters=query_parameters)


def _scalar_query_parameter(name: str, field_type: str, value: Any) -> Any:
    from google.cloud import bigquery

    return bigquery.ScalarQueryParameter(name, field_type, value)


def _array_query_parameter(name: str, field_type: str, values: list[Any]) -> Any:
    from google.cloud import bigquery

    return bigquery.ArrayQueryParameter(name, field_type, values)


def _escape_identifier(value: str) -> str:
    if "`" in value:
        raise AppVersionsError("BigQuery identifiers cannot contain backticks.")
    return value


def _escape_string_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _string_value(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
