import base64
import binascii
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from packaging.version import InvalidVersion, Version

from mobile_releases import STORE_PLATFORMS, fetch_live_mobile_releases

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
GITHUB_TIMEOUT_SECONDS = 5
REVISION_COMPARE_WORKERS = 8
PLATFORMS_GITHUB_API_URL = "https://api.github.com/repos/ApollosProject/apollos-platforms"

TIMESTAMP_COLUMN_CANDIDATES = (
    "timestamp",
    "received_at",
    "sent_at",
    "original_timestamp",
    "loaded_at",
)

FIELD_CANDIDATES = {
    "church": ("church", "group_id", "groupId"),
    "build_church": ("build_church", "buildChurch"),
    "apollos_platform": ("apollos_platform", "apollosPlatform", "apollosplatform"),
    "apollos_version": ("apollos_version", "apollosVersion"),
    "app_version": ("app_version", "appVersion"),
    "native_build": ("context_app_build",),
    "native_version": ("context_app_version",),
    "app_update_id": ("app_update_id", "appUpdateId"),
    "bundle_id": ("bundle_id", "bundleId"),
    "application_name": ("application_name", "applicationName"),
    "source_revision": ("source_revision", "sourceRevision"),
    "source_version": ("source_version", "sourceVersion"),
    "deployment_track": ("deployment_track", "deploymentTrack"),
}

ROKU_ANALYTICS_VERSION_CANDIDATES = ("context_library_version",)
RELEASE_TAG_PLATFORMS = {"amazon", "tv", "tvos"}
STABLE_RELEASE_TAG_PATTERN = re.compile(r"^v\d{4}\.\d{2}\.\d{2}\.\d{2}$")
ALPHA_RELEASE_TAG_PATTERN = re.compile(r"^(v\d{4}\.\d{2}\.\d{2}\.\d{2})-alpha\.\d+$")
SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{7,40}$")
RUNTIME_VERSION_PATTERN = re.compile(r"\d+(?:\.\d+)*")
INTERNAL_DEPLOYMENT_TRACKS = {"beta", "development", "internal", "preview", "prerelease"}
DEPLOY_PLATFORMS = {"ios", "android", "tvos", "androidtv", "amazon", "roku"}


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
    if not os.getenv("REDIS_URL", "").strip():
        return _compute_app_versions_context()
    from fleet_health_cache import _get_redis_client

    try:
        client = _get_redis_client()
        raw = client.get(_app_versions_cache_key()) if client else None
        if raw:
            context = json.loads(raw)
            if isinstance(context, dict) and isinstance(context.get("rows"), list):
                return context
    except Exception:
        logging.exception("Unable to read app versions cache")
    return {
        "status": "unavailable",
        "rows": [],
        "lookback_days": _get_app_versions_config().lookback_days,
        "error_message": "App release data is refreshing. The worker must be running.",
    }


def _app_versions_cache_key() -> str:
    return f"apps:live-runtime:v3:{_get_app_versions_config()!r}"


def refresh_app_versions_cache() -> None:
    from fleet_health_cache import _get_redis_client

    client = _get_redis_client()
    if client is None:
        return
    context = _compute_app_versions_context()
    try:
        client.setex(_app_versions_cache_key(), 300, json.dumps(context, default=str))
    except Exception:
        logging.exception("Unable to store app versions cache")


def _compute_app_versions_context() -> dict[str, Any]:
    config = _get_app_versions_config()
    checked_at = time.time()
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
        "checked_at": datetime.fromtimestamp(checked_at).astimezone().strftime("%Y-%m-%d %H:%M %Z"),
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
    latest_rows = _select_latest_observed_versions(
        [row for row in rows if str(row.get("apollos_platform")).lower() not in STORE_PLATFORMS],
        source_context["stable_release_revisions"],
    )
    latest_rows += _select_live_mobile_versions(rows, fetch_live_mobile_releases(rows))
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


def app_control_rows(context: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    if context.get("status") != "ready":
        return {}
    matches: dict[tuple[str, str, str], dict[str, Any] | None] = {}
    for row in context["rows"]:
        key = _app_identity_key(row)
        matches[key] = None if key in matches else row
    return {
        key: row
        for key, row in matches.items()
        if row is not None
        and key[1] in DEPLOY_PLATFORMS
        and row.get("deploy_target_count") == 1
        and row.get("church")
        and row.get("bundle_id")
        and app_control_slug(row)
    }


def app_control_slug(row: dict[str, Any]) -> str | None:
    slug = _string_value(row.get("build_church")) or _string_value(row.get("church"))
    return slug if slug and re.fullmatch(r"[A-Za-z0-9_-]+", slug) else None


def dispatch_app_deploy(church: str, platform: str) -> None:
    token = os.getenv("GITHUB_ACTIONS_TOKEN", "").strip()
    if not token:
        raise AppVersionsError("GITHUB_ACTIONS_TOKEN is not configured")
    ref = ""
    page = 1
    while True:
        response = requests.get(
            f"{PLATFORMS_GITHUB_API_URL}/tags",
            params={"per_page": 100, "page": page},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=GITHUB_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        response.raise_for_status()
        tags = response.json()
        ref = max(
            [ref]
            + [
                tag["name"]
                for tag in tags
                if STABLE_RELEASE_TAG_PATTERN.fullmatch(tag.get("name", ""))
            ]
        )
        if len(tags) < 100:
            break
        page += 1
    if not ref:
        raise AppVersionsError("No stable release tag found")
    response = requests.post(
        f"{PLATFORMS_GITHUB_API_URL}/actions/workflows/"
        f"{os.getenv('GITHUB_DEPLOY_WORKFLOW_ID', '173574865')}/dispatches",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        json={
            "ref": ref,
            "inputs": {"church": church, "platform": platform, "track": "production"},
        },
        timeout=GITHUB_TIMEOUT_SECONDS,
        allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 204:
        raise AppVersionsError("GitHub did not confirm workflow dispatch")


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
            NULLIF(build_church, '') AS build_church,
            LOWER(COALESCE(
              NULLIF(apollos_platform, ''),
              IF(source_dataset = 'apollos_roku', 'roku', NULL),
              IF(source_dataset = 'apollos_tv', 'tv', NULL),
              'unknown'
            )) AS apollos_platform,
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
            native_build,
            native_version,
            app_update_id,
            source_revision,
            source_version,
            deployment_track,
            source_dataset,
            source_table,
            version_source
          FROM version_events
          WHERE apollos_version IS NOT NULL OR build_church IS NOT NULL
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
              IF(apollos_version IS NOT NULL, church, NULL) IGNORE NULLS
              ORDER BY IF(church = 'Unknown church', 1, 0), church
              LIMIT 1
            )[SAFE_OFFSET(0)] AS church,
            ARRAY_AGG(build_church IGNORE NULLS ORDER BY seen_at DESC LIMIT 1)
              [SAFE_OFFSET(0)] AS build_church,
            IF(
              COUNT(DISTINCT build_church) > 0,
              COUNT(DISTINCT build_church),
              COUNT(DISTINCT IF(apollos_version IS NOT NULL, church, NULL))
            ) AS deploy_target_count
          FROM app_identity_events
          GROUP BY app_identity_key
        ),
        version_observations AS (
          SELECT
            events.app_identity_key,
            display_churches.church,
            display_churches.build_church,
            display_churches.deploy_target_count,
            events.apollos_platform,
            events.application_name,
            events.bundle_id,
            events.apollos_version,
            events.app_version,
            events.native_build,
            events.native_version,
            events.app_update_id,
            events.source_revision,
            events.source_version,
            events.deployment_track,
            events.source_dataset,
            events.source_table,
            events.version_source,
            MAX(events.seen_at) AS latest_seen_at
          FROM app_identity_events events
          JOIN display_churches
            USING (app_identity_key)
          WHERE events.apollos_version IS NOT NULL
          GROUP BY
            events.app_identity_key,
            display_churches.church,
            display_churches.build_church,
            display_churches.deploy_target_count,
            events.apollos_platform,
            events.application_name,
            events.bundle_id,
            events.apollos_version,
            events.app_version,
            events.native_build,
            events.native_version,
            events.app_update_id,
            events.source_revision,
            events.source_version,
            events.deployment_track,
            events.source_dataset,
            events.source_table,
            events.version_source
        )
        SELECT
          observation.church,
          observation.build_church,
          observation.deploy_target_count,
          observation.apollos_platform,
          observation.application_name,
          observation.bundle_id,
          observation.apollos_version,
          observation.app_version,
          observation.native_build,
          observation.native_version,
          observation.app_update_id,
          observation.source_revision,
          observation.source_version,
          observation.deployment_track,
          observation.source_dataset,
          observation.source_table,
          observation.version_source,
          observation.latest_seen_at
        FROM version_observations observation
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


def _github_json(path: str, params: dict[str, str] | None = None) -> Any:
    headers = {"Accept": "application/vnd.github+json"}
    if token := os.getenv("GITHUB_TOKEN", "").strip():
        headers["Authorization"] = f"Bearer {token}"
    try:
        response = requests.get(
            f"{PLATFORMS_GITHUB_API_URL}/{path}",
            params=params,
            headers=headers,
            timeout=GITHUB_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError):
        logging.warning("Unable to load Platforms %s from GitHub", path, exc_info=True)
        return None


def _fetch_platform_source_context() -> dict[str, Any]:
    tags = _github_json("tags", {"per_page": "100"}) or []
    commits = _github_json("commits", {"sha": "master", "path": "templates/roku", "per_page": "1"})
    stable_tags = [tag for tag in tags if STABLE_RELEASE_TAG_PATTERN.fullmatch(tag.get("name", ""))]
    latest_tag = max(stable_tags, key=lambda tag: _version_key(tag["name"]), default=None)
    release_runtimes: dict[str, str | None] = {"mobile": None, "tv": None}
    if latest_tag:
        for template in release_runtimes:
            config = _github_json(
                f"contents/templates/{template}/app.config.ts", {"ref": latest_tag["name"]}
            )
            if isinstance(config, dict) and config.get("encoding") == "base64":
                try:
                    contents = base64.b64decode(config["content"]).decode("utf-8")
                    match = re.search(
                        r"^\s*runtimeVersion:\s*['\"](\d+)['\"]", contents, re.MULTILINE
                    )
                    release_runtimes[template] = match.group(1) if match else None
                except (KeyError, TypeError, binascii.Error, UnicodeDecodeError):
                    pass
    return {
        "stable_release_revisions": {tag["name"]: tag["commit"]["sha"] for tag in stable_tags},
        "mobile_release_runtime": release_runtimes["mobile"],
        "tv_release_runtime": release_runtimes["tv"],
        "roku_target_revision": commits[0]["sha"] if commits else None,
    }


def _fetch_roku_revision_statuses(
    rows: list[dict[str, Any]], target_revision: str | None
) -> dict[str, str]:
    revisions = list(
        {
            revision
            for row in rows
            if (_string_value(row.get("apollos_platform")) or "").lower() == "roku"
            and (revision := _string_value(row.get("source_revision")))
        }
    )
    if not target_revision or not revisions:
        return {}
    with ThreadPoolExecutor(max_workers=min(REVISION_COMPARE_WORKERS, len(revisions))) as executor:
        statuses = executor.map(
            lambda revision: _fetch_revision_compare_status(target_revision, revision),
            revisions,
        )
    return {revision: status for revision, status in zip(revisions, statuses) if status}


def _fetch_revision_compare_status(target_revision: str, deployed_revision: str) -> str | None:
    if _revisions_match(target_revision, deployed_revision):
        return "identical"
    if not SHA_PATTERN.fullmatch(target_revision) or not SHA_PATTERN.fullmatch(deployed_revision):
        return None
    payload = _github_json(f"compare/{target_revision}...{deployed_revision}")
    return _string_value(payload.get("status")) if isinstance(payload, dict) else None


def _revisions_match(left: str, right: str) -> bool:
    if not SHA_PATTERN.fullmatch(left) or not SHA_PATTERN.fullmatch(right):
        return False
    left, right = left.lower(), right.lower()
    return left.startswith(right) or right.startswith(left)


def _annotate_version_status(
    rows: list[dict[str, Any]], source_context: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    source_context = source_context or {}
    roku_statuses = source_context.get("roku_revision_statuses") or {}
    latest_by_platform: dict[str, str] = {}
    for row in rows:
        platform = (_string_value(row.get("apollos_platform")) or "unknown").lower()
        version = _string_value(
            row.get(
                "canonical_source_version"
                if platform in RELEASE_TAG_PLATFORMS
                else "apollos_version"
            )
        )
        if platform in STORE_PLATFORMS:
            continue
        if version and (
            platform not in latest_by_platform
            or compare_versions(version, latest_by_platform[platform]) > 0
        ):
            latest_by_platform[platform] = version

    annotated = []
    for row in rows:
        updated = dict(row)
        platform = (_string_value(row.get("apollos_platform")) or "unknown").lower()
        version = _string_value(row.get("apollos_version"))
        comparable_runtime = bool(version and RUNTIME_VERSION_PATTERN.fullmatch(version))
        source_version = _string_value(row.get("source_version"))
        source_revision = _string_value(row.get("source_revision"))
        release_version = _string_value(row.get("canonical_source_version"))
        latest_version = (
            _string_value(
                source_context.get(
                    "tv_release_runtime" if platform == "androidtv" else "mobile_release_runtime"
                )
            )
            if platform in STORE_PLATFORMS
            else latest_by_platform.get(platform)
            if platform in RELEASE_TAG_PLATFORMS
            else None
        )
        is_outdated = False
        freshness_display = row.get("live_runtime_display") or version or "Unknown"
        revision_status = None
        if platform in RELEASE_TAG_PLATFORMS:
            freshness_display = release_version or source_version or "TBD"
            if release_version and latest_version:
                is_outdated = compare_versions(release_version, latest_version) < 0
        elif platform == "roku":
            freshness_display = source_revision[:7] if source_revision else "TBD"
            revision_status = roku_statuses.get(source_revision or "")
            is_outdated = revision_status == "behind"
        elif platform in STORE_PLATFORMS and comparable_runtime and latest_version and version:
            is_outdated = compare_versions(version, latest_version) < 0

        updated["is_outdated"] = is_outdated
        updated["freshness_display"] = freshness_display
        updated["comparison_display"] = (
            (_string_value(source_context.get("roku_target_revision")) or "")[:7] or "unknown"
            if platform == "roku"
            else latest_version or "unknown"
        )
        if platform == "roku":
            version_status_label = {
                "behind": "Behind source",
                "identical": "At source",
                "ahead": "Ahead of source",
            }.get(revision_status or "", "Unverified")
        elif (
            platform not in STORE_PLATFORMS | RELEASE_TAG_PLATFORMS
            or (platform in RELEASE_TAG_PLATFORMS and not release_version)
            or (platform in STORE_PLATFORMS and not comparable_runtime)
            or freshness_display == "TBD"
        ):
            version_status_label = "Unverified"
        elif latest_version:
            if platform in STORE_PLATFORMS:
                version_status_label = (
                    "Behind release"
                    if is_outdated
                    else "Ahead of release"
                    if compare_versions(version or "", latest_version) > 0
                    else "At release"
                )
            else:
                version_status_label = "Behind top seen" if is_outdated else "Top seen"
        else:
            version_status_label = "Unverified"
        if platform in STORE_PLATFORMS:
            if row.get("live_runtime_display") and not version:
                version_status_label = "Multiple live runtimes"
            elif comparable_runtime and not latest_version:
                updated["live_status_detail"] = "Release target unavailable"
        else:
            updated["live_status_detail"] = (
                "Store publication not verified; analytics observation only."
            )
            if version_status_label != "Unverified":
                version_status_label = f"Observed: {version_status_label}"
        version_status_class = (
            "observed"
            if platform not in STORE_PLATFORMS
            or version_status_label in {"Unverified", "Multiple live runtimes"}
            else "outdated"
            if is_outdated
            else "current"
        )
        updated["version_status_label"] = version_status_label
        updated["version_status_class"] = version_status_class
        annotated.append(updated)
    annotated.sort(
        key=lambda row: (
            not row.get("is_outdated"),
            row.get("version_status_label") != "Unverified",
            str(row.get("apollos_platform") or ""),
            str(row.get("church") or ""),
            str(row.get("application_name") or ""),
        )
    )
    return annotated


def _select_live_mobile_versions(
    rows: list[dict[str, Any]], releases: dict[tuple[str, str], dict[str, Any]]
) -> list[dict[str, Any]]:
    by_app: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = _app_identity_key(row)
        if key[1] in STORE_PLATFORMS:
            by_app.setdefault(key, []).append(row)

    selected = []
    for (_, platform, bundle), observations in by_app.items():
        # Display identity can come from any observation; runtime cannot.
        updated = max(
            observations, key=lambda row: _timestamp_sort_key(row.get("latest_seen_at"))
        ).copy()
        updated["apollos_version"] = None
        updated["live_status_detail"] = "Store release could not be verified"
        release = releases.get((platform, bundle))
        if release and "deploy_target_count" in release:
            updated["build_church"] = release["build_church"]
            updated["deploy_target_count"] = release["deploy_target_count"]
        if release and release.get("live_status_detail"):
            updated["live_status_detail"] = release["live_status_detail"]
        builds = release.get("builds") if release else None
        if builds == []:
            updated["live_status_detail"] = "No published store build"
        elif builds:
            runtimes = set()
            for build in builds:
                matches = {
                    _string_value(row.get("apollos_version"))
                    for row in observations
                    if _string_value(row.get("native_build")) == build["native_build"]
                    # Queued events can carry old properties with a newer native context.
                    and (
                        not row.get("app_version")
                        or not row.get("native_version")
                        or row["app_version"] == row["native_version"]
                    )
                    and (
                        platform != "ios"
                        or _string_value(row.get("native_version")) == build["native_version"]
                    )
                }
                if len(matches) != 1 or not RUNTIME_VERSION_PATTERN.fullmatch(
                    next(iter(matches)) or ""
                ):
                    updated["live_status_detail"] = "Live build has no unique runtime match"
                    break
                runtimes.update(matches)
            else:
                verified = sorted((runtime for runtime in runtimes if runtime), key=_version_key)
                updated["live_runtime_display"] = ", ".join(verified)
                updated["apollos_version"] = verified[0] if len(verified) == 1 else None
                updated["live_status_detail"] = "Published store build matched by native build ID"
        selected.append(updated)
    return selected


def _select_latest_observed_versions(
    rows: list[dict[str, Any]],
    stable_release_revisions: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    stable_release_revisions = stable_release_revisions or {}
    latest_by_app: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        source_version = _string_value(row.get("source_version")) or ""
        release_version = (
            source_version if STABLE_RELEASE_TAG_PATTERN.fullmatch(source_version) else None
        )
        match = ALPHA_RELEASE_TAG_PATTERN.fullmatch(source_version)
        if match and _revisions_match(
            _string_value(row.get("source_revision")) or "",
            stable_release_revisions.get(match.group(1), ""),
        ):
            release_version = match.group(1)
        track = (_string_value(row.get("deployment_track")) or "").lower()
        if track in INTERNAL_DEPLOYMENT_TRACKS or (
            (match and not release_version) or source_version.lower() == "master"
        ):
            continue
        candidate = dict(row)
        candidate["canonical_source_version"] = release_version
        platform = (_string_value(row.get("apollos_platform")) or "unknown").lower()
        if platform in RELEASE_TAG_PLATFORMS:
            candidate["apollos_version"] = release_version
        elif platform == "roku":
            candidate["apollos_version"] = _string_value(row.get("source_version"))
        key = _app_identity_key(candidate)
        current = latest_by_app.get(key)
        if current is None or _is_newer_observed_version(candidate, current):
            latest_by_app[key] = candidate
    return list(latest_by_app.values())


def _app_identity_key(row: dict[str, Any]) -> tuple[str, str, str]:
    church = _string_value(row.get("church")) or "Unknown church"
    platform = (_string_value(row.get("apollos_platform")) or "unknown").lower()
    bundle_id = (_string_value(row.get("bundle_id")) or "unknown").lower()
    if bundle_id in {"unknown", "roku"}:
        return church, platform, bundle_id
    return "", platform, bundle_id


def _is_newer_observed_version(
    candidate: dict[str, Any],
    current: dict[str, Any],
) -> bool:
    candidate_version = _string_value(candidate.get("apollos_version"))
    current_version = _string_value(current.get("apollos_version"))
    platform = (_string_value(candidate.get("apollos_platform")) or "unknown").lower()
    if platform in {"ios", "android"}:
        candidate_valid = bool(
            candidate_version and RUNTIME_VERSION_PATTERN.fullmatch(candidate_version)
        )
        current_valid = bool(current_version and RUNTIME_VERSION_PATTERN.fullmatch(current_version))
        if candidate_valid != current_valid:
            return candidate_valid
        if not candidate_valid:
            candidate_version = current_version = None
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

    candidate_source_version = _string_value(candidate.get("source_version"))
    current_source_version = _string_value(current.get("source_version"))
    if candidate_source_version and current_source_version:
        source_version_compare = compare_versions(candidate_source_version, current_source_version)
        if source_version_compare != 0:
            return source_version_compare > 0
    elif candidate_source_version != current_source_version:
        return bool(candidate_source_version)

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
        platform = (_string_value(row.get("apollos_platform")) or "unknown").lower()
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
                "uses_store_runtime": platform in STORE_PLATFORMS,
                "rows": platform_rows,
                "row_count": len(platform_rows),
                "outdated_count": sum(1 for row in platform_rows if row.get("is_outdated")),
            }
        )
    return tabs


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
