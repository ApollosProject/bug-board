import base64
import sys
import types
import unittest
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import patch


def _install_import_shims() -> None:
    dotenv_module = cast(Any, types.ModuleType("dotenv"))
    dotenv_module.load_dotenv = lambda *args, **kwargs: None
    sys.modules.setdefault("dotenv", dotenv_module)

    requests_module = cast(Any, types.ModuleType("requests"))
    requests_module.RequestException = Exception
    requests_module.HTTPError = Exception

    class DummySession:
        def __init__(self):
            self.headers = {}

    requests_module.Session = DummySession
    requests_module.get = lambda *args, **kwargs: None
    sys.modules.setdefault("requests", requests_module)

    gql_module = cast(Any, types.ModuleType("gql"))

    class DummyClient:
        def __init__(self, *args, **kwargs):
            self.requests = []

        def execute(self, request, **kwargs):
            self.requests.append(request)
            return {}

    class DummyGraphQLRequest:
        def __init__(self, request, *, variable_values=None, operation_name=None):
            self.request = request
            self.variable_values = variable_values
            self.operation_name = operation_name

    gql_module.Client = DummyClient
    gql_module.GraphQLRequest = DummyGraphQLRequest
    gql_module.gql = lambda query: query
    sys.modules.setdefault("gql", gql_module)
    sys.modules.setdefault("gql.transport", types.ModuleType("gql.transport"))

    aiohttp_module = cast(Any, types.ModuleType("gql.transport.aiohttp"))
    aiohttp_module.AIOHTTPTransport = lambda *args, **kwargs: None
    sys.modules.setdefault("gql.transport.aiohttp", aiohttp_module)


_install_import_shims()

import app as app_module  # noqa: E402
import app_versions  # noqa: E402


class AppVersionsContextTest(unittest.TestCase):
    def test_default_config_targets_apollos_bigquery_datasets(self):
        with patch.dict(app_versions.os.environ, {}, clear=False):
            for env_name in (
                "BIGQUERY_ANALYTICS_PROJECT_ID",
                "BIGQUERY_ANALYTICS_DATASET",
                "BIGQUERY_ANALYTICS_DATASETS",
                "BIGQUERY_ANALYTICS_TABLES",
            ):
                app_versions.os.environ.pop(env_name, None)

            config = app_versions._get_app_versions_config()

        self.assertEqual(config.project_id, "apollos-project")
        self.assertEqual(config.datasets, ("apollos", "apollos_tv", "apollos_roku"))
        self.assertIn("app_became_active", config.tables)
        self.assertIn("identifies", config.tables)
        self.assertEqual(config.limit, 1000)

    def test_builds_bigquery_credentials_from_base64_service_account_json(self):
        credentials = object()
        service_account_json = base64.b64encode(
            b'{"client_email":"bigquery-reader@example.com","token_uri":"https://oauth2.googleapis.com/token"}'
        ).decode("ascii")

        with patch.dict(
            app_versions.os.environ,
            {"BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64": service_account_json},
            clear=False,
        ):
            with patch(
                "google.oauth2.service_account.Credentials.from_service_account_info",
                return_value=credentials,
            ) as build_credentials:
                result = app_versions._build_bigquery_credentials()

        self.assertIs(result, credentials)
        args, kwargs = build_credentials.call_args
        self.assertEqual(args[0]["client_email"], "bigquery-reader@example.com")
        self.assertEqual(kwargs["scopes"], ["https://www.googleapis.com/auth/cloud-platform"])

    def test_bigquery_credentials_require_service_account_json(self):
        with patch.dict(
            app_versions.os.environ,
            {
                "BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64": "",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(
                app_versions.AppVersionsError,
                "Set BIGQUERY_SERVICE_ACCOUNT_JSON_BASE64",
            ):
                app_versions._build_bigquery_credentials()

    def test_annotates_outdated_apps_by_platform_latest_runtime(self):
        rows = [
            {
                "church": "one-church",
                "apollos_platform": "ios",
                "application_name": "One Church",
                "bundle_id": "com.one",
                "apollos_version": "97",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            },
            {
                "church": "two-church",
                "apollos_platform": "ios",
                "application_name": "Two Church",
                "bundle_id": "com.two",
                "apollos_version": "101",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc),
            },
            {
                "church": "tv-church",
                "apollos_platform": "tvos",
                "application_name": "TV Church",
                "bundle_id": "com.tv",
                "apollos_version": "1.0.0",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc),
            },
            {
                "church": "unknown-platform",
                "apollos_platform": "unknown",
                "application_name": "Unknown Platform",
                "bundle_id": "com.unknown",
                "apollos_version": "8.2.13",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc),
            },
            {
                "church": "roku-church",
                "apollos_platform": "roku",
                "application_name": "Roku",
                "bundle_id": "roku",
                "apollos_version": "2.0.0",
                "version_source": "analytics_library",
                "latest_seen_at": datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc),
            },
        ]

        annotated = app_versions._annotate_version_status(rows)

        one_church = next(row for row in annotated if row["church"] == "one-church")
        two_church = next(row for row in annotated if row["church"] == "two-church")
        tv_church = next(row for row in annotated if row["church"] == "tv-church")
        unknown_platform = next(row for row in annotated if row["church"] == "unknown-platform")
        roku_church = next(row for row in annotated if row["church"] == "roku-church")
        self.assertTrue(one_church["is_outdated"])
        self.assertEqual(one_church["latest_apollos_version"], "101")
        self.assertEqual(one_church["version_source_label"], "Runtime")
        self.assertFalse(two_church["is_outdated"])
        self.assertFalse(tv_church["is_outdated"])
        self.assertFalse(unknown_platform["is_outdated"])
        self.assertEqual(unknown_platform["latest_apollos_version"], "8.2.13")
        self.assertFalse(roku_church["is_outdated"])
        self.assertEqual(roku_church["version_source_label"], "Analytics library")
        self.assertEqual(roku_church["version_status_label"], "Observed")
        self.assertEqual(annotated[0]["church"], "one-church")

    def test_builds_platform_tabs_with_outdated_counts(self):
        rows = [
            {"apollos_platform": "ios", "is_outdated": True},
            {"apollos_platform": "ios", "is_outdated": False},
            {"apollos_platform": "android", "is_outdated": False},
            {"apollos_platform": "androidtv", "is_outdated": False},
            {"apollos_platform": "unknown", "is_outdated": False},
        ]

        tabs = app_versions.build_platform_tabs(rows)

        self.assertEqual(
            [tab["key"] for tab in tabs],
            ["all", "android", "androidtv", "ios", "unknown"],
        )
        self.assertEqual(tabs[0]["row_count"], 5)
        self.assertEqual(tabs[0]["outdated_count"], 1)
        ios_tab = next(tab for tab in tabs if tab["key"] == "ios")
        self.assertEqual(ios_tab["label"], "iOS")
        self.assertEqual(ios_tab["row_count"], 2)
        self.assertEqual(ios_tab["outdated_count"], 1)
        androidtv_tab = next(tab for tab in tabs if tab["key"] == "androidtv")
        self.assertEqual(androidtv_tab["label"], "AndroidTV")

    def test_builds_query_from_discovered_segment_columns(self):
        config = app_versions.AppVersionsConfig(
            project_id="analytics-project",
            datasets=("apollos", "apollos_tv"),
            tables=("tracks", "identifies"),
            lookback_days=14,
            limit=50,
        )
        schema = {
            ("apollos", "tracks"): {
                "timestamp": "timestamp",
                "church": "church",
                "apollos_version": "apollos_version",
                "app_version": "app_version",
                "bundle_id": "bundle_id",
                "application_name": "application_name",
                "apollos_platform": "apollos_platform",
            },
            ("apollos_tv", "identifies"): {
                "received_at": "received_at",
                "groupid": "groupId",
                "apollosversion": "apollosVersion",
                "appversion": "appVersion",
            },
            ("apollos_roku", "identifies"): {
                "timestamp": "timestamp",
                "church": "church",
                "apollosplatform": "apollosplatform",
                "context_library_version": "context_library_version",
            },
        }

        with patch.object(app_versions, "_query_job_config", side_effect=lambda params: params):
            with patch.object(
                app_versions,
                "_scalar_query_parameter",
                side_effect=lambda name, field_type, value: (name, field_type, value),
            ):
                query, query_config = app_versions._build_app_versions_query(config, schema)

        self.assertIn("`analytics-project.apollos.tracks`", query)
        self.assertIn("`analytics-project.apollos_tv.identifies`", query)
        self.assertIn("`analytics-project.apollos_roku.identifies`", query)
        self.assertIn("NULLIF(CAST(`apollos_version` AS STRING), '') AS apollos_version", query)
        self.assertIn("NULLIF(CAST(`apollosVersion` AS STRING), '') AS apollos_version", query)
        self.assertIn(
            "NULLIF(CAST(`context_library_version` AS STRING), '') AS apollos_version",
            query,
        )
        self.assertIn("NULLIF(CAST(`groupId` AS STRING), '') AS church", query)
        self.assertIn("'analytics_library' AS version_source", query)
        self.assertIn("TIMESTAMP_SUB(", query)
        self.assertIn("INTERVAL @lookback_days DAY", query)
        self.assertEqual(
            query_config,
            [("lookback_days", "INT64", 14), ("limit", "INT64", 50)],
        )


class AppVersionsRouteTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_app_versions_route_honors_forwarded_prefix_for_links(self):
        context = {
            "status": "unavailable",
            "status_label": "Unavailable",
            "error_message": "Unable to query BigQuery analytics data.",
            "rows": [],
            "platform_tabs": [],
            "lookback_days": 30,
            "configured_datasets": ("apollos", "apollos_tv"),
            "configured_tables": ("identifies", "screens", "app_became_active"),
        }

        with patch.object(app_module, "get_app_versions_context", return_value=context):
            response = self.client.get("/app-versions", headers={"X-Forwarded-Prefix": "/grid"})

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("App Versions", body)
        self.assertIn("App version data is unavailable", body)
        self.assertIn('href="/grid/app-versions"', body)
        self.assertIn('href="/grid/team"', body)

    def test_app_versions_route_renders_platform_tabs(self):
        rows = [
            {
                "church": "one-church",
                "bundle_id": "com.one",
                "application_name": "One Church",
                "app_version": "1.0.0",
                "apollos_platform": "ios",
                "apollos_version": "97",
                "latest_apollos_version": "101",
                "is_outdated": True,
                "latest_seen_display": "2026-05-12 10:00 AM EDT",
                "user_count": 5,
                "event_count": 10,
            },
            {
                "church": "two-church",
                "bundle_id": "com.two",
                "application_name": "Two Church",
                "app_version": "1.0.0",
                "apollos_platform": "android",
                "apollos_version": "97",
                "latest_apollos_version": "97",
                "is_outdated": False,
                "latest_seen_display": "2026-05-12 10:00 AM EDT",
                "user_count": 7,
                "event_count": 12,
            },
        ]
        context = {
            "status": "ready",
            "status_label": "Ready",
            "rows": rows,
            "platform_tabs": app_versions.build_platform_tabs(rows),
            "lookback_days": 30,
            "configured_datasets": ("apollos", "apollos_tv"),
            "configured_tables": ("apollos.identifies", "apollos_tv.identifies"),
        }

        with patch.object(app_module, "get_app_versions_context", return_value=context):
            response = self.client.get("/app-versions")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('role="tablist"', body)
        self.assertIn('data-version-tab="ios"', body)
        self.assertIn('data-version-tab="android"', body)
        self.assertIn('id="version-panel-ios"', body)
        self.assertIn("One Church", body)
        self.assertIn("Two Church", body)


if __name__ == "__main__":
    unittest.main()
