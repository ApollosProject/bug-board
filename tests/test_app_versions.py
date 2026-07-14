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
                "church": "old-tv-church",
                "apollos_platform": "tvos",
                "application_name": "Old TV Church",
                "bundle_id": "com.oldtv",
                "apollos_version": "1.0.0",
                "app_version": "1.0.0",
                "source_version": "v2026.05.01.00",
                "source_revision": "abcdef123456",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc),
            },
            {
                "church": "new-tv-church",
                "apollos_platform": "tvos",
                "application_name": "New TV Church",
                "bundle_id": "com.newtv",
                "apollos_version": "1.0.0",
                "app_version": "1.0.0",
                "source_version": "v2026.05.12.00",
                "source_revision": "123456abcdef",
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
        old_tv_church = next(row for row in annotated if row["church"] == "old-tv-church")
        new_tv_church = next(row for row in annotated if row["church"] == "new-tv-church")
        unknown_platform = next(row for row in annotated if row["church"] == "unknown-platform")
        roku_church = next(row for row in annotated if row["church"] == "roku-church")
        self.assertTrue(one_church["is_outdated"])
        self.assertEqual(one_church["latest_apollos_version"], "101")
        self.assertEqual(one_church["version_source_label"], "Runtime")
        self.assertFalse(two_church["is_outdated"])
        self.assertFalse(tv_church["is_outdated"])
        self.assertTrue(tv_church["is_source_status_tbd"])
        self.assertEqual(tv_church["version_status_label"], "TBD")
        self.assertEqual(tv_church["source_display"], "TBD")
        self.assertTrue(old_tv_church["is_outdated"])
        self.assertTrue(old_tv_church["is_source_outdated"])
        self.assertEqual(old_tv_church["source_display"], "v2026.05.01.00")
        self.assertFalse(new_tv_church["is_outdated"])
        self.assertFalse(unknown_platform["is_outdated"])
        self.assertEqual(unknown_platform["latest_apollos_version"], "8.2.13")
        self.assertFalse(roku_church["is_outdated"])
        self.assertEqual(roku_church["version_source_label"], "Analytics library")
        self.assertEqual(roku_church["version_status_label"], "TBD")
        self.assertEqual(annotated[0]["church"], "one-church")

    def test_annotates_outdated_apps_by_app_store_version(self):
        rows = [
            {
                "church": "bayside",
                "apollos_platform": "ios",
                "application_name": "Bayside",
                "bundle_id": "com.subsplashconsulting.Bayside-Church",
                "apollos_version": "97",
                "app_version": "5.20.18",
                "latest_app_version": "5.20.30",
                "latest_app_version_source": "app_store",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc),
            },
            {
                "church": "red-rocks",
                "apollos_platform": "ios",
                "application_name": "Red Rocks",
                "bundle_id": "com.subsplashconsulting.D4KJF4",
                "apollos_version": "97",
                "app_version": "18.2.22",
                "latest_app_version": "18.2.22",
                "latest_app_version_source": "app_store",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 13, 0, tzinfo=timezone.utc),
            },
        ]

        annotated = app_versions._annotate_version_status(rows)

        bayside = next(row for row in annotated if row["church"] == "bayside")
        red_rocks = next(row for row in annotated if row["church"] == "red-rocks")
        self.assertFalse(bayside["is_outdated"])
        self.assertFalse(bayside["is_runtime_outdated"])
        self.assertTrue(bayside["is_app_version_outdated"])
        self.assertEqual(bayside["latest_app_version_source_label"], "App Store")
        self.assertFalse(red_rocks["is_outdated"])

    def test_prefers_production_runtime_over_newer_internal_runtime(self):
        rows = [
            {
                "church": "preview",
                "apollos_platform": "ios",
                "bundle_id": "com.apollos.preview",
                "apollos_version": "101",
                "app_version": "1.0.0",
                "source_version": "v2026.07.06.00",
                "deployment_track": "production",
            },
            {
                "church": "preview",
                "apollos_platform": "ios",
                "bundle_id": "com.apollos.preview",
                "apollos_version": "102",
                "app_version": "1.0.1",
                "source_version": "v2026.07.13.00-alpha.1",
                "deployment_track": "internal",
            },
        ]

        selected = app_versions._select_latest_observed_versions(rows)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["apollos_version"], "101")
        self.assertEqual(selected[0]["observation_scope"], "production")

    def test_promoted_legacy_alpha_resolves_to_stable_release(self):
        stable_revision = "d84ce358279ff5c020200e4087df7abdcbb6f552"
        rows = [
            {
                "church": "preview",
                "apollos_platform": "androidtv",
                "bundle_id": "com.apollos.preview",
                "source_version": "v2026.07.06.00-alpha.1",
                "source_revision": stable_revision,
                "app_version": "1.0.1",
            },
            {
                "church": "preview",
                "apollos_platform": "androidtv",
                "bundle_id": "com.apollos.preview",
                "source_version": "v2026.07.13.00-alpha.1",
                "source_revision": "74365acc061c543ea0f7a6b89acd38dde8e17d6d",
                "app_version": "1.0.2",
            },
        ]

        selected = app_versions._select_latest_observed_versions(
            rows,
            {"v2026.07.06.00": stable_revision},
        )
        annotated = app_versions._annotate_version_status(
            selected,
            {"stable_release_revisions": {"v2026.07.06.00": stable_revision}},
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["source_version"], "v2026.07.06.00-alpha.1")
        self.assertEqual(selected[0]["canonical_source_version"], "v2026.07.06.00")
        self.assertEqual(annotated[0]["freshness_display"], "v2026.07.06.00")
        self.assertEqual(annotated[0]["version_status_label"], "Current")

    def test_does_not_promote_legacy_alpha_without_matching_revision(self):
        row = {
            "church": "preview",
            "apollos_platform": "androidtv",
            "bundle_id": "com.apollos.preview",
            "source_version": "v2026.07.06.00-alpha.1",
        }

        decorated = app_versions._decorate_observation(
            row,
            {"v2026.07.06.00": "d84ce358279ff5c020200e4087df7abdcbb6f552"},
        )

        self.assertIsNone(decorated["canonical_source_version"])
        self.assertEqual(decorated["observation_scope"], "legacy_internal")

    def test_annotates_roku_against_latest_roku_commit_on_master(self):
        current_revision = "82938ad63d6ba28c5913b366ae53503cdce4992b"
        old_revision = "ba95e2f5fe554f430d113f79761c1655376b8239"
        rows = [
            {
                "church": "current-roku",
                "apollos_platform": "roku",
                "bundle_id": "roku",
                "source_revision": current_revision,
                "source_version": "2.1.5407",
                "version_source": "analytics_library",
            },
            {
                "church": "old-roku",
                "apollos_platform": "roku",
                "bundle_id": "roku",
                "source_revision": old_revision,
                "source_version": "2.0.5384",
                "version_source": "analytics_library",
            },
        ]

        annotated = app_versions._annotate_version_status(
            rows,
            {
                "stable_release_revisions": {},
                "roku_target_revision": current_revision,
                "roku_revision_statuses": {
                    current_revision: "identical",
                    old_revision: "behind",
                },
            },
        )

        current = next(row for row in annotated if row["church"] == "current-roku")
        old = next(row for row in annotated if row["church"] == "old-roku")
        self.assertEqual(current["freshness_display"], "82938ad")
        self.assertEqual(current["version_status_label"], "Current")
        self.assertFalse(current["is_outdated"])
        self.assertEqual(old["freshness_display"], "ba95e2f")
        self.assertEqual(old["version_status_label"], "Outdated")
        self.assertTrue(old["is_outdated"])

    def test_enriches_app_store_versions_by_bundle_id(self):
        rows = [
            {
                "church": "bayside",
                "apollos_platform": "ios",
                "application_name": "Bayside",
                "bundle_id": "com.subsplashconsulting.Bayside-Church",
                "apollos_version": "67",
                "app_version": "5.20.18",
            },
            {
                "church": "android",
                "apollos_platform": "android",
                "application_name": "Android",
                "bundle_id": "com.example.android",
                "apollos_version": "97",
                "app_version": "1.0.0",
            },
        ]

        with patch.object(
            app_versions,
            "_fetch_app_store_versions",
            return_value={
                "com.subsplashconsulting.Bayside-Church": {
                    "bundleId": "com.subsplashconsulting.Bayside-Church",
                    "version": "5.20.30",
                    "currentVersionReleaseDate": "2026-04-14T16:34:35Z",
                    "trackName": "Bayside Church",
                },
            },
        ) as fetch_app_store_versions:
            enriched = app_versions._enrich_app_store_versions(rows)

        fetch_app_store_versions.assert_called_once_with(["com.subsplashconsulting.Bayside-Church"])
        bayside = next(row for row in enriched if row["church"] == "bayside")
        android = next(row for row in enriched if row["church"] == "android")
        self.assertEqual(bayside["latest_app_version"], "5.20.30")
        self.assertEqual(bayside["latest_app_version_source"], "app_store")
        self.assertEqual(bayside["latest_app_version_seen_at"], "2026-04-14T16:34:35Z")
        self.assertEqual(android["latest_app_version"], "1.0.0")
        self.assertEqual(android["latest_app_version_source"], "observed")

    def test_limits_app_store_lookup_count(self):
        rows = [
            {
                "church": f"church-{index}",
                "apollos_platform": "ios",
                "application_name": f"App {index}",
                "bundle_id": f"com.example.{index}",
                "apollos_version": "67",
                "app_version": "1.0.0",
            }
            for index in range(app_versions.APP_STORE_LOOKUP_LIMIT + 1)
        ]

        with patch.object(
            app_versions,
            "_fetch_app_store_versions",
            return_value={},
        ) as fetch_app_store_versions:
            app_versions._enrich_app_store_versions(rows)

        lookup_bundle_ids = fetch_app_store_versions.call_args.args[0]
        self.assertEqual(len(lookup_bundle_ids), app_versions.APP_STORE_LOOKUP_LIMIT)
        self.assertNotIn(
            f"com.example.{app_versions.APP_STORE_LOOKUP_LIMIT}",
            lookup_bundle_ids,
        )

    def test_fetches_each_app_store_bundle_id_individually(self):
        class Response:
            def __init__(self, payload: dict[str, Any]):
                self.payload = payload

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, Any]:
                return self.payload

        responses = [
            Response(
                {
                    "results": [
                        {
                            "bundleId": "com.example.one",
                            "version": "1.2.3",
                        },
                    ],
                }
            ),
            Response(
                {
                    "results": [
                        {
                            "bundleId": "com.example.two",
                            "version": "2.3.4",
                        },
                    ],
                }
            ),
        ]

        with patch.object(
            app_versions.requests,
            "get",
            side_effect=responses,
        ) as get:
            versions = app_versions._fetch_app_store_versions(
                ["com.example.one", "com.example.two"]
            )

        self.assertEqual(versions["com.example.one"]["version"], "1.2.3")
        self.assertEqual(versions["com.example.two"]["version"], "2.3.4")
        self.assertEqual(get.call_count, 2)
        self.assertCountEqual(
            [call.kwargs["params"] for call in get.call_args_list],
            [
                {"bundleId": "com.example.one", "country": "us"},
                {"bundleId": "com.example.two", "country": "us"},
            ],
        )

    def test_selects_highest_observed_version_instead_of_most_recent_event(self):
        rows = [
            {
                "church": "grow_church",
                "apollos_platform": "android",
                "application_name": "Grow Church",
                "bundle_id": "com.apollos.growchurch",
                "apollos_version": "67",
                "app_version": "1.0.13",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc),
                "event_count": 30,
                "user_count": 10,
            },
            {
                "church": "grow_church",
                "apollos_platform": "android",
                "application_name": "Grow Church",
                "bundle_id": "com.apollos.growchurch",
                "apollos_version": "97",
                "app_version": "1.0.31",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc),
                "event_count": 30,
                "user_count": 10,
            },
            {
                "church": "other_church",
                "apollos_platform": "android",
                "application_name": "Other Church",
                "bundle_id": "com.apollos.other",
                "apollos_version": "96",
                "app_version": "2.0.0",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 11, 0, tzinfo=timezone.utc),
                "event_count": 12,
                "user_count": 7,
            },
        ]

        selected = app_versions._select_latest_observed_versions(rows)

        grow_church = next(row for row in selected if row["church"] == "grow_church")
        self.assertEqual(len(selected), 2)
        self.assertEqual(grow_church["apollos_version"], "97")
        self.assertEqual(grow_church["app_version"], "1.0.31")

    def test_app_identity_uses_bundle_when_application_name_changes(self):
        rows = [
            {
                "church": "red_rocks_church",
                "apollos_platform": "ios",
                "application_name": "Red Rocks Church ",
                "bundle_id": "com.subsplashconsulting.D4KJF4",
                "apollos_version": "65",
                "app_version": "18.2.2",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 7, 45, tzinfo=timezone.utc),
                "event_count": 1996,
                "user_count": 580,
            },
            {
                "church": "red_rocks_church",
                "apollos_platform": "ios",
                "application_name": "Red Rocks",
                "bundle_id": "com.subsplashconsulting.D4KJF4",
                "apollos_version": "97",
                "app_version": "18.2.22",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 9, 41, tzinfo=timezone.utc),
                "event_count": 46805,
                "user_count": 3134,
            },
        ]

        selected = app_versions._select_latest_observed_versions(rows)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["application_name"], "Red Rocks")
        self.assertEqual(selected[0]["apollos_version"], "97")
        self.assertEqual(selected[0]["app_version"], "18.2.22")

    def test_app_identity_uses_bundle_when_church_is_missing(self):
        rows = [
            {
                "church": "Unknown church",
                "apollos_platform": "androidtv",
                "application_name": "Apollos Preview",
                "bundle_id": "com.apollos.apollospreview",
                "apollos_version": "1.0.0",
                "app_version": "1.0.20",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 9, 41, tzinfo=timezone.utc),
                "event_count": 579,
                "user_count": 29,
            },
            {
                "church": "apollos_preview",
                "apollos_platform": "androidtv",
                "application_name": "Apollos Preview",
                "bundle_id": "com.apollos.apollospreview",
                "apollos_version": "1.0.0",
                "app_version": "1.0.20",
                "version_source": "runtime",
                "latest_seen_at": datetime(2026, 5, 12, 9, 41, tzinfo=timezone.utc),
                "event_count": 113,
                "user_count": 28,
            },
        ]

        selected = app_versions._select_latest_observed_versions(rows)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["church"], "apollos_preview")
        self.assertEqual(selected[0]["bundle_id"], "com.apollos.apollospreview")

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
            ["android", "androidtv", "ios", "unknown"],
        )
        ios_tab = next(tab for tab in tabs if tab["key"] == "ios")
        self.assertEqual(ios_tab["label"], "iOS")
        self.assertEqual(ios_tab["freshness_column_label"], "Expo Runtime")
        self.assertEqual(ios_tab["row_count"], 2)
        self.assertEqual(ios_tab["outdated_count"], 1)
        androidtv_tab = next(tab for tab in tabs if tab["key"] == "androidtv")
        self.assertEqual(androidtv_tab["label"], "AndroidTV")
        self.assertEqual(androidtv_tab["freshness_column_label"], "Release")

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
                "source_revision": "source_revision",
                "source_version": "source_version",
                "deployment_track": "deployment_track",
                "bundle_id": "bundle_id",
                "application_name": "application_name",
                "apollos_platform": "apollos_platform",
            },
            ("apollos_tv", "identifies"): {
                "received_at": "received_at",
                "groupid": "groupId",
                "apollosversion": "apollosVersion",
                "appversion": "appVersion",
                "sourcerevision": "sourceRevision",
                "sourceversion": "sourceVersion",
                "deploymenttrack": "deploymentTrack",
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
        self.assertIn("NULLIF(CAST(`source_revision` AS STRING), '') AS source_revision", query)
        self.assertIn("NULLIF(CAST(`sourceVersion` AS STRING), '') AS source_version", query)
        self.assertIn(
            "NULLIF(CAST(`deployment_track` AS STRING), '') AS deployment_track",
            query,
        )
        self.assertIn(
            "NULLIF(CAST(`deploymentTrack` AS STRING), '') AS deployment_track",
            query,
        )
        self.assertIn(
            "NULLIF(CAST(`context_library_version` AS STRING), '') AS apollos_version",
            query,
        )
        self.assertIn("CAST(NULL AS STRING) AS source_revision", query)
        self.assertIn("CAST(NULL AS STRING) AS deployment_track", query)
        self.assertIn("NULLIF(CAST(`groupId` AS STRING), '') AS church", query)
        self.assertIn("'analytics_library' AS version_source", query)
        self.assertIn("TIMESTAMP_SUB(", query)
        self.assertIn("INTERVAL @lookback_days DAY", query)
        self.assertIn("filtered_events AS", query)
        self.assertIn("source_dataset = 'apollos_tv'", query)
        self.assertIn("IF(source_dataset = 'apollos_tv', 'tv', NULL)", query)
        self.assertIn("apollos_platform IN ('amazon', 'androidtv', 'tvos', 'tv')", query)
        self.assertIn(
            "apollos_platform NOT IN ('amazon', 'androidtv', 'tvos', 'tv', 'roku')",
            query,
        )
        self.assertIn("app_identity_events AS", query)
        self.assertIn("display_churches AS", query)
        self.assertIn("version_observations AS", query)
        self.assertIn("app_totals AS", query)
        self.assertIn("MAX(events.seen_at) AS latest_seen_at", query)
        self.assertIn("GROUP BY app_identity_key", query)
        self.assertIn("USING (app_identity_key)", query)
        self.assertEqual(
            query_config,
            [("lookback_days", "INT64", 14)],
        )


class AppVersionsRouteTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_apps_route_honors_forwarded_prefix_for_links(self):
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
            response = self.client.get("/apps", headers={"X-Forwarded-Prefix": "/grid"})

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("<h1>Apps</h1>", body)
        self.assertIn("App data is unavailable", body)
        self.assertIn('href="/grid/apps"', body)
        self.assertIn('href="/grid/projects"', body)

    def test_legacy_app_versions_route_renders_apps_dashboard(self):
        with patch.object(
            app_module,
            "get_app_versions_context",
            return_value={
                "status": "ready",
                "status_label": "Ready",
                "rows": [],
                "platform_tabs": [],
                "lookback_days": 30,
                "configured_datasets": ("apollos",),
                "configured_tables": ("apollos.identifies",),
            },
        ):
            response = self.client.get("/app-versions")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("<h1>Apps</h1>", body)
        self.assertIn("No apps found", body)

    def test_apps_route_renders_platform_tabs(self):
        rows = [
            {
                "church": "one-church",
                "bundle_id": "com.one",
                "application_name": "One Church",
                "app_version": "1.0.0",
                "latest_app_version": "1.0.1",
                "latest_app_version_source": "app_store",
                "latest_app_version_source_label": "App Store",
                "apollos_platform": "ios",
                "apollos_version": "97",
                "latest_apollos_version": "101",
                "freshness_display": "97",
                "source_display": "v2026.05.12.00 (abc1234)",
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
                "latest_app_version": "1.0.0",
                "latest_app_version_source": "observed",
                "latest_app_version_source_label": "Observed",
                "apollos_platform": "android",
                "apollos_version": "97",
                "latest_apollos_version": "97",
                "freshness_display": "97",
                "source_display": "TBD",
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
            response = self.client.get("/apps")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("<title>Apps</title>", body)
        self.assertIn('role="tablist"', body)
        self.assertNotIn('data-version-tab="all"', body)
        self.assertIn('data-version-tab="ios"', body)
        self.assertIn('data-version-tab="android"', body)
        self.assertIn('id="version-panel-ios"', body)
        self.assertNotIn("Latest observed", body)
        self.assertIn("One Church", body)
        self.assertNotIn("<th>Latest App</th>", body)
        self.assertNotIn("<th>Observed Version</th>", body)
        self.assertIn("<th>Expo Runtime</th>", body)
        self.assertNotIn("<th>Source</th>", body)
        self.assertIn("1.0.1", body)
        self.assertIn("App Store 1.0.1", body)
        self.assertNotIn("App Store 1.0.0", body)
        self.assertIn("<code>97</code>", body)
        self.assertIn("Two Church", body)
        self.assertNotIn("<th>Platform</th>", body)
        self.assertNotIn("<th>Latest Observed</th>", body)


if __name__ == "__main__":
    unittest.main()
