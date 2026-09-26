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
                "church": "bad-runtime",
                "apollos_platform": "ios",
                "application_name": "Bad Runtime",
                "bundle_id": "com.bad",
                "apollos_version": "v999",
                "latest_seen_at": datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc),
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
                "source_version": "v2026.05.01.00-alpha.1",
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
                "source_revision": "ba95e2f5fe554f430d113f79761c1655376b8239",
                "version_source": "analytics_library",
                "latest_seen_at": datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc),
            },
        ]

        selected = app_versions._select_latest_observed_versions(
            rows, {"v2026.05.01.00": "abcdef123456"}
        )
        annotated = app_versions._annotate_version_status(
            selected,
            {
                "roku_revision_statuses": {rows[-1]["source_revision"]: "behind"},
                "roku_target_revision": "deadbeefabcdef0",
            },
        )

        one_church = next(row for row in annotated if row["church"] == "one-church")
        two_church = next(row for row in annotated if row["church"] == "two-church")
        bad_runtime = next(row for row in annotated if row["church"] == "bad-runtime")
        tv_church = next(row for row in annotated if row["church"] == "tv-church")
        old_tv_church = next(row for row in annotated if row["church"] == "old-tv-church")
        new_tv_church = next(row for row in annotated if row["church"] == "new-tv-church")
        unknown_platform = next(row for row in annotated if row["church"] == "unknown-platform")
        roku_church = next(row for row in annotated if row["church"] == "roku-church")
        self.assertTrue(one_church["is_outdated"])
        self.assertEqual(one_church["freshness_display"], "97")
        self.assertEqual(one_church["version_status_label"], "Behind top seen")
        self.assertFalse(two_church["is_outdated"])
        self.assertEqual(bad_runtime["version_status_label"], "Unverified")
        self.assertEqual(tv_church["version_status_label"], "Unverified")
        self.assertTrue(old_tv_church["is_outdated"])
        self.assertEqual(old_tv_church["freshness_display"], "v2026.05.01.00")
        self.assertFalse(new_tv_church["is_outdated"])
        self.assertFalse(unknown_platform["is_outdated"])
        self.assertEqual(unknown_platform["version_status_label"], "Unverified")
        self.assertTrue(roku_church["is_outdated"])
        self.assertEqual(roku_church["version_status_label"], "Behind source")
        self.assertEqual(roku_church["freshness_display"], "ba95e2f")
        self.assertEqual(annotated[0]["church"], "one-church")
        self.assertTrue(app_versions._revisions_match("abcdef123456", "abcdef1"))
        self.assertFalse(app_versions._revisions_match("abcdef123456", "abc"))

    def test_store_lookup_does_not_claim_a_build_is_outdated(self):
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
        self.assertFalse(red_rocks["is_outdated"])

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
        self.assertIsNotNone(bayside["store_checked_display"])
        self.assertEqual(android["latest_app_version"], "1.0.0")
        self.assertEqual(android["latest_app_version_source"], "observed")

    def test_looks_up_all_app_store_bundles(self):
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
            enriched = app_versions._enrich_app_store_versions(rows)

        self.assertTrue(all(row.get("store_checked_display") for row in enriched))
        lookup_bundle_ids = fetch_app_store_versions.call_args.args[0]
        self.assertEqual(len(lookup_bundle_ids), app_versions.APP_STORE_LOOKUP_LIMIT + 1)
        self.assertIn(f"com.example.{app_versions.APP_STORE_LOOKUP_LIMIT}", lookup_bundle_ids)

    def test_batches_app_store_lookup_across_all_bundle_ids(self):
        class Response:
            def __init__(self, payload: dict[str, Any]):
                self.payload = payload

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, Any]:
                return self.payload

        responses = [
            Response({"results": [{"bundleId": "com.example.one", "version": "1.40"}]}),
            Response({"results": [{"bundleId": "com.example.last", "version": "2.3.4"}]}),
        ]
        bundle_ids = (
            ["com.example.one"]
            + [f"com.example.{i}" for i in range(app_versions.APP_STORE_LOOKUP_LIMIT - 1)]
            + ["com.example.last"]
        )

        def response_for_batch(url, *, params, timeout):
            if "com.example.one" in params["bundleId"].split(","):
                return responses[0]
            return responses[1]

        with patch.object(app_versions.requests, "get", side_effect=response_for_batch) as get:
            versions = app_versions._fetch_app_store_versions(bundle_ids)

        self.assertEqual(versions["com.example.one"]["version"], "1.40")
        self.assertEqual(versions["com.example.last"]["version"], "2.3.4")
        self.assertEqual(get.call_count, 2)
        self.assertCountEqual(
            [call.kwargs["params"] for call in get.call_args_list],
            [
                {
                    "bundleId": ",".join(bundle_ids[: app_versions.APP_STORE_LOOKUP_LIMIT]),
                    "country": "us",
                },
                {"bundleId": "com.example.last", "country": "us"},
            ],
        )

    def test_failed_batch_does_not_hide_healthy_app_versions(self):
        class Response:
            def __init__(self, ids):
                self.ids = ids

            def raise_for_status(self):
                pass

            def json(self):
                return {
                    "results": [
                        {"bundleId": bundle_id, "version": "1.40"} for bundle_id in self.ids
                    ]
                }

        def lookup(url, *, params, timeout):
            ids = params["bundleId"].split(",")
            if "com.example.bad" in ids:
                error = app_versions.requests.HTTPError("bad bundle")
                error.response = types.SimpleNamespace(status_code=400)
                raise error
            return Response(ids)

        bundle_ids = ["com.example.bad"] + [
            f"com.example.good{i}" for i in range(app_versions.APP_STORE_LOOKUP_LIMIT)
        ]
        with patch.object(app_versions.requests, "get", side_effect=lookup):
            versions = app_versions._fetch_app_store_versions(bundle_ids)

        self.assertEqual(len(versions), app_versions.APP_STORE_LOOKUP_LIMIT)
        self.assertEqual(versions["com.example.good0"]["version"], "1.40")
        self.assertIn(f"com.example.good{app_versions.APP_STORE_LOOKUP_LIMIT - 1}", versions)
        self.assertNotIn("com.example.bad", versions)

    def test_transient_or_malformed_batch_does_not_retry_every_bundle(self):
        class Response:
            def __init__(self, results):
                self.results = results

            def raise_for_status(self):
                pass

            def json(self):
                return {"results": self.results}

        bundle_ids = [f"com.example.{i}" for i in range(app_versions.APP_STORE_LOOKUP_LIMIT)]
        bundle_ids.append("com.example.last")
        for failure in ("timeout", "malformed"):
            with self.subTest(failure=failure):

                def lookup(url, *, params, timeout):
                    ids = params["bundleId"].split(",")
                    if len(ids) > 1:
                        if failure == "timeout":
                            raise app_versions.requests.RequestException("timeout")
                        return Response({"unexpected": "shape"})
                    return Response([{"bundleId": ids[0], "version": "1.40"}])

                with patch.object(app_versions.requests, "get", side_effect=lookup) as get:
                    versions = app_versions._fetch_app_store_versions(bundle_ids)
                self.assertEqual(get.call_count, 2)
                self.assertEqual(versions["com.example.last"]["version"], "1.40")
                self.assertEqual(len(versions), 1)

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
        rows.append({**rows[0], "apollos_version": "999", "deployment_track": "internal"})
        rows.append({**rows[0], "apollos_version": "v999", "app_version": "1.0.99"})
        rows.append(
            {
                **rows[2],
                "church": "only_bad",
                "bundle_id": "com.apollos.bad",
                "apollos_version": "v999",
            }
        )

        selected = app_versions._select_latest_observed_versions(rows)

        grow_church = next(row for row in selected if row["church"] == "grow_church")
        only_bad = next(row for row in selected if row["church"] == "only_bad")
        self.assertEqual(len(selected), 3)
        self.assertEqual(grow_church["apollos_version"], "97")
        self.assertEqual(grow_church["app_version"], "1.0.31")
        self.assertEqual(
            app_versions._annotate_version_status([only_bad])[0]["version_status_label"],
            "Unverified",
        )

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
            {"apollos_platform": "iOS", "is_outdated": True},
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
        self.assertIn("AS deployment_track", query)
        self.assertIn(
            "NULLIF(CAST(`context_library_version` AS STRING), '') AS apollos_version",
            query,
        )
        self.assertIn("CAST(NULL AS STRING) AS source_revision", query)
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
        self.assertNotIn("app_totals AS", query)
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
                "store_checked_display": "2026-05-12 10:15 AM EDT",
                "apollos_platform": "ios",
                "apollos_version": "97",
                "is_outdated": True,
                "latest_seen_at": "2026-05-12 10:00 AM EDT",
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
                "apollos_platform": "android",
                "apollos_version": "97",
                "is_outdated": False,
                "latest_seen_at": "2026-05-12 10:00 AM EDT",
                "user_count": 7,
                "event_count": 12,
            },
        ]
        rows.append(
            {
                "church": "three-church",
                "bundle_id": "com.three",
                "application_name": "Three Church",
                "app_version": "1.0.2",
                "store_checked_display": "2026-05-12 10:16 AM EDT",
                "apollos_platform": "ios",
                "apollos_version": "101",
            }
        )
        rows = app_versions._annotate_version_status(rows)
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
        self.assertIn("One Church", body)
        self.assertIn("<th>Seen build</th>", body)
        self.assertIn("<th>Apple lookup (US)</th>", body)
        self.assertIn("<th>Expo Runtime</th>", body)
        self.assertIn("Top seen 101", body)
        self.assertIn("<th>Status</th>", body)
        self.assertIn("Checked 2026-05-12 10:15 AM EDT", body)
        self.assertIn("Last seen 2026-05-12 10:00 AM EDT", body)
        self.assertIn("Behind top seen", body)
        unavailable = body[body.index("com.three") : body.index("</tr>", body.index("com.three"))]
        self.assertIn("Not available", unavailable)
        self.assertIn("Checked 2026-05-12 10:16 AM EDT", unavailable)
        self.assertIn("<code>97</code>", body)
        self.assertIn("Two Church", body)
        self.assertNotIn("App Store (live)", body)

    def test_preview_shows_church_slug_and_distinguishes_seen_from_apple_lookup(self):
        row = {
            "church": "apollos_demo",
            "bundle_id": "com.differential.apollospreview",
            "application_name": "Apollos Preview",
            "app_version": "1.0.0",
            "latest_app_version": "1.40",
            "latest_app_version_source": "app_store",
            "store_checked_display": "2026-09-25 08:45 PM EDT",
            "apollos_platform": "ios",
            "apollos_version": "106",
        }
        rows = app_versions._annotate_version_status(
            [
                row,
                {
                    **row,
                    "bundle_id": "com.example.invalid",
                    "application_name": "Bad App",
                    "apollos_version": "v999",
                },
            ]
        )
        context = {
            "status": "ready",
            "rows": rows,
            "platform_tabs": app_versions.build_platform_tabs(rows),
            "lookback_days": 30,
        }
        with patch.object(app_module, "get_app_versions_context", return_value=context):
            body = self.client.get("/apps").get_data(as_text=True)
        self.assertIn("<strong>apollos_demo</strong>", body)
        self.assertIn("Apollos Preview", body)
        self.assertIn("Last seen unknown", body)
        self.assertIn("Checked 2026-09-25 08:45 PM EDT", body)
        self.assertIn("Top seen", body)
        bad_start = body.index("com.example.invalid")
        bad_row = body[bad_start : body.index("</tr>", bad_start)]
        self.assertIn("Comparison unavailable", bad_row)
        self.assertNotIn("Top seen 106", bad_row)
        self.assertIn("1.40", body)
        self.assertNotIn("App Store (live)", body)


if __name__ == "__main__":
    unittest.main()
