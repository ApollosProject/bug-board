import base64
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import Mock, patch


def _install_import_shims() -> None:
    dotenv_module = cast(Any, types.ModuleType("dotenv"))
    dotenv_module.load_dotenv = lambda *args, **kwargs: None
    sys.modules.setdefault("dotenv", dotenv_module)

    requests_module = cast(Any, types.ModuleType("requests"))

    class RequestException(Exception):
        pass

    requests_module.RequestException = RequestException
    requests_module.HTTPError = RequestException

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
    def test_redis_requests_never_make_cold_store_calls_and_cache_expires(self):
        client = Mock()
        client.get.return_value = None
        with patch.dict(app_versions.os.environ, {"REDIS_URL": "redis://local-fixture"}):
            with patch("fleet_health_cache._get_redis_client", return_value=client):
                with patch.object(app_versions, "_compute_app_versions_context") as compute:
                    self.assertEqual(
                        app_versions.get_app_versions_context()["status"], "unavailable"
                    )
                    compute.assert_not_called()
                    compute.return_value = {"status": "ready", "rows": [], "platform_tabs": []}
                    app_versions.refresh_app_versions_cache()
                    key, ttl, payload = client.setex.call_args.args
                    self.assertEqual(ttl, 300)
                    client.get.return_value = payload
                    compute.reset_mock()
                    self.assertEqual(app_versions.get_app_versions_context()["status"], "ready")
                    compute.assert_not_called()
                    client.get.assert_called_with(key)

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

    def test_annotates_mobile_against_release_and_tv_against_observations(self):
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
                "mobile_release_runtime": "101",
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
        self.assertEqual(one_church["version_status_label"], "Behind release")
        self.assertFalse(two_church["is_outdated"])
        self.assertEqual(two_church["version_status_label"], "At release")
        self.assertEqual(bad_runtime["version_status_label"], "Unverified")
        self.assertEqual(tv_church["version_status_label"], "Unverified")
        self.assertTrue(old_tv_church["is_outdated"])
        self.assertEqual(old_tv_church["freshness_display"], "v2026.05.01.00")
        self.assertFalse(new_tv_church["is_outdated"])
        self.assertFalse(unknown_platform["is_outdated"])
        self.assertEqual(unknown_platform["version_status_label"], "Unverified")
        self.assertTrue(roku_church["is_outdated"])
        self.assertEqual(roku_church["version_status_label"], "Observed: Behind source")
        for observed in (old_tv_church, new_tv_church, roku_church, unknown_platform):
            self.assertEqual(observed["version_status_class"], "observed")
            self.assertIn("Store publication not verified", observed["live_status_detail"])
        for status, label in (
            ("identical", "Observed: At source"),
            ("ahead", "Observed: Ahead of source"),
        ):
            checked = app_versions._annotate_version_status(
                [rows[-1]], {"roku_revision_statuses": {rows[-1]["source_revision"]: status}}
            )
            self.assertEqual(checked[0]["version_status_label"], label)
            self.assertEqual(checked[0]["version_status_class"], "observed")
        self.assertEqual(roku_church["freshness_display"], "ba95e2f")
        self.assertEqual(annotated[0]["church"], "one-church")
        self.assertTrue(app_versions._revisions_match("abcdef123456", "abcdef1"))
        self.assertFalse(app_versions._revisions_match("abcdef123456", "abc"))

    def test_androidtv_pipeline_matches_the_published_build_not_the_highest_seen_tag(self):
        tv = {
            "church": "cedar_creek",
            "bundle_id": "com.cedarcreek",
            "apollos_platform": "androidtv",
            "native_build": "33",
            "native_version": "1.0.33",
            "apollos_version": "2",
            "source_version": "v2026.08.31.00",
            "deployment_track": "production",
        }
        rows = [
            tv,
            {
                **tv,
                "native_build": "34",
                "native_version": "1.0.34",
                "apollos_version": "3",
                "source_version": "v2026.09.24.00",
            },
            {**tv, "apollos_platform": "android", "apollos_version": "112"},
        ]
        client = Mock()
        client.query.return_value.result.return_value = rows
        context = {
            "stable_release_revisions": {},
            "mobile_release_runtime": "112",
            "tv_release_runtime": "3",
        }
        releases = {
            (platform, "com.cedarcreek"): {"builds": [{"native_build": "33"}]}
            for platform in ("android", "androidtv")
        }
        with (
            patch.object(app_versions, "_build_bigquery_client", return_value=client),
            patch.object(app_versions, "_fetch_segment_schema", return_value={}),
            patch.object(app_versions, "_build_app_versions_query", return_value=("query", None)),
            patch.object(app_versions, "_fetch_platform_source_context", return_value=context),
            patch.object(app_versions, "fetch_live_mobile_releases", return_value=releases),
        ):
            selected, _ = app_versions.fetch_app_versions(app_versions._get_app_versions_config())
        self.assertEqual(len(selected), 2)
        television, mobile = selected
        self.assertEqual(television["apollos_platform"], "androidtv")
        self.assertEqual(television["freshness_display"], "2")
        self.assertEqual(television["comparison_display"], "3")
        self.assertEqual(television["version_status_label"], "Behind release")
        self.assertEqual(mobile["version_status_label"], "At release")
        context["tv_release_runtime"] = None
        self.assertEqual(
            app_versions._annotate_version_status([television], context)[0]["version_status_label"],
            "Unverified",
        )
        unmatched = app_versions._select_live_mobile_versions([tv], {})
        self.assertEqual(
            app_versions._annotate_version_status(unmatched, context)[0]["version_status_label"],
            "Unverified",
        )

    def test_mobile_runtime_matches_the_live_native_build_not_newer_test_builds(self):
        rows = [
            {
                "apollos_platform": platform,
                "bundle_id": "com.church",
                "church": "church",
                "native_version": "1.0",
                "native_build": build,
                "apollos_version": runtime,
                "deployment_track": "internal",
            }
            for platform in ("ios", "android")
            for build, runtime in (("123", "112"), ("124", "114"))
        ]
        releases = {
            (platform, "com.church"): {"builds": [{"native_build": "123", "native_version": "1.0"}]}
            for platform in ("ios", "android")
        }
        selected = app_versions._select_live_mobile_versions(rows, releases)
        self.assertEqual([row["apollos_version"] for row in selected], ["112", "112"])
        # A promoted TestFlight/internal binary is live regardless of its baked-in track label.
        annotated = app_versions._annotate_version_status(
            selected, {"mobile_release_runtime": "112"}
        )
        self.assertTrue(all(row["version_status_label"] == "At release" for row in annotated))
        for platform in ("ios", "android"):
            releases[(platform, "com.church")]["builds"][0]["native_build"] = "missing"
        self.assertTrue(
            all(
                row["apollos_version"] is None
                for row in app_versions._select_live_mobile_versions(rows, releases)
            )
        )

    def test_store_runtime_ignores_events_with_contradictory_app_and_native_versions(self):
        for platform in ("ios", "android", "androidtv"):
            with self.subTest(platform=platform):
                current = {
                    "apollos_platform": platform,
                    "bundle_id": "com.church",
                    "native_build": "123",
                    "native_version": "1.1",
                    "app_version": "1.1",
                    "apollos_version": "104",
                }
                stale = {**current, "app_version": "1.0", "apollos_version": "103"}
                releases = {
                    (platform, "com.church"): {
                        "builds": [{"native_build": "123", "native_version": "1.1"}]
                    }
                }
                selected = app_versions._select_live_mobile_versions([current, stale], releases)
                self.assertEqual(selected[0]["apollos_version"], "104")
                unmatched = app_versions._select_live_mobile_versions([stale], releases)
                self.assertIsNone(unmatched[0]["apollos_version"])
                self.assertEqual(
                    unmatched[0]["live_status_detail"], "Live build has no unique runtime match"
                )

    def test_mobile_does_not_guess_from_marketing_version_or_conflicting_runtimes(self):
        row = {
            "apollos_platform": "ios",
            "bundle_id": "com.church",
            "church": "church",
            "native_version": "1.0",
            "native_build": "123",
            "apollos_version": "112",
        }
        release = {
            ("ios", "com.church"): {"builds": [{"native_build": "123", "native_version": "1.0"}]}
        }
        for observations in (
            [{**row, "native_build": None}],
            [{**row, "native_version": "0.9"}],
            [row, {**row, "apollos_version": "114"}],
            [row, {**row, "apollos_version": "invalid"}],
        ):
            with self.subTest(observations=observations):
                selected = app_versions._select_live_mobile_versions(observations, release)
                self.assertIsNone(selected[0]["apollos_version"])
                annotated = app_versions._annotate_version_status(
                    selected, {"mobile_release_runtime": "112"}
                )
                self.assertEqual(annotated[0]["version_status_label"], "Unverified")
                self.assertEqual(annotated[0]["freshness_display"], "Unknown")
        self.assertIsNone(
            app_versions._select_live_mobile_versions([row], {})[0]["apollos_version"]
        )

    def test_multiple_published_android_runtimes_are_not_claimed_fully_current(self):
        rows = [
            {
                "apollos_platform": "android",
                "bundle_id": "com.church",
                "church": "church",
                "native_build": build,
                "apollos_version": runtime,
            }
            for build, runtime in (("123", "104"), ("124", "112"))
        ]
        release = {
            ("android", "com.church"): {
                "builds": [{"native_build": "123"}, {"native_build": "124"}]
            }
        }
        selected = app_versions._select_live_mobile_versions(rows, release)
        annotated = app_versions._annotate_version_status(
            selected, {"mobile_release_runtime": "112"}
        )
        self.assertEqual(annotated[0]["freshness_display"], "104, 112")
        self.assertEqual(annotated[0]["version_status_label"], "Multiple live runtimes")
        self.assertEqual(annotated[0]["version_status_class"], "observed")

    def test_mobile_compares_to_release_not_highest_observed(self):
        rows = [
            {"apollos_platform": "ios", "apollos_version": version, "church": version}
            for version in ("111", "112", "114")
        ]
        annotated = app_versions._annotate_version_status(rows, {"mobile_release_runtime": "112"})
        self.assertEqual(
            {row["church"]: row["version_status_label"] for row in annotated},
            {"111": "Behind release", "112": "At release", "114": "Ahead of release"},
        )
        self.assertEqual(annotated[0]["comparison_display"], "112")
        self.assertTrue(annotated[0]["is_outdated"])
        self.assertTrue(all(not row["is_outdated"] for row in annotated[1:]))
        unavailable = app_versions._annotate_version_status(rows)
        self.assertTrue(all(row["version_status_label"] == "Unverified" for row in unavailable))

    def test_source_context_uses_latest_stable_tag_runtime(self):
        def github(path, params=None):
            if path == "tags":
                return [
                    {"name": name, "commit": {"sha": name}}
                    for name in ("v2026.09.25.00-alpha.1", "v2026.09.24.00", "v2026.09.01.00")
                ]
            if path in (
                "contents/templates/mobile/app.config.ts",
                "contents/templates/tv/app.config.ts",
            ):
                self.assertEqual(params, {"ref": "v2026.09.24.00"})
                runtime = "3" if "/tv/" in path else "112"
                return {
                    "encoding": "base64",
                    "content": base64.b64encode(
                        f"  runtimeVersion: '{runtime}',".encode()
                    ).decode(),
                }
            return []

        with patch.object(app_versions, "_github_json", side_effect=github):
            context = app_versions._fetch_platform_source_context()
        self.assertEqual(context["mobile_release_runtime"], "112")
        self.assertEqual(context["tv_release_runtime"], "3")
        with patch.object(app_versions, "_github_json", return_value=None):
            self.assertIsNone(
                app_versions._fetch_platform_source_context()["mobile_release_runtime"]
            )

    def test_store_versions_do_not_determine_mobile_status(self):
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
        self.assertTrue(androidtv_tab["uses_store_runtime"])
        self.assertFalse(tabs[-1]["uses_store_runtime"])

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
                "buildchurch": "buildChurch",
                "apollos_version": "apollos_version",
                "app_version": "app_version",
                "context_app_build": "context_app_build",
                "context_app_version": "context_app_version",
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
        self.assertIn("CAST(`context_app_build` AS STRING)", query)
        self.assertIn("observation.native_build", query)
        self.assertIn("observation.native_version", query)
        self.assertIn("CAST(NULL AS STRING) AS native_build", query)
        self.assertIn(
            "NULLIF(CAST(`context_library_version` AS STRING), '') AS apollos_version",
            query,
        )
        self.assertIn("CAST(NULL AS STRING) AS source_revision", query)
        self.assertIn("NULLIF(CAST(`groupId` AS STRING), '') AS church", query)
        self.assertIn("NULLIF(CAST(`buildChurch` AS STRING), '') AS build_church", query)
        self.assertIn("[SAFE_OFFSET(0)] AS church", query)
        self.assertIn("[SAFE_OFFSET(0)] AS build_church", query)
        self.assertIn("observation.build_church", query)
        self.assertIn("COUNT(DISTINCT build_church)", query)
        self.assertIn("COUNT(DISTINCT IF(apollos_version IS NOT NULL, church, NULL))", query)
        self.assertIn("observation.deploy_target_count", query)
        self.assertNotIn("AND `apollos_version` IS NOT NULL", query)
        self.assertIn("WHERE apollos_version IS NOT NULL OR build_church IS NOT NULL", query)
        self.assertIn("IF(apollos_version IS NOT NULL, church, NULL) IGNORE NULLS", query)
        self.assertIn("WHERE events.apollos_version IS NOT NULL", query)
        self.assertIn("'analytics_library' AS version_source", query)
        self.assertIn("TIMESTAMP_SUB(", query)
        self.assertIn("INTERVAL @lookback_days DAY", query)
        self.assertIn("filtered_events AS", query)
        self.assertIn("source_dataset = 'apollos_tv'", query)
        self.assertIn("IF(source_dataset = 'apollos_tv', 'tv', NULL)", query)
        self.assertIn("LOWER(COALESCE(\n              NULLIF(apollos_platform, '')", query)
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
        self.original_config = dict(app_module.app.config)
        app_module.app.config.update(
            GITHUB_OAUTH_CLIENT_ID="test-client",
            GITHUB_OAUTH_CLIENT_SECRET="test-secret",
            GITHUB_OAUTH_CALLBACK_URL="http://localhost/auth/github/callback",
            GITHUB_OAUTH_ORG="ApollosProject",
            SECRET_KEY="test-secret-key-at-least-32-characters",
        )

    def tearDown(self):
        app_module.app.config.clear()
        app_module.app.config.update(self.original_config)

    def test_deploy_requires_auth_and_csrf_and_uses_cached_build_slug(self):
        path = "/apps/deploy/ios/com.preview/apollos_demo"
        row = {
            "apollos_platform": "ios",
            "bundle_id": "com.preview",
            "church": "apollos_demo",
            "build_church": "apollos_preview",
            "deploy_target_count": 1,
        }
        context = {"status": "ready", "rows": [row], "platform_tabs": [], "lookback_days": 30}
        with patch.object(app_module, "get_app_versions_context", return_value=context):
            self.assertEqual(self.client.post(path).status_code, 403)
            app_module.app.config["GITHUB_OAUTH_ENABLED"] = True
            with self.client.session_transaction() as session:
                session.update(
                    github_login="engineer", github_user_id=42, github_org="ApollosProject"
                )
            with patch.object(app_module, "dispatch_app_deploy") as dispatch:
                self.assertEqual(self.client.post(path).status_code, 403)
                self.assertEqual(self.client.get("/apps").status_code, 200)
                with self.client.session_transaction() as session:
                    csrf = session["app_deploy_csrf"]
                self.assertEqual(self.client.post(path, data={"csrf": "invalid"}).status_code, 403)
                for invalid_path in (
                    "/apps/deploy/ios/com.other/apollos_demo",
                    "/apps/deploy/tv/com.preview/apollos_demo",
                ):
                    self.assertEqual(
                        self.client.post(invalid_path, data={"csrf": csrf}).status_code, 404
                    )
                context["rows"].append({**row, "church": "other_church"})
                self.assertEqual(self.client.post(path, data={"csrf": csrf}).status_code, 404)
                context["rows"].pop()
                row["deploy_target_count"] = 2
                self.assertEqual(self.client.post(path, data={"csrf": csrf}).status_code, 404)
                row["deploy_target_count"] = 1
                dispatch.assert_not_called()
                response = self.client.post(path, data={"csrf": csrf})
                self.assertEqual(response.status_code, 303)
                dispatch.assert_called_once_with("apollos_preview", "ios")
                self.assertIn("Deployment started", self.client.get("/apps").get_data(as_text=True))
            app_module.app.config["GITHUB_OAUTH_ENABLED"] = False
            self.assertEqual(self.client.post(path, data={"csrf": csrf}).status_code, 403)

    def test_directory_target_restores_cached_button_and_dispatches_correct_church(self):
        observation = {
            "apollos_platform": "ios",
            "bundle_id": "com.preview",
            "church": "selected_church",
            "deploy_target_count": 7,
            "apollos_version": "112",
        }
        app_module.app.config["GITHUB_OAUTH_ENABLED"] = True
        with self.client.session_transaction() as session:
            session.update(github_login="engineer", github_user_id=42, github_org="ApollosProject")
        for slugs in (["preview"], ["preview", "duplicate"], []):
            observation["deploy_target_count"] = 1 if len(slugs) > 1 else 7
            with (
                self.subTest(slugs=slugs),
                patch.dict(app_versions.os.environ, {"APOLLOS_API_KEY": "test"}),
                patch(
                    "mobile_releases._fetch_app_churches",
                    return_value=[{"slug": slug, "appleBundleId": "com.preview"} for slug in slugs],
                ),
                patch("mobile_releases._fetch_release", return_value=None),
            ):
                releases = app_versions.fetch_live_mobile_releases([observation])
                rows = app_versions._annotate_version_status(
                    app_versions._select_live_mobile_versions([observation], releases)
                )
                context = json.loads(json.dumps({"status": "ready", "rows": rows}))
                with (
                    patch.object(app_module, "get_app_versions_context", return_value=context),
                    patch.object(app_module, "dispatch_app_deploy") as dispatch,
                ):
                    body = self.client.get("/apps").get_data(as_text=True)
                    self.assertIn("Unverified", body)  # Store failure doesn't block deployment.
                    with self.client.session_transaction() as session:
                        csrf = session["app_deploy_csrf"]
                    path = "/apps/deploy/ios/com.preview/selected_church"
                    response = self.client.post(path, data={"csrf": csrf})
                    if len(slugs) == 1:
                        self.assertIn(f'action="{path}"', body)
                        self.assertEqual(response.status_code, 303)
                        dispatch.assert_called_once_with("preview", "ios")
                    else:
                        self.assertNotIn('action="/apps/deploy/', body)
                        self.assertEqual(response.status_code, 404)
                        dispatch.assert_not_called()
        self.assertEqual(observation["deploy_target_count"], 7)

    def test_dispatch_uses_stable_release_and_production_track(self):
        tags = Mock()
        tags.json.return_value = [
            {"name": "v2026.09.27.00-alpha.1"},
            {"name": "v2026.09.26.00"},
        ]
        with patch.dict(app_versions.os.environ, {"GITHUB_ACTIONS_TOKEN": "test-token"}):
            with patch.object(app_versions.requests, "get", return_value=tags) as get:
                with patch.object(app_versions.requests, "post", create=True) as post:
                    post.return_value.status_code = 204
                    app_versions.dispatch_app_deploy("church_one", "roku")
        get.assert_called_once()
        self.assertEqual(
            post.call_args.kwargs["json"],
            {
                "ref": "v2026.09.26.00",
                "inputs": {"church": "church_one", "platform": "roku", "track": "production"},
            },
        )

    def test_dispatch_searches_all_tag_pages(self):
        first = Mock()
        first.json.return_value = [{"name": "v2026.09.26.00"}] + [
            {"name": f"v2026.09.27.{i:02d}-alpha.1"} for i in range(99)
        ]
        second = Mock()
        second.json.return_value = [{"name": "v2026.09.28.00"}]
        with (
            patch.dict(app_versions.os.environ, {"GITHUB_ACTIONS_TOKEN": "test-token"}),
            patch.object(app_versions.requests, "get", side_effect=[first, second]) as get,
            patch.object(app_versions.requests, "post", create=True) as post,
        ):
            post.return_value.status_code = 204
            app_versions.dispatch_app_deploy("church_one", "ios")
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args_list[1].kwargs["params"]["page"], 2)
        self.assertEqual(post.call_args.kwargs["json"]["ref"], "v2026.09.28.00")

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
        self.assertIn("com.one", body)
        self.assertIn("<th>App</th>", body)
        self.assertIn("<th>Live runtime</th>", body)
        self.assertIn("<th>Status</th>", body)
        for label in ("App", "Live runtime", "Status"):
            self.assertEqual(
                body.count(f'<span class="version-cell-label version-muted">{label}</span>'), 3
            )
        self.assertNotIn("<td>\n                  <td>", body)
        self.assertNotIn("Apple lookup", body)
        self.assertNotIn("<th>Seen build</th>", body)
        self.assertIn("<code>97</code>", body)
        self.assertIn("Two Church", body)

    def test_observation_only_platforms_never_render_as_verified_current(self):
        observations = [
            {
                "apollos_platform": platform,
                "church": "church",
                "apollos_version": "3",
                "source_version": "v2026.09.24.00",
                "source_revision": "abcdef123456",
            }
            for platform in ("amazon", "tvos", "tv", "roku", "unknown")
        ]
        rows = app_versions._annotate_version_status(
            app_versions._select_latest_observed_versions(observations),
            {"roku_revision_statuses": {"abcdef123456": "identical"}},
        )
        with patch.object(
            app_module,
            "get_app_versions_context",
            return_value={
                "status": "ready",
                "rows": rows,
                "platform_tabs": app_versions.build_platform_tabs(rows),
                "lookback_days": 30,
            },
        ):
            body = self.client.get("/apps").get_data(as_text=True)
        self.assertNotIn("version-status--current", body.split('<main class="container">')[1])
        self.assertEqual(
            body.count("Store publication not verified; analytics observation only."), 5
        )
        self.assertIn("Observed: Top seen", body)
        self.assertIn("Observed: At source", body)
        self.assertIn("<th>Observed version</th>", body)
        self.assertNotIn("<th>Live runtime</th>", body)

    def test_deploy_button_only_for_unique_valid_targets(self):
        rows = [
            {
                "apollos_platform": "ios",
                "bundle_id": "com.unknown",
                "church": "Unknown church",
                "deploy_target_count": 1,
            },
            {
                "apollos_platform": "ios",
                "bundle_id": "com.duplicate",
                "church": "church_one",
                "deploy_target_count": 1,
            },
            {
                "apollos_platform": "ios",
                "bundle_id": "com.duplicate",
                "church": "church_two",
                "deploy_target_count": 1,
            },
            {
                "apollos_platform": "ios",
                "bundle_id": "com.ambiguous",
                "church": "church_one",
                "build_church": "slug_one",
                "deploy_target_count": 2,
            },
            {
                "apollos_platform": "ios",
                "bundle_id": "com.stale",
                "church": "church_one",
                "build_church": "slug_one",
            },
        ]
        context = {
            "status": "ready",
            "rows": rows,
            "platform_tabs": app_versions.build_platform_tabs(rows),
            "lookback_days": 30,
        }
        app_module.app.config["GITHUB_OAUTH_ENABLED"] = True
        with self.client.session_transaction() as session:
            session.update(github_login="engineer", github_user_id=42, github_org="ApollosProject")
        with (
            patch.object(app_module, "get_app_versions_context", return_value=context),
            patch.object(
                app_versions, "_app_identity_key", wraps=app_versions._app_identity_key
            ) as identity,
        ):
            body = self.client.get("/apps").get_data(as_text=True)
        self.assertEqual(identity.call_count, len(rows))
        self.assertNotIn('action="/apps/deploy/', body)

    def test_preview_shows_build_slug_without_extra_build_columns(self):
        row = {
            "church": "apollos_demo",
            "build_church": "apollos_preview",
            "deploy_target_count": 1,
            "bundle_id": "com.differential.apollospreview",
            "application_name": "Apollos Preview",
            "app_version": "1.0.0",
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
        context = json.loads(json.dumps(context))  # Redis cache separates row and tab objects.
        self.assertIsNot(context["rows"][0], context["platform_tabs"][0]["rows"][0])
        app_module.app.config["GITHUB_OAUTH_ENABLED"] = True
        with self.client.session_transaction() as session:
            session.update(github_login="engineer", github_user_id=42, github_org="ApollosProject")
        with patch.object(app_module, "get_app_versions_context", return_value=context):
            body = self.client.get("/apps").get_data(as_text=True)
        self.assertIn(
            'action="/apps/deploy/ios/com.differential.apollospreview/apollos_demo"',
            body,
        )
        self.assertIn("button.version-deploy[type=submit] {\n    width: auto;", body)
        self.assertIn('class="secondary outline version-deploy"', body)
        self.assertIn(
            'aria-label="Deploy iOS for apollos_preview (com.differential.apollospreview)"',
            body,
        )
        self.assertIn(">Deploy iOS</button>", body)
        app_module.app.config["GITHUB_OAUTH_ENABLED"] = False
        with patch.object(app_module, "get_app_versions_context", return_value=context):
            self.assertNotIn(
                'action="/apps/deploy/', self.client.get("/apps").get_data(as_text=True)
            )
        self.assertIn("apollos_preview", body)
        self.assertIn("Apollos Preview", body)
        self.assertNotIn("Checked 2026-09-25", body)
        self.assertNotIn("Top seen", body)
        self.assertIn("Unverified", body)
        self.assertNotIn("1.40", body)


if __name__ == "__main__":
    unittest.main()
