import unittest
from unittest.mock import Mock, patch

import mobile_releases


class MobileReleasesTest(unittest.TestCase):
    def test_android_uses_published_lifecycle_not_latest_production_upload(self):
        payload = {
            "releases": [
                {
                    "track": "production",
                    "releaseLifecycleState": state,
                    "activeArtifacts": [{"versionCode": code}],
                }
                for state, code in (
                    ("RELEASE_LIFECYCLE_STATE_IN_REVIEW", 46),
                    ("RELEASE_LIFECYCLE_STATE_APPROVED_NOT_PUBLISHED", 39),
                    ("RELEASE_LIFECYCLE_STATE_PUBLISHED", 35),
                    ("RELEASE_LIFECYCLE_STATE_PUBLISHED", 34),
                    ("RELEASE_LIFECYCLE_STATE_DRAFT", 47),
                )
            ]
        }
        payload["releases"].append(
            {
                "track": "internal",
                "releaseLifecycleState": "RELEASE_LIFECYCLE_STATE_PUBLISHED",
                "activeArtifacts": [{"versionCode": 99}],
            }
        )
        self.assertEqual(
            mobile_releases._published_android_builds(payload, "production"),
            [
                {"native_build": "35"},
                {"native_build": "34"},
            ],
        )

    def test_google_looks_up_the_platform_track_and_excludes_unpublished_builds(self):
        releases = [
            {
                "track": track,
                "releaseLifecycleState": f"RELEASE_LIFECYCLE_STATE_{state}",
                "activeArtifacts": [{"versionCode": code}],
            }
            for track, state, code in (
                ("production", "PUBLISHED", 112),
                ("tv:production", "PUBLISHED", 2),
                ("tv:production", "IN_REVIEW", 3),
                ("tv:production", "APPROVED_NOT_PUBLISHED", 4),
                ("tv:internal", "PUBLISHED", 5),
            )
        ]
        for platform, track, code in (
            ("android", "production", "112"),
            ("androidtv", "tv:production", "2"),
        ):
            with (
                self.subTest(platform=platform),
                patch.object(mobile_releases, "_config", side_effect=["com.church", "e30="]),
                patch("google.oauth2.service_account.Credentials.from_service_account_info"),
                patch("google.auth.transport.requests.AuthorizedSession") as session_class,
            ):
                session = session_class.return_value.__enter__.return_value
                session.get.return_value.json.return_value = {"releases": releases}
                result = mobile_releases._fetch_release("church", platform, "com.church")
                self.assertEqual(result["builds"], [{"native_build": code}])
                self.assertEqual(
                    session.get.call_args.args[0],
                    f"{mobile_releases.GOOGLE_API_URL}/com.church/tracks/{track}/releases",
                )

    def test_google_quota_errors_are_safe_and_permission_errors_remain_failures(self):
        for status, message, quota in (
            (403, "Listing releases quota exceeded.", True),
            (429, "secret", True),
            (403, "Permission denied: secret", False),
        ):
            with (
                self.subTest(status=status, message=message),
                patch.object(mobile_releases, "_config", side_effect=["com.church", "e30="]),
                patch("google.oauth2.service_account.Credentials.from_service_account_info"),
                patch("google.auth.transport.requests.AuthorizedSession") as session_class,
                self.assertLogs(level="WARNING") as logs,
            ):
                response = session_class.return_value.__enter__.return_value.get.return_value
                response.status_code = status
                response.json.return_value = {"error": {"message": message}}
                response.raise_for_status.side_effect = mobile_releases.requests.HTTPError("secret")
                result = mobile_releases._fetch_release("church", "android", "com.church")
                self.assertEqual(
                    result,
                    {"live_status_detail": mobile_releases.STORE_QUOTA_DETAIL} if quota else None,
                )
                self.assertNotIn("secret", " ".join(logs.output))

    def test_shared_cache_reuses_success_and_quota_and_refetches_after_expiry(self):
        for release, ttl in (
            ({"builds": [{"native_build": "123"}], "checked_at": 1}, 1800),
            ({"builds": []}, 1800),
            ({"live_status_detail": mobile_releases.STORE_QUOTA_DETAIL}, 3600),
        ):
            with (
                self.subTest(release=release),
                patch.object(mobile_releases, "_get_redis_client") as redis,
                patch.object(mobile_releases, "_fetch_release", return_value=release) as fetch,
            ):
                client = redis.return_value
                client.get.return_value = None
                self.assertEqual(
                    mobile_releases._lookup_release("android", "com.preview", {"preview"}),
                    release,
                )
                key, seconds, raw = client.setex.call_args.args
                self.assertEqual(key, "apps:store-release:v1:android:com.preview")
                self.assertEqual(seconds, ttl)
                client.get.return_value = raw
                fetch.reset_mock()
                self.assertEqual(
                    mobile_releases._lookup_release("android", "com.preview", {"preview"}),
                    release,
                )
                fetch.assert_not_called()
                client.get.return_value = None  # Redis TTL expired; never reuse old builds.
                mobile_releases._lookup_release("android", "com.preview", {"preview"})
                fetch.assert_called_once()
                mobile_releases._lookup_release("androidtv", "com.preview", {"preview"})
                self.assertEqual(
                    client.get.call_args.args[0], "apps:store-release:v1:androidtv:com.preview"
                )

    def test_cache_failures_fall_back_without_caching_mismatches_or_logging_secrets(self):
        with (
            patch.object(mobile_releases, "_get_redis_client") as redis,
            patch.object(
                mobile_releases, "_fetch_release", side_effect=[None, {"builds": []}]
            ) as fetch,
            self.assertLogs(level="WARNING") as logs,
        ):
            client = redis.return_value
            client.get.side_effect = ValueError("secret")
            client.setex.side_effect = ValueError("secret")
            self.assertEqual(
                mobile_releases._lookup_release("android", "com.preview", {"one", "two"}),
                {"builds": []},
            )
            self.assertEqual(fetch.call_count, 2)
            client.setex.assert_called_once()
            self.assertNotIn("secret", " ".join(logs.output))
        with (
            patch.object(mobile_releases, "_get_redis_client", return_value=None),
            patch.object(mobile_releases, "_fetch_release", return_value=None),
        ):
            self.assertIsNone(mobile_releases._lookup_release("ios", "com.preview", {"preview"}))

    def test_apple_selects_latest_live_version_and_its_selected_build(self):
        payload = {
            "data": [
                {
                    "attributes": {"versionString": version, "appStoreState": state},
                    "relationships": {"build": {"data": {"id": build}}},
                }
                for version, state, build in (
                    ("1.9", "READY_FOR_SALE", "old"),
                    ("1.10", "READY_FOR_SALE", "live"),
                    ("1.11", "PENDING_DEVELOPER_RELEASE", "pending"),
                )
            ],
            "included": [
                {"type": "builds", "id": "live", "attributes": {"version": "123"}},
            ],
        }
        self.assertEqual(
            mobile_releases._published_apple_builds(payload),
            [
                {"native_build": "123", "native_version": "1.10"},
            ],
        )
        # Apple's live response exposes the legacy filter state and the modern state together.
        payload["data"][1]["attributes"]["appVersionState"] = "READY_FOR_DISTRIBUTION"
        self.assertEqual(
            mobile_releases._published_apple_builds(payload),
            [{"native_build": "123", "native_version": "1.10"}],
        )
        payload["links"] = {"next": "more"}
        with self.assertRaises(ValueError):
            mobile_releases._published_apple_builds(payload)

    def test_apple_requires_one_exact_bundle_match_among_prefix_matches(self):
        app = {"id": "demo", "attributes": {"bundleId": "com.demo"}}
        prefix_match = {"id": "preview", "attributes": {"bundleId": "com.demo.preview"}}
        key = {"key": "unused", "key_id": "unused", "issuer_id": "unused"}
        for apps in ([prefix_match, app], [prefix_match], [app, app]):
            with (
                self.subTest(apps=apps),
                patch.object(mobile_releases, "_config", side_effect=[None, key]),
                patch("google.auth.crypt.es256.ES256Signer.from_string"),
                patch("google.auth.jwt.encode", return_value=b"test-token"),
                patch.object(mobile_releases.requests, "Session") as session_class,
            ):
                session = session_class.return_value.__enter__.return_value
                session.get.side_effect = [
                    Mock(json=Mock(return_value={"data": apps})),
                    Mock(json=Mock(return_value={"data": []})),
                ]
                if apps == [prefix_match, app]:
                    self.assertEqual(mobile_releases._apple_builds("demo", "com.demo"), [])
                    self.assertEqual(
                        session.get.call_args.args[0],
                        f"{mobile_releases.APPLE_API_URL}/apps/demo/appStoreVersions",
                    )
                else:
                    with self.assertRaises(ValueError):
                        mobile_releases._apple_builds("demo", "com.demo")
                    self.assertEqual(session.get.call_count, 1)

    def test_bundle_casing_merges_hints_and_preserves_configured_store_identifier(self):
        bundle = "com.subsplashconsulting.ND38ZC"
        for platform, field, helper in (
            ("ios", "appleBundleId", "_apple_builds"),
            ("android", "androidPkgId", "_android_builds"),
            ("androidtv", "androidPkgId", "_android_builds"),
        ):
            for bundles in ([bundle.lower()], [bundle, bundle.lower()], [bundle.lower(), bundle]):
                for directory in ([], [{"slug": "city_first", field: bundle}]):
                    with (
                        self.subTest(platform=platform, bundles=bundles, directory=directory),
                        patch.dict(mobile_releases.os.environ, {"APOLLOS_API_KEY": "test"}),
                        patch.object(
                            mobile_releases, "_fetch_app_churches", return_value=directory
                        ),
                        patch.object(mobile_releases, "_config", return_value=bundle) as config,
                        patch.object(
                            mobile_releases, helper, return_value=[{"native_build": "123"}]
                        ) as store,
                    ):
                        releases = mobile_releases.fetch_live_mobile_releases(
                            [
                                {
                                    "apollos_platform": platform,
                                    "bundle_id": b,
                                    "church": "city_first",
                                }
                                for b in bundles
                            ]
                        )
                        release = releases[(platform, bundle.lower())]
                        self.assertEqual(release["builds"], [{"native_build": "123"}])
                        self.assertEqual(len(releases), 1)
                        config.assert_called_once()
                        args = ("city_first", bundle)
                        if platform != "ios":
                            args += (mobile_releases.GOOGLE_TRACKS[platform],)
                        store.assert_called_once_with(*args)
                        if directory:
                            self.assertEqual(release["build_church"], "city_first")
                            self.assertEqual(release["deploy_target_count"], 1)

    def test_case_variant_directory_targets_remain_ambiguous(self):
        with (
            patch.dict(mobile_releases.os.environ, {"APOLLOS_API_KEY": "test"}),
            patch.object(
                mobile_releases,
                "_fetch_app_churches",
                return_value=[
                    {"slug": "one", "appleBundleId": "com.App"},
                    {"slug": "two", "appleBundleId": "com.app"},
                ],
            ),
            patch.object(mobile_releases, "_fetch_release", return_value={"builds": []}),
        ):
            release = mobile_releases.fetch_live_mobile_releases(
                [{"apollos_platform": "ios", "bundle_id": "com.app"}]
            )[("ios", "com.app")]
        self.assertIsNone(release["build_church"])
        self.assertEqual(release["deploy_target_count"], 2)

    def test_directory_resolves_build_church_without_crossing_platforms(self):
        rows = [{"apollos_platform": "ios", "bundle_id": "com.preview", "church": "demo"}]
        directory = [
            {"slug": "preview", "appleBundleId": "com.preview"},
            {"slug": "other", "androidPkgId": "com.preview"},
            {"slug": "../invalid", "appleBundleId": "com.preview"},
        ]
        release = {"builds": []}
        with (
            patch.dict(mobile_releases.os.environ, {"APOLLOS_API_KEY": "test"}),
            patch.object(mobile_releases, "_fetch_app_churches", return_value=directory),
            patch.object(mobile_releases, "_fetch_release", side_effect=[None, release]) as fetch,
        ):
            self.assertEqual(
                mobile_releases.fetch_live_mobile_releases(rows),
                {
                    ("ios", "com.preview"): {
                        **release,
                        "build_church": "preview",
                        "deploy_target_count": 1,
                    }
                },
            )
        self.assertEqual(
            [call.args for call in fetch.call_args_list],
            [("demo", "ios", "com.preview"), ("preview", "ios", "com.preview")],
        )

    def test_directory_identity_survives_missing_analytics_slug_and_store_failure(self):
        rows = [{"apollos_platform": "ios", "bundle_id": "com.preview"}]
        for slugs in (["preview", "preview"], ["preview", "duplicate"]):
            with (
                self.subTest(slugs=slugs),
                patch.dict(mobile_releases.os.environ, {"APOLLOS_API_KEY": "test"}),
                patch.object(
                    mobile_releases,
                    "_fetch_app_churches",
                    return_value=[{"slug": slug, "appleBundleId": "com.preview"} for slug in slugs],
                ),
                patch.object(mobile_releases, "_fetch_release", return_value=None),
            ):
                self.assertEqual(
                    mobile_releases.fetch_live_mobile_releases(rows),
                    {
                        ("ios", "com.preview"): {
                            "build_church": "preview" if len(set(slugs)) == 1 else None,
                            "deploy_target_count": len(set(slugs)),
                        }
                    },
                )

    def test_directory_rejects_errors_and_malformed_responses(self):
        directory = [{"slug": "preview", "appleBundleId": "com.preview"}]
        for payload in (
            {"data": {"churches": directory}},
            {"data": {"churches": directory}, "errors": [{"message": "secret"}]},
            {"data": None},
            {"data": {"churches": None}},
            {"data": {"churches": [None]}},
            [],
        ):
            with (
                self.subTest(payload=payload),
                patch.dict(mobile_releases.os.environ, {"APOLLOS_API_KEY": "test"}),
                patch.object(
                    mobile_releases.requests,
                    "post",
                    return_value=Mock(json=Mock(return_value=payload)),
                ),
            ):
                if payload == {"data": {"churches": directory}}:
                    self.assertEqual(mobile_releases._fetch_app_churches(), directory)
                else:
                    with self.assertLogs(level="WARNING") as logs:
                        self.assertEqual(mobile_releases._fetch_app_churches(), [])
                    self.assertNotIn("secret", " ".join(logs.output))

    def test_directory_failure_falls_back_to_observed_church_without_logging_secrets(self):
        rows = [{"apollos_platform": "ios", "bundle_id": "com.demo", "church": "demo"}]
        with (
            patch.dict(mobile_releases.os.environ, {"APOLLOS_API_KEY": "test"}),
            patch.object(mobile_releases.requests, "post", side_effect=ValueError("secret")),
            patch.object(mobile_releases, "_fetch_release", return_value={"builds": []}),
            self.assertLogs(level="WARNING") as logs,
        ):
            self.assertEqual(
                mobile_releases.fetch_live_mobile_releases(rows),
                {("ios", "com.demo"): {"builds": []}},
            )
        self.assertNotIn("secret", " ".join(logs.output))

    def test_lookup_verifies_bundle_before_loading_any_store_credential(self):
        with patch.object(mobile_releases, "_config", return_value="com.other") as config:
            self.assertIsNone(
                mobile_releases._fetch_release("selected_church", "ios", "com.preview")
            )
        config.assert_called_once_with("selected_church", "APP.APPLE_BUNDLE_ID")

    def test_missing_credentials_and_lookup_failure_do_not_claim_live_builds(self):
        rows = [{"apollos_platform": "ios", "bundle_id": "com.preview", "church": "preview"}]
        with patch.dict(mobile_releases.os.environ, {"APOLLOS_API_KEY": ""}):
            self.assertEqual(mobile_releases.fetch_live_mobile_releases(rows), {})
        with patch.object(mobile_releases, "_config", side_effect=ValueError("secret")):
            with self.assertLogs(level="WARNING") as logs:
                self.assertIsNone(mobile_releases._fetch_release("preview", "ios", "com.preview"))
        self.assertNotIn("secret", " ".join(logs.output))


if __name__ == "__main__":
    unittest.main()
