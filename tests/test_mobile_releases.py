import unittest
from unittest.mock import patch

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
            mobile_releases._published_android_builds(payload),
            [
                {"native_build": "35"},
                {"native_build": "34"},
            ],
        )

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
