import unittest
from unittest.mock import MagicMock, patch

import app as app_module
import regression_cache


def _summary():
    return {
        "days": 30,
        "configured": True,
        "complete": True,
        "regression_count": 4,
        "attributed_count": 3,
        "author_regression_rate": 2.5,
        "authored_regression_count": 2,
        "authored_pr_count": 80,
        "reviewer_escape_rate": 1.2,
        "approved_regression_count": 1,
        "reviewed_pr_count": 84,
        "author_metrics": [
            {
                "slug": "alice",
                "github_username": "alice-gh",
                "regression_count": 2,
                "pr_count": 20,
                "rate": 10.0,
            }
        ],
        "reviewer_metrics": [
            {
                "slug": "alice",
                "github_username": "alice-gh",
                "regression_count": 1,
                "pr_count": 25,
                "rate": 4.0,
            }
        ],
    }


class RegressionCacheTest(unittest.TestCase):
    def test_cache_roundtrip(self):
        values: dict[str, str] = {}
        client = MagicMock()
        client.get.side_effect = values.get
        client.setex.side_effect = lambda key, ttl, value: values.__setitem__(key, value)

        with (
            patch.object(regression_cache, "_get_redis_client", return_value=client),
            patch.object(
                regression_cache,
                "_read_non_negative_int_env",
                return_value=86400,
            ),
        ):
            self.assertTrue(regression_cache.store_cached_regression_summary(_summary()))
            self.assertEqual(regression_cache.get_cached_regression_summary(), _summary())

    def test_store_rejects_unconfigured_summary(self):
        values: dict[str, str] = {}
        client = MagicMock()
        client.get.side_effect = values.get
        client.setex.side_effect = lambda key, ttl, value: values.__setitem__(key, value)
        unconfigured = {
            "days": 30,
            "configured": False,
            "complete": False,
            "author_metrics": [],
            "reviewer_metrics": [],
        }

        with (
            patch.object(regression_cache, "_get_redis_client", return_value=client),
            patch.object(
                regression_cache,
                "_read_non_negative_int_env",
                return_value=86400,
            ),
        ):
            self.assertTrue(regression_cache.store_cached_regression_summary(_summary()))
            self.assertFalse(regression_cache.store_cached_regression_summary(unconfigured))
            self.assertEqual(regression_cache.get_cached_regression_summary(), _summary())
            self.assertEqual(client.setex.call_count, 1)

    def test_refresh_failure_returns_no_fake_summary(self):
        with (
            patch("regressions.build_regression_summary", side_effect=RuntimeError("boom")),
            patch.object(regression_cache, "get_cached_regression_summary", return_value=None),
        ):
            self.assertIsNone(regression_cache.refresh_regression_summary_cache())

    def test_refresh_failure_preserves_last_good_summary(self):
        values: dict[str, str] = {}
        client = MagicMock()
        client.get.side_effect = values.get
        client.setex.side_effect = lambda key, ttl, value: values.__setitem__(key, value)

        with (
            patch.object(regression_cache, "_get_redis_client", return_value=client),
            patch.object(
                regression_cache,
                "_read_non_negative_int_env",
                return_value=86400,
            ),
            patch("regressions.build_regression_summary", side_effect=RuntimeError("boom")),
        ):
            self.assertTrue(regression_cache.store_cached_regression_summary(_summary()))
            self.assertEqual(regression_cache.refresh_regression_summary_cache(), _summary())
            self.assertEqual(regression_cache.get_cached_regression_summary(), _summary())

    def test_homepage_partial_only_reads_cached_summary(self):
        client = app_module.app.test_client()
        with patch.object(
            app_module,
            "get_cached_regression_summary",
            return_value=_summary(),
        ):
            response = client.get("/partials/index/regressions")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Regression Signals (30d)", body)
        self.assertIn("2.5%", body)
        self.assertIn("1.2%", body)

        index = client.get("/").get_data(as_text=True)
        self.assertIn('id="regressions"', index)
        self.assertIn("'/partials/index/regressions'", index)

    def test_homepage_partial_handles_cache_miss_without_live_work(self):
        client = app_module.app.test_client()
        with patch.object(
            app_module,
            "get_cached_regression_summary",
            return_value=None,
        ):
            response = client.get("/partials/index/regressions")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Regression metrics are refreshing.", response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
