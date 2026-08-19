import json
import unittest
from unittest.mock import MagicMock, patch

import app as app_module
import leaderboard_cache


class LeaderboardCacheTest(unittest.TestCase):
    def test_roundtrip_rejects_mismatch_and_keeps_cache_on_refresh_failure(self):
        payload = {"days": 30, "leaderboard_entries": [{"slug": "michael", "score": 10}]}
        values: dict[str, str] = {}
        client = MagicMock()
        client.get.side_effect = values.get
        client.setex.side_effect = lambda key, ttl, value: values.__setitem__(key, value)

        with patch.object(leaderboard_cache, "_get_redis_client", return_value=client):
            with patch.object(leaderboard_cache, "_read_non_negative_int_env", return_value=900):
                self.assertTrue(leaderboard_cache.store_cached_leaderboard(30, payload))
                self.assertEqual(leaderboard_cache.get_cached_leaderboard(30), payload)
                values["leaderboard:index:30"] = json.dumps({"payload": {**payload, "days": 7}})
                self.assertIsNone(leaderboard_cache.get_cached_leaderboard(30))

        with patch.object(leaderboard_cache, "get_cached_leaderboard", return_value=payload):
            with patch.object(leaderboard_cache, "store_cached_leaderboard") as store:
                with patch("app.compute_leaderboard_context", side_effect=RuntimeError("boom")):
                    self.assertEqual(leaderboard_cache.refresh_leaderboard_cache(30), payload)
        store.assert_not_called()

    def test_cached_hit_miss_and_other_windows(self):
        payload = {
            "days": 30,
            "leaderboard_entries": [
                {"slug": "michael", "display_name": "Michael", "score": 1257, "breakdown": None}
            ],
        }
        live = {"days": 7, "leaderboard_entries": []}
        client = app_module.app.test_client()
        app_module._build_leaderboard_context.cache_clear()
        self.addCleanup(app_module._build_leaderboard_context.cache_clear)
        with (
            patch.object(app_module, "should_use_redis_cache", return_value=True),
            patch.object(app_module, "_is_development_mode", return_value=False),
            patch.object(app_module, "compute_leaderboard_context", return_value=live) as compute,
        ):
            with patch.object(app_module, "get_cached_leaderboard", return_value=payload):
                hit = client.get("/partials/index/leaderboard?days=30")
            with patch.object(app_module, "get_cached_leaderboard", return_value=None):
                miss = client.get("/partials/index/leaderboard?days=30")
                other = client.get("/partials/index/leaderboard?days=7")

        self.assertIn("Michael", hit.get_data(as_text=True))
        self.assertIn("Leaderboard is refreshing.", miss.get_data(as_text=True))
        compute.assert_called_once()
        self.assertEqual(compute.call_args.args[0], 7)
        self.assertEqual(other.status_code, 200)

    def test_debug_mode_computes_live_leaderboard_on_redis_cache_miss(self):
        live = {
            "days": 30,
            "leaderboard_entries": [
                {"slug": "michael", "display_name": "Michael", "score": 42, "breakdown": None}
            ],
        }
        client = app_module.app.test_client()
        app_module._build_leaderboard_context.cache_clear()
        self.addCleanup(app_module._build_leaderboard_context.cache_clear)
        with (
            patch.object(app_module, "should_use_redis_cache", return_value=True),
            patch.object(app_module, "_is_development_mode", return_value=True),
            patch.object(app_module, "get_cached_leaderboard", return_value=None),
            patch.object(app_module, "compute_leaderboard_context", return_value=live) as compute,
        ):
            response = client.get("/partials/index/leaderboard?days=30")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Michael", body)
        self.assertNotIn("Leaderboard is refreshing.", body)
        compute.assert_called_once()
        self.assertEqual(compute.call_args.args[0], 30)
