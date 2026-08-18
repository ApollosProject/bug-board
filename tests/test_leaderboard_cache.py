import json
import unittest
from unittest.mock import patch

import app as app_module
import leaderboard_cache


class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.writes = []

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.writes.append((key, ttl, value))
        self.values[key] = value

    def set(self, key, value):
        self.writes.append((key, None, value))
        self.values[key] = value


class LeaderboardCacheTest(unittest.TestCase):
    def test_reads_valid_payload(self):
        payload = {"days": 30, "leaderboard_entries": [{"slug": "michael", "score": 10}]}
        client = FakeRedis(
            {"leaderboard:index:30": json.dumps({"cached_at_epoch": 1, "payload": payload})}
        )

        with patch.object(leaderboard_cache, "_get_redis_client", return_value=client):
            cached = leaderboard_cache.get_cached_leaderboard(30)

        self.assertEqual(cached, payload)

    def test_returns_none_when_missing(self):
        with patch.object(leaderboard_cache, "_get_redis_client", return_value=FakeRedis()):
            self.assertIsNone(leaderboard_cache.get_cached_leaderboard(30))

    def test_stores_payload_with_ttl(self):
        client = FakeRedis()
        payload = {"days": 30, "leaderboard_entries": []}

        with patch.object(leaderboard_cache, "_get_redis_client", return_value=client):
            with patch.object(leaderboard_cache, "_read_non_negative_int_env", return_value=900):
                stored = leaderboard_cache.store_cached_leaderboard(30, payload)

        self.assertTrue(stored)
        key, ttl, raw = client.writes[0]
        self.assertEqual(key, "leaderboard:index:30")
        self.assertEqual(ttl, 900)
        self.assertEqual(json.loads(raw)["payload"], payload)


class LeaderboardPartialCacheTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_uses_redis_payload_without_live_compute(self):
        payload = {
            "days": 30,
            "leaderboard_entries": [
                {
                    "slug": "michael",
                    "display_name": "Michael",
                    "score": 1257,
                    "breakdown": None,
                }
            ],
        }

        with patch.object(app_module, "should_use_redis_cache", return_value=True):
            with patch.object(app_module, "get_cached_leaderboard", return_value=payload):
                with patch.object(app_module, "compute_leaderboard_context") as compute:
                    response = self.client.get("/partials/index/leaderboard?days=30")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Michael", body)
        self.assertIn("1257", body)
        compute.assert_not_called()

    def test_shows_refreshing_state_on_cache_miss(self):
        with patch.object(app_module, "should_use_redis_cache", return_value=True):
            with patch.object(app_module, "get_cached_leaderboard", return_value=None):
                with patch.object(app_module, "compute_leaderboard_context") as compute:
                    response = self.client.get("/partials/index/leaderboard?days=30")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Leaderboard is refreshing.", body)
        compute.assert_not_called()
