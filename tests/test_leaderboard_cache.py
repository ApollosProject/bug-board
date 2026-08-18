import json
import unittest
from unittest.mock import patch

import leaderboard_cache


class _FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def setex(self, key, _ttl, value):
        self.store[key] = value

    def set(self, key, value):
        self.store[key] = value


class LeaderboardCacheTest(unittest.TestCase):
    def test_round_trips_leaderboard_payload(self):
        fake_redis = _FakeRedis()
        context = {
            "days": 30,
            "leaderboard_entries": [
                {"slug": "nick", "display_name": "Nick", "score": 20, "breakdown": None}
            ],
        }

        with patch.object(leaderboard_cache, "_get_redis_client", return_value=fake_redis):
            self.assertTrue(leaderboard_cache.store_cached_leaderboard(30, context))
            cached = leaderboard_cache.get_cached_leaderboard(30)

        self.assertIsNotNone(cached)
        assert cached is not None
        self.assertEqual(cached["days"], 30)
        self.assertEqual(cached["leaderboard_entries"], context["leaderboard_entries"])
        self.assertIn("cached_at_epoch", cached)

    def test_rejects_stale_payload(self):
        fake_redis = _FakeRedis()
        fake_redis.set(
            leaderboard_cache.leaderboard_cache_key(30),
            json.dumps(
                {
                    "cached_at_epoch": 1,
                    "days": 30,
                    "leaderboard_entries": [],
                }
            ),
        )

        with patch.object(leaderboard_cache, "_get_redis_client", return_value=fake_redis):
            with patch.object(leaderboard_cache, "_read_non_negative_int_env", return_value=60):
                cached = leaderboard_cache.get_cached_leaderboard(30)

        self.assertIsNone(cached)
