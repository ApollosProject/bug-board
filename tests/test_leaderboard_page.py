import unittest
from unittest.mock import patch

import app as app_module


class LeaderboardPageTest(unittest.TestCase):
    def setUp(self):
        app_module.reset_leaderboard_runtime_cache()
        self.addCleanup(app_module.reset_leaderboard_runtime_cache)
        self.client = app_module.app.test_client()

    def test_index_embeds_cached_leaderboard_without_a_followup_fetch(self):
        context = {
            "days": 30,
            "leaderboard_entries": [
                {
                    "slug": "nick",
                    "display_name": "Nick",
                    "score": 40,
                    "breakdown": "Urgent issues: 40 pts",
                }
            ],
        }

        with patch.object(app_module, "peek_leaderboard_context", return_value=context):
            with patch.object(app_module, "prefetch_leaderboard_context") as prefetch:
                response = self.client.get("/")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        prefetch.assert_called_once_with(30)
        self.assertIn("Leaderboard (30d)", body)
        self.assertIn("Nick", body)
        self.assertIn(">40<", body)
        self.assertNotIn("loadSection('leaderboard'", body)

    def test_index_prefetches_when_leaderboard_is_not_cached(self):
        with patch.object(app_module, "peek_leaderboard_context", return_value=None):
            with patch.object(app_module, "prefetch_leaderboard_context") as prefetch:
                response = self.client.get("/")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        prefetch.assert_called_once_with(30)
        self.assertIn('id="leaderboard"', body)
        self.assertIn('aria-busy="true"', body)
        self.assertIn("loadSection('leaderboard'", body)

    def test_leaderboard_partial_serves_stale_cache_without_recomputing(self):
        stale_context = {
            "days": 30,
            "leaderboard_entries": [
                {
                    "slug": "austin",
                    "display_name": "Austin",
                    "score": 15,
                    "breakdown": None,
                }
            ],
        }

        with patch.object(
            app_module,
            "_read_leaderboard_memory_cache",
            return_value=(stale_context, False),
        ):
            with patch.object(
                app_module, "_read_leaderboard_redis_cache", return_value=(None, False)
            ):
                with patch.object(app_module, "_start_leaderboard_refresh") as refresh:
                    refresh.return_value.result.side_effect = AssertionError(
                        "stale cache should not wait for a rebuild"
                    )
                    response = self.client.get("/partials/index/leaderboard?days=30")

        self.assertEqual(response.status_code, 200)
        refresh.assert_called_once_with(30)
        body = response.get_data(as_text=True)
        self.assertIn("Austin", body)
        self.assertIn(">15<", body)

    def test_compute_leaderboard_context_fetches_github_and_cycle_points_once(self):
        with (
            patch.object(app_module, "get_completed_issues_summary", return_value=[]),
            patch.object(
                app_module, "merged_prs_for_leaderboard", return_value=({}, {})
            ) as github_fetch,
            patch.object(
                app_module, "calculate_cycle_project_points", return_value=({}, {})
            ) as cycle_fetch,
            patch.object(app_module, "load_config", return_value={"people": {}}),
        ):
            context = app_module.compute_leaderboard_context(30)

        github_fetch.assert_called_once_with(30)
        cycle_fetch.assert_called_once_with(30)
        self.assertEqual(context["days"], 30)
        self.assertEqual(context["leaderboard_entries"], [])
