import unittest
from unittest.mock import patch

import app as app_module
from leaderboard_export import build_leaderboard_export_rows, render_leaderboard_csv


class LeaderboardExportTest(unittest.TestCase):
    def test_csv_lists_score_parameters_and_route(self):
        rows = build_leaderboard_export_rows(
            [
                {
                    "slug": "a",
                    "display_name": "A",
                    "score": 10,
                    "points": {"urgent": 20, "prs": 10},
                    "counts": {"urgent": 1, "prs": 10},
                },
                {
                    "slug": "b",
                    "display_name": "B",
                    "score": 2,
                    "points": {"prs": 2},
                    "counts": {"prs": 2},
                },
            ]
        )
        self.assertEqual([row["slug"] for row in rows], ["a", "b"])
        self.assertEqual((rows[0]["urgent_issues"], rows[0]["prs_merged"]), (1, 10))
        self.assertIn("person,slug,score,urgent_issues", render_leaderboard_csv(rows))
        ctx = {
            "days": 30,
            "preset_days": 30,
            "window_query": {"days": 30},
            "leaderboard_entries": [{"slug": "a", "display_name": "A", "score": 10}],
        }
        client = app_module.app.test_client()
        with patch.object(app_module, "_leaderboard_page_context", return_value=ctx):
            csv_text = client.get("/leaderboard.csv").get_data(as_text=True)
            html = client.get("/partials/index/leaderboard").get_data(as_text=True)
        self.assertTrue(csv_text.startswith("person,slug,score"))
        self.assertIn("/leaderboard.csv?days=30", html)
        with patch.object(
            app_module, "_leaderboard_page_context", return_value={"leaderboard_unavailable": True}
        ):
            self.assertEqual(client.get("/leaderboard.csv").status_code, 503)
