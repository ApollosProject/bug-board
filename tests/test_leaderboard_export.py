import unittest
from unittest.mock import patch

import app as app_module
from leaderboard_export import build_leaderboard_export_rows, render_leaderboard_csv


class LeaderboardExportTest(unittest.TestCase):
    def test_csv_lists_score_parameters_and_route(self):
        people = {"a": {}, "b": {}, "c": {}}
        rows = build_leaderboard_export_rows(
            [
                {
                    "slug": "a",
                    "display_name": "A",
                    "score": 10,
                    "points": {"prs": 10, "urgent": 20},
                    "counts": {"prs": 10, "urgent": 1},
                },
                {
                    "slug": "b",
                    "display_name": "B",
                    "score": 2,
                    "points": {"prs": 2},
                    "counts": {"prs": 2},
                },
            ],
            people=people,
            regression_summary={
                "configured": True,
                "author_metrics": [{"slug": "c", "regression_count": 2, "rate": 4.0}],
            },
        )
        self.assertEqual([row["slug"] for row in rows], ["a", "b", "c"])
        self.assertEqual(
            (rows[0]["urgent_issues"], rows[0]["score_stdev"], rows[2]["regressions_authored"]),
            (1, "1.4", 2),
        )
        self.assertIn("person,slug,score,score_stdev,urgent_issues", render_leaderboard_csv(rows))
        client = app_module.app.test_client()
        ctx = {
            "days": 30,
            "preset_days": 30,
            "window_query": {"days": 30},
            "leaderboard_entries": [
                {
                    "slug": "a",
                    "display_name": "A",
                    "score": 10,
                    "points": {"prs": 10},
                    "counts": {"prs": 10},
                }
            ],
        }
        with patch.object(app_module, "_leaderboard_page_context", return_value=ctx):
            csv_text = client.get("/leaderboard.csv").get_data(as_text=True)
            html = client.get("/partials/index/leaderboard").get_data(as_text=True)
        self.assertTrue(csv_text.startswith("person,slug,score"))
        self.assertIn("a,10,", csv_text)
        self.assertIn("/leaderboard.csv?days=30", html)
        self.assertIn('class="leaderboard-export"', html)
        self.assertIn(">Export CSV</a>", html)
        self.assertNotIn("<h2>\n  Leaderboard", html)
        with open("static/styles.css") as styles_file:
            styles = styles_file.read()
        heading = styles.split(".leaderboard-heading {", 1)[1].split("}", 1)[0]
        export = styles.split("a.leaderboard-export {", 1)[1].split("}", 1)[0]
        self.assertIn("display: flex;", heading)
        self.assertIn("justify-content: space-between;", heading)
        self.assertIn("font-size: 0.8rem;", export)
        self.assertIn("text-decoration: none;", export)
        with patch.object(
            app_module, "_leaderboard_page_context", return_value={"leaderboard_unavailable": True}
        ):
            self.assertEqual(client.get("/leaderboard.csv").status_code, 503)
