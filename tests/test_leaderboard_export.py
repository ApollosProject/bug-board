import csv
import io
import unittest
from unittest.mock import patch

import app as app_module
from leaderboard_export import build_leaderboard_export_rows, render_leaderboard_csv


class LeaderboardExportTest(unittest.TestCase):
    def test_context_keeps_all_credited_authors_for_export_only(self):
        config = {
            "people": {
                "eng": {
                    "team": "engineering",
                    "linear_username": "eng",
                    "github_username": "eng-gh",
                },
                "other": {
                    "team": "unassigned",
                    "linear_username": "other",
                    "github_username": "other-gh",
                },
            }
        }
        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(
                app_module,
                "get_completed_issues_summary_for_labels",
                return_value=[],
            ),
            patch.object(
                app_module,
                "get_merged_pr_activity",
                # This map is the contract after Cursor co-author attribution.
                return_value=({"eng-gh": [{}], "other-gh": [{}, {}]}, {}),
            ),
            patch.object(app_module, "calculate_cycle_project_points", return_value=({}, {})),
        ):
            context = app_module.compute_leaderboard_context(30)

        self.assertEqual([entry["slug"] for entry in context["leaderboard_entries"]], ["eng"])
        export_entries = {entry["slug"]: entry for entry in context["leaderboard_export_entries"]}
        self.assertEqual(set(export_entries), {"eng", "other"})
        self.assertEqual(export_entries["other"]["counts"]["prs"], 2)

    def test_csv_lists_score_parameters_and_route(self):
        people = {
            "a": {"team": "engineering"},
            "b": {"team": "engineering"},
            "c": {"team": "engineering"},
            "d": {"team": "unassigned"},
        }
        rows = build_leaderboard_export_rows(
            [
                {
                    "slug": "d",
                    "display_name": "D",
                    "score": 50,
                    "points": {"prs": 50},
                    "counts": {"prs": 50},
                },
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
        self.assertEqual([row["slug"] for row in rows], ["d", "a", "b", "c"])
        self.assertEqual(
            (
                rows[0]["prs_merged"],
                rows[0]["score_stdev"],
                rows[1]["urgent_issues"],
                rows[1]["score_stdev"],
                rows[3]["regressions_authored"],
            ),
            (50, "", 1, "1.4", 2),
        )
        self.assertEqual(
            (rows[0]["pr_points_stdev"], rows[0]["urgent_points_stdev"]),
            ("", ""),
        )
        self.assertEqual(rows[1]["pr_points_stdev"], "1.4")
        self.assertIn("person,slug,score,score_stdev,urgent_issues", render_leaderboard_csv(rows))
        client = app_module.app.test_client()
        ctx = {
            "days": 30,
            "preset_days": 30,
            "window_query": {"days": 30},
            "leaderboard_entries": [
                {
                    "slug": "michael",
                    "display_name": "Michael",
                    "score": 10,
                    "points": {"prs": 10},
                    "counts": {"prs": 10},
                }
            ],
            "leaderboard_export_entries": [
                {
                    "slug": "michael",
                    "display_name": "Michael",
                    "score": 10,
                    "points": {"prs": 10},
                    "counts": {"prs": 10},
                },
                {
                    "slug": "andy",
                    "display_name": "Andy",
                    "score": 5,
                    "points": {"prs": 5},
                    "counts": {"prs": 5},
                },
            ],
        }
        with patch.object(app_module, "_leaderboard_page_context", return_value=ctx):
            csv_text = client.get("/leaderboard.csv").get_data(as_text=True)
            html = client.get("/partials/index/leaderboard").get_data(as_text=True)
        self.assertTrue(csv_text.startswith("person,slug,score"))
        exported = {row["slug"]: row for row in csv.DictReader(io.StringIO(csv_text))}
        self.assertEqual(exported["andy"]["prs_merged"], "5")
        self.assertEqual(exported["andy"]["score_stdev"], "")
        self.assertIn("Michael", html)
        self.assertNotIn("Andy", html)
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
