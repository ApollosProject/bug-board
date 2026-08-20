import csv
import io
import unittest
from unittest.mock import patch

import app as app_module
from leaderboard_export import (
    CSV_COLUMNS,
    build_leaderboard_export_rows,
    leaderboard_csv_filename,
    render_leaderboard_csv,
)


def _entry(slug, display_name, score, points=None, counts=None):
    return {
        "slug": slug,
        "display_name": display_name,
        "score": score,
        "breakdown": None,
        "points": points or {},
        "counts": counts or {},
    }


class LeaderboardExportTest(unittest.TestCase):
    def test_csv_includes_parameters_stdevs_and_zero_score_regressions(self):
        rows = build_leaderboard_export_rows(
            [
                _entry("alice", "Alice", 10, {"prs": 10, "urgent": 20}, {"prs": 10, "urgent": 1}),
                _entry("bob", "Bob", 2, {"prs": 2}, {"prs": 2}),
            ],
            engineering_people={
                "alice": {"team": "engineering", "linear_username": "alice"},
                "bob": {"team": "engineering", "linear_username": "bob"},
                "cara": {"team": "engineering", "linear_username": "cara"},
                "andy": {"team": "unassigned", "linear_username": "andy"},
            },
            regression_summary={
                "configured": True,
                "author_metrics": [{"slug": "cara", "regression_count": 2, "rate": 4.0}],
                "reviewer_metrics": [{"slug": "cara", "regression_count": 1, "rate": 1.5}],
            },
        )

        self.assertEqual([row["slug"] for row in rows], ["alice", "bob", "cara"])
        self.assertNotIn("andy", [row["slug"] for row in rows])
        self.assertEqual(rows[0]["urgent_issues"], 1)
        self.assertEqual(rows[0]["urgent_points"], 20)
        self.assertEqual(rows[0]["score_stdev"], "1.4")
        self.assertEqual(rows[1]["score_stdev"], "-0.5")
        self.assertEqual(rows[2]["score"], 0)
        self.assertEqual(rows[2]["regressions_authored"], 2)
        self.assertEqual(rows[2]["author_regression_rate"], 4.0)
        self.assertEqual(rows[2]["regressions_approved"], 1)
        self.assertEqual(rows[2]["reviewer_escape_rate"], 1.5)
        self.assertEqual(rows[0]["regressions_authored"], 0)
        self.assertEqual(rows[0]["author_regression_rate"], "")

        body = render_leaderboard_csv(rows)
        header = next(csv.reader(io.StringIO(body)))
        self.assertEqual(header, list(CSV_COLUMNS))
        self.assertIn("score_stdev", header)
        self.assertIn("urgent_issues", header)
        self.assertIn("cycle_member_points_stdev", header)
        self.assertIn("reviewer_escape_rate", header)

    def test_unconfigured_regressions_leave_rate_columns_blank(self):
        rows = build_leaderboard_export_rows(
            [_entry("alice", "Alice", 4)],
            engineering_people={"alice": {"team": "engineering"}},
            regression_summary=None,
        )

        self.assertEqual(rows[0]["regressions_authored"], "")
        self.assertEqual(rows[0]["author_regression_rate"], "")
        self.assertEqual(rows[0]["regressions_approved"], "")
        self.assertEqual(rows[0]["reviewer_escape_rate"], "")

    def test_filename_uses_preset_or_date_range(self):
        self.assertEqual(
            leaderboard_csv_filename(
                {"preset_days": 30, "start": "2026-01-01", "end": "2026-01-31"}
            ),
            "leaderboard-30d.csv",
        )
        self.assertEqual(
            leaderboard_csv_filename({"start": "2026-01-01", "end": "2026-01-31"}),
            "leaderboard-2026-01-01-to-2026-01-31.csv",
        )


class LeaderboardExportRouteTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_route_downloads_csv_and_partial_links_to_it(self):
        context = {
            "days": 30,
            "preset_days": 30,
            "start": "2026-07-20",
            "end": "2026-08-19",
            "window_label": "30d",
            "window_query": {"days": 30},
            "leaderboard_entries": [
                _entry("alice", "Alice", 10, {"prs": 10}, {"prs": 10}),
                _entry("bob", "Bob", 2, {"prs": 2}, {"prs": 2}),
            ],
        }
        config = {
            "people": {
                "alice": {"team": "engineering", "linear_username": "alice"},
                "bob": {"team": "engineering", "linear_username": "bob"},
            }
        }
        regressions = {
            "configured": True,
            "author_metrics": [{"slug": "alice", "regression_count": 3, "rate": 2.5}],
            "reviewer_metrics": [{"slug": "bob", "regression_count": 1, "rate": 0.8}],
        }
        with (
            patch.object(app_module, "_leaderboard_page_context", return_value=context),
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_cached_regression_summary", return_value=regressions),
        ):
            response = self.client.get("/leaderboard.csv?days=30")
            partial = self.client.get("/partials/index/leaderboard?days=30")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.content_type.startswith("text/csv"))
        self.assertIn(
            'attachment; filename="leaderboard-30d.csv"', response.headers["Content-Disposition"]
        )
        body = response.get_data(as_text=True)
        self.assertIn("person,slug,score,score_stdev", body)
        self.assertIn("alice,10,", body)
        self.assertIn("3,2.5,", body)
        self.assertIn("Export CSV", partial.get_data(as_text=True))
        self.assertIn('href="/leaderboard.csv?days=30"', partial.get_data(as_text=True))

        with open("static/styles.css") as styles_file:
            styles = styles_file.read()
        self.assertIn(".leaderboard-heading {", styles)
        self.assertIn(".leaderboard-export {", styles)

    def test_refreshing_leaderboard_returns_503(self):
        with patch.object(
            app_module,
            "_leaderboard_page_context",
            return_value={"leaderboard_unavailable": True, "preset_days": 30},
        ):
            response = self.client.get("/leaderboard.csv")

        self.assertEqual(response.status_code, 503)
        self.assertIn("refreshing", response.get_data(as_text=True))


class LeaderboardEntryParametersTest(unittest.TestCase):
    def test_entries_keep_structured_points_and_counts(self):
        config = {
            "people": {
                "alice": {
                    "team": "engineering",
                    "linear_username": "alice",
                    "github_username": "alice-gh",
                },
                "bob": {
                    "team": "engineering",
                    "linear_username": "bob",
                    "github_username": "bob-gh",
                },
            }
        }
        completed = [
            {"assignee": {"name": "alice", "displayName": "Alice"}, "priority": 1},
            {"assignee": {"name": "alice", "displayName": "Alice"}, "priority": 3},
        ]
        with patch.object(app_module, "load_config", return_value=config):
            entries = app_module._build_leaderboard_entries(
                completed_work=completed,
                merged_reviews={"alice-gh": ["r1", "r2"]},
                merged_authored_prs={"bob-gh": ["p1"]},
                cycle_lead_points={"Alice": 30},
                cycle_member_points={"Bob": 15},
            )

        by_slug = {entry["slug"]: entry for entry in entries}
        self.assertEqual(by_slug["alice"]["points"]["urgent"], 20)
        self.assertEqual(by_slug["alice"]["counts"]["urgent"], 1)
        self.assertEqual(by_slug["alice"]["points"]["reviews"], 2)
        self.assertEqual(by_slug["alice"]["counts"]["reviews"], 2)
        self.assertEqual(by_slug["alice"]["points"]["cycle_lead"], 30)
        self.assertEqual(by_slug["bob"]["points"]["prs"], 1)
        self.assertEqual(by_slug["bob"]["points"]["cycle_member"], 15)
        self.assertEqual(by_slug["alice"]["score"], 57)
        self.assertEqual(by_slug["bob"]["score"], 16)
