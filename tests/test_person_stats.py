import unittest
from pathlib import Path
from unittest.mock import patch

import app as app_module
import person_stats


def _issue(*, priority=4, bug=False):
    return {
        "priority": priority,
        "labels": {"nodes": [{"name": "Bug"}] if bug else []},
        "assignee_time_to_fix": 1,
        "updatedAt": "2026-01-02T00:00:00.000Z",
        "completedAt": "2026-01-02T00:00:00.000Z",
        "title": "Issue",
        "url": "https://linear.example/issue",
        "project": None,
        "platform": None,
    }


class PersonStatsTest(unittest.TestCase):
    def test_z_scores_skip_zero_variance_and_keep_copy_short(self):
        self.assertIsNone(person_stats.z_score(9, [4, 4]))
        self.assertEqual(person_stats.z_score(10, [10, 2]), 1.0)
        self.assertEqual(person_stats.format_stdev_tooltip([10, 2]), "eng avg 6.0 · σ 4.0")

    def test_time_metric_stdevs_reward_shorter_times(self):
        person_metrics = {
            "prs_merged": 10,
            "priority_bug_avg_time_to_fix": 2,
            "avg_all_time_to_fix": 10,
            "lead_completed_projects_avg_early_late": -3,
        }
        team_metrics = [
            person_metrics,
            {
                "prs_merged": 2,
                "priority_bug_avg_time_to_fix": 10,
                "avg_all_time_to_fix": 2,
                "lead_completed_projects_avg_early_late": 5,
            },
        ]

        stdevs = person_stats.metric_stdevs_for_person(person_metrics, team_metrics)

        self.assertEqual(stdevs["prs_merged"]["label"], "+1.0σ")
        self.assertNotIn("tone", stdevs["prs_merged"])
        self.assertEqual(stdevs["priority_bug_avg_time_to_fix"]["label"], "+1.0σ")
        self.assertEqual(stdevs["avg_all_time_to_fix"]["label"], "−1.0σ")
        self.assertEqual(stdevs["lead_completed_projects_avg_early_late"]["label"], "+1.0σ")
        self.assertTrue(
            stdevs["priority_bug_avg_time_to_fix"]["tooltip"].endswith("lower is better")
        )

    def test_pr_cards_color_at_one_and_a_half_sigma(self):
        self.assertEqual(person_stats.stdev_tone(1.5), "high")
        self.assertEqual(person_stats.stdev_tone(-1.5), "low")
        self.assertIsNone(person_stats.stdev_tone(1.49))
        self.assertIsNone(person_stats.stdev_tone(-1.49))

        high_person = {"prs_merged": 10, "prs_reviewed": 10, "all_work_done": 10}
        clustered_low = {"prs_merged": 0, "prs_reviewed": 0, "all_work_done": 0}
        low_person = {"prs_merged": 0, "prs_reviewed": 0, "all_work_done": 0}
        clustered_high = {"prs_merged": 10, "prs_reviewed": 10, "all_work_done": 10}

        high_stdevs = person_stats.metric_stdevs_for_person(
            high_person, [high_person, clustered_low, clustered_low, clustered_low]
        )
        low_stdevs = person_stats.metric_stdevs_for_person(
            low_person, [low_person, clustered_high, clustered_high, clustered_high]
        )

        self.assertEqual(high_stdevs["prs_merged"]["tone"], "high")
        self.assertEqual(high_stdevs["prs_reviewed"]["tone"], "high")
        self.assertEqual(high_stdevs["all_work_done"]["tone"], "high")
        self.assertEqual(low_stdevs["prs_merged"]["tone"], "low")
        self.assertEqual(low_stdevs["prs_reviewed"]["tone"], "low")
        self.assertEqual(low_stdevs["all_work_done"]["tone"], "low")

    def test_person_cards_color_pr_headings_beyond_stdev_threshold(self):
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)
        config = {
            "people": {
                slug: {
                    "team": "engineering",
                    "linear_username": slug,
                    "github_username": f"{slug}-gh",
                }
                for slug in ("alice", "bob", "cara", "drew")
            }
        }

        def fake_completed(login, days=30, window=None):
            return [_issue() for _ in range(10)] if login == "alice" else []

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", side_effect=fake_completed),
            patch.object(app_module, "get_projects", return_value=[]),
            patch.object(
                app_module,
                "get_merged_pr_counts_for_user",
                side_effect=lambda username, days=30, window=None: (
                    (10, 10) if username == "alice-gh" else (0, 0)
                ),
            ),
            patch.object(app_module, "get_support_slugs", return_value=set()),
        ):
            context = app_module._build_person_context("alice", 30, 1)

        self.assertEqual(context["metric_stdevs"]["prs_merged"]["tone"], "high")
        self.assertEqual(context["metric_stdevs"]["prs_reviewed"]["tone"], "high")
        self.assertEqual(context["metric_stdevs"]["all_work_done"]["tone"], "high")
        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertEqual(body.count('<h1 class="high">10</h1>'), 3)
        self.assertNotIn('<h1 class="high">0</h1>', body)
        self.assertNotIn("2/week", body)
        self.assertNotIn("5/week", body)

    def test_person_cards_include_on_page_stdev_badges(self):
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)
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

        def fake_completed(login, days=30, window=None):
            return (
                [_issue(priority=2, bug=True), *[_issue() for _ in range(7)]]
                if login == "alice"
                else [_issue(), _issue()]
            )

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", side_effect=fake_completed),
            patch.object(app_module, "get_projects", return_value=[]),
            patch.object(
                app_module,
                "get_merged_pr_counts_for_user",
                side_effect=lambda username, days=30, window=None: (
                    (10, 4) if username == "alice-gh" else (2, 0)
                ),
            ),
            patch.object(app_module, "get_support_slugs", return_value=set()),
        ):
            context = app_module._build_person_context("alice", 30, 1)

        self.assertEqual(context["metric_stdevs"]["prs_merged"]["tooltip"], "eng avg 6.0 · σ 4.0")
        self.assertNotIn("tone", context["metric_stdevs"]["prs_merged"])
        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertIn("<h1>10</h1>", body)
        self.assertIn("<h1>8</h1>", body)
        self.assertNotIn('<h1 class="high">10</h1>', body)
        self.assertNotIn('class="low"', body)
        self.assertNotIn("2/week", body)
        self.assertIn('data-placement="bottom"', body)
        styles = Path(__file__).resolve().parents[1].joinpath("static/styles.css").read_text()
        self.assertIn("max-width: min(12rem, calc(100vw - 2rem));", styles)
        self.assertIn("white-space: normal;", styles)
