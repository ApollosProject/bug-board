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
        self.assertEqual(stdevs["priority_bug_avg_time_to_fix"]["label"], "+1.0σ")
        self.assertEqual(stdevs["avg_all_time_to_fix"]["label"], "−1.0σ")
        self.assertEqual(stdevs["lead_completed_projects_avg_early_late"]["label"], "+1.0σ")
        self.assertTrue(
            stdevs["priority_bug_avg_time_to_fix"]["tooltip"].endswith("lower is better")
        )

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
        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertIn('data-placement="bottom"', body)
        styles = Path(__file__).resolve().parents[1].joinpath("static/styles.css").read_text()
        self.assertIn("max-width: min(12rem, calc(100vw - 2rem));", styles)
        self.assertIn("white-space: normal;", styles)
