import unittest
from unittest.mock import patch

import app as app_module
import person_stats


def _issue(*, priority=4, bug=False, time_to_fix=1):
    labels = [{"name": "Bug"}] if bug else []
    return {
        "priority": priority,
        "labels": {"nodes": labels},
        "assignee_time_to_fix": time_to_fix,
        "updatedAt": "2026-01-02T00:00:00.000Z",
        "completedAt": "2026-01-02T00:00:00.000Z",
        "title": "Issue",
        "url": "https://linear.example/issue",
        "project": None,
        "platform": None,
    }


class PersonStatsTest(unittest.TestCase):
    def test_completed_work_metrics_match_person_page_averages(self):
        issues = [
            _issue(priority=2, bug=True, time_to_fix=2),
            _issue(priority=1, bug=True, time_to_fix=4),
            _issue(priority=4, time_to_fix=9),
        ]

        metrics = person_stats.completed_work_metrics(issues)

        self.assertEqual(metrics["priority_bugs_fixed"], 2)
        self.assertEqual(metrics["priority_bug_avg_time_to_fix"], 3)
        self.assertEqual(metrics["all_work_done"], 3)
        self.assertEqual(metrics["avg_all_time_to_fix"], 5)

    def test_z_score_uses_population_stdev_and_skips_thin_samples(self):
        self.assertIsNone(person_stats.z_score(4, [4]))
        self.assertEqual(person_stats.z_score(4, [4, 4]), 0.0)
        self.assertEqual(person_stats.z_score(10, [10, 2]), 1.0)
        self.assertEqual(person_stats.z_score(2, [10, 2]), -1.0)

    def test_stdev_labels_are_compact(self):
        self.assertEqual(person_stats.format_stdev_label(1.0), "+1.0σ")
        self.assertEqual(person_stats.format_stdev_label(-1.04), "−1.0σ")
        self.assertEqual(person_stats.format_stdev_label(0.01), "0.0σ")

    def test_metric_stdevs_omit_missing_and_single_values(self):
        person = {
            "prs_merged": 10,
            "prs_reviewed": 4,
            "priority_bugs_fixed": 2,
            "priority_bug_avg_time_to_fix": 3,
            "all_work_done": 8,
            "avg_all_time_to_fix": 5,
            "lead_current_projects": 0,
            "lead_completed_projects": 2,
            "lead_incomplete_projects": 0,
            "lead_completed_projects_avg_early_late": 0.0,
        }
        teammate = {
            **person,
            "prs_merged": 2,
            "prs_reviewed": 0,
            "priority_bugs_fixed": 0,
            "priority_bug_avg_time_to_fix": None,
            "all_work_done": 2,
            "avg_all_time_to_fix": 1,
            "lead_completed_projects": 0,
            "lead_completed_projects_avg_early_late": None,
        }

        stdevs = person_stats.metric_stdevs_for_person(person, [person, teammate])

        self.assertEqual(stdevs["prs_merged"]["label"], "+1.0σ")
        self.assertIn("above the engineering group average", stdevs["prs_merged"]["tooltip"])
        self.assertEqual(stdevs["all_work_done"]["label"], "+1.0σ")
        self.assertNotIn("priority_bug_avg_time_to_fix", stdevs)
        self.assertNotIn("lead_completed_projects_avg_early_late", stdevs)


class PersonContextStdevsTest(unittest.TestCase):
    def setUp(self):
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)

    def test_person_cards_include_engineering_group_stdevs(self):
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
                "vinny": {
                    "team": "unassigned",
                    "linear_username": "vincent",
                    "github_username": "vinnyjth",
                },
            }
        }
        projects = [
            {
                "id": "proj-1",
                "name": "Shipped",
                "status": {"name": "Released"},
                "completedAt": "2026-01-10T00:00:00.000Z",
                "startDate": "2026-01-01",
                "targetDate": "2026-01-12",
                "lead": {"displayName": "Alice"},
            },
            {
                "id": "proj-2",
                "name": "Also Shipped",
                "status": {"name": "Completed"},
                "completedAt": "2026-01-10T00:00:00.000Z",
                "startDate": "2026-01-01",
                "targetDate": "2026-01-08",
                "lead": {"displayName": "Alice"},
            },
        ]
        completed_logins = []
        github_usernames = []

        def fake_completed(login, days=30, window=None):
            completed_logins.append(login)
            if login == "alice":
                return [
                    _issue(priority=2, bug=True, time_to_fix=2),
                    _issue(priority=2, bug=True, time_to_fix=4),
                    *[_issue() for _ in range(6)],
                ]
            if login == "bob":
                return [_issue(), _issue()]
            raise AssertionError(f"unexpected completed-issues login {login}")

        def fake_pr_counts(username, days=30, window=None):
            github_usernames.append(username)
            if username == "alice-gh":
                return (10, 4)
            if username == "bob-gh":
                return (2, 0)
            raise AssertionError(f"unexpected github username {username}")

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", side_effect=fake_completed),
            patch.object(app_module, "get_projects", return_value=projects),
            patch.object(app_module, "get_merged_pr_counts_for_user", side_effect=fake_pr_counts),
            patch.object(app_module, "get_support_slugs", return_value=set()),
        ):
            context = app_module._build_person_context("alice", 30, 1)

        self.assertEqual(sorted(completed_logins), ["alice", "bob"])
        self.assertEqual(sorted(github_usernames), ["alice-gh", "bob-gh"])
        self.assertEqual(context["prs_merged"], 10)
        self.assertEqual(context["all_work_done"], 8)
        self.assertEqual(context["lead_completed_projects"], 2)
        self.assertEqual(context["metric_stdevs"]["prs_merged"]["label"], "+1.0σ")
        self.assertEqual(context["metric_stdevs"]["all_work_done"]["label"], "+1.0σ")
        self.assertEqual(context["metric_stdevs"]["lead_completed_projects"]["label"], "+1.0σ")
        self.assertIn(
            "engineering group average",
            context["metric_stdevs"]["prs_merged"]["tooltip"],
        )

        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)

        self.assertIn('class="metric-stdev"', body)
        self.assertIn("+1.0σ", body)
        self.assertIn("above the engineering group average", body)

    def test_single_engineer_has_no_stdev_badges(self):
        config = {
            "people": {
                "alice": {
                    "team": "engineering",
                    "linear_username": "alice",
                    "github_username": "alice-gh",
                }
            }
        }

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", return_value=[]),
            patch.object(app_module, "get_projects", return_value=[]),
            patch.object(app_module, "get_merged_pr_counts_for_user", return_value=(3, 1)),
            patch.object(app_module, "get_support_slugs", return_value=set()),
        ):
            context = app_module._build_person_context("alice", 30, 1)

        self.assertEqual(context["metric_stdevs"], {})
        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertNotIn("metric-stdev", body)

    def test_metric_stdev_styles_stay_subtle(self):
        with open("static/styles.css") as styles_file:
            styles = styles_file.read()

        rule = styles.split(".metric-stdev {", 1)[1].split("}", 1)[0]
        self.assertIn("font-size: 0.7rem;", rule)
        self.assertIn("opacity: 0.7;", rule)
        self.assertIn("color: var(--pico-muted-color);", rule)


if __name__ == "__main__":
    unittest.main()
