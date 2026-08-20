import base64
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

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


def _project(
    lead_name: str,
    project_id: str,
    *,
    status: str,
    completed: bool = False,
    start_date: str | None = None,
    target_date: str | None = None,
) -> dict:
    return {
        "id": project_id,
        "name": f"Project {project_id}",
        "url": f"https://linear.example/project/{project_id}",
        "status": {"name": status},
        "completedAt": "2026-08-01T00:00:00.000Z" if completed else None,
        "startDate": start_date,
        "targetDate": target_date,
        "lead": {"displayName": lead_name},
        "members": [],
    }


def _outlier_metrics(*, better: bool) -> dict[str, int]:
    high = 10 if better else 0
    low = 0 if better else 10
    metrics = dict.fromkeys(person_stats.CARD_METRIC_KEYS, high)
    for key in person_stats.LOWER_IS_BETTER_METRIC_KEYS:
        metrics[key] = low
    return metrics


def _linear_id_link_details(url: str) -> tuple[str, str, list[str]]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    encoded_filter = query["filter"][0]
    encoded_filter += "=" * (-len(encoded_filter) % 4)
    id_filter = json.loads(base64.urlsafe_b64decode(encoded_filter).decode("utf-8"))
    return parsed.path, query["layout"][0], id_filter["and"][0]["id"]["in"]


class PersonStatsTest(unittest.TestCase):
    def test_z_scores_skip_zero_variance_and_keep_copy_short(self):
        self.assertIsNone(person_stats.z_score(9, [4, 4]))
        self.assertEqual(person_stats.z_score(10, [10, 2]), 1.0)
        self.assertEqual(person_stats.format_stdev_tooltip([10, 2]), "eng avg 6.0 · σ 4.0")

    def test_z_scores_trim_extremes_without_clipping_exceptional_people(self):
        prs_merged = [378, 77, 9, 81, 0, 99, 60, 26]
        team_metrics = [{"prs_merged": value} for value in prs_merged]

        low = person_stats.metric_stdevs_for_person({"prs_merged": 9}, team_metrics)["prs_merged"]
        high = person_stats.metric_stdevs_for_person({"prs_merged": 378}, team_metrics)[
            "prs_merged"
        ]

        self.assertEqual(low["label"], "−1.6σ")
        self.assertEqual(low["tone"], "low")
        self.assertEqual(low["tooltip"], "eng trimmed avg 58.7 · σ 31.6")
        self.assertEqual(high["label"], "+10.1σ")
        self.assertEqual(high["tone"], "high")

    def test_z_scores_fall_back_when_trimming_removes_all_variance(self):
        values = [20, 10, 10, 10, 0]

        self.assertAlmostEqual(person_stats.z_score(20, values) or 0, 1.58, places=2)
        self.assertEqual(person_stats.format_stdev_tooltip(values), "eng avg 10.0 · σ 6.3")

    def test_every_card_metric_has_an_explicit_stdev_direction(self):
        classified = person_stats.LOWER_IS_BETTER_METRIC_KEYS | frozenset(
            {
                "prs_merged",
                "prs_reviewed",
                "priority_bugs_fixed",
                "all_work_done",
                "lead_current_projects",
                "lead_completed_projects",
            }
        )
        self.assertEqual(classified, frozenset(person_stats.CARD_METRIC_KEYS))
        self.assertEqual(
            frozenset(person_stats.STDEV_DIRECTION_HINTS),
            person_stats.LOWER_IS_BETTER_METRIC_KEYS,
        )

    def test_stdev_signs_treat_plus_as_better(self):
        better = {
            "prs_merged": 10,
            "prs_reviewed": 10,
            "priority_bugs_fixed": 10,
            "priority_bug_avg_time_to_fix": 2,
            "all_work_done": 10,
            "avg_all_time_to_fix": 2,
            "lead_current_projects": 10,
            "lead_completed_projects": 10,
            "lead_incomplete_projects": 0,
            "lead_completed_projects_avg_early_late": -3,
        }
        worse = {
            "prs_merged": 2,
            "prs_reviewed": 2,
            "priority_bugs_fixed": 2,
            "priority_bug_avg_time_to_fix": 10,
            "all_work_done": 2,
            "avg_all_time_to_fix": 10,
            "lead_current_projects": 2,
            "lead_completed_projects": 2,
            "lead_incomplete_projects": 8,
            "lead_completed_projects_avg_early_late": 5,
        }

        better_stdevs = person_stats.metric_stdevs_for_person(better, [better, worse])
        worse_stdevs = person_stats.metric_stdevs_for_person(worse, [better, worse])

        for key in person_stats.CARD_METRIC_KEYS:
            with self.subTest(key=key):
                self.assertTrue(better_stdevs[key]["label"].startswith("+"), better_stdevs[key])
                self.assertTrue(worse_stdevs[key]["label"].startswith("−"), worse_stdevs[key])

        self.assertEqual(better_stdevs["prs_merged"]["label"], "+1.0σ")
        self.assertNotIn("tone", better_stdevs["prs_merged"])
        self.assertEqual(better_stdevs["priority_bug_avg_time_to_fix"]["label"], "+1.0σ")
        self.assertEqual(worse_stdevs["avg_all_time_to_fix"]["label"], "−1.0σ")
        self.assertEqual(better_stdevs["lead_incomplete_projects"]["label"], "+1.0σ")
        self.assertEqual(better_stdevs["lead_completed_projects_avg_early_late"]["label"], "+1.0σ")
        self.assertTrue(
            better_stdevs["priority_bug_avg_time_to_fix"]["tooltip"].endswith("lower is better")
        )
        self.assertTrue(
            better_stdevs["lead_completed_projects_avg_early_late"]["tooltip"].endswith(
                "earlier is better"
            )
        )

    def test_all_card_metrics_color_at_one_and_a_half_sigma(self):
        self.assertEqual(person_stats.stdev_tone(1.5), "high")
        self.assertEqual(person_stats.stdev_tone(-1.5), "low")
        self.assertIsNone(person_stats.stdev_tone(1.49))
        self.assertIsNone(person_stats.stdev_tone(-1.49))

        high_person = _outlier_metrics(better=True)
        clustered_low = _outlier_metrics(better=False)
        low_person = _outlier_metrics(better=False)
        clustered_high = _outlier_metrics(better=True)

        high_stdevs = person_stats.metric_stdevs_for_person(
            high_person, [high_person, clustered_low, clustered_low, clustered_low]
        )
        low_stdevs = person_stats.metric_stdevs_for_person(
            low_person, [low_person, clustered_high, clustered_high, clustered_high]
        )

        for key in person_stats.CARD_METRIC_KEYS:
            with self.subTest(key=key):
                self.assertEqual(high_stdevs[key]["tone"], "high")
                self.assertEqual(low_stdevs[key]["tone"], "low")

    def test_person_cards_color_headings_beyond_stdev_threshold(self):
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
            return [_issue(priority=2, bug=True) for _ in range(10)] if login == "alice" else []

        projects = [
            *[
                _project("Alice", f"alice-done-{index}", status="Completed", completed=True)
                for index in range(10)
            ],
            *[_project("Alice", f"alice-open-{index}", status="Incomplete") for index in range(10)],
        ]

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", side_effect=fake_completed),
            patch.object(app_module, "get_projects", return_value=projects),
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

        for key in (
            "prs_merged",
            "prs_reviewed",
            "priority_bugs_fixed",
            "all_work_done",
            "lead_completed_projects",
        ):
            self.assertEqual(context["metric_stdevs"][key]["tone"], "high")
        self.assertEqual(context["metric_stdevs"]["lead_incomplete_projects"]["tone"], "low")
        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertEqual(body.count('<h1 class="high">10</h1>'), 5)
        self.assertEqual(body.count('<h1 class="low">10</h1>'), 1)
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

    def test_project_metric_cards_link_to_their_exact_linear_project_lists(self):
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)
        config = {
            "people": {
                "alice": {
                    "team": "engineering",
                    "linear_username": "alice",
                }
            }
        }
        projects = [
            _project("Alice", "alice-current", status="In Progress"),
            _project(
                "Alice",
                "alice-completed-with-dates",
                status="Completed",
                completed=True,
                start_date="2026-07-01",
                target_date="2026-07-30",
            ),
            _project(
                "Alice",
                "alice-completed-without-dates",
                status="Completed",
                completed=True,
            ),
            _project("Alice", "alice-incomplete", status="Incomplete"),
            _project("Alice", "alice-canceled", status="Canceled"),
            _project("Bob", "bob-current", status="In Progress"),
        ]

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", return_value=[]),
            patch.object(app_module, "get_projects", return_value=projects),
            patch.object(app_module, "get_support_slugs", return_value=set()),
            patch.object(app_module, "get_cached_regression_summary", return_value=None),
        ):
            context = app_module._build_person_context("alice", 30, 1)

        expected_project_ids = {
            "lead_current_projects": ["alice-current"],
            "lead_completed_projects": [
                "alice-completed-with-dates",
                "alice-completed-without-dates",
            ],
            "lead_incomplete_projects": ["alice-incomplete"],
            "lead_completed_projects_avg_early_late": ["alice-completed-with-dates"],
        }
        for metric, project_ids in expected_project_ids.items():
            with self.subTest(metric=metric):
                path, layout, linked_project_ids = _linear_id_link_details(
                    context["project_metric_urls"][metric]
                )
                self.assertEqual(path, "/differential/projects/all")
                self.assertEqual(layout, "list")
                self.assertEqual(linked_project_ids, project_ids)

        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertEqual(body.count("linear.app/differential/projects/all?filter="), 4)
        self.assertNotIn('href="https://linear.app/differential/projects/all"', body)

    def test_issue_metric_cards_link_to_their_exact_linear_issue_lists(self):
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)
        config = {
            "people": {
                "alice": {
                    "team": "engineering",
                    "linear_username": "alice",
                }
            }
        }
        completed = [
            {
                **_issue(priority=1, bug=True),
                "id": "alice-priority-bug",
                "identifier": "APO-1",
                "title": "Urgent bug",
            },
            {
                **_issue(priority=4, bug=False),
                "id": "alice-other-work",
                "identifier": "APO-2",
                "title": "Chore",
            },
            {
                **_issue(priority=2, bug=False),
                "id": "alice-high-non-bug",
                "identifier": "APO-3",
                "title": "High chore",
            },
        ]

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", return_value=completed),
            patch.object(app_module, "get_projects", return_value=[]),
            patch.object(app_module, "get_support_slugs", return_value=set()),
            patch.object(app_module, "get_cached_regression_summary", return_value=None),
        ):
            context = app_module._build_person_context("alice", 30, 1)

        self.assertEqual(
            context["issue_metric_urls"]["priority_bugs_fixed"],
            "https://linear.app/differential/issues/APO-1",
        )
        self.assertEqual(
            context["issue_metric_urls"]["all_work_done"],
            "https://linear.app/differential/issues/APO-1,APO-2,APO-3",
        )

        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertEqual(body.count("linear.app/differential/issues/APO-"), 2)
        self.assertNotIn("linear.app/differential/team/APO/all?filter=", body)
        self.assertNotIn("sla-issues-7e2098ebf79e", body)
        self.assertNotIn("linear.app/differential/profiles/alice", body)

    def test_completed_project_weeks_treat_two_short_projects_like_one_long_project(self):
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
        projects = [
            _project(
                "Alice",
                "alice-two-week-1",
                status="Completed",
                completed=True,
                start_date="2026-03-02",
                target_date="2026-03-15",
            ),
            _project(
                "Alice",
                "alice-two-week-2",
                status="Completed",
                completed=True,
                start_date="2026-03-16",
                target_date="2026-03-29",
            ),
            _project(
                "Bob",
                "bob-four-week",
                status="Completed",
                completed=True,
                start_date="2026-03-02",
                target_date="2026-03-29",
            ),
        ]

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", return_value=[]),
            patch.object(app_module, "get_projects", return_value=projects),
            patch.object(app_module, "get_merged_pr_counts_for_user", return_value=(0, 0)),
            patch.object(app_module, "get_support_slugs", return_value=set()),
        ):
            alice = app_module._build_person_context("alice", 30, 1)
            bob = app_module._build_person_context("bob", 30, 1)

        self.assertEqual(alice["lead_completed_projects"], 4)
        self.assertEqual(bob["lead_completed_projects"], 4)
        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **alice)
        self.assertIn("Completed Project Weeks", body)
        self.assertNotIn("Completed Projects</a>", body)

    def test_current_work_starts_collapsed(self):
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)
        config = {
            "people": {
                "alice": {
                    "team": "engineering",
                    "linear_username": "alice",
                }
            }
        }
        current_issue = {
            **_issue(),
            "title": "Ship the current work",
            "url": "https://linear.example/issue/current",
            "daysUpdated": 2,
            "project": {"name": "Project alpha"},
        }
        other_issue = {
            **_issue(),
            "title": "Unrelated chore",
            "url": "https://linear.example/issue/other",
            "daysUpdated": 4,
            "project": {"name": "Side quest"},
        }

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(
                app_module,
                "get_open_issues_for_person",
                return_value=[current_issue, other_issue],
            ),
            patch.object(app_module, "get_completed_issues_for_person", return_value=[]),
            patch.object(
                app_module,
                "get_projects",
                return_value=[_project("Alice", "alpha", status="In Progress")],
            ),
            patch.object(app_module, "get_support_slugs", return_value=set()),
            patch.object(app_module, "get_cached_regression_summary", return_value=None),
        ):
            context = app_module._build_person_context("alice", 30, 1)

        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)

        current_work = body.split("<h2>Current Project Work</h2>", 1)[1].split("<h2>", 1)[0]
        self.assertIn("<summary>Project alpha</summary>", current_work)
        self.assertIn("Ship the current work", current_work)
        self.assertIn("<details>", current_work)
        self.assertNotIn("<details open>", current_work)
        self.assertIn("<h2>Other Work</h2>", body)
