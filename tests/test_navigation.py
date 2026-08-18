import unittest
from datetime import datetime
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import app as app_module


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 7, 31, 12, tzinfo=tz)


class NavigationTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_local_pages_render_in_header_menu_not_footer(self):
        response = self.client.get("/projects")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)

        header = body.split("</header>", 1)[0]
        footer = body.split("<footer", 1)[1]

        self.assertIn('class="dropdown site-menu"', header)
        self.assertIn('aria-label="Open local pages menu"', header)
        self.assertIn('href="/apps"', header)
        self.assertIn('href="/projects"', header)
        self.assertIn(">Projects</a>", header)
        self.assertIn('href="/failing-dags"', header)

        self.assertNotIn('href="/apps"', footer)
        self.assertNotIn('href="/projects"', footer)
        self.assertNotIn('href="/failing-dags"', footer)

    def test_header_menu_overrides_pico_left_aligned_dropdown(self):
        with open("static/styles.css") as styles_file:
            styles = styles_file.read()

        self.assertIn("details.dropdown.site-menu > summary + ul", styles)
        site_menu_rule = styles.split("details.dropdown.site-menu > summary + ul", 1)[1]
        site_menu_rule = site_menu_rule.split("}", 1)[0]

        self.assertIn("left: auto;", site_menu_rule)
        self.assertIn("right: 0;", site_menu_rule)
        self.assertIn("max-width: calc(100vw - 2rem);", site_menu_rule)

    def test_busy_indicators_use_a_css_rotation_animation(self):
        with open("static/styles.css") as styles_file:
            styles = styles_file.read()

        self.assertIn("@keyframes busy-spinner", styles)
        busy_rule = styles.split(
            '[aria-busy="true"]:not(input, select, textarea, html)::before',
            1,
        )[1]
        busy_rule = busy_rule.split("}", 1)[0]

        self.assertIn("animation: busy-spinner 0.8s linear infinite;", busy_rule)
        self.assertIn("background-image: none;", busy_rule)

    def test_person_context_scopes_github_counts_and_reuses_projects(self):
        config = {
            "people": {
                "brandon": {
                    "linear_username": "brandon",
                    "github_username": "bkraeling",
                }
            }
        }
        projects = []
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)

        with (
            patch.object(app_module, "datetime", FixedDateTime),
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", return_value=[]),
            patch.object(app_module, "get_projects", return_value=projects) as fetch,
            patch.object(
                app_module, "get_merged_pr_counts_for_user", return_value=(60, 53)
            ) as counts,
            patch.object(app_module, "get_support_slugs", return_value={"brandon"}) as support,
        ):
            context = app_module._build_person_context("brandon", 30, 1)

        self.assertEqual((context["prs_merged"], context["prs_reviewed"]), (60, 53))
        merged_pr_query = parse_qs(urlparse(context["github_merged_prs_url"]).query)["q"][0]
        self.assertIn("author:bkraeling", merged_pr_query)
        self.assertIn("merged:>=2026-07-01", merged_pr_query)
        with app_module.app.test_request_context():
            body = app_module.render_template("partials/person_content.html", **context)
        self.assertIn("merged%3A%3E%3D2026-07-01", body)
        fetch.assert_called_once_with()
        self.assertEqual(counts.call_args.args[0], "bkraeling")
        self.assertEqual(counts.call_args.args[1], 30)
        support.assert_called_once_with(config=config, projects=projects)

    def test_index_and_person_pages_accept_date_ranges(self):
        index = self.client.get("/?start=2026-01-01&end=2026-01-31&days=7")
        index_body = index.get_data(as_text=True)
        self.assertEqual(index.status_code, 200)
        self.assertIn('name="start"', index_body)
        self.assertIn('name="end"', index_body)
        self.assertIn('value="2026-01-01"', index_body)
        self.assertIn('value="2026-01-31"', index_body)
        self.assertIn("start", index_body)
        self.assertIn("/partials/index/leaderboard?", index_body)
        self.assertIn("2026-01-01", index_body)
        self.assertIn("2026-01-31", index_body)

        config = {
            "people": {
                "brandon": {
                    "linear_username": "brandon",
                    "github_username": "bkraeling",
                }
            }
        }
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)
        with patch.object(app_module, "load_config", return_value=config):
            person = self.client.get("/team/brandon?start=2026-01-01&end=2026-01-31")
        person_body = person.get_data(as_text=True)
        self.assertEqual(person.status_code, 200)
        self.assertIn('"start": "2026-01-01"', person_body)
        self.assertIn('"end": "2026-01-31"', person_body)
        self.assertIn("`/partials/team/${slug}/content?${windowQs}`", person_body)

    def test_person_context_uses_date_range_for_github_search(self):
        config = {
            "people": {
                "brandon": {
                    "linear_username": "brandon",
                    "github_username": "bkraeling",
                }
            }
        }
        app_module._build_person_context.cache_clear()
        self.addCleanup(app_module._build_person_context.cache_clear)

        with (
            patch.object(app_module, "load_config", return_value=config),
            patch.object(app_module, "get_open_issues_for_person", return_value=[]),
            patch.object(app_module, "get_completed_issues_for_person", return_value=[]),
            patch.object(app_module, "get_projects", return_value=[]),
            patch.object(
                app_module, "get_merged_pr_counts_for_user", return_value=(2, 1)
            ) as counts,
            patch.object(app_module, "get_support_slugs", return_value=set()),
        ):
            context = app_module._build_person_context("brandon", 30, 1, "2026-01-01", "2026-01-31")

        self.assertEqual(context["preset_days"], None)
        self.assertEqual(context["start"], "2026-01-01")
        self.assertEqual(context["end"], "2026-01-31")
        self.assertEqual(context["window_label"], "Jan 1 – Jan 31, 2026")
        merged_pr_query = parse_qs(urlparse(context["github_merged_prs_url"]).query)["q"][0]
        self.assertIn("merged:>=2026-01-01", merged_pr_query)
        self.assertIn("merged:<=2026-01-31", merged_pr_query)
        self.assertEqual(counts.call_args.args[0], "bkraeling")
        self.assertEqual(
            counts.call_args.args[2].query_args(),
            {"start": "2026-01-01", "end": "2026-01-31"},
        )

    def test_projects_page_and_timeline_use_project_labels(self):
        context = {
            "project_timeline": {
                "weeks": [],
                "rows": [],
                "unassigned_ready_projects": [
                    {
                        "name": "Add Tap Feed to Shortcuts",
                        "url": "https://linear.example/project/tap-feed-shortcuts",
                    }
                ],
                "date_range": "Jul 13 – Aug 23",
                "today_percent": 1.2,
            },
            "cycle_projects_by_initiative": {},
            "completed_cycle_projects": [],
        }

        response = self.client.get("/projects")
        self.assertIn("<title>Projects</title>", response.get_data(as_text=True))

        with patch.object(app_module, "_build_team_context", return_value=context):
            partial_response = self.client.get("/partials/projects/content")

        partial_body = partial_response.get_data(as_text=True)
        self.assertEqual(partial_response.status_code, 200)
        self.assertIn("<h2>Projects</h2>", partial_body)
        self.assertIn("<h3>Timeline</h3>", partial_body)
        self.assertIn('aria-label="Ready unassigned projects"', partial_body)
        self.assertIn("Unassigned projects", partial_body)
        self.assertIn(
            'href="https://linear.example/project/tap-feed-shortcuts"',
            partial_body,
        )
        self.assertNotIn("Current Focus", partial_body)

    def test_legacy_team_url_renders_the_projects_page(self):
        response = self.client.get("/team")

        self.assertEqual(response.status_code, 200)
        self.assertIn("<title>Projects</title>", response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
