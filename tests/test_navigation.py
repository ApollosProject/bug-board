import unittest
from unittest.mock import patch

import app as app_module


class NavigationTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_local_pages_render_in_header_menu_not_footer(self):
        response = self.client.get("/team")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)

        header = body.split("</header>", 1)[0]
        footer = body.split("<footer", 1)[1]

        self.assertIn('class="dropdown site-menu"', header)
        self.assertIn('aria-label="Open local pages menu"', header)
        self.assertIn('href="/apps"', header)
        self.assertIn(">Teams</a>", header)
        self.assertIn('href="/failing-dags"', header)

        self.assertNotIn('href="/apps"', footer)
        self.assertNotIn('href="/team"', footer)
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
        fetch.assert_called_once_with()
        counts.assert_called_once_with("bkraeling", 30)
        support.assert_called_once_with(config=config, projects=projects)

    def test_team_labels_use_short_name(self):
        context = {
            "developers": [],
            "developer_projects": {},
            "cycle_projects_by_initiative": {},
            "completed_cycle_projects": [],
            "on_call_support": [],
            "support_issues": {},
        }

        response = self.client.get("/team")
        self.assertIn("<title>Teams</title>", response.get_data(as_text=True))

        with patch.object(app_module, "_build_team_context", return_value=context):
            partial_response = self.client.get("/partials/team/content")

        partial_body = partial_response.get_data(as_text=True)
        self.assertEqual(partial_response.status_code, 200)
        self.assertIn("<h2>Teams</h2>", partial_body)
        self.assertNotIn("Engineering Teams", partial_body)


if __name__ == "__main__":
    unittest.main()
