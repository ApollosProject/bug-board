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
