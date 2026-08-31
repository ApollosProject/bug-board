import os
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import api
import app as app_module

API_KEY = "test-api-key"
PERSON_CONTEXT = {
    "person_slug": "zach",
    "person_name": "Zach",
    "linear_username": "zach",
    "github_username": "solideo-gloria",
    "github_merged_prs_url": "https://github.com/pulls?q=merged",
    "days": 26,
    "preset_days": None,
    "start": "2026-08-31",
    "end": "2026-09-25",
    "window_label": "2026-08-31 – 2026-09-25",
    "prs_merged": 32,
    "prs_reviewed": 11,
    "priority_bugs_fixed": 4,
    "priority_bug_avg_time_to_fix": 3,
    "all_work_done": 19,
    "avg_all_time_to_fix": None,
    "lead_current_projects": 1,
    "lead_completed_projects": 2,
    "lead_incomplete_projects": 0,
    "lead_completed_projects_avg_early_late": "2d late",
    "lead_completed_projects_avg_early_late_days": 2.0,
    "metric_stdevs": {
        "prs_merged": {
            "label": "+2.4σ",
            "tone": "high",
            "tooltip": "eng trimmed avg 12.0 · σ 8.3",
            "z": 2.41,
            "eng_avg": 12.0,
            "eng_stdev": 8.3,
        }
    },
    "regression_metrics_status": "ready",
    "regressions_authored": 1,
    "author_regression_rate": 3.1,
    "regressions_approved": 0,
    "reviewer_escape_rate": None,
    "issue_metric_urls": {"all_work_done": "https://linear.app/issues"},
    "project_metric_urls": {"lead_current_projects": "https://linear.app/projects"},
}


class ApiKeyAuthTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        self.env = patch.dict(os.environ, {api.API_KEY_ENV_VAR: API_KEY}, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)
        context_patch = patch.object(
            app_module, "_build_person_context", return_value=PERSON_CONTEXT
        )
        self.build_person_context = context_patch.start()
        self.addCleanup(context_patch.stop)

    def test_valid_key_returns_the_person_metrics(self):
        response = self.client.get(
            "/api/team/zach?start=2026-08-31&end=2026-09-25",
            headers={"Authorization": f"Bearer {API_KEY}"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        payload = response.get_json()
        self.assertEqual(payload["person"]["slug"], "zach")
        self.assertEqual(payload["window"]["start"], "2026-08-31")
        self.assertEqual(payload["window"]["end"], "2026-09-25")
        self.assertEqual(payload["metrics"]["prs_merged"]["value"], 32)
        self.assertEqual(payload["metrics"]["prs_merged"]["vs_team"]["eng_avg"], 12.0)

    def test_the_requested_window_reaches_the_context_builder(self):
        self.client.get(
            "/api/team/zach?start=2026-08-31&end=2026-09-25",
            headers={"Authorization": f"Bearer {API_KEY}"},
        )

        slug, days, _cache_epoch, start, end = self.build_person_context.call_args.args
        self.assertEqual((slug, start, end), ("zach", "2026-08-31", "2026-09-25"))
        self.assertEqual(days, 26)

    def test_the_key_is_also_accepted_from_the_x_api_key_header(self):
        response = self.client.get("/api/team/zach", headers={"X-API-Key": API_KEY})

        self.assertEqual(response.status_code, 200)

    def test_a_missing_key_is_unauthorized(self):
        response = self.client.get("/api/team/zach")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "unauthorized")
        self.assertIn("Bearer", response.headers["WWW-Authenticate"])
        self.build_person_context.assert_not_called()

    def test_a_wrong_key_is_unauthorized(self):
        response = self.client.get("/api/team/zach", headers={"Authorization": "Bearer nope"})

        self.assertEqual(response.status_code, 401)
        self.build_person_context.assert_not_called()

    def test_an_unknown_person_is_not_found(self):
        response = self.client.get(
            "/api/team/nobody", headers={"Authorization": f"Bearer {API_KEY}"}
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["error"], "unknown_person")

    def test_the_api_is_unavailable_when_no_key_is_configured(self):
        with patch.dict(os.environ, {api.API_KEY_ENV_VAR: "  "}, clear=False):
            response = self.client.get(
                "/api/team/zach", headers={"Authorization": f"Bearer {API_KEY}"}
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "api_key_not_configured")
        self.build_person_context.assert_not_called()

    def test_the_api_key_replaces_the_github_oauth_session(self):
        with patch.dict(app_module.app.config, {"GITHUB_OAUTH_ENABLED": True}, clear=False):
            authorized = self.client.get(
                "/api/team/zach", headers={"Authorization": f"Bearer {API_KEY}"}
            )
            unauthorized = self.client.get("/api/team/zach")

        self.assertEqual(authorized.status_code, 200)
        self.assertEqual(unauthorized.status_code, 401)

    def test_every_api_route_requires_the_api_key(self):
        api_endpoints = {
            rule.endpoint
            for rule in app_module.app.url_map.iter_rules()
            if str(rule).startswith("/api/")
        }

        self.assertTrue(api_endpoints)
        self.assertEqual(api_endpoints - api.API_KEY_ENDPOINTS, set())


class RealPersonContextTest(unittest.TestCase):
    """Guard the fixture above against drift in the dashboard context."""

    def test_the_payload_reads_the_keys_the_dashboard_context_actually_sets(self):
        patches = {
            "get_open_issues_for_person": [],
            "get_completed_issues_for_person": [],
            "get_projects": [],
            "get_merged_pr_counts_for_user": (7, 3),
            "get_cached_regression_summary": None,
            "get_support_slugs": set(),
        }
        with ExitStack() as stack:
            for name, value in patches.items():
                stack.enter_context(patch.object(app_module, name, return_value=value))
            context = app_module._build_person_context("zach", 30, -1)

        payload = api.person_metrics_payload(context)

        self.assertEqual(payload["person"]["slug"], "zach")
        self.assertEqual(payload["metrics"]["prs_merged"]["value"], 7)
        self.assertEqual(payload["metrics"]["prs_reviewed"]["value"], 3)
        self.assertLessEqual(set(payload["metrics"]), set(PERSON_CONTEXT))
        for key, entry in payload["metrics"].items():
            with self.subTest(key=key):
                self.assertIn(key, context)
                self.assertIsInstance(entry["display"], str)


class PersonMetricsPayloadTest(unittest.TestCase):
    def test_day_counts_and_missing_values_render_like_the_dashboard(self):
        metrics = api.person_metrics_payload(PERSON_CONTEXT)["metrics"]

        self.assertEqual(metrics["priority_bug_avg_time_to_fix"]["display"], "3d")
        self.assertIsNone(metrics["avg_all_time_to_fix"]["value"])
        self.assertEqual(metrics["avg_all_time_to_fix"]["display"], "n/a")
        self.assertEqual(metrics["lead_completed_projects_avg_early_late"]["value"], 2.0)
        self.assertEqual(metrics["lead_completed_projects_avg_early_late"]["display"], "2d late")

    def test_metrics_without_a_team_comparison_have_no_vs_team_block(self):
        metrics = api.person_metrics_payload(PERSON_CONTEXT)["metrics"]

        self.assertEqual(metrics["prs_merged"]["vs_team"]["z"], 2.41)
        self.assertIsNone(metrics["prs_reviewed"]["vs_team"])

    def test_links_merge_the_issue_and_project_metric_urls(self):
        links = api.person_metrics_payload(PERSON_CONTEXT)["links"]

        self.assertEqual(links["github_merged_prs"], "https://github.com/pulls?q=merged")
        self.assertEqual(links["all_work_done"], "https://linear.app/issues")
        self.assertEqual(links["lead_current_projects"], "https://linear.app/projects")


if __name__ == "__main__":
    unittest.main()
