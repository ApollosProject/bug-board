import sys
import types
import unittest
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import patch


def _install_import_shims() -> None:
    dotenv_module = cast(Any, types.ModuleType("dotenv"))
    dotenv_module.load_dotenv = lambda *args, **kwargs: None
    sys.modules.setdefault("dotenv", dotenv_module)

    requests_module = cast(Any, types.ModuleType("requests"))
    requests_module.get = lambda *args, **kwargs: None
    sys.modules.setdefault("requests", requests_module)

    gql_module = cast(Any, types.ModuleType("gql"))

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def execute(self, query, variable_values=None):
            return {}

    gql_module.Client = DummyClient
    gql_module.gql = lambda query: query
    sys.modules.setdefault("gql", gql_module)

    transport_module = cast(Any, types.ModuleType("gql.transport"))
    sys.modules.setdefault("gql.transport", transport_module)

    aiohttp_module = cast(Any, types.ModuleType("gql.transport.aiohttp"))

    class DummyAIOHTTPTransport:
        def __init__(self, *args, **kwargs):
            pass

    aiohttp_module.AIOHTTPTransport = DummyAIOHTTPTransport
    sys.modules.setdefault("gql.transport.aiohttp", aiohttp_module)


_install_import_shims()

import airflow_fleet_health  # noqa: E402
import app as app_module  # noqa: E402


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 3, 8, 17, 30, tzinfo=timezone.utc)


class EvaluateFleetHealthTest(unittest.TestCase):
    def test_includes_full_failed_dag_list_and_truncated_top_list(self):
        active_dags = {f"dag-{index:02d}" for index in range(1, 22)}
        latest_states = {
            dag_id: "failed" if dag_id <= "dag-11" else "success"
            for dag_id in active_dags
        }

        with patch.object(airflow_fleet_health, "_require_env", side_effect=["x", "y"]):
            with patch.object(airflow_fleet_health, "_build_session", return_value=object()):
                with patch.object(
                    airflow_fleet_health,
                    "_fetch_active_dags",
                    return_value=active_dags,
                ):
                    with patch.object(
                        airflow_fleet_health,
                        "_fetch_latest_states_by_dag",
                        return_value=(latest_states, 0),
                    ):
                        with patch.object(
                            airflow_fleet_health,
                            "datetime",
                            FixedDateTime,
                        ):
                            payload, status = airflow_fleet_health.evaluate_fleet_health()

        self.assertEqual(status, 503)
        self.assertEqual(payload["failed_runs"], 11)
        self.assertEqual(len(payload["failed_dags"]), 11)
        self.assertEqual(len(payload["top_failed_dags"]), 10)
        self.assertEqual(payload["top_failed_dags"], payload["failed_dags"][:10])
        self.assertEqual(payload["failed_dags"][0]["dag_id"], "dag-01")
        self.assertEqual(payload["failed_dags"][-1]["dag_id"], "dag-11")


class FailingDagsDashboardTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_dashboard_renders_full_failed_dag_list(self):
        payload = {
            "status": "degraded",
            "checked_at": "2026-03-08T17:00:00+00:00",
            "active_dags_total": 3,
            "evaluated_dags": 3,
            "failed_fetches": 0,
            "dags_without_runs": 0,
            "non_terminal_dags": 0,
            "failed_runs": 2,
            "failure_ratio": 2 / 3,
            "threshold_ratio": 0.10,
            "failed_dags": [
                {"dag_id": "alpha_dag", "state": "failed"},
                {"dag_id": "beta_dag", "state": "failed"},
            ],
            "top_failed_dags": [
                {"dag_id": "alpha_dag", "state": "failed"},
            ],
        }

        with patch.object(
            app_module,
            "_get_airflow_fleet_health_payload",
            return_value=(payload, 503),
        ):
            response = self.client.get("/failing-dags")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Failing DAGs", body)
        self.assertIn("alpha_dag", body)
        self.assertIn("beta_dag", body)
        self.assertIn("Open in Astro", body)
        self.assertIn("The underlying fleet check is currently returning HTTP 503.", body)

    def test_dashboard_marks_legacy_top_failed_dags_payload_as_partial(self):
        payload = {
            "status": "degraded",
            "checked_at": "2026-03-08T17:00:00+00:00",
            "active_dags_total": 4,
            "evaluated_dags": 4,
            "failed_fetches": 0,
            "dags_without_runs": 0,
            "non_terminal_dags": 0,
            "failed_runs": 3,
            "failure_ratio": 0.75,
            "threshold_ratio": 0.10,
            "top_failed_dags": [
                {"dag_id": "alpha_dag", "state": "failed"},
                {"dag_id": "beta_dag", "state": "failed"},
            ],
        }

        with patch.object(
            app_module,
            "_get_airflow_fleet_health_payload",
            return_value=(payload, 503),
        ):
            response = self.client.get("/failing-dags")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("alpha_dag", body)
        self.assertIn("beta_dag", body)
        self.assertIn("This cache entry only contains a partial DAG list.", body)

    def test_dashboard_uses_cached_payload_without_token_when_configured(self):
        payload = {
            "status": "healthy",
            "failed_runs": 0,
            "failed_dags": [],
        }

        with patch.dict(app_module.os.environ, {"AIRFLOW_FLEET_MONITOR_TOKEN": "secret"}):
            with patch.object(app_module, "should_use_redis_cache", return_value=True):
                with patch.object(
                    app_module,
                    "get_cached_fleet_health",
                    return_value=(payload, 200),
                ):
                    with patch.object(app_module, "evaluate_fleet_health") as evaluate_mock:
                        response = self.client.get("/failing-dags")

        self.assertEqual(response.status_code, 200)
        evaluate_mock.assert_not_called()

    def test_dashboard_skips_live_eval_without_cache_when_token_is_configured(self):
        with patch.dict(app_module.os.environ, {"AIRFLOW_FLEET_MONITOR_TOKEN": "secret"}):
            with patch.object(app_module, "should_use_redis_cache", return_value=False):
                with patch.object(app_module, "evaluate_fleet_health") as evaluate_mock:
                    response = self.client.get("/failing-dags")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Failing DAG data is currently unavailable.", body)
        evaluate_mock.assert_not_called()

    def test_health_endpoint_requires_monitor_token_when_configured(self):
        with patch.dict(app_module.os.environ, {"AIRFLOW_FLEET_MONITOR_TOKEN": "secret"}):
            response = self.client.get("/airflow-fleet-health")

        self.assertEqual(response.status_code, 401)


class ProjectStatusClassificationTest(unittest.TestCase):
    def test_released_project_is_inactive_and_completed(self):
        project = {"status": {"name": "Released"}}

        self.assertTrue(app_module.is_inactive_project(project))
        self.assertTrue(app_module.is_completed_project(project))
        self.assertFalse(app_module.is_incomplete_project(project))


class TeamContextProjectFilteringTest(unittest.TestCase):
    def setUp(self):
        app_module._build_team_context.cache_clear()
        app_module._build_person_context.cache_clear()

    def tearDown(self):
        app_module._build_team_context.cache_clear()
        app_module._build_person_context.cache_clear()

    def test_released_project_moves_out_of_current_focus(self):
        config = {
            "people": {
                "darryl": {
                    "team": "apollos_engineering",
                    "linear_username": "darryl",
                }
            },
            "platforms": {},
        }
        released_project = {
            "id": "proj-1",
            "name": "16KB Page Sizes for Android",
            "url": "https://linear.example/project/16kb",
            "health": "onTrack",
            "status": {"name": "Released"},
            "completedAt": "2025-11-21T00:00:00.000Z",
            "startDate": "2025-11-11",
            "targetDate": "2025-11-21",
            "lead": {"displayName": "Darryl"},
            "initiatives": {"nodes": [{"id": "init-1", "name": "Apollos+DL1 Cycle 6.2025"}]},
            "members": [],
        }

        with patch.object(app_module, "load_config", return_value=config):
            with patch.object(app_module, "get_projects", return_value=[released_project]):
                with patch.object(app_module, "get_support_slugs", return_value=[]):
                    with patch.object(app_module, "get_open_issues", return_value=[]):
                        context = app_module._build_team_context(1)

        self.assertEqual(context["developers"], [])
        self.assertEqual(context["developer_projects"], {})
        self.assertEqual(context["cycle_projects_by_initiative"], {})
        self.assertEqual(
            [project["name"] for project in context["completed_cycle_projects"]],
            ["16KB Page Sizes for Android"],
        )

    def test_released_project_is_not_counted_as_current(self):
        config = {
            "people": {
                "darryl": {
                    "team": "apollos_engineering",
                    "linear_username": "darryl",
                }
            }
        }
        released_project = {
            "id": "proj-1",
            "name": "16KB Page Sizes for Android",
            "url": "https://linear.example/project/16kb",
            "health": "onTrack",
            "status": {"name": "Released"},
            "completedAt": "2025-11-21T00:00:00.000Z",
            "startDate": "2025-11-11",
            "targetDate": "2025-11-21",
            "lead": {"displayName": "Darryl"},
            "initiatives": {"nodes": [{"id": "init-1", "name": "Apollos+DL1 Cycle 6.2025"}]},
            "members": [],
        }

        with patch.object(app_module, "load_config", return_value=config):
            with patch.object(app_module, "get_open_issues_for_person", return_value=[]):
                with patch.object(app_module, "get_completed_issues_for_person", return_value=[]):
                    with patch.object(app_module, "by_project", return_value={}):
                        with patch.object(app_module, "by_platform", return_value={}):
                            with patch.object(app_module, "get_projects", return_value=[released_project]):
                                with patch.object(app_module, "get_support_slugs", return_value=[]):
                                    context = app_module._build_person_context("darryl", 7, 1)

        self.assertEqual(context["lead_current_projects"], 0)
        self.assertEqual(context["lead_completed_projects"], 1)
        self.assertEqual(context["lead_incomplete_projects"], 0)


if __name__ == "__main__":
    unittest.main()
