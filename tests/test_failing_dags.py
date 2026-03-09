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
        self.assertNotIn("Astro Failed DAGs", body)
        self.assertNotIn('scope="col">State</th>', body)
        self.assertNotIn(">failed</td>", body)
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

    def test_dashboard_explains_missing_airflow_credentials(self):
        with patch.dict(app_module.os.environ, {}, clear=False):
            for env_name in (
                "AIRFLOW_API_BASE_URL",
                "AIRFLOW_API_TOKEN",
                "AIRFLOW_FLEET_MONITOR_TOKEN",
                "REDIS_URL",
            ):
                app_module.os.environ.pop(env_name, None)

            with patch.object(app_module, "should_use_redis_cache", return_value=False):
                with patch.object(
                    app_module,
                    "evaluate_fleet_health",
                    side_effect=app_module.AirflowFleetHealthError(
                        "AIRFLOW_API_BASE_URL is not configured."
                    ),
                ):
                    response = self.client.get("/failing-dags")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Setup required", body)
        self.assertIn("Review app setup", body)
        self.assertIn("Airflow credentials are missing for this app.", body)
        self.assertIn("AIRFLOW_API_BASE_URL", body)
        self.assertIn("AIRFLOW_API_TOKEN", body)
        self.assertIn("Add the missing", body)
        self.assertIn("Airflow API values below", body)
        self.assertIn("health metrics, and failing DAG list automatically.", body)
        self.assertNotIn("Failing DAG data is currently unavailable.", body)
        self.assertNotIn("The underlying fleet check is currently returning HTTP 503.", body)
        self.assertNotIn("<strong>Active DAGs</strong>", body)

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

    def test_dashboard_explains_missing_airflow_credentials_from_cached_payload(self):
        payload = {
            "status": "unknown",
            "failed_runs": 0,
            "failed_dags": [],
        }

        with patch.dict(app_module.os.environ, {}, clear=False):
            for env_name in (
                "AIRFLOW_API_BASE_URL",
                "AIRFLOW_API_TOKEN",
                "REDIS_URL",
            ):
                app_module.os.environ.pop(env_name, None)

            with patch.object(app_module, "should_use_redis_cache", return_value=True):
                with patch.object(
                    app_module,
                    "get_cached_fleet_health",
                    return_value=(payload, 503),
                ):
                    with patch.object(app_module, "evaluate_fleet_health") as evaluate_mock:
                        response = self.client.get("/failing-dags")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Setup required", body)
        self.assertIn("Required before this page can load", body)
        self.assertIn("AIRFLOW_API_BASE_URL", body)
        self.assertIn("AIRFLOW_API_TOKEN", body)
        self.assertNotIn("Failing DAG data is currently unavailable.", body)
        evaluate_mock.assert_not_called()

    def test_dashboard_skips_live_eval_without_cache_when_token_is_configured(self):
        with patch.dict(
            app_module.os.environ,
            {
                "AIRFLOW_API_BASE_URL": "https://airflow.example.com",
                "AIRFLOW_API_TOKEN": "token",
                "AIRFLOW_FLEET_MONITOR_TOKEN": "secret",
            },
        ):
            with patch.object(app_module, "should_use_redis_cache", return_value=False):
                with patch.object(app_module, "evaluate_fleet_health") as evaluate_mock:
                    response = self.client.get("/failing-dags")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Failing DAG data is currently unavailable.", body)
        evaluate_mock.assert_not_called()

    def test_dashboard_keeps_generic_unavailable_state_when_live_eval_is_disabled(self):
        with patch.dict(
            app_module.os.environ,
            {"AIRFLOW_FLEET_MONITOR_TOKEN": "secret"},
            clear=False,
        ):
            for env_name in ("AIRFLOW_API_BASE_URL", "AIRFLOW_API_TOKEN", "REDIS_URL"):
                app_module.os.environ.pop(env_name, None)

            with patch.object(app_module, "should_use_redis_cache", return_value=False):
                with patch.object(app_module, "evaluate_fleet_health") as evaluate_mock:
                    response = self.client.get("/failing-dags")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Failing DAG data is currently unavailable.", body)
        self.assertNotIn("Setup required", body)
        self.assertNotIn("AIRFLOW_API_BASE_URL", body)
        self.assertNotIn("AIRFLOW_API_TOKEN", body)
        evaluate_mock.assert_not_called()

    def test_health_endpoint_requires_monitor_token_when_configured(self):
        with patch.dict(app_module.os.environ, {"AIRFLOW_FLEET_MONITOR_TOKEN": "secret"}):
            response = self.client.get("/airflow-fleet-health")

        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
