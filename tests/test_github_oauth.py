import base64
import hashlib
import os
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from flask import Flask

import app as app_module
import github_oauth


class GitHubOAuthTest(unittest.TestCase):
    def setUp(self):
        self.config_patch = patch.dict(
            app_module.app.config,
            {
                "GITHUB_OAUTH_ENABLED": True,
                "GITHUB_OAUTH_CLIENT_ID": "oauth-client-id",
                "GITHUB_OAUTH_CLIENT_SECRET": "oauth-client-secret",
                "GITHUB_OAUTH_CALLBACK_URL": ("https://bug-board.example/auth/github/callback"),
                "GITHUB_OAUTH_ORG": "ApollosProject",
                "SECRET_KEY": "test-secret-key-with-at-least-32-characters",
                "SESSION_COOKIE_SECURE": False,
            },
            clear=False,
        )
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)
        self.client = app_module.app.test_client()

    @staticmethod
    def _response(status_code, payload):
        response = Mock(status_code=status_code)
        response.json.return_value = payload
        if status_code >= 400:
            response.raise_for_status.side_effect = github_oauth.requests.HTTPError()
        return response

    def _seed_oauth_session(self, next_url="/"):
        with self.client.session_transaction() as oauth_session:
            oauth_session[github_oauth.OAUTH_STATE_SESSION_KEY] = "expected-state"
            oauth_session[github_oauth.OAUTH_VERIFIER_SESSION_KEY] = "v" * 64
            oauth_session[github_oauth.OAUTH_NEXT_SESSION_KEY] = next_url

    def test_protected_routes_redirect_to_login_but_health_remains_public(self):
        response = self.client.get("/projects?days=30")

        self.assertEqual(response.status_code, 302)
        login_url = urlsplit(response.location)
        self.assertEqual(login_url.path, "/login")
        self.assertEqual(parse_qs(login_url.query)["next"], ["/projects?days=30"])

        static_response = self.client.get("/static/resources/engineering-expectations.pdf")
        self.assertEqual(static_response.status_code, 302)
        self.assertEqual(urlsplit(static_response.location).path, "/login")

        health_response = self.client.get("/healthz")
        self.assertEqual(health_response.status_code, 200)
        self.assertEqual(health_response.get_json(), {"status": "ok"})

    def test_htmx_requests_receive_a_full_page_redirect_instruction(self):
        response = self.client.get(
            "/partials/index/leaderboard",
            headers={
                "HX-Request": "true",
                "HX-Current-URL": "https://bug-board.example/?days=30",
            },
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(
            response.headers["HX-Redirect"],
            "/login?next=%2F%3Fdays%3D30",
        )

    def test_login_uses_state_pkce_and_a_safe_return_path(self):
        verifier = "v" * 64
        with patch.object(
            github_oauth.secrets,
            "token_urlsafe",
            side_effect=["random-state", verifier],
        ):
            response = self.client.get("/login?next=https://attacker.example")

        self.assertEqual(response.status_code, 302)
        authorization_url = urlsplit(response.location)
        self.assertEqual(
            f"{authorization_url.scheme}://{authorization_url.netloc}{authorization_url.path}",
            github_oauth.GITHUB_AUTHORIZE_URL,
        )
        params = parse_qs(authorization_url.query)
        expected_challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        self.assertEqual(params["state"], ["random-state"])
        self.assertEqual(params["scope"], ["read:org"])
        self.assertEqual(params["code_challenge"], [expected_challenge])
        self.assertEqual(params["code_challenge_method"], ["S256"])
        self.assertEqual(
            params["redirect_uri"],
            ["https://bug-board.example/auth/github/callback"],
        )

        with self.client.session_transaction() as oauth_session:
            self.assertEqual(oauth_session[github_oauth.OAUTH_STATE_SESSION_KEY], "random-state")
            self.assertEqual(oauth_session[github_oauth.OAUTH_VERIFIER_SESSION_KEY], verifier)
            self.assertEqual(oauth_session[github_oauth.OAUTH_NEXT_SESSION_KEY], "/")

    def test_callback_verifies_identity_and_active_org_membership(self):
        self._seed_oauth_session("/projects?days=30")
        token_response = self._response(200, {"access_token": "temporary-token"})
        identity_response = self._response(200, {"login": "octocat", "id": 123})
        membership_response = self._response(200, {"state": "active"})

        with (
            patch.object(
                github_oauth.requests,
                "post",
                return_value=token_response,
                create=True,
            ) as post,
            patch.object(
                github_oauth.requests,
                "get",
                side_effect=[identity_response, membership_response],
                create=True,
            ) as get,
        ):
            response = self.client.get(
                "/auth/github/callback?code=temporary-code&state=expected-state"
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/projects?days=30")
        self.assertEqual(post.call_args.kwargs["data"]["code_verifier"], "v" * 64)
        self.assertEqual(
            post.call_args.kwargs["data"]["redirect_uri"],
            "https://bug-board.example/auth/github/callback",
        )
        self.assertEqual(get.call_args_list[0].args[0], "https://api.github.com/user")
        self.assertEqual(
            get.call_args_list[1].args[0],
            "https://api.github.com/user/memberships/orgs/ApollosProject",
        )
        self.assertEqual(
            get.call_args_list[1].kwargs["headers"]["Authorization"],
            "Bearer temporary-token",
        )

        with self.client.session_transaction() as authenticated_session:
            self.assertEqual(authenticated_session["github_login"], "octocat")
            self.assertEqual(authenticated_session["github_user_id"], 123)
            self.assertEqual(authenticated_session["github_org"], "ApollosProject")
            self.assertNotIn("access_token", authenticated_session)
            self.assertNotIn(github_oauth.OAUTH_STATE_SESSION_KEY, authenticated_session)
            self.assertNotIn(github_oauth.OAUTH_VERIFIER_SESSION_KEY, authenticated_session)

        protected_response = self.client.get("/projects")
        self.assertEqual(protected_response.status_code, 200)

    def test_callback_rejects_non_members_without_authenticating_them(self):
        self._seed_oauth_session()
        token_response = self._response(200, {"access_token": "temporary-token"})
        identity_response = self._response(200, {"login": "octocat", "id": 123})
        membership_response = self._response(404, {"message": "Not Found"})

        with (
            patch.object(
                github_oauth.requests,
                "post",
                return_value=token_response,
                create=True,
            ),
            patch.object(
                github_oauth.requests,
                "get",
                side_effect=[identity_response, membership_response],
                create=True,
            ),
        ):
            response = self.client.get(
                "/auth/github/callback?code=temporary-code&state=expected-state"
            )

        self.assertEqual(response.status_code, 403)
        self.assertIn("active members of the ApollosProject", response.get_data(as_text=True))
        with self.client.session_transaction() as rejected_session:
            self.assertNotIn("github_login", rejected_session)

    def test_callback_rejects_invalid_state_before_contacting_github(self):
        self._seed_oauth_session()

        with (
            patch.object(github_oauth.requests, "post", create=True) as post,
            patch.object(github_oauth.requests, "get", create=True) as get,
        ):
            response = self.client.get(
                "/auth/github/callback?code=temporary-code&state=wrong-state"
            )

        self.assertEqual(response.status_code, 400)
        post.assert_not_called()
        get.assert_not_called()

    def test_partially_configured_oauth_fails_closed(self):
        with patch.dict(
            app_module.app.config,
            {"GITHUB_OAUTH_CLIENT_SECRET": "", "SECRET_KEY": None},
            clear=False,
        ):
            response = self.client.get("/")

        self.assertEqual(response.status_code, 503)
        self.assertIn("Authentication unavailable", response.get_data(as_text=True))

    def test_existing_app_url_does_not_enable_oauth_by_itself(self):
        with patch.dict(os.environ, {"APP_URL": "https://bug-board.example"}, clear=True):
            test_app = Flask("oauth-disabled-test")
            github_oauth.register_github_oauth(test_app)

        self.assertFalse(test_app.config["GITHUB_OAUTH_ENABLED"])
        self.assertEqual(
            test_app.config["GITHUB_OAUTH_CALLBACK_URL"],
            "https://bug-board.example/auth/github/callback",
        )


if __name__ == "__main__":
    unittest.main()
