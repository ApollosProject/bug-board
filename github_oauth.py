import base64
import hashlib
import hmac
import logging
import os
import re
import secrets
from datetime import timedelta
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

import requests
from flask import Flask, Response, current_app, redirect, render_template, request, session

GITHUB_AUTHORIZE_URL = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
GITHUB_API_URL = "https://api.github.com"
GITHUB_API_VERSION = "2026-03-10"
GITHUB_REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_GITHUB_ORG = "ApollosProject"

OAUTH_STATE_SESSION_KEY = "github_oauth_state"
OAUTH_VERIFIER_SESSION_KEY = "github_oauth_code_verifier"
OAUTH_NEXT_SESSION_KEY = "github_oauth_next"
AUTHENTICATED_LOGIN_SESSION_KEY = "github_login"
AUTHENTICATED_USER_ID_SESSION_KEY = "github_user_id"
AUTHENTICATED_ORG_SESSION_KEY = "github_org"

PUBLIC_ENDPOINTS = {
    "github_login",
    "github_oauth_callback",
    "github_logout",
    "healthz",
}


class GitHubOAuthError(Exception):
    """Raised when GitHub cannot complete the OAuth identity check."""


class GitHubOrgAccessDenied(Exception):
    """Raised when GitHub does not report an active organization membership."""


def _truthy(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _configured_callback_url() -> str:
    explicit_url = os.getenv("GITHUB_OAUTH_CALLBACK_URL", "").strip()
    if explicit_url:
        return explicit_url

    app_url = os.getenv("APP_URL", "").strip().rstrip("/")
    if app_url:
        return f"{app_url}/auth/github/callback"
    return ""


def _callback_url_is_safe(callback_url: str) -> bool:
    parsed = urlsplit(callback_url)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return False
    if parsed.scheme == "https" and parsed.netloc:
        return True
    return parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost"}


def _authentication_configuration_errors() -> list[str]:
    required_values = {
        "GITHUB_OAUTH_CLIENT_ID": current_app.config.get("GITHUB_OAUTH_CLIENT_ID"),
        "GITHUB_OAUTH_CLIENT_SECRET": current_app.config.get("GITHUB_OAUTH_CLIENT_SECRET"),
        "FLASK_SECRET_KEY": current_app.secret_key,
        "GITHUB_OAUTH_CALLBACK_URL or APP_URL": current_app.config.get("GITHUB_OAUTH_CALLBACK_URL"),
        "GITHUB_OAUTH_ORG": current_app.config.get("GITHUB_OAUTH_ORG"),
    }
    errors = [name for name, value in required_values.items() if not str(value or "").strip()]

    secret_key = str(current_app.secret_key or "")
    if secret_key and len(secret_key) < 32:
        errors.append("FLASK_SECRET_KEY must contain at least 32 characters")

    callback_url = str(current_app.config.get("GITHUB_OAUTH_CALLBACK_URL") or "")
    if callback_url and not _callback_url_is_safe(callback_url):
        errors.append("GITHUB_OAUTH_CALLBACK_URL must use HTTPS (or HTTP on localhost)")

    org = str(current_app.config.get("GITHUB_OAUTH_ORG") or "")
    if org and not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})", org):
        errors.append("GITHUB_OAUTH_ORG must be a valid GitHub organization name")

    return errors


def _render_auth_message(title: str, message: str, status: int, allow_retry: bool = False):
    response = render_template(
        "auth.html",
        title=title,
        message=message,
        allow_retry=allow_retry,
    )
    return response, status


def _safe_next_url(value: str | None) -> str:
    candidate = (value or "").strip()
    if not candidate.startswith("/") or candidate.startswith("//") or "\\" in candidate:
        return "/"

    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc or parsed.path.startswith("//"):
        return "/"
    return urlunsplit(("", "", parsed.path or "/", parsed.query, ""))


def _current_relative_url() -> str:
    relative_url = request.full_path
    return relative_url[:-1] if relative_url.endswith("?") else relative_url


def _authentication_return_url() -> str:
    if request.headers.get("HX-Request", "").lower() != "true":
        return _current_relative_url()

    current_url = request.headers.get("HX-Current-URL")
    if not current_url:
        return "/"
    parsed = urlsplit(current_url)
    relative_url = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
    return _safe_next_url(relative_url)


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _github_api_headers(access_token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "apollos-bug-board",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }


def _exchange_code_for_token(code: str, code_verifier: str, callback_url: str) -> str:
    try:
        response = requests.post(
            GITHUB_TOKEN_URL,
            headers={"Accept": "application/json"},
            data={
                "client_id": current_app.config["GITHUB_OAUTH_CLIENT_ID"],
                "client_secret": current_app.config["GITHUB_OAUTH_CLIENT_SECRET"],
                "code": code,
                "code_verifier": code_verifier,
                "redirect_uri": callback_url,
            },
            timeout=GITHUB_REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise GitHubOAuthError("GitHub token exchange failed") from exc

    access_token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(access_token, str) or not access_token:
        raise GitHubOAuthError("GitHub did not return an access token")
    return access_token


def _get_github_identity(access_token: str) -> tuple[str, int]:
    try:
        response = requests.get(
            f"{GITHUB_API_URL}/user",
            headers=_github_api_headers(access_token),
            timeout=GITHUB_REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise GitHubOAuthError("GitHub identity lookup failed") from exc

    login = payload.get("login") if isinstance(payload, dict) else None
    user_id = payload.get("id") if isinstance(payload, dict) else None
    if not isinstance(login, str) or not login or not isinstance(user_id, int):
        raise GitHubOAuthError("GitHub returned an invalid identity")
    return login, user_id


def _require_active_org_membership(access_token: str, org: str) -> None:
    try:
        response = requests.get(
            f"{GITHUB_API_URL}/user/memberships/orgs/{quote(org, safe='')}",
            headers=_github_api_headers(access_token),
            timeout=GITHUB_REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        if response.status_code in {403, 404}:
            raise GitHubOrgAccessDenied
        response.raise_for_status()
        payload = response.json()
    except GitHubOrgAccessDenied:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise GitHubOAuthError("GitHub organization membership lookup failed") from exc

    if not isinstance(payload, dict) or payload.get("state") != "active":
        raise GitHubOrgAccessDenied


def _has_authenticated_session(org: str) -> bool:
    login = session.get(AUTHENTICATED_LOGIN_SESSION_KEY)
    user_id = session.get(AUTHENTICATED_USER_ID_SESSION_KEY)
    session_org = session.get(AUTHENTICATED_ORG_SESSION_KEY)
    return (
        isinstance(login, str)
        and bool(login)
        and isinstance(user_id, int)
        and isinstance(session_org, str)
        and session_org.casefold() == org.casefold()
    )


def register_github_oauth(app: Flask) -> None:
    client_id = os.getenv("GITHUB_OAUTH_CLIENT_ID", "").strip()
    client_secret = os.getenv("GITHUB_OAUTH_CLIENT_SECRET", "").strip()
    callback_url = _configured_callback_url()
    oauth_enabled = _truthy(os.getenv("GITHUB_OAUTH_ENABLED")) or bool(client_id or client_secret)

    app.config.update(
        GITHUB_OAUTH_ENABLED=oauth_enabled,
        GITHUB_OAUTH_CLIENT_ID=client_id,
        GITHUB_OAUTH_CLIENT_SECRET=client_secret,
        GITHUB_OAUTH_CALLBACK_URL=callback_url,
        GITHUB_OAUTH_ORG=os.getenv("GITHUB_OAUTH_ORG", DEFAULT_GITHUB_ORG).strip()
        or DEFAULT_GITHUB_ORG,
        PERMANENT_SESSION_LIFETIME=timedelta(hours=8),
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=callback_url.startswith("https://"),
    )
    secret_key = os.getenv("FLASK_SECRET_KEY", "")
    if secret_key:
        app.secret_key = secret_key

    @app.before_request
    def require_github_org_membership():
        if not current_app.config.get("GITHUB_OAUTH_ENABLED"):
            return None
        if request.endpoint in PUBLIC_ENDPOINTS:
            return None

        configuration_errors = _authentication_configuration_errors()
        if configuration_errors:
            logging.error(
                "GitHub OAuth configuration error: %s",
                "; ".join(configuration_errors),
            )
            return _render_auth_message(
                "Authentication unavailable",
                "Bug Board authentication is not configured correctly.",
                503,
            )

        org = str(current_app.config["GITHUB_OAUTH_ORG"])
        if _has_authenticated_session(org):
            return None

        login_url = f"/login?{urlencode({'next': _authentication_return_url()})}"
        if request.headers.get("HX-Request", "").lower() == "true":
            response = Response(status=401)
            response.headers["HX-Redirect"] = login_url
            response.headers["Cache-Control"] = "private, no-store"
            return response
        return redirect(login_url)

    @app.after_request
    def prevent_authenticated_response_caching(response: Response):
        if current_app.config.get("GITHUB_OAUTH_ENABLED") and request.endpoint != "healthz":
            response.headers["Cache-Control"] = "private, no-store"
        return response

    @app.get("/login")
    def github_login():
        configuration_errors = _authentication_configuration_errors()
        if configuration_errors:
            logging.error(
                "GitHub OAuth configuration error: %s",
                "; ".join(configuration_errors),
            )
            return _render_auth_message(
                "Authentication unavailable",
                "Bug Board authentication is not configured correctly.",
                503,
            )

        state = secrets.token_urlsafe(32)
        code_verifier = secrets.token_urlsafe(64)
        callback_url = str(current_app.config["GITHUB_OAUTH_CALLBACK_URL"])
        session.clear()
        session.permanent = True
        session[OAUTH_STATE_SESSION_KEY] = state
        session[OAUTH_VERIFIER_SESSION_KEY] = code_verifier
        session[OAUTH_NEXT_SESSION_KEY] = _safe_next_url(request.args.get("next"))

        params = {
            "allow_signup": "false",
            "client_id": current_app.config["GITHUB_OAUTH_CLIENT_ID"],
            "code_challenge": _pkce_challenge(code_verifier),
            "code_challenge_method": "S256",
            "prompt": "select_account",
            "redirect_uri": callback_url,
            "scope": "read:org",
            "state": state,
        }
        return redirect(f"{GITHUB_AUTHORIZE_URL}?{urlencode(params)}")

    @app.get("/auth/github/callback")
    def github_oauth_callback():
        configuration_errors = _authentication_configuration_errors()
        if configuration_errors:
            logging.error(
                "GitHub OAuth configuration error: %s",
                "; ".join(configuration_errors),
            )
            return _render_auth_message(
                "Authentication unavailable",
                "Bug Board authentication is not configured correctly.",
                503,
            )

        expected_state = session.pop(OAUTH_STATE_SESSION_KEY, None)
        code_verifier = session.pop(OAUTH_VERIFIER_SESSION_KEY, None)
        next_url = _safe_next_url(session.pop(OAUTH_NEXT_SESSION_KEY, None))
        received_state = request.args.get("state")
        state_is_valid = (
            isinstance(expected_state, str)
            and isinstance(received_state, str)
            and hmac.compare_digest(expected_state, received_state)
        )
        if not state_is_valid or not isinstance(code_verifier, str):
            session.clear()
            return _render_auth_message(
                "Sign-in failed",
                "The GitHub sign-in request expired or could not be verified. Please try again.",
                400,
                allow_retry=True,
            )

        code = request.args.get("code")
        if request.args.get("error") or not code:
            session.clear()
            return _render_auth_message(
                "Sign-in canceled",
                "GitHub authorization was not completed.",
                403,
                allow_retry=True,
            )

        try:
            access_token = _exchange_code_for_token(
                code,
                code_verifier,
                str(current_app.config["GITHUB_OAUTH_CALLBACK_URL"]),
            )
            login, user_id = _get_github_identity(access_token)
            org = str(current_app.config["GITHUB_OAUTH_ORG"])
            _require_active_org_membership(access_token, org)
        except GitHubOrgAccessDenied:
            session.clear()
            return _render_auth_message(
                "Access denied",
                f"Bug Board is available only to active members of the {org} GitHub organization.",
                403,
                allow_retry=True,
            )
        except GitHubOAuthError:
            logging.exception("GitHub OAuth sign-in failed")
            session.clear()
            return _render_auth_message(
                "Sign-in unavailable",
                "GitHub could not complete sign-in. Please try again.",
                502,
                allow_retry=True,
            )

        session.clear()
        session.permanent = True
        session[AUTHENTICATED_LOGIN_SESSION_KEY] = login
        session[AUTHENTICATED_USER_ID_SESSION_KEY] = user_id
        session[AUTHENTICATED_ORG_SESSION_KEY] = org
        return redirect(next_url)

    @app.get("/logout")
    def github_logout():
        if not current_app.config.get("GITHUB_OAUTH_ENABLED"):
            return redirect("/")
        configuration_errors = _authentication_configuration_errors()
        if configuration_errors:
            logging.error(
                "GitHub OAuth configuration error: %s",
                "; ".join(configuration_errors),
            )
            return _render_auth_message(
                "Authentication unavailable",
                "Bug Board authentication is not configured correctly.",
                503,
            )
        session.clear()
        return _render_auth_message(
            "Signed out",
            "Your Bug Board session has ended.",
            200,
            allow_retry=True,
        )
