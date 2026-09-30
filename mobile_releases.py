"""Published store builds, not uploads or releases waiting for review."""

import base64
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import requests

from fleet_health_cache import _get_redis_client

CLUSTER_API_URL = "https://cluster.apollos.app/api/config"
APPLE_API_URL = "https://api.appstoreconnect.apple.com/v1"
GOOGLE_API_URL = "https://androidpublisher.googleapis.com/androidpublisher/v3/applications"
TIMEOUT_SECONDS = 5
GOOGLE_TRACKS = {"android": "production", "androidtv": "tv:production"}
STORE_PLATFORMS = {"ios", *GOOGLE_TRACKS}
STORE_RELEASE_CACHE_SECONDS = 1800
STORE_QUOTA_BACKOFF_SECONDS = 3600
STORE_QUOTA_DETAIL = "Store API quota exceeded; retrying later"


class StoreQuotaExceeded(Exception):
    pass


def fetch_live_mobile_releases(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    candidates: dict[tuple[str, str], set[str]] = {}
    for row in rows:
        platform = str(row.get("apollos_platform") or "").lower()
        bundle = str(row.get("bundle_id") or "").strip().lower()
        if platform not in STORE_PLATFORMS or not re.fullmatch(r"[\w.-]+", bundle):
            continue
        churches = candidates.setdefault((platform, bundle), set())
        church = str(row.get("build_church") or row.get("church") or "")
        if re.fullmatch(r"[A-Za-z0-9_-]+", church):
            churches.add(church)
    if not os.getenv("APOLLOS_API_KEY") or not candidates:
        return {}

    # Older analytics may identify the selected church, not the app's build church.
    directory_targets: dict[tuple[str, str], set[str]] = {}
    for app_church in _fetch_app_churches():
        slug = app_church.get("slug") or ""
        if not isinstance(slug, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", slug):
            continue
        for platform in STORE_PLATFORMS:
            field = "appleBundleId" if platform == "ios" else "androidPkgId"
            directory_bundle = app_church.get(field)
            if not isinstance(directory_bundle, str):
                continue
            key = (platform, directory_bundle.lower())
            if key in candidates:
                candidates[key].add(slug)
                directory_targets.setdefault(key, set()).add(slug)

    def lookup(item):
        (platform, bundle), churches = item
        identity = {}
        targets = directory_targets.get((platform, bundle), set())
        if targets:
            identity = {
                "build_church": next(iter(targets)) if len(targets) == 1 else None,
                "deploy_target_count": len(targets),
            }
        release = _lookup_release(platform, bundle, churches)
        return (platform, bundle), {**(release or {}), **identity}

    with ThreadPoolExecutor(max_workers=min(16, len(candidates))) as executor:
        return dict(executor.map(lookup, candidates.items()))


def _lookup_release(platform: str, bundle: str, churches: set[str]) -> dict[str, Any] | None:
    client = _get_redis_client()
    cache_key = f"apps:store-release:v1:{platform}:{bundle}"
    if client is not None:
        try:
            raw = client.get(cache_key)
            if raw:
                cached = json.loads(raw)
                if isinstance(cached, dict) and (
                    "builds" in cached or cached.get("live_status_detail") == STORE_QUOTA_DETAIL
                ):
                    return cached
        except Exception:
            logging.warning("Store release cache read unavailable for %s %s", platform, bundle)
    for church in sorted(churches):
        release = _fetch_release(church, platform, bundle)
        if release is not None:
            if client is not None:
                ttl = (
                    STORE_QUOTA_BACKOFF_SECONDS
                    if release.get("live_status_detail") == STORE_QUOTA_DETAIL
                    else STORE_RELEASE_CACHE_SECONDS
                )
                try:
                    client.setex(cache_key, ttl, json.dumps(release))
                except Exception:
                    logging.warning(
                        "Store release cache write unavailable for %s %s", platform, bundle
                    )
            return release
    return None


def _fetch_app_churches() -> list[dict[str, Any]]:
    try:
        response = requests.post(
            "https://cluster.apollos.app/graphql",
            json={"query": "query { churches { slug appleBundleId androidPkgId } }"},
            headers={"x-api-key": os.environ["APOLLOS_API_KEY"]},
            timeout=TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("errors"):
            raise ValueError("App directory unavailable")
        churches = payload["data"]["churches"]
        if not isinstance(churches, list) or not all(isinstance(c, dict) for c in churches):
            raise ValueError("Invalid app directory")
        return churches
    except (requests.RequestException, KeyError, TypeError, ValueError) as exc:
        logging.warning("App directory unavailable (%s)", type(exc).__name__)
        return []


def _config(church: str, key: str) -> Any:
    response = requests.get(
        f"{CLUSTER_API_URL}/{church}/{key}",
        headers={"x-api-key": os.environ["APOLLOS_API_KEY"]},
        timeout=TIMEOUT_SECONDS,
        allow_redirects=False,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json().get("value")


def _fetch_release(church: str, platform: str, bundle: str) -> dict[str, Any] | None:
    try:
        key = "APP.APPLE_BUNDLE_ID" if platform == "ios" else "APP.ANDROID_PKG_ID"
        configured_bundle = _config(church, key)
        if not isinstance(configured_bundle, str) or configured_bundle.lower() != bundle.lower():
            return None  # Selected church is only a lookup hint, not app identity.
        builds = (
            _apple_builds(church, configured_bundle)
            if platform == "ios"
            else _android_builds(church, configured_bundle, GOOGLE_TRACKS[platform])
        )
        return {"builds": builds, "checked_at": time.time()}
    except StoreQuotaExceeded:
        logging.warning("Store API quota exceeded for %s %s", platform, bundle)
        return {"live_status_detail": STORE_QUOTA_DETAIL}
    except Exception as exc:
        # API errors can contain credentials; do not log exception text or response bodies.
        logging.warning(
            "Store lookup unavailable for %s %s (%s)", platform, bundle, type(exc).__name__
        )
        return None


def _apple_builds(church: str, bundle: str) -> list[dict[str, str]]:
    from google.auth import jwt
    from google.auth.crypt.es256 import ES256Signer

    encoded = _config(church, "APP.APPLE_API_KEY_B64")
    value = base64.b64decode(encoded).decode() if encoded else _config(church, "APP.APPLE_API_KEY")
    key = json.loads(value) if isinstance(value, str) else value
    pem = key["key"]
    if key.get("is_key_content_base64"):
        pem = base64.b64decode(pem).decode()
    token = jwt.encode(
        ES256Signer.from_string(pem, key_id=key["key_id"]),
        {
            "iss": key["issuer_id"],
            "iat": int(time.time()) - 10,
            "exp": int(time.time()) + 600,
            "aud": "appstoreconnect-v1",
        },
    ).decode()
    with requests.Session() as session:
        session.headers["Authorization"] = f"Bearer {token}"
        response = session.get(
            f"{APPLE_API_URL}/apps", params={"filter[bundleId]": bundle}, timeout=TIMEOUT_SECONDS
        )
        response.raise_for_status()
        apps = [app for app in response.json()["data"] if app["attributes"]["bundleId"] == bundle]
        if len(apps) != 1:
            raise ValueError("App identity is not unique")
        response = session.get(
            f"{APPLE_API_URL}/apps/{apps[0]['id']}/appStoreVersions",
            params={
                "filter[platform]": "IOS",
                # Legacy filter name; appVersionState calls this READY_FOR_DISTRIBUTION.
                "filter[appStoreState]": "READY_FOR_SALE",
                "include": "build",
                "limit": "200",
            },
            timeout=TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return _published_apple_builds(response.json())


def _published_apple_builds(payload: dict[str, Any]) -> list[dict[str, str]]:
    from packaging.version import Version

    versions = [
        version
        for version in payload["data"]
        if version["attributes"].get("appVersionState", version["attributes"].get("appStoreState"))
        in {"READY_FOR_DISTRIBUTION", "READY_FOR_SALE"}
    ]
    if payload.get("links", {}).get("next"):
        raise ValueError("Incomplete Apple release list")
    if not versions:
        return []
    latest = max(versions, key=lambda version: Version(version["attributes"]["versionString"]))
    build_id = latest["relationships"]["build"]["data"]["id"]
    build = next(
        item for item in payload["included"] if item["type"] == "builds" and item["id"] == build_id
    )
    return [
        {
            "native_build": build["attributes"]["version"],
            "native_version": latest["attributes"]["versionString"],
        }
    ]


def _android_builds(church: str, bundle: str, track: str) -> list[dict[str, str]]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2 import service_account

    info = json.loads(base64.b64decode(_config(church, "APP.GOOGLE_API_KEY_B64")))
    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/androidpublisher"]
    )
    with AuthorizedSession(credentials, refresh_timeout=TIMEOUT_SECONDS) as session:
        # Unlike edits.tracks, this read-only API distinguishes approved from published.
        response = session.get(
            f"{GOOGLE_API_URL}/{bundle}/tracks/{track}/releases", timeout=TIMEOUT_SECONDS
        )
        if response.status_code == 429 or (
            response.status_code == 403
            and response.json().get("error", {}).get("message")
            == "Listing releases quota exceeded."
        ):
            raise StoreQuotaExceeded
        response.raise_for_status()
        return _published_android_builds(response.json(), track)


def _published_android_builds(payload: dict[str, Any], track: str) -> list[dict[str, str]]:
    return [
        {"native_build": str(artifact["versionCode"])}
        for release in payload.get("releases", [])
        if release.get("track") == track
        and release.get("releaseLifecycleState") == "RELEASE_LIFECYCLE_STATE_PUBLISHED"
        for artifact in release["activeArtifacts"]
    ]
