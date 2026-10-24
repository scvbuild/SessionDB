"""Download a cloud Claude session's events straight from the claude.ai API.

The endpoint and headers are the ones Claude Code itself uses to read cloud session
history; they are not a documented public API, so every unexpected response raises
CloudFetchError instead of returning partial data.
"""

from __future__ import annotations

import json
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

API_BASE = "https://api.anthropic.com"
KEYCHAIN_SERVICE = "Claude Code-credentials"
PAGE_LIMIT = 100
MAX_PAGES = 1000
MAX_LIST_PAGES = 50
# python.org Python ships without a CA bundle unless "Install Certificates" was run.
MACOS_CA_BUNDLE = "/etc/ssl/cert.pem"

Opener = Callable[[urllib.request.Request], Any]


class CloudFetchError(RuntimeError):
    pass


def credentials(
    claude_json: Path = Path.home() / ".claude.json",
    run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> tuple[str, str]:
    """Return (access token, organization UUID) from the Claude Code sign-in on this Mac."""
    result = run(["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"],
                 capture_output=True, text=True)
    if result.returncode != 0:
        raise CloudFetchError(f"Claude Code sign-in not found in the Keychain ({KEYCHAIN_SERVICE})")
    try:
        token = json.loads(result.stdout)["claudeAiOauth"]["accessToken"]
        organization = json.loads(claude_json.read_text(encoding="utf-8"))["oauthAccount"]["organizationUuid"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise CloudFetchError(f"Claude Code sign-in is not in the expected format: {type(exc).__name__}") from None
    if not isinstance(token, str) or not token or not isinstance(organization, str) or not organization:
        raise CloudFetchError("Claude Code sign-in is not in the expected format")
    return token, organization


def default_opener() -> Opener:
    context = ssl.create_default_context(cafile=MACOS_CA_BUNDLE if Path(MACOS_CA_BUNDLE).exists() else None)
    return lambda request: urllib.request.urlopen(request, timeout=30, context=context)


def _get_json(url: str, headers: dict[str, str], opener: Opener) -> Any:
    try:
        with opener(urllib.request.Request(url, headers=headers)) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read()[:300].decode("utf-8", "replace")
        hint = " (sign-in expired? open Claude Code to renew it)" if exc.code == 401 else ""
        raise CloudFetchError(f"HTTP {exc.code} from {url.split('?')[0]}{hint}: {detail}") from None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise CloudFetchError(f"network error: {exc}") from None
    except ValueError:
        raise CloudFetchError(f"response from {url.split('?')[0]} was not JSON") from None


def _headers(token: str, organization: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}", "anthropic-version": "2023-06-01",
        "anthropic-beta": "ccr-byoc-2025-07-29", "x-organization-uuid": organization,
    }


def list_sessions(token: str, organization: str, opener: Opener | None = None) -> list[dict[str, Any]]:
    """Return every cloud session record of the account (no ordering is assumed)."""
    opener = opener or default_opener()
    sessions: dict[str, dict[str, Any]] = {}
    cursor = None
    for _ in range(MAX_LIST_PAGES):
        # Without these the server omits routine runs and sessions hidden by tag.
        query = {"limit": str(PAGE_LIMIT), "include_trigger_sessions": "true", "exclude_tags": "-",
                 **({"cursor": cursor} if cursor else {})}
        page = _get_json(f"{API_BASE}/v1/code/sessions?{urllib.parse.urlencode(query)}",
                         _headers(token, organization), opener)
        items = page.get("data") if isinstance(page, dict) else None
        if not isinstance(items, list):
            raise CloudFetchError("sessions page has no data list")
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                raise CloudFetchError("session record without an id")
            sessions[item["id"]] = item
        cursor = page.get("next_cursor")
        if not items or not cursor:
            return list(sessions.values())
    raise CloudFetchError(f"more than {MAX_LIST_PAGES} pages of sessions")


def fetch_session_events(
    session_id: str, token: str, organization: str, opener: Opener | None = None,
) -> list[dict[str, Any]]:
    """Return every event of a cloud session, oldest first."""
    opener = opener or default_opener()
    headers = _headers(token, organization)
    path = f"{API_BASE}/v1/code/sessions/{urllib.parse.quote(session_id, safe='')}/events"
    events: dict[int, dict[str, Any]] = {}
    cursor = None
    for _ in range(MAX_PAGES):
        query = {"limit": str(PAGE_LIMIT), "sort_order": "desc", **({"cursor": cursor} if cursor else {})}
        page = _get_json(f"{path}?{urllib.parse.urlencode(query)}", headers, opener)
        items = page.get("data") if isinstance(page, dict) else None
        if not isinstance(items, list):
            raise CloudFetchError("events page has no data list")
        for item in items:
            try:
                events[int(item["sequence_num"])] = item
            except (KeyError, TypeError, ValueError):
                raise CloudFetchError("event without a numeric sequence_num") from None
        cursor = page.get("next_cursor")
        if not items or not cursor:
            break
    else:
        raise CloudFetchError(f"more than {MAX_PAGES} pages of events")
    if not events:
        raise CloudFetchError(f"cloud session {session_id} has no events")
    return [events[number] for number in sorted(events)]


def events_to_export(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reshape API events into the claude-code-remote list_events export form.

    list_events lifts an event's uuid out of its payload, except for control events, and
    drops the payload's own type; matching it keeps imports of either source identical.
    """
    exported = []
    for event in events:
        kind, payload = event.get("event_type"), event.get("payload")
        if not isinstance(kind, str) or not kind or not isinstance(payload, dict) \
                or not isinstance(event.get("created_at"), str):
            raise CloudFetchError(f"event {event.get('sequence_num')} lacks event_type, payload or created_at")
        inner = {key: value for key, value in payload.items() if key != "type"}
        body: dict[str, Any] = {}
        if inner.get("uuid") and not kind.startswith("control_"):
            body["uuid"] = inner.pop("uuid")
        body["internal_anthropic_catchall"] = inner
        exported.append({"created_at": event["created_at"], kind: body})
    return exported
