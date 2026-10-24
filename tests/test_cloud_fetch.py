import io
import json
import subprocess
import tempfile
import unittest
import urllib.error
import urllib.parse
from pathlib import Path

import cloud_fetch
import full_corpus


def api_event(seq, kind, payload, created_at=None):
    return {"sequence_num": str(seq), "event_type": kind, "event_id": f"e{seq}",
            "created_at": created_at or f"2026-10-21T07:58:{seq:02d}Z", "source": "worker",
            "payload": {"type": kind, **payload}}


API_EVENTS = [
    api_event(1, "control_request", {"uuid": "c1", "request": {"subtype": "set_permission_mode"}}),
    api_event(2, "user", {"uuid": "u1", "message": {"role": "user", "content": "fix the button"}}),
    api_event(3, "assistant", {"uuid": "a1", "message": {"role": "assistant", "content": [
        {"type": "tool_use", "id": "t1", "name": "post_widget", "input": {}}]}}),
    api_event(10, "result", {"uuid": "r1", "subtype": "success", "usage": {"input_tokens": 3}}),
]


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def pager(pages):
    """Serve pages keyed by cursor (None for the first request) and record each URL."""
    calls = []

    def opener(request):
        calls.append(request)
        cursor = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query).get("cursor", [None])[0]
        page = pages[cursor]
        if isinstance(page, Exception):
            raise page
        return Response(page if isinstance(page, bytes) else json.dumps(page).encode())
    return opener, calls


class FetchTests(unittest.TestCase):
    def test_pages_are_followed_and_returned_oldest_first(self):
        # Newest first, as the API serves it; "10" must sort after "3", not before.
        opener, calls = pager({
            None: {"data": [API_EVENTS[3], API_EVENTS[2]], "next_cursor": "p2"},
            "p2": {"data": [API_EVENTS[1], API_EVENTS[0]], "next_cursor": "p3"},
            "p3": {"data": [], "next_cursor": "p4"},
        })
        events = cloud_fetch.fetch_session_events("cse_x", "tok", "org", opener)
        self.assertEqual([e["sequence_num"] for e in events], ["1", "2", "3", "10"])
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0].get_header("Authorization"), "Bearer tok")
        self.assertEqual(calls[0].get_header("X-organization-uuid"), "org")

    def test_http_and_network_failures_raise_clear_errors(self):
        expired = urllib.error.HTTPError("u", 401, "Unauthorized", {}, io.BytesIO(b'{"error":"expired"}'))
        cases = {
            "sign-in expired": {None: expired},
            "network error": {None: urllib.error.URLError("offline")},
            "not JSON": {None: b"<html>"},
            "no data list": {None: {"items": []}},
            "sequence_num": {None: {"data": [{"event_type": "user"}]}},
            "no events": {None: {"data": []}},
        }
        for message, pages in cases.items():
            opener, _ = pager(pages)
            with self.subTest(message), self.assertRaisesRegex(cloud_fetch.CloudFetchError, message):
                cloud_fetch.fetch_session_events("cse_x", "tok", "org", opener)

    def test_a_failure_on_a_later_page_returns_nothing_partial(self):
        opener, _ = pager({None: {"data": [API_EVENTS[3]], "next_cursor": "p2"},
                           "p2": urllib.error.URLError("dropped")})
        with self.assertRaises(cloud_fetch.CloudFetchError):
            cloud_fetch.fetch_session_events("cse_x", "tok", "org", opener)

    def test_export_shape_matches_list_events(self):
        exported = cloud_fetch.events_to_export(API_EVENTS[:2])
        self.assertEqual(exported[0], {"created_at": API_EVENTS[0]["created_at"], "control_request": {
            "internal_anthropic_catchall": {"uuid": "c1", "request": {"subtype": "set_permission_mode"}}}})
        self.assertEqual(exported[1], {"created_at": API_EVENTS[1]["created_at"], "user": {
            "uuid": "u1", "internal_anthropic_catchall": {"message": {"role": "user", "content": "fix the button"}}}})
        with self.assertRaises(cloud_fetch.CloudFetchError):
            cloud_fetch.events_to_export([{"sequence_num": "1", "event_type": "user", "payload": "x"}])


class CredentialTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.claude_json = Path(self.tmp.name) / ".claude.json"
        self.claude_json.write_text(json.dumps({"oauthAccount": {"organizationUuid": "org"}}))

    def keychain(self, returncode, stdout=""):
        return lambda *a, **k: subprocess.CompletedProcess(a, returncode, stdout, "")

    def test_reads_token_and_organization(self):
        run = self.keychain(0, json.dumps({"claudeAiOauth": {"accessToken": "tok"}}))
        self.assertEqual(cloud_fetch.credentials(self.claude_json, run), ("tok", "org"))

    def test_missing_or_malformed_sign_in_fails_without_echoing_it(self):
        for run in (self.keychain(44), self.keychain(0, "not json"), self.keychain(0, '{"other": "secret"}')):
            with self.assertRaises(cloud_fetch.CloudFetchError) as caught:
                cloud_fetch.credentials(self.claude_json, run)
            self.assertNotIn("secret", str(caught.exception))
        self.claude_json.write_text("{}")
        with self.assertRaises(cloud_fetch.CloudFetchError):
            cloud_fetch.credentials(self.claude_json, self.keychain(0, json.dumps({"claudeAiOauth": {"accessToken": "t"}})))


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def sync(self, events, **kwargs):
        return full_corpus.sync_cloud_session("cse_x", "proj", "Title", root=self.root,
                                              fetch=lambda sid: events, **kwargs)

    def test_a_later_download_only_appends(self):
        first = Path(self.sync(API_EVENTS[:3])["path"]).read_bytes()
        result = self.sync(API_EVENTS)
        self.assertTrue(Path(result["path"]).read_bytes().startswith(first))
        self.assertFalse(self.sync(API_EVENTS)["changed"])

    def test_matches_an_import_of_the_list_events_export(self):
        via_api = Path(self.sync(API_EVENTS)["path"]).read_bytes()
        export = self.root / "export.json"
        export.write_text(json.dumps(cloud_fetch.events_to_export(API_EVENTS)))
        other = self.root / "other"
        via_export = Path(full_corpus.import_cloud_export(export, "cse_x", "proj", "Title", root=other)["path"])
        self.assertEqual(via_api, via_export.read_bytes())

    def test_fetch_failure_writes_nothing(self):
        def broken(sid):
            raise cloud_fetch.CloudFetchError("network error: offline")
        with self.assertRaises(cloud_fetch.CloudFetchError):
            full_corpus.sync_cloud_session("cse_x", "proj", root=self.root, fetch=broken)
        self.assertEqual(list(self.root.iterdir()), [])


    def test_session_listing_follows_pages_and_rejects_bad_records(self):
        opener, calls = pager({None: {"data": [{"id": "cse_a"}], "next_cursor": "p2"},
                               "p2": {"data": [{"id": "cse_b"}], "next_cursor": None}})
        self.assertEqual([s["id"] for s in cloud_fetch.list_sessions("t", "o", opener)], ["cse_a", "cse_b"])
        query = urllib.parse.parse_qs(urllib.parse.urlparse(calls[0].full_url).query)
        self.assertEqual((query["include_trigger_sessions"], query["exclude_tags"]), (["true"], ["-"]))
        opener, _ = pager({None: {"data": [{"title": "no id"}]}})
        with self.assertRaisesRegex(cloud_fetch.CloudFetchError, "without an id"):
            cloud_fetch.list_sessions("t", "o", opener)


def cloud_session(sid, last="2026-10-22T04:00:00Z", kind="anthropic_cloud", worker="idle", title="T"):
    return {"id": sid, "environment_kind": kind, "worker_status": worker, "last_event_at": last, "title": title}


class SyncAllTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "cloud-threads"
        self.state = Path(self.tmp.name) / "cloud-sync" / "state.json"
        self.fetched = []

    def fetch(self, sid):
        self.fetched.append(sid)
        if sid == "cse_bad":
            raise cloud_fetch.CloudFetchError("HTTP 500")
        return API_EVENTS

    def run_sync(self, sessions, **kwargs):
        return full_corpus.sync_all_cloud_sessions(
            self.root, self.state, list_sessions=lambda: sessions, fetch=self.fetch, **kwargs)

    def test_only_finished_cloud_sessions_that_changed_are_downloaded(self):
        sessions = [cloud_session("cse_a"), cloud_session("cse_local", kind="bridge"),
                    cloud_session("cse_busy", worker="running")]
        report = self.run_sync(sessions)
        self.assertEqual(report["imported"], ["cse_a"])
        self.assertEqual(report["skipped_kinds"], {"bridge": 1})
        self.assertEqual(report["busy"], ["cse_busy"])
        self.assertTrue((self.root / full_corpus.CLOUD_DEFAULT_PROJECT / "cse_a.jsonl").exists())
        self.fetched.clear()
        self.assertEqual(self.run_sync(sessions)["unchanged"], 1)
        self.assertEqual(self.fetched, [])
        self.run_sync([cloud_session("cse_a", last="2026-10-22T05:00:00Z")])
        self.assertEqual(self.fetched, ["cse_a"])

    def test_one_failing_session_does_not_stop_the_others_and_is_retried(self):
        report = self.run_sync([cloud_session("cse_bad"), cloud_session("cse_a")])
        self.assertFalse(report["complete"])
        self.assertEqual(report["imported"], ["cse_a"])
        self.assertIn("HTTP 500", report["failed"]["cse_bad"])
        self.fetched.clear()
        self.run_sync([cloud_session("cse_bad"), cloud_session("cse_a")])
        self.assertEqual(self.fetched, ["cse_bad"])

    def test_an_imported_session_keeps_its_folder_and_title(self):
        earlier = full_corpus.sync_cloud_session("cse_a", "test-project", "Old title", root=self.root,
                                                 fetch=lambda sid: API_EVENTS[:3])
        report = self.run_sync([cloud_session("cse_a", title="New title")])
        self.assertTrue(report["complete"], report)
        saved = Path(earlier["path"])
        self.assertEqual(list(self.root.glob("*/cse_a.jsonl")), [saved])
        self.assertEqual(json.loads(saved.read_text().splitlines()[0])["aiTitle"], "Old title")

    def test_dry_run_writes_nothing(self):
        report = self.run_sync([cloud_session("cse_a")], dry_run=True)
        self.assertEqual(report["imported"], ["cse_a"])
        self.assertEqual(self.fetched, [])
        self.assertFalse(self.root.exists())
        self.assertFalse(self.state.exists())

    def test_a_listing_failure_or_corrupt_state_fails_the_run_clearly(self):
        def down():
            raise cloud_fetch.CloudFetchError("network error: offline")
        with self.assertRaises(cloud_fetch.CloudFetchError):
            full_corpus.sync_all_cloud_sessions(self.root, self.state, list_sessions=down, fetch=self.fetch)
        self.state.parent.mkdir(parents=True, exist_ok=True)
        self.state.write_text("{broken")
        with self.assertRaisesRegex(ValueError, "not valid JSON"):
            self.run_sync([cloud_session("cse_a")])
        self.assertEqual(self.fetched, [])

    def test_a_second_run_while_one_holds_the_lock_stops(self):
        import fcntl
        self.state.parent.mkdir(parents=True)
        with open(self.state.with_suffix(".lock"), "a") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            report = self.run_sync([cloud_session("cse_a")])
        self.assertEqual(report["error"], "another cloud sync is running")
        self.assertEqual(self.fetched, [])

if __name__ == "__main__":
    unittest.main()
