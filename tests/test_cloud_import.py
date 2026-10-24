import json
import tempfile
import unittest
from pathlib import Path

import full_corpus
import sessiondb


def catchall(**fields):
    return {"uuid": fields.pop("uuid"), "internal_anthropic_catchall": fields}


EVENTS = [
    {"created_at": "2026-10-21T07:58:51Z", "control_request": {
        "internal_anthropic_catchall": {"request": {"subtype": "set_permission_mode"}}}},
    {"created_at": "2026-10-21T07:58:52Z", "system": catchall(
        uuid="s1", session_id="x", subtype="init", model="claude-test")},
    {"created_at": "2026-10-21T07:58:53Z", "user": catchall(
        uuid="u1", message={"role": "user", "content": "please fix the Decline button"})},
    {"created_at": "2026-10-21T07:58:54Z", "assistant": catchall(
        uuid="a1", session_id="x", timestamp="2026-10-21T07:58:54.5Z", message={
            "role": "assistant", "model": "claude-test",
            "content": [{"type": "tool_use", "id": "t1", "name": "post_widget", "input": {"title": "card"}}],
            "usage": {"input_tokens": 10, "output_tokens": 5}})},
    {"created_at": "2026-10-21T07:58:55Z", "user": catchall(
        uuid="u2", tool_use_result={"ok": True}, message={"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "posted"}]})},
    {"created_at": "2026-10-21T07:58:56Z", "result": catchall(
        uuid="r1", subtype="success", usage={"input_tokens": 10}, result="done")},
    {"created_at": "2026-10-21T07:58:57Z", "rate_limit_event": {"rate_limit_info": {"status": "allowed"}}},
]


class CloudImportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cloud = self.root / "sessiondb" / sessiondb.CLOUD_THREADS_DIR_NAME

    def export(self, events, name="export.json"):
        path = self.root / name
        path.write_text(json.dumps(events))
        return path

    def run_import(self, events, **kwargs):
        return full_corpus.import_cloud_export(
            self.export(events), "cse_test", "cloud-proj", "Card panels", root=self.cloud, **kwargs)

    def test_every_converted_row_is_supported_and_searchable(self):
        result = self.run_import(EVENTS)
        self.assertEqual(result["rows"], len(EVENTS) + 1)
        db_path = self.root / "db.sqlite"
        old_roots = full_corpus.ROOTS
        full_corpus.ROOTS = {"claude": (self.cloud,)}
        try:
            report = full_corpus.fast_sync(db_path, self.root / "manifest.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        db = sessiondb.connect(db_path, read_only=True)
        try:
            rows = db.execute("SELECT supported,project,session_id,tool FROM events").fetchall()
        finally:
            db.close()
        self.assertEqual(len(rows), len(EVENTS) + 1)
        self.assertTrue(all(row["supported"] for row in rows))
        self.assertEqual({row["project"] for row in rows}, {"cloud-proj"})
        self.assertEqual({row["session_id"] for row in rows}, {"cse_test"})
        self.assertIn("post_widget", {row["tool"] for row in rows})

    def test_a_longer_export_only_appends(self):
        first = Path(self.run_import(EVENTS[:4])["path"]).read_bytes()
        result = self.run_import(EVENTS)
        self.assertTrue(result["changed"])
        self.assertTrue(Path(result["path"]).read_bytes().startswith(first))
        self.assertFalse(self.run_import(EVENTS)["changed"])

    def test_a_truncated_export_is_refused_unless_replaced(self):
        path = Path(self.run_import(EVENTS)["path"])
        saved = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "--replace"):
            self.run_import(EVENTS[:3])
        self.assertEqual(path.read_bytes(), saved)
        self.assertTrue(self.run_import(EVENTS[:3], replace=True)["changed"])

    def test_malformed_exports_fail_clearly_and_write_nothing(self):
        for events in ([], {"events": EVENTS}, [{"created_at": "t", "user": {}, "assistant": {}}],
                       [{"user": {}}]):
            with self.subTest(events=events), self.assertRaises(ValueError):
                self.run_import(events)
        broken = self.root / "broken.json"
        broken.write_text('[{"created_at": "t", "user"')
        with self.assertRaises(ValueError):
            full_corpus.import_cloud_export(broken, "cse_test", "cloud-proj", root=self.cloud)
        self.assertFalse(self.cloud.exists())

    def test_unsafe_names_are_refused(self):
        for session, project in (("../x", "p"), ("cse_test", "a/b"), ("", "p")):
            with self.subTest(session=session, project=project), self.assertRaises(ValueError):
                full_corpus.import_cloud_export(self.export(EVENTS), session, project, root=self.cloud)

    def test_project_comes_from_the_folder_under_cloud_threads(self):
        for namespace in ("sessiondb", "sessiondb-public"):
            path = Path(f"/synthetic/.local/share/{namespace}/cloud-threads/cloud-proj/cse_1.jsonl")
            self.assertEqual(sessiondb.project_from_path(path), "cloud-proj")
        self.assertIsNone(sessiondb.project_from_path(Path("/synthetic/other/cloud-threads/project/file.jsonl")))


if __name__ == "__main__":
    unittest.main()
