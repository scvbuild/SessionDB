"""Rebuild into a verified copy, then promote it with a rollback copy kept."""

import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import full_corpus
import rebuild_copy
import sessiondb
import token_usage

FIXTURES = Path(__file__).parent / "fixtures" / "token_usage"
GOLDEN = json.loads((FIXTURES / "golden.json").read_text(encoding="utf-8"))


def token_dump(db):
    return {table: sorted(tuple(row) for row in db.execute(f"SELECT * FROM {table}"))
            for table in token_usage.TOKEN_TABLES}


def downgrade_to_v11(path):
    db = sqlite3.connect(path)
    for view in ("token_usage_events", "token_context_events", "token_runs_effective"):
        db.execute(f"DROP VIEW {view}")
    for table in token_usage.TOKEN_TABLES:
        db.execute(f"DROP TABLE {table}")
    db.execute("UPDATE metadata SET value='11' WHERE key='schema_version'")
    db.commit()
    db.execute("VACUUM")
    db.close()


class RebuildCopyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.live = self.root / "data" / "sessiondb.sqlite"
        self.live.parent.mkdir()
        db = sessiondb.connect(self.live)
        try:
            for name, entry in GOLDEN.items():
                target = self.root / "transcripts" / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(FIXTURES / name, target)
                sessiondb.ingest(db, entry["provider"], target)
            db.commit()
            self.expected_tokens = token_dump(db)
            self.events = db.execute("SELECT count(*) FROM events").fetchone()[0]
        finally:
            db.close()
        downgrade_to_v11(self.live)

    def tearDown(self):
        self.temp.cleanup()

    def live_bytes(self):
        return {path.name: path.read_bytes() for path in self.live.parent.iterdir()
                if path.is_file() and path.name != "ingest.lock"}

    def test_v11_store_is_refused_by_current_code(self):
        with self.assertRaisesRegex(ValueError, "rebuild-into-copy"):
            sessiondb.connect(self.live, read_only=True)

    def test_rebuild_into_copy_adds_tokens_and_changes_nothing_else(self):
        before = self.live_bytes()
        target = self.root / "copy" / "sessiondb.sqlite"
        result = rebuild_copy.rebuild_into_copy(self.live, target)
        self.assertTrue(result["complete"], result)
        self.assertTrue(result["published"])
        self.assertEqual(result["fingerprint_mismatches"], [])
        self.assertIn("events", result["fingerprints_compared"])
        self.assertIn("positional_projection_fts_data", result["fingerprints_compared"])
        self.assertEqual(result["token_backfill"]["error"], 0)
        self.assertEqual(result["token_backfill"]["records_read"], self.events)
        self.assertTrue(result["idempotency"]["pass"])
        self.assertEqual(self.live_bytes(), before)
        copy = sessiondb.connect(target, read_only=True)
        try:
            self.assertEqual(token_dump(copy), self.expected_tokens)
        finally:
            copy.close()
        self.assertTrue(Path(str(target) + rebuild_copy.REPORT_SUFFIX).exists())
        with self.assertRaises(FileExistsError):
            rebuild_copy.rebuild_into_copy(self.live, target)

    def test_a_failed_verification_publishes_nothing(self):
        target = self.root / "copy" / "sessiondb.sqlite"
        with mock.patch.object(rebuild_copy, "verify_rebuilt_copy", return_value={"complete": False}):
            result = rebuild_copy.rebuild_into_copy(self.live, target)
        self.assertFalse(result["published"])
        self.assertEqual(sorted(path.name for path in target.parent.iterdir()), [])

    def make_copy(self):
        target = self.root / "copy" / "sessiondb.sqlite"
        self.assertTrue(rebuild_copy.rebuild_into_copy(self.live, target)["complete"])
        return target

    def test_promote_swaps_files_keeps_rollback_and_serves_appends(self):
        target = self.make_copy()
        old = self.live_bytes()
        rollback = self.root / "rollback"
        plan = rebuild_copy.promote_copy(self.live, target, rollback, agents=())
        self.assertFalse(plan["executed"])
        self.assertTrue(plan["complete"], plan)
        self.assertEqual(self.live_bytes(), old)
        result = rebuild_copy.promote_copy(self.live, target, rollback, execute=True, agents=())
        self.assertTrue(result["complete"], result)
        self.assertEqual({p.name: p.read_bytes() for p in rollback.iterdir()},
                         {name: data for name, data in old.items() if name != "sessiondb.sqlite.retrieval.lock"})
        self.assertFalse(target.exists())
        db = sessiondb.connect(self.live)
        try:
            self.assertEqual(token_dump(db), self.expected_tokens)
            self.assertTrue(sessiondb.report(db, self.live)["pack_verification"]["pass"])
            transcript = self.root / "transcripts" / "synthetic_claude_stream.jsonl"
            with transcript.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "type": "assistant", "uuid": "u99", "sessionId": "stream-1",
                    "timestamp": "2026-10-01T00:01:00Z",
                    "message": {"id": "m99", "usage": {"input_tokens": 1, "output_tokens": 2}},
                }) + "\n")
            self.assertEqual(sessiondb.ingest(db, "claude", transcript)["status"], "appended")
            self.assertEqual(db.execute(
                "SELECT count(*) FROM token_usage WHERE source_event_id='m99'").fetchone()[0], 1)
        finally:
            db.close()

    def test_a_failed_install_restores_the_live_store(self):
        target = self.make_copy()
        old = self.live_bytes()
        copy_files = {p.name: p.read_bytes() for p in target.parent.iterdir() if p.is_file()}
        rollback = self.root / "rollback"
        result = rebuild_copy.promote_copy(
            self.live, target, rollback, execute=True, agents=(), install_command=["/usr/bin/false"],
        )
        self.assertFalse(result["complete"])
        self.assertIn("install failed", result["error"])
        self.assertEqual(self.live_bytes(), old)
        self.assertEqual({p.name: p.read_bytes() for p in target.parent.iterdir() if p.is_file()}, copy_files)

    def test_a_changed_or_foreign_copy_is_refused(self):
        target = self.make_copy()
        with sqlite3.connect(target) as db:
            db.execute("INSERT INTO metadata VALUES('tamper','1')")
        result = rebuild_copy.promote_copy(self.live, target, self.root / "rollback", execute=True, agents=())
        self.assertFalse(result["executed"])
        self.assertIn("copy database changed since it was verified", result["check"]["problems"])


class ReRedactCopyTest(unittest.TestCase):
    SECRETS = ("quotedsecretvalue1", "urlpassword2")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.live = self.root / "data" / "sessiondb.sqlite"
        self.live.parent.mkdir()
        transcript = self.root / "transcripts" / "c1.jsonl"
        transcript.parent.mkdir()
        text = '{"api_key": "quotedsecretvalue1"} and https://alice:urlpassword2@example.com/repo'
        transcript.write_text(json.dumps({
            "type": "user", "uuid": "u1", "sessionId": "c1", "timestamp": "2026-10-01T01:00:00Z",
            "message": {"role": "user", "content": text},
        }) + "\n")
        db = sessiondb.connect(self.live)
        try:
            # Stands in for text indexed before the current redaction rules existed.
            with mock.patch.object(sessiondb, "redact", side_effect=lambda value: value):
                sessiondb.ingest(db, "claude", transcript)
            db.commit()
        finally:
            db.close()

    def tearDown(self):
        self.temp.cleanup()

    def search_text(self, path):
        db = sessiondb.connect(path, read_only=True)
        try:
            texts = []
            with sessiondb.projection_pack_path(db).open("rb") as handle:
                for (rowid,) in db.execute("SELECT event_rowid FROM search_projection_refs"):
                    regions, _ = sessiondb.read_projection_for_event(db, handle, rowid, {})
                    texts.extend(text for origin in regions for text in regions[origin])
            return "\n".join(texts)
        finally:
            db.close()

    def test_copy_rederives_search_text_with_current_redaction(self):
        target = self.root / "copy" / "sessiondb.sqlite"
        result = rebuild_copy.rebuild_into_copy(self.live, target, rebuild_projections=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["fingerprint_mismatches"], [])
        self.assertIn("events", result["fingerprints_compared"])
        rebuilt = self.search_text(target)
        for secret in self.SECRETS:
            self.assertNotIn(secret, rebuilt)
            self.assertIn(secret, self.search_text(self.live))
        self.assertTrue(rebuild_copy.check_copy(self.live, target)["ok"])

    def test_plain_copy_keeps_the_stored_search_text(self):
        target = self.root / "copy" / "sessiondb.sqlite"
        self.assertTrue(rebuild_copy.rebuild_into_copy(self.live, target)["complete"])
        for secret in self.SECRETS:
            self.assertIn(secret, self.search_text(target))


class TokenRebuildCommandTest(unittest.TestCase):
    def test_failed_source_is_rederived_after_the_cause_is_gone(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = sessiondb.connect(root / "db.sqlite")
            try:
                path = root / "a.jsonl"
                shutil.copyfile(FIXTURES / "claude.jsonl", path)
                sessiondb.ingest(db, "claude", path)
                key = db.execute("SELECT source_key FROM sources").fetchone()[0]
                expected = token_dump(db)
                db.execute("UPDATE token_source_state SET status='error', error='x' WHERE source_key=?", (key,))
                db.execute("DELETE FROM token_usage")
                db.commit()
                result = rebuild_copy.backfill_token_usage(db, only_incomplete=True)
                self.assertEqual((result["sources"], result["ok"]), (1, 1))
                self.assertEqual(token_dump(db), expected)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
