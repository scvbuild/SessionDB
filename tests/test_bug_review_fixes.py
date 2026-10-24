"""Regressions for the 2026-10-15 bug review items fixed on 2026-10-18."""
import argparse
import contextlib
import importlib.util
import json
import os
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import full_corpus
import sessiondb

SESSIONDB_ROOT = Path(__file__).resolve().parents[1]


class BugReviewFixesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def write(self, path, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return path

    def test_read_only_open_keeps_uri_characters_in_the_path(self):
        folder = self.root / "odd ?#% name"
        folder.mkdir()
        path = folder / "store.sqlite"
        sqlite3.connect(path).close()
        db = sessiondb.connect(path, read_only=True)
        try:
            opened = Path(db.execute("PRAGMA database_list").fetchone()["file"])
        finally:
            db.close()
        self.assertEqual(opened.resolve(), path.resolve())

    def test_concurrent_ingest_of_the_same_append_is_not_duplicated(self):
        db_path = self.root / "store.sqlite"
        db = sessiondb.connect(db_path)
        other = sessiondb.connect(db_path)
        source = self.write(self.root / "session.jsonl", [
            {"type": "user", "sessionId": "s", "timestamp": "2026-10-01T00:00:00Z",
             "message": {"content": "first"}},
        ])
        sessiondb.ingest(db, "claude", source)
        with source.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"type": "user", "sessionId": "s", "timestamp": "2026-10-01T00:01:00Z",
                                     "message": {"content": "second"}}) + "\n")
        real_snapshot = sessiondb.stable_source_snapshot
        raced = []

        def snapshot_then_race(*args, **kwargs):
            result = real_snapshot(*args, **kwargs)
            if not raced:
                raced.append(True)
                sessiondb.ingest(other, "claude", source)
            return result

        with mock.patch.object(sessiondb, "stable_source_snapshot", side_effect=snapshot_then_race):
            sessiondb.ingest(db, "claude", source)
        events = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        other.close()
        db.close()
        self.assertEqual(events, 2)

    def test_casefold_offsets_point_into_the_original_text(self):
        value = "ß" * 600 + "needle" + "x" * 50
        args = argparse.Namespace(readable_query="needle", readable_offset=None)
        segment = sessiondb.bounded_readable_text(value, args, 4000)["readable_segments"][0]
        self.assertEqual(segment["start"], 600 - 256)
        self.assertEqual(sessiondb.casefold_find(value, "NEEDLE"), 600)
        self.assertEqual(sessiondb.casefold_find(value, "missing"), -1)

    def test_project_comes_from_the_folder_under_claude_projects(self):
        path = Path("/Users/example/projects/app/.claude/projects/-Users-me-app/abc.jsonl")
        self.assertEqual(sessiondb.project_from_path(path), "-Users-me-app")
        self.assertIsNone(sessiondb.project_from_path(Path("/Users/example/projects/x/.claude/settings.json")))

    def test_naive_record_times_are_utc_like_stored_times(self):
        self.addCleanup(time.tzset)
        with mock.patch.dict(os.environ, {"TZ": "Asia/Ho_Chi_Minh"}):
            time.tzset()
            naive = sessiondb._timestamp_epoch("2026-10-15T10:00:00")
        self.assertEqual(naive, sessiondb._timestamp_epoch("2026-10-15T10:00:00Z"))

    def test_failed_build_keeps_the_readiness_receipt_usable(self):
        db = sessiondb.connect(self.root / "store.sqlite")
        source = self.write(self.root / "s.jsonl", [
            {"type": "user", "sessionId": "s", "timestamp": "2026-10-01T00:00:00Z", "message": {"content": "x"}},
        ])
        sessiondb.ingest(db, "claude", source)
        receipt = self.root / "receipt.json"
        receipt.write_text("{}", encoding="utf-8")
        ledger = self.root / "ledger"
        ledger.mkdir()
        consumed = ledger / "consumed.json"

        def consume(_db, _path, _count):
            consumed.write_text("{}")
            return consumed

        with mock.patch.object(sessiondb, "FULL_REBUILD_RECEIPT_MIN_EVENTS", 1), \
                mock.patch.object(sessiondb, "consume_rebuild_readiness_receipt", side_effect=consume), \
                mock.patch.object(sessiondb, "put_search_projection", side_effect=RuntimeError("disk full")):
            with self.assertRaisesRegex(RuntimeError, "disk full"):
                sessiondb.build_search_projections(db, readiness_receipt=receipt)
        db.close()
        self.assertTrue(receipt.exists())
        self.assertFalse(consumed.exists())

    def test_interrupted_backup_after_compaction_never_pairs_an_old_database_with_a_new_pack(self):
        db_path = self.root / "live" / "store.sqlite"
        db_path.parent.mkdir()
        db = sessiondb.connect(db_path)
        sessiondb.ingest(db, "claude", self.write(self.root / "a.jsonl", [
            {"type": "user", "sessionId": "a", "timestamp": "2026-10-01T00:00:00Z", "message": {"content": "a"}},
        ]))
        db.close()
        target = self.root / "backup"
        self.assertTrue(full_corpus.backup(db_path, target)["complete"])
        # The previous backup's pack is no longer a prefix of the live one, as after compaction.
        old_pack = target / (db_path.name + ".objects")
        old_pack.write_bytes(b"\xff" + old_pack.read_bytes()[1:])
        real_replace = os.replace

        def interrupt_database_publish(src, dst):
            if Path(dst) == target / db_path.name:
                raise KeyboardInterrupt
            return real_replace(src, dst)

        with mock.patch.object(full_corpus.os, "replace", side_effect=interrupt_database_publish):
            with self.assertRaises(KeyboardInterrupt):
                full_corpus.backup(db_path, target)
        self.assertFalse((target / db_path.name).exists())





if __name__ == "__main__":
    unittest.main()
