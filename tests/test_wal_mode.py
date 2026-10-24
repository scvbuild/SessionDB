import json
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import freshness_service
import full_corpus
import sessiondb

READER = """
import sqlite3, sys
db = sqlite3.connect(sys.argv[1], uri=True, isolation_level=None)
db.execute("BEGIN")
db.execute("SELECT count(*) FROM metadata").fetchone()
print("reading", flush=True)
sys.stdin.readline()
assert db.execute("SELECT count(*) FROM metadata WHERE key='wal_test'").fetchone()[0] == 0
db.close()
"""

class WalModeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db_path = self.root / "sessiondb.sqlite"
        sessiondb.connect(self.db_path).close()

    def tearDown(self):
        self.tmp.cleanup()

    def open_reader_mid_read(self):
        """Another process holding a read open, as agent searches and audits do."""
        reader = subprocess.Popen(
            [sys.executable, "-c", READER, sessiondb.read_only_uri(self.db_path)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        self.assertEqual(reader.stdout.readline().strip(), "reading")
        return reader

    def finish(self, reader):
        reader.communicate("done\n", timeout=10)
        self.assertEqual(reader.returncode, 0)

    def write_once(self):
        writer = sessiondb.connect(self.db_path, timeout=0.2)
        try:
            writer.execute("INSERT OR REPLACE INTO metadata(key, value) VALUES ('wal_test', 'x')")
            writer.commit()
        finally:
            writer.close()

    def test_writer_commits_while_a_long_reader_holds_the_store(self):
        reader = self.open_reader_mid_read()
        try:
            with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                self.write_once()  # control: rollback-journal mode blocks the commit
        finally:
            self.finish(reader)
        self.assertEqual(sessiondb.enable_wal(self.db_path), "wal")
        reader = self.open_reader_mid_read()
        try:
            self.write_once()  # the reader keeps its snapshot; it checks it never saw this row
        finally:
            self.finish(reader)

    def test_wal_switch_leaves_the_store_unchanged_while_a_reader_is_open(self):
        reader = self.open_reader_mid_read()
        try:
            self.assertEqual(sessiondb.enable_wal(self.db_path, timeout=0.1), "delete")
        finally:
            self.finish(reader)
        self.assertEqual(sessiondb.enable_wal(self.db_path), "wal")

    def test_read_only_open_recreates_missing_side_files(self):
        sessiondb.enable_wal(self.db_path)
        for suffix in ("-wal", "-shm"):
            Path(str(self.db_path) + suffix).unlink()
        db = sessiondb.connect(self.db_path, read_only=True)
        try:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        finally:
            db.close()
        for suffix in ("-wal", "-shm"):
            self.assertTrue(Path(str(self.db_path) + suffix).exists())

    def test_side_file_repair_ignores_rollback_and_missing_stores(self):
        sessiondb.ensure_wal_side_files(self.db_path)
        self.assertFalse(Path(str(self.db_path) + "-wal").exists())
        missing = self.root / "missing.sqlite"
        sessiondb.ensure_wal_side_files(missing)
        self.assertFalse(missing.exists())

    def test_sync_switches_to_wal_and_reports_the_mode(self):
        report = {"complete": True, "elapsed_seconds": 0.1, "status_counts": {}, "hard_failures": [],
                  "sources_drifted_after_sync": [], "issues": []}
        with mock.patch.object(freshness_service.full_corpus, "fast_sync", return_value=report), \
                mock.patch.object(freshness_service, "publish_projection_integrity_receipt",
                                  return_value={"state": "current"}), \
                mock.patch("builtins.print"):
            result = freshness_service.run_sync(
                self.db_path, self.root / "freshness", "test",
            )
        self.assertEqual(result["journal_mode"], "wal")

    def test_sync_beside_a_long_reader_neither_waits_nor_loses_the_incremental_receipt(self):
        transcripts = self.root / "transcripts"
        transcripts.mkdir()
        source = transcripts / "session.jsonl"
        db_path = self.root / "watched.sqlite"
        self.db_path = db_path

        def append(text):
            with source.open("a") as handle:
                handle.write(json.dumps({"type": "user", "sessionId": "s", "message": {"content": text}}) + "\n")

        def sync(**kwargs):
            started = time.monotonic()
            result = freshness_service.run_sync(
                db_path, self.root / "freshness", "test", **kwargs,
            )
            return result, time.monotonic() - started

        old_roots = full_corpus.ROOTS
        full_corpus.ROOTS = {"claude": (transcripts,)}
        try:
            with mock.patch("builtins.print"):
                append("first")
                sync()
                append("second")
                sync(changed_paths=[str(source)])
                reader = self.open_reader_mid_read()
                try:
                    append("third")
                    result, elapsed = sync(changed_paths=[str(source)])
                finally:
                    reader.communicate("done\n", timeout=10)
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(result["pass"])
        self.assertEqual(result["projection_integrity_receipt"]["verification"], "since_previous_receipt")
        self.assertLess(elapsed, 5)

    def test_failed_wal_switch_does_not_stop_the_sync(self):
        with mock.patch.object(sessiondb, "enable_wal", side_effect=sqlite3.OperationalError("disk I/O error")), \
                mock.patch("builtins.print") as printed:
            self.assertIsNone(freshness_service.store_journal_mode(self.db_path))
        self.assertIn("WAL switch failed", printed.call_args[0][0])
        self.assertIsNone(freshness_service.store_journal_mode(self.root / "missing.sqlite"))

    def test_store_keeper_follows_a_replaced_store_and_reports_one_error(self):
        keeper = freshness_service.StoreKeeper()
        try:
            keeper.refresh(self.db_path)
            first = keeper.db
            self.assertIsNotNone(first)
            keeper.refresh(self.db_path)
            self.assertIs(keeper.db, first)
            replacement = self.root / "replacement.sqlite"
            sessiondb.connect(replacement).close()
            replacement.replace(self.db_path)
            keeper.refresh(self.db_path)
            self.assertIsNot(keeper.db, first)
            missing = self.root / "missing.sqlite"
            keeper.close()
            with mock.patch("builtins.print") as printed:
                keeper.refresh(missing)
                keeper.refresh(missing)
            self.assertIsNone(keeper.db)
            self.assertEqual(printed.call_count, 1)
        finally:
            keeper.close()


if __name__ == "__main__":
    unittest.main()
