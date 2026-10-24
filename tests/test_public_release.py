import contextlib
import argparse
import io
import json
import os
import shutil
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


class PublicReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.database = self.root / "data" / "test.sqlite"
        self.database.parent.mkdir()
        self.home = self.root / "home"
        self.claude = self.home / ".claude" / "projects" / "demo"
        self.codex = self.home / ".codex" / "sessions" / "demo"
        self.claude.mkdir(parents=True)
        self.codex.mkdir(parents=True)
        examples = Path(__file__).resolve().parents[1] / "examples"
        shutil.copyfile(examples / "claude.jsonl", self.claude / "claude.jsonl")
        shutil.copyfile(examples / "codex.jsonl", self.codex / "codex.jsonl")
        self.environment = dict(os.environ, HOME=str(self.home), PYTHONDONTWRITEBYTECODE="1")
        self.environment.pop("PYTHONPATH", None)

    def tearDown(self):
        self.temp.cleanup()

    def command(self, name, *arguments, expected=0):
        bin_dir = os.environ.get("SESSIONDB_TEST_BIN")
        if not bin_dir:
            self.skipTest("installed command tests require SESSIONDB_TEST_BIN")
        completed = subprocess.run(
            [str(Path(bin_dir) / name), *map(str, arguments)],
            cwd=self.root, env=self.environment, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(completed.returncode, expected, completed.stderr + completed.stdout)
        return json.loads(completed.stdout)

    def sync(self):
        with mock.patch.object(full_corpus, "ROOTS", {
            "claude": (self.claude,), "codex": (self.codex,),
        }), mock.patch("subprocess.Popen", side_effect=AssertionError("agent process launched")), \
                contextlib.redirect_stdout(io.StringIO()):
            return freshness_service.run_sync(self.database, self.database.parent / "freshness", "test")

    def test_sync_preserves_both_providers_and_publishes_receipt_without_process(self):
        result = self.sync()
        self.assertTrue(result["pass"], result)
        self.assertFalse(result["automatic_repair"])
        self.assertFalse(result["attention_required"])
        self.assertEqual(result["projection_integrity_receipt"]["state"], "current")
        db = sessiondb.connect(self.database, read_only=True)
        try:
            self.assertEqual({row[0] for row in db.execute("SELECT DISTINCT provider FROM events")}, {"claude", "codex"})
        finally:
            db.close()

    def test_unknown_secret_shape_is_actionable_without_process_and_other_provider_survives(self):
        self.assertTrue(self.sync()["pass"])
        path = self.claude / "unknown.jsonl"
        path.write_text(json.dumps({"type": "future-active-event", "sessionId": "gap",
                                    "authorization": "Bearer synthetic-example-secret",
                                    "message": {"content": "future unsupported action"}}) + "\n")
        result = self.sync()
        self.assertFalse(result["pass"], result)
        self.assertTrue(result["attention_required"])
        self.assertEqual(result["recovery_state"], "action_required")
        self.assertTrue(result["issues"])
        db = sessiondb.connect(self.database, read_only=True)
        try:
            coverage = sessiondb.query_coverage(db, argparse.Namespace(
                provider="codex", project=None, session=None, role=None, tool=None,
                lineage=None, since=None, until=None,
            ))
            self.assertTrue(coverage["complete"], coverage)
        finally:
            db.close()
        path.unlink()
        remaining = self.sync()["issues"]
        self.assertEqual(remaining[0]["issue"], result["issues"][0]["issue"])
        self.assertEqual(remaining[0]["fallback_readable"], 0)
        self.assertFalse(list(self.root.rglob("repair-agent*")))

    def test_missing_source_is_not_hidden_as_recovering(self):
        result = {"pass": False, "coverage_complete": False,
                  "missing_discovered_sources": ["/synthetic/missing.jsonl"]}
        with contextlib.redirect_stdout(io.StringIO()):
            published = freshness_service.publish_status(result, self.root / "status")
        self.assertTrue(published["attention_required"])
        self.assertEqual(published["recovery_state"], "action_required")
        self.assertEqual(freshness_service.scheduled_recheck(published, 100), (160, "error_retry"))

    def test_receipt_ledger_is_separate_from_private_installation(self):
        with mock.patch.dict(os.environ, {"HOME": str(self.home)}):
            self.assertEqual(sessiondb.rebuild_receipt_ledger_dir(), self.home / ".local/share/sessiondb-public/data/rebuild-receipts")

    def test_installed_ingest_search_details_export_backup(self):
        self.command("sessiondb", "--db", self.database, "ingest", "--provider", "claude", self.claude / "claude.jsonl")
        self.command("sessiondb", "--db", self.database, "ingest", "--provider", "codex", self.codex / "codex.jsonl")
        built = self.command("sessiondb", "--db", self.database, "build-search-index")
        self.assertTrue(built["complete"])
        search = self.command("sessiondb", "--db", self.database, "search-index", "--text", '"release checklist"')
        self.assertTrue(search["entries"], search)
        follow = search["follow_up"]["details"]
        arguments = ["--db", self.database, "details", "--snapshot-id", follow["snapshot_id"],
                     "--bundle", follow["bundle"], "--result-id", search["entries"][0]["id"], "--readable"]
        details = self.command("sessiondb", *arguments)
        self.assertTrue(details)
        query = self.command("sessiondb", "--db", self.database, "query", "--provider", "codex", "--clean")
        self.assertTrue(query["events"], query)
        self.command("sessiondb-freshness", "--db", self.database, "--once")
        exported = self.command("sessiondb-export", "--db", self.database,
                                "--start", "2026-10-01T00:00:00Z", "--end", "2026-10-02T00:00:00Z",
                                "--output-dir", self.root / "export", "--state-dir", self.root / "export-state")
        self.assertEqual(exported["coverage"], "complete", exported)
        backup = self.command("sessiondb-admin", "backup", "--db", self.database, "--target", self.root / "backup.sqlite")
        self.assertTrue(backup)
        self.assertFalse((self.home / ".local/share/sessiondb/data").exists())

    @unittest.skipUnless(sys.platform == "darwin", "native event watcher needs macOS")
    def test_installed_native_watcher_events_and_restart(self):
        if os.environ.get("SESSIONDB_REPAIR_VERIFY"):
            self.skipTest("candidate parser is verified by source replay; no installed candidate watcher")
        bin_dir = os.environ.get("SESSIONDB_TEST_BIN", str(Path(sys.executable).parent))
        if not (Path(bin_dir) / "sessiondb-freshness").exists():
            self.skipTest("installed watcher requires SESSIONDB_TEST_BIN")
        status = self.database.parent / "freshness" / "watcher_status.json"

        def wait_for(predicate):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.fail("watcher exited: " + process.communicate()[1])
                try:
                    value = json.loads(status.read_text())
                    if predicate(value):
                        return value
                except (OSError, ValueError):
                    pass
                time.sleep(0.05)
            self.fail("watcher did not publish the expected state")

        for cycle in range(2):
            process = subprocess.Popen(
                [str(Path(bin_dir) / "sessiondb-freshness"), "--db", str(self.database),
                 "--debounce", "0.1", "--max-debounce", "0.2", "--min-sync-interval", "0"],
                cwd=self.root, env=self.environment, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
            )
            try:
                wait_for(lambda value: value.get("reason") == "startup" and value.get("pass"))
                with (self.claude / "claude.jsonl").open("a") as handle:
                    handle.write(json.dumps({"type": "user", "sessionId": "demo-claude",
                                             "message": {"content": f"native watcher cycle {cycle}"}}) + "\n")
                wait_for(lambda value: value.get("reason") == "filesystem_event" and value.get("pass"))
                self.assertIsNone(process.poll())
            finally:
                process.terminate()
                process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0)
            status.unlink()

    @unittest.skipUnless(sys.platform == "darwin", "native event watcher needs macOS")
    def test_source_change_restarts_watcher_and_resumes_ingestion(self):
        if os.environ.get("SESSIONDB_REPAIR_VERIFY"):
            self.skipTest("candidate verification does not start nested watchers")
        copied = self.root / "source"
        shutil.copytree(Path(freshness_service.__file__).parent, copied,
                        ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"))
        environment = dict(self.environment, PYTHONPATH=str(copied))
        status = self.database.parent / "freshness/watcher_status.json"
        log = self.root / "watcher.log"
        with log.open("w") as output:
            process = subprocess.Popen([sys.executable, "-m", "freshness_service", "--db", str(self.database),
                                        "--min-sync-interval", "0", "--debounce", "0.1"],
                                       cwd=self.root, env=environment, stdout=output, stderr=output)
            try:
                def wait_for(predicate):
                    deadline = time.monotonic() + 20
                    while time.monotonic() < deadline:
                        if process.poll() is not None:
                            self.fail(log.read_text())
                        if predicate():
                            return
                        time.sleep(0.1)
                    self.fail(log.read_text())
                wait_for(lambda: status.exists() and json.loads(status.read_text()).get("pass"))
                initial = status.stat().st_mtime_ns
                source = copied / "sessiondb.py"
                source.write_text(source.read_text() + "\n")
                wait_for(lambda: "restarting the watcher" in log.read_text()
                         and status.stat().st_mtime_ns != initial)
                with (self.claude / "claude.jsonl").open("a") as handle:
                    handle.write(json.dumps({"type": "user", "sessionId": "demo-claude",
                                             "message": {"content": "after source restart"}}) + "\n")
                wait_for(lambda: json.loads(status.read_text()).get("reason") == "filesystem_event")
                self.assertTrue(json.loads(status.read_text())["pass"])
            finally:
                process.terminate()
                process.wait(timeout=10)
            self.assertEqual(process.returncode, 0)
