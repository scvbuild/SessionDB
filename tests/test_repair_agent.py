import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import freshness_service
import repair_agent
import sessiondb


BASE = '''ADAPTER_VERSION = {"claude": "v1", "codex": "v1"}
SCHEMA_VERSION = "12"
CLAUDE_TYPES = {"user"}
ADAPTER_MIGRATIONS = {}
'''
PATCH = '''ADAPTER_VERSION = {"claude": "v2", "codex": "v1"}
SCHEMA_VERSION = "12"
CLAUDE_TYPES = {"user", "note"}
ADAPTER_MIGRATIONS = {("claude", "v1", "v2"): AdapterMigration("category-scoped", frozenset({"note"}), unsupported_only=True)}
'''


class RepairTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        (self.repo / "src").mkdir(parents=True)
        self.source = self.repo / "src/sessiondb.py"
        self.source.write_text(BASE)
        self.job = self.root / "job"
        self.job.mkdir()
        (self.repo / "tests").mkdir()
        (self.job / "candidate/tests").mkdir(parents=True)
        (self.job / "candidate/tests/test_repair_regression.py").write_text("def test_affected(): pass\ndef test_control(): pass\n")
        self.state = self.root / "freshness"
        self.state.mkdir()
        self.spec = {"state_dir": str(self.state.resolve()), "issues": ['issue-a']}

    def tearDown(self):
        self.temp.cleanup()

    def test_schema_other_provider_and_missing_migration_are_rejected(self):
        repair_agent.check_parser_change(BASE, PATCH, {"claude"})
        for text in (PATCH.replace('SCHEMA_VERSION = "12"', 'SCHEMA_VERSION = "99"'),
                     PATCH.replace('"codex": "v1"', '"codex": "v2"'),
                     PATCH.replace('"category-scoped"', '"full-provider"'),
                     PATCH.replace('CLAUDE_TYPES', 'NEW_GLOBAL')):
            with self.subTest(text=text), self.assertRaises(ValueError):
                repair_agent.check_parser_change(BASE, text, {"claude"})

    def test_activation_and_verification_are_distinct_and_rollback_preserves_backup(self):
        repair_agent.activate(self.repo, self.job, BASE, PATCH, self.spec)
        journal = repair_agent.read_json(self.repo / ".sessiondb-repair/activation.json")
        self.assertEqual(journal["phase"], "activated")
        self.assertEqual(self.source.read_text(), PATCH)
        self.assertEqual((self.job / "original-sessiondb.py").read_text(), BASE)
        self.assertTrue(Path(journal["test"]).exists())
        with mock.patch.object(repair_agent, "LOADED_CODE_HASH", journal["after"]):
            self.assertEqual(repair_agent.verify_activation(self.repo, self.state, {"issues": [], "error": "failed"}), "rolled_back")
        self.assertEqual(self.source.read_text(), BASE)
        self.assertTrue((self.job / "original-sessiondb.py").exists())
        self.assertFalse(Path(journal["test"]).exists())

    def test_successful_fresh_sync_verifies_but_unloaded_code_does_not(self):
        repair_agent.activate(self.repo, self.job, BASE, PATCH, self.spec)
        self.assertEqual(repair_agent.verify_activation(self.repo, self.state, {}), "restart_required")
        with mock.patch.object(repair_agent, "LOADED_CODE_HASH", repair_agent.file_hash(self.source)):
            self.assertEqual(repair_agent.verify_activation(self.repo, self.state, {}), "verified")
        self.assertEqual(self.source.read_text(), PATCH)

    def test_interrupted_activation_recovers_and_restart_is_idempotent(self):
        real_write = repair_agent.write_json
        def interrupted(path, value):
            if value.get("phase") == "activated":
                raise OSError("interrupted after source swap")
            real_write(path, value)
        with mock.patch.object(repair_agent, "write_json", side_effect=interrupted), self.assertRaises(OSError):
            repair_agent.activate(self.repo, self.job, BASE, PATCH, self.spec)
        self.assertEqual(self.source.read_text(), PATCH)
        self.assertTrue(repair_agent.recover(self.repo))
        self.assertFalse(repair_agent.recover(self.repo))
        self.assertEqual(self.source.read_text(), BASE)

    def test_stale_source_and_newer_edits_are_not_overwritten(self):
        self.source.write_text(BASE + "\n# user edit\n")
        with self.assertRaisesRegex(ValueError, "source changed"):
            repair_agent.activate(self.repo, self.job, BASE, PATCH, self.spec)
        self.source.write_text(BASE)
        repair_agent.activate(self.repo, self.job, BASE, PATCH, self.spec)
        self.source.write_text(PATCH + "\n# another edit\n")
        journal = repair_agent.read_json(self.repo / ".sessiondb-repair/activation.json")
        with self.assertRaisesRegex(ValueError, "newer work"):
            repair_agent.rollback(self.repo, journal)
        self.assertTrue(self.source.read_text().endswith("# another edit\n"))

    def test_lock_rejects_a_concurrent_process_and_releases_after_failure(self):
        lock = self.root / "test.lock"
        command = [sys.executable, "-c", "import sys;from pathlib import Path;from repair_agent import locked\nwith locked(Path(sys.argv[1])): pass", str(lock)]
        with self.assertRaises(RuntimeError):
            with repair_agent.locked(lock):
                blocked = subprocess.run(command, capture_output=True)
                self.assertNotEqual(blocked.returncode, 0)
                raise RuntimeError("failure inside lock")
        self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)

    def test_failed_command_and_timeout_are_clear_and_child_is_reaped(self):
        with self.assertRaisesRegex(RuntimeError, "exited 4"):
            repair_agent.run_command([sys.executable, "-c", "raise SystemExit(4)"], self.root, self.root / "failure.log", 5)
        with self.assertRaises(subprocess.TimeoutExpired):
            repair_agent.run_command([sys.executable, "-c", "import time;time.sleep(30)"], self.root, self.root / "timeout.log", 0.1)

    def test_bounded_input_redacts_secrets_and_omits_source_paths(self):
        record = {"provider": "claude", "category": "note", "source_path": "/secret/transcript.jsonl",
                  "structure": {"password": "never-send", "message": "api_key=fixture-secret", "nested": [{"token": "never-send"}]}}
        data = repair_agent.repair_input({"groups": [{"records": [record]}]})
        text = json.dumps(data)
        self.assertNotIn("never-send", text)
        self.assertNotIn("fixture-secret", text)
        self.assertNotIn("/secret/", text)
        with mock.patch.object(repair_agent, "MAX_INPUT_BYTES", 10), self.assertRaises(ValueError):
            repair_agent.repair_input({"groups": [{"records": [record]}]})

    def test_no_launch_by_default_and_broad_failure_not_eligible(self):
        result = {"issues": [{"provider": "claude", "category": "note", "issue": "unknown"}],
                  "repair_evidence": {"complete": True}}
        with mock.patch.object(repair_agent, "verify_activation", return_value=None), \
                mock.patch("subprocess.Popen", side_effect=AssertionError("unexpected process")):
            self.assertEqual(repair_agent.advance(result, self.state, None)["state"], "disabled")
            self.assertTrue(repair_agent.eligible(result))
            self.assertFalse(repair_agent.eligible({**result, "hard_failures": ["broken"]}))

    def test_confirmation_then_invalid_evidence_cannot_launch_or_repeat(self):
        result = {"issues": [{"provider": "claude", "category": "note", "issue": "unknown"}],
                  "repair_evidence": {"complete": True, "path": str(self.root / "missing.json")}}
        config = {"repo": str(self.repo), "provider": "codex"}
        with mock.patch.object(repair_agent, "verify_activation", return_value=None), \
                mock.patch("subprocess.Popen", side_effect=AssertionError("unexpected process")), \
                mock.patch.object(repair_agent, "validate_repair_evidence", side_effect=ValueError("bad evidence")) as validate:
            self.assertEqual(repair_agent.advance(result, self.state, config, now=0)["state"], "confirming")
            self.assertEqual(repair_agent.advance(result, self.state, config, now=301)["state"], "blocked")
            later = repair_agent.advance(result, self.state, config, now=700)
            self.assertEqual(later["state"], "blocked")
            self.assertIn("bad evidence", later["error"])
            self.assertEqual(validate.call_count, 1)

    def test_invalid_saved_state_is_not_replaced(self):
        path = self.state / "repair-state.json"
        path.write_text("not json")
        with mock.patch.object(repair_agent, "verify_activation", return_value=None):
            self.assertEqual(repair_agent.advance({}, self.state, None)["state"], "blocked")
        self.assertEqual(path.read_text(), "not json")

    def test_failed_repair_does_not_stop_unaffected_ingestion(self):
        source = self.root / "claude"
        source.mkdir()
        (source / "message.jsonl").write_text(json.dumps({"type": "user", "sessionId": "control", "message": {"content": "normal control"}}) + "\n")
        with mock.patch.object(freshness_service.full_corpus, "ROOTS", {"claude": (source,)}), \
                mock.patch.object(repair_agent, "advance", return_value={"state": "blocked", "error": "agent unavailable"}), \
                contextlib.redirect_stdout(io.StringIO()):
            result = freshness_service.run_sync(self.root / "store.sqlite", self.state, "test", repair_config={"provider": "codex"})
        self.assertTrue(result["coverage_complete"])
        self.assertTrue(result["attention_required"])
        self.assertEqual(result["repair"]["state"], "blocked")

    def test_provider_commands_keep_native_permissions_and_no_shell_interpolation(self):
        for provider in ("codex", "claude"):
            command = repair_agent.agent_command({"provider": provider, "executable": "/test/agent", "model": "example"}, self.root, self.root / "schema.json")
            self.assertNotIn("--dangerously-skip-permissions", command)
            self.assertNotIn("--dangerously-bypass-approvals-and-sandbox", command)
            self.assertIn("--model", command)
        self.assertIn("workspace-write", repair_agent.agent_command({"provider": "codex", "executable": "/test/agent"}, self.root, self.root / "schema.json"))
        with mock.patch("shutil.which", return_value=None), self.assertRaisesRegex(ValueError, "unavailable"):
            repair_agent.agent_command({"provider": "claude"}, self.root, self.root / "schema.json")

    def test_candidate_cannot_rewrite_tests_or_use_symlinks(self):
        candidate = self.root / "candidate"
        (candidate / "src").mkdir(parents=True)
        (candidate / "tests").mkdir()
        (candidate / "src/sessiondb.py").write_text(BASE)
        (candidate / "tests/test_existing.py").write_text("original")
        baseline = repair_agent.tree_files(candidate)
        (candidate / "src/sessiondb.py").write_text(PATCH)
        (candidate / "tests/test_repair_regression.py").write_text("def test_affected(): pass\ndef test_control(): pass\n")
        repair_agent.check_candidate(candidate, baseline, BASE, {"claude"})
        (candidate / "tests/test_existing.py").write_text("changed")
        with self.assertRaisesRegex(ValueError, "outside"):
            repair_agent.check_candidate(candidate, baseline, BASE, {"claude"})
        (candidate / "link").symlink_to(self.source)
        with self.assertRaisesRegex(ValueError, "symbolic"):
            repair_agent.tree_files(candidate)

    def test_restart_preserves_watcher_arguments(self):
        with mock.patch.object(sys, "argv", ["sessiondb-freshness", "--db", "fixture.sqlite", "--auto-repair", "claude"]), \
                mock.patch("os.execv") as execute:
            freshness_service.restart_process()
        self.assertEqual(execute.call_args.args[1][1:3], ["-m", "freshness_service"])
        self.assertEqual(execute.call_args.args[1][-2:], ["--auto-repair", "claude"])

    @unittest.skipUnless(sys.platform == "darwin", "macOS sandbox")
    def test_verification_sandbox_denies_external_data_writes_and_network(self):
        if os.environ.get("SESSIONDB_REPAIR_VERIFY"):
            self.skipTest("sandbox already active; nesting is not supported")
        protected = self.root / "protected-home"
        protected.mkdir()
        secret = protected / "fixture.txt"
        secret.write_text("synthetic private content")
        outside = self.root / "outside.txt"
        script = '''import os,socket,sys
with open('/dev/null','r+b'): pass
for path,mode in ((sys.argv[1],'r'),(sys.argv[2],'w')):
    try:
        open(path,mode)
    except PermissionError:
        pass
    else:
        raise AssertionError('access escaped sandbox')
try:
    connection=socket.socket()
    connection.bind(('127.0.0.1',0))
except PermissionError:
    pass
else:
    raise AssertionError('network escaped sandbox')
'''
        with mock.patch.object(Path, "home", return_value=protected):
            command = repair_agent.verification_command([sys.executable, "-c", script, str(secret), str(outside)], self.job)
        repair_agent.run_command(command, self.job, self.job / "sandbox.log", 10)
        self.assertFalse(outside.exists())

    def test_corrupt_agent_result_is_rejected(self):
        (self.root / "agent-result.json").write_text("not JSON")
        with self.assertRaises(ValueError):
            repair_agent.agent_result("codex", self.root, self.root / "unused.log")
        log = self.root / "claude.log"
        log.write_text(json.dumps({"is_error": True, "structured_output": {"outcome": "repaired"}}))
        with self.assertRaisesRegex(ValueError, "agent error"):
            repair_agent.agent_result("claude", self.root, log)

    def test_checkout_change_invalidates_candidate(self):
        for name in ("README.md", "pyproject.toml", "LICENSE", "MANIFEST.in"):
            (self.repo / name).write_text("fixture")
        candidate = self.root / "copy"
        baseline = repair_agent.copy_candidate(self.repo, candidate)
        repair_agent.check_checkout(self.repo, baseline)
        self.source.write_text(PATCH)
        with self.assertRaisesRegex(ValueError, "changed during repair"):
            repair_agent.check_checkout(self.repo, baseline)
