"""Token-usage projection: provider accounting, appends, idempotency, isolation."""

import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

import full_corpus
import sessiondb
import token_usage

FIXTURES = Path(__file__).parent / "fixtures" / "token_usage"
GOLDEN = json.loads((FIXTURES / "golden.json").read_text(encoding="utf-8"))
RUN_KEYS = ("run_id", "provider", "root_run_id", "parent_run_id", "run_kind", "model",
            "started_at", "ended_at", "status")
USAGE_KEYS = ("event_id", "run_id", "provider", "observed_at", "input_tokens", "output_tokens",
              "cache_read_input_tokens", "cache_write_input_tokens", "reasoning_output_tokens",
              "total_tokens", "source_event_id")
CONTEXT_KEYS = ("event_id", "run_id", "provider", "observed_at", "input_context_tokens",
                "cached_input_context_tokens", "model_context_window", "event_kind",
                "compaction_phase", "source_event_id")


def sorted_rows(rows):
    return sorted(rows, key=lambda item: json.dumps(item, sort_keys=True))


class TokenUsageTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = sessiondb.connect(self.root / "db.sqlite")

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def place(self, name):
        target = self.root / "src" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(FIXTURES / name, target)
        return target

    def source_key(self, path):
        return self.db.execute(
            "SELECT source_key FROM sources WHERE path=?", (str(path.resolve()),),
        ).fetchone()[0]

    def projection(self, path):
        key = self.source_key(path)
        runs = [{k: row[k] for k in RUN_KEYS} for row in self.db.execute(
            "SELECT * FROM token_runs WHERE source_key=?", (key,))]
        usage = [{k: row[k] for k in USAGE_KEYS} for row in self.db.execute(
            "SELECT * FROM token_usage WHERE source_key=?", (key,))]
        context = [{k: row[k] for k in CONTEXT_KEYS} for row in self.db.execute(
            "SELECT * FROM token_context WHERE source_key=?", (key,))]
        return {"runs": sorted_rows(runs), "usage": sorted_rows(usage), "context": sorted_rows(context)}

    def expected(self, name):
        entry = GOLDEN[name]
        return {key: entry[key] for key in ("runs", "usage", "context")}

    def state(self, path):
        return self.db.execute(
            "SELECT * FROM token_source_state WHERE source_key=?", (self.source_key(path),),
        ).fetchone()

    def test_whole_file_matches_token_ledger(self):
        for name, entry in GOLDEN.items():
            with self.subTest(name=name):
                path = self.place(name)
                sessiondb.ingest(self.db, entry["provider"], path)
                self.assertEqual(self.state(path)["status"], "ok", self.state(path)["error"])
                self.assertEqual(self.projection(path), self.expected(name))

    def test_appended_rows_give_the_same_result_as_one_read(self):
        for name, entry in GOLDEN.items():
            with self.subTest(name=name):
                lines = (FIXTURES / name).read_text(encoding="utf-8").splitlines(keepends=True)
                path = self.root / "appended" / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("", encoding="utf-8")
                statuses = set()
                for index, text in enumerate(lines):
                    with path.open("a", encoding="utf-8") as handle:
                        handle.write(text)
                    statuses.add(sessiondb.ingest(self.db, entry["provider"], path)["status"])
                if len(lines) > 1:
                    self.assertIn("appended", statuses)
                self.assertEqual(self.state(path)["status"], "ok", self.state(path)["error"])
                self.assertEqual(self.projection(path), self.expected(name))

    def test_ingesting_the_same_file_twice_inserts_nothing(self):
        path = self.place("synthetic_claude_stream.jsonl")
        sessiondb.ingest(self.db, "claude", path)
        before = {table: self.db.execute(f"SELECT * FROM {table} ORDER BY 1,2").fetchall()
                  for table in token_usage.TOKEN_TABLES}
        changes = self.db.total_changes
        result = sessiondb.ingest(self.db, "claude", path)
        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(self.db.total_changes, changes)
        path.touch()
        sessiondb.ingest(self.db, "claude", path)
        after = {table: self.db.execute(f"SELECT * FROM {table} ORDER BY 1,2").fetchall()
                 for table in token_usage.TOKEN_TABLES}
        self.assertEqual([[tuple(r) for r in rows] for rows in before.values()],
                         [[tuple(r) for r in rows] for rows in after.values()])

    def test_stored_records_rebuild_the_same_projection(self):
        for name, entry in GOLDEN.items():
            with self.subTest(name=name):
                path = self.place(name)
                sessiondb.ingest(self.db, entry["provider"], path)
                self.db.commit()
                key = self.source_key(path)
                ingested = self.projection(path)
                rows = token_usage.stored_source_rows(
                    self.db, key, sessiondb.read_raw_event, sessiondb.EVENT_COLUMNS, sessiondb.EVENT_JOIN,
                )
                result = token_usage.project_source(self.db, entry["provider"], key, str(path), rows)
                self.assertEqual(result["status"], "ok")
                self.assertEqual(self.projection(path), ingested)

    def test_conflict_marks_only_that_source_and_ingestion_still_succeeds(self):
        good = self.place("claude.jsonl")
        rows = [json.loads(text) for text in (FIXTURES / "synthetic_claude_stream.jsonl").read_text().splitlines()]
        rows.append(dict(rows[0], uuid="u9"))
        rows[-1]["message"] = dict(rows[0]["message"], usage=dict(rows[0]["message"]["usage"], input_tokens=11))
        bad = self.root / "src" / "bad.jsonl"
        bad.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        sessiondb.ingest(self.db, "claude", good)
        result = sessiondb.ingest(self.db, "claude", bad)
        self.assertEqual(result["status"], "imported")
        self.assertIn("conflicting usage", result["token_usage_error"])
        self.assertEqual(self.state(bad)["status"], "error")
        self.assertEqual(self.db.execute(
            "SELECT count(*) FROM token_usage WHERE source_key=?", (self.source_key(bad),)).fetchone()[0], 0)
        self.assertEqual(self.db.execute(
            "SELECT count(*) FROM events WHERE source_key=?", (self.source_key(bad),)).fetchone()[0], len(rows))
        self.assertEqual(self.projection(good), self.expected("claude.jsonl"))
        status = token_usage.status(self.db)
        self.assertEqual((status["ok"], status["error"], status["missing"]), (1, 1, 0))
        # A failed source stays frozen on append instead of projecting a partial tail.
        with bad.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(dict(rows[3], uuid="u10", message=dict(rows[3]["message"], id="m10"))) + "\n")
        sessiondb.ingest(self.db, "claude", bad)
        self.assertEqual(self.state(bad)["status"], "error")
        self.assertEqual(self.db.execute(
            "SELECT count(*) FROM token_usage WHERE source_key=?", (self.source_key(bad),)).fetchone()[0], 0)

    def test_rewritten_file_replaces_its_rows(self):
        path = self.place("codex.jsonl")
        sessiondb.ingest(self.db, "codex", path)
        lines = path.read_text().splitlines(keepends=True)
        path.write_text(lines[0], encoding="utf-8")
        result = sessiondb.ingest(self.db, "codex", path)
        self.assertEqual(result["status"], "imported")
        self.assertEqual(self.db.execute("SELECT count(*) FROM token_usage").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM token_runs").fetchone()[0], 1)

    def test_a_copied_call_counts_once_in_the_events_view(self):
        parent = self.root / "src" / "parent.jsonl"
        fork = self.root / "src" / "fork.jsonl"
        parent.parent.mkdir(parents=True, exist_ok=True)

        def assistant(session, uuid, message_id, output):
            return {"type": "assistant", "uuid": uuid, "sessionId": session, "timestamp": "2026-10-03T06:42:12Z",
                    "message": {"id": message_id, "model": "claude-opus-5",
                                "usage": {"input_tokens": 10, "output_tokens": output}}}
        parent.write_text(json.dumps(assistant("parent", "u1", "shared", 5)) + "\n", encoding="utf-8")
        fork.write_text(json.dumps(assistant("fork", "u1", "shared", 5)) + "\n"
                        + json.dumps(assistant("fork", "u2", "own", 7)) + "\n", encoding="utf-8")
        sessiondb.ingest(self.db, "claude", parent)
        sessiondb.ingest(self.db, "claude", fork)
        rows = dict(self.db.execute("SELECT source_event_id, run_id FROM token_usage_events").fetchall())
        self.assertEqual(rows, {"shared": "parent", "own": "fork"})
        contexts = self.db.execute("SELECT count(*) FROM token_context_events").fetchone()[0]
        self.assertEqual(contexts, 2)

    def test_codex_session_index_makes_no_run(self):
        path = self.root / "src" / "session_index.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "id": "019f0000-0000-7000-8000-000000000001", "thread_name": "A task",
            "updated_at": "2026-10-01T00:00:00Z",
        }) + "\n", encoding="utf-8")
        sessiondb.ingest(self.db, "codex", path)
        self.assertEqual(self.state(path)["status"], "ok")
        self.assertEqual(self.projection(path), {"runs": [], "usage": [], "context": []})

    def test_removed_source_takes_its_rows_with_it(self):
        path = self.place("codex.jsonl")
        sessiondb.ingest(self.db, "codex", path)
        source_id = self.db.execute("SELECT source_id FROM sources").fetchone()[0]
        full_corpus._remove_source(self.db, source_id)
        for table in token_usage.TOKEN_TABLES:
            self.assertEqual(self.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0, table)

    def test_child_lineage_and_effective_run_view(self):
        root = self.place("claude_root_delegation.jsonl")
        child = self.place("claude_child_delegation.jsonl")
        sessiondb.ingest(self.db, "claude", child)
        sessiondb.ingest(self.db, "claude", root)
        runs = {row["run_id"]: dict(row) for row in self.db.execute("SELECT * FROM token_runs_effective")}
        children = [run for run in runs.values() if run["parent_run_id"]]
        self.assertTrue(children)
        for run in children:
            self.assertEqual(run["run_kind"], "subagent")
            self.assertIn(run["parent_run_id"], runs)
            self.assertTrue(run["source_ref"])

    def effective(self, run_id):
        return dict(self.db.execute("SELECT * FROM token_runs_effective WHERE run_id=?", (run_id,)).fetchone())

    def test_launch_record_links_a_root_transcript_to_its_parent(self):
        parent = self.place("claude_root_delegation.jsonl")
        sessiondb.ingest(self.db, "claude", parent)
        parent_run = self.db.execute("SELECT run_id FROM token_runs WHERE parent_run_id IS NULL").fetchone()[0]
        sessiondb.ingest(self.db, "codex", self.place("codex.jsonl"))
        self.assertEqual(self.effective("root-session")["run_kind"], "root")
        with self.db:
            first = token_usage.record_launch(self.db, "root-session", parent_run, None,
                                              "2026-10-01T00:00:00Z", "codex_delegate", model="m")
            again = token_usage.record_launch(self.db, "root-session", parent_run, None,
                                              "2026-10-02T00:00:00Z", "codex_delegate")
        self.assertTrue(first["recorded"])
        self.assertFalse(again["recorded"])
        run = self.effective("root-session")
        self.assertEqual((run["run_kind"], run["parent_run_id"], run["root_run_id"]),
                         ("delegated", parent_run, parent_run))
        with self.assertRaisesRegex(ValueError, "already recorded"):
            token_usage.record_launch(self.db, "root-session", "other", None, "2026-10-01T00:00:00Z", "x")
        # Usage keeps the child's own run; the root groups it under the parent session.
        own = self.db.execute("SELECT count(*) FROM token_usage_events WHERE run_id='root-session'").fetchone()[0]
        self.assertGreater(own, 0)

    def test_transcript_lineage_wins_over_a_launch_record(self):
        child = self.place("claude_child_delegation.jsonl")
        sessiondb.ingest(self.db, "claude", child)
        run = self.db.execute("SELECT run_id, parent_run_id FROM token_runs WHERE parent_run_id IS NOT NULL").fetchone()
        with self.db:
            token_usage.record_launch(self.db, run[0], "someone-else", None, "2026-10-01T00:00:00Z", "x")
        effective = self.effective(run[0])
        self.assertEqual((effective["parent_run_id"], effective["run_kind"]), (run[1], "subagent"))

    def test_root_is_the_top_of_a_multi_level_chain(self):
        rows = [("top", None, "top", "root"), ("mid", "top", "mid", "subagent"),
                ("leaf", "mid", "mid", "guardian"), ("orphan", "gone", "orphan", "subagent"),
                ("loop-a", "loop-b", "loop-a", "subagent"), ("loop-b", "loop-a", "loop-b", "subagent")]
        with self.db:
            self.db.executemany(
                "INSERT INTO token_runs(source_key,run_id,provider,root_run_id,parent_run_id,run_kind,status)"
                " VALUES (1,?,'codex',?,?,?,'completed')",
                [(run, root, parent, kind) for run, parent, root, kind in rows])
        roots = {row[0]: row[1] for row in self.db.execute("SELECT run_id, root_run_id FROM token_runs_effective")}
        self.assertEqual((roots["top"], roots["mid"], roots["leaf"], roots["orphan"]), ("top", "top", "top", "gone"))
        self.assertIn(roots["loop-a"], ("loop-a", "loop-b"))

    def test_older_view_definition_is_replaced_on_open(self):
        self.db.execute("DROP VIEW token_runs_effective")
        self.db.execute("CREATE VIEW token_runs_effective AS SELECT * FROM token_runs_transcript")
        self.db.commit()
        self.db.close()
        self.db = sessiondb.connect(self.root / "db.sqlite")
        sql = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE name='token_runs_effective'").fetchone()[0]
        self.assertEqual(sql, token_usage.RUNS_EFFECTIVE_VIEW)

    def test_ledger_registrations_are_imported_once(self):
        ledger = sqlite3.connect(self.root / "ledger.sqlite")
        ledger.execute("CREATE TABLE delegations (launch_id TEXT, root_run_id TEXT, parent_run_id TEXT,"
                       " child_run_id TEXT, launched_at TEXT, model TEXT, reasoning_effort TEXT,"
                       " context_mode TEXT, purpose TEXT, attributes_json TEXT)")
        ledger.executemany("INSERT INTO delegations VALUES (?,?,?,?,?,?,?,?,?,?)", [
            ("a", "p", "p", "child-1", "2026-10-02T00:00:00Z", "gpt", "high", "isolated", "x", "{}"),
            ("b", "p", "p", "child-1", "2026-10-01T00:00:00Z", "gpt", "high", "isolated", "x", "{}"),
            ("c", "p", "p", "child-2", "2026-10-01T00:00:00Z", None, None, None, None,
             '{"launch_source":"claude_tool_result_agent_id"}'),
        ])
        with self.db:
            first = token_usage.import_ledger_launches(self.db, ledger)
            again = token_usage.import_ledger_launches(self.db, ledger)
        self.assertEqual((first["recorded"], again["already"], again["recorded"]), (2, 2, 0))
        self.assertEqual(self.db.execute("SELECT launched_at FROM token_launches WHERE child_run_id='child-1'").fetchone()[0],
                         "2026-10-01T00:00:00Z")
        ledger.close()

    def test_no_transcript_text_reaches_the_token_tables(self):
        for name, entry in GOLDEN.items():
            sessiondb.ingest(self.db, entry["provider"], self.place(name))
        dump = []
        for table in token_usage.TOKEN_TABLES:
            dump.extend(json.dumps([value for value in tuple(row)]) for row in self.db.execute(f"SELECT * FROM {table}"))
        text = "\n".join(dump)
        for secret in ("secret", "SECRET", "prompt"):
            self.assertNotIn(secret, text)

    def test_claude_cache_write_split_is_kept(self):
        path = self.place("synthetic_claude_stream.jsonl")
        sessiondb.ingest(self.db, "claude", path)
        row = self.db.execute(
            "SELECT cache_write_5m_input_tokens, cache_write_1h_input_tokens, model FROM token_usage "
            "WHERE source_event_id='m1'").fetchone()
        self.assertEqual(tuple(row), (0, 2, "claude-opus-5"))

    def test_out_of_range_numeric_timestamp_is_a_projection_error(self):
        for value in (1e20, float("inf"), float("nan")):
            with self.assertRaises(token_usage.TokenProjectionError):
                token_usage.rfc3339(value)


if __name__ == "__main__":
    unittest.main()
