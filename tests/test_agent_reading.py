import argparse
import json
import tempfile
import unittest
from pathlib import Path

import sessiondb


def _read_args(**overrides):
    values = dict(session="thread-a", provider=None, line=None, source=None, context=6, tail=None,
                  after_event=None, since=None, until=None, tools="brief", role=None, full=False,
                  offset=0, show_hidden=False, max_bytes=sessiondb.READ_PAGE_BYTES, evidence=False)
    values.update(overrides)
    return argparse.Namespace(**values)


def _codex(kind, timestamp, **payload):
    return {"type": kind, "timestamp": timestamp, "payload": payload}


class _ReadingFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "pilot.sqlite"
        self.db = sessiondb.connect(self.db_path)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def ingest(self, provider, rows, name):
        path = self.root / name
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
        sessiondb.ingest(self.db, provider, path)
        return path

    def read(self, **overrides):
        return sessiondb.read_session_text(self.db_path, _read_args(**overrides))

    def codex_session(self, assistant_text="Archived 102 sessions."):
        rows = [
            _codex("session_meta", "2026-09-03T05:31:04Z", id="thread-a"),
            _codex("response_item", "2026-09-03T05:31:05Z", type="message", role="developer",
                   content=[{"type": "input_text", "text": "system prompt text"}]),
            _codex("response_item", "2026-09-03T05:31:06Z", type="message", role="user", content=[
                {"type": "input_text", "text": "<environment_context>cwd</environment_context>"},
                {"type": "input_text", "text": "<requirements>archive all sessions that have been handoff"}]),
            _codex("event_msg", "2026-09-03T05:31:06Z", type="user_message",
                   message="<requirements>archive all sessions that have been handoff"),
            _codex("response_item", "2026-09-03T05:31:08Z", type="function_call", name="exec_command",
                   call_id="c1", arguments=json.dumps({"cmd": "list threads", "token": "sk-live-secret-123"})),
            _codex("response_item", "2026-09-03T05:31:09Z", type="function_call_output", call_id="c1",
                   output="102 threads handed off " + "word " * 200 + " FINAL-MARKER"),
            _codex("response_item", "2026-09-03T05:31:10Z", type="message", role="assistant",
                   content=[{"type": "output_text", "text": assistant_text}]),
            _codex("event_msg", "2026-09-03T05:31:10Z", type="agent_message", message=assistant_text),
        ]
        self.ingest("codex", rows, "rollout-2026-09-03T12-31-04-thread-a.jsonl")

class AgentReadingTest(_ReadingFixture):
    def test_conversation_view_hides_but_never_deletes(self):
        self.codex_session()
        text = self.read()
        self.assertNotIn("system prompt text", text)
        self.assertNotIn("environment_context", text)
        self.assertEqual(text.count("archive all sessions that have been handoff"), 1)  # copies hidden
        self.assertIn("<requirements>", text)  # an unknown tag is the user's text, not harness
        self.assertIn("hidden parts", text)
        shown = self.read(show_hidden=True)
        self.assertIn("environment_context", shown)
        self.assertIn("system prompt text", shown)

    def test_similar_long_messages_are_not_collapsed(self):
        rows = [_codex("session_meta", "2026-09-03T05:31:04Z", id="thread-c")]
        for n, ending in enumerate(("first conclusion", "second conclusion")):
            rows.append(_codex("response_item", f"2026-09-03T05:31:1{n}Z", type="message", role="assistant",
                               content=[{"type": "output_text", "text": "same intro " * 80 + ending}]))
        self.ingest("codex", rows, "rollout-2026-09-03T12-31-04-thread-c.jsonl")
        text = self.read(session="thread-c")
        self.assertIn("first conclusion", text)
        self.assertIn("second conclusion", text)

    def test_tool_lines_are_redacted_before_shortening_and_full_output_is_reachable(self):
        self.codex_session()
        brief = self.read()
        self.assertIn("→ exec_command: list threads", brief)
        self.assertIn("← exec_command: 102 threads handed off", brief)
        self.assertNotIn("FINAL-MARKER", brief)
        self.assertNotIn("sk-live-secret", brief)
        output_line = int(next(line for line in brief.splitlines() if "← exec_command" in line)[2:].split()[0])
        full = self.read(line=output_line, full=True)
        self.assertIn("FINAL-MARKER", full)
        call_line = int(next(line for line in brief.splitlines() if "→ exec_command" in line)[2:].split()[0])
        self.assertNotIn("sk-live-secret", self.read(line=call_line, full=True))
        paged = self.read(line=output_line, full=True, max_bytes=300)
        self.assertIn("--offset", paged)

    def test_mixed_claude_record_keeps_its_text_for_role_filters(self):
        self.ingest("claude", [
            {"type": "user", "sessionId": "mixed-c", "timestamp": "2026-09-01T00:00:00Z",
             "message": {"role": "user", "content": "please check"}},
            {"type": "assistant", "sessionId": "mixed-c", "timestamp": "2026-09-01T00:00:01Z",
             "message": {"role": "assistant", "content": [
                 {"type": "text", "text": "I will run the check now"},
                 {"type": "tool_use", "id": "t1", "name": "Bash",
                  "input": {"command": "ls", "password": "hunter2"}}]}},
        ], "mixed-c.jsonl")
        text = self.read(session="mixed-c")
        self.assertIn("assistant] I will run the check now", text)
        self.assertIn("→ Bash: ls", text)
        self.assertNotIn("hunter2", self.read(session="mixed-c", line=2, full=True))
        self.assertIn("I will run the check now", self.read(session="mixed-c", role="assistant"))

    def test_anchor_is_exact_and_always_shown(self):
        self.codex_session(assistant_text="answer " + "words " * 3000)
        text = self.read(line=7, context=3, max_bytes=2000)
        self.assertIn("[L7 ", text)
        with self.assertRaises(ValueError):
            self.read(line=999)
        filtered = self.read(line=6, role="user")
        self.assertIn("outside the filters", filtered)

    def test_paging_keeps_filters_and_expired_cursor_is_an_error(self):
        self.codex_session()
        page = self.read(max_bytes=400, tools="none", until="2026-09-03T06:00:00Z")
        hint = next(line for line in page.splitlines() if line.startswith("[more:"))
        self.assertIn("--tools none", hint)
        self.assertIn("--until 2026-09-03T06:00:00Z", hint)
        self.assertIn("--after-event", hint)
        with self.assertRaises(ValueError):
            self.read(after_event="0" * 16)

    def test_missing_watcher_status_is_reported(self):
        self.codex_session()
        self.assertIn("[coverage unknown: watcher status missing]", self.read())

    def test_readable_passage_keeps_words_and_numbers(self):
        passage = ("2026-09-03T05:57:44.896Z … event_msg … 01a065bf-cfb1-75e1-9689-8e16c01b721b … "
                   "toolu_01ABC … 410000 … the useful sentence stays here")
        self.assertEqual(sessiondb.readable_passage(passage),
                         "event_msg … 410000 … the useful sentence stays here")

    def test_search_text_groups_sessions_with_evidence_and_exact_read_commands(self):
        self.codex_session()
        self.ingest("codex", [{"id": "thread-a", "thread_name": "Archive handed-off sessions",
                               "updated_at": "2026-09-03T05:31:35Z"}], sessiondb.CODEX_SESSION_INDEX_NAME)
        args = argparse.Namespace(text="handoff", mode="ranked", provider=None, project=None, session=None,
                                  role=None, tool=None, lineage=None, since=None, until=None,
                                  diversify=True, order="relevance")
        result = sessiondb.bundle_if_oversized(self.db_path, sessiondb.search_index(self.db, args), None)
        text = sessiondb.render_search_text(result, self.db_path, args)
        self.assertIn("thread-a · Archive handed-off sessions", text)
        self.assertIn("evidence around", text)
        self.assertRegex(text, r"read: read --session thread-a --provider codex --source rollout-[\w.-]+\.jsonl --line \d+")
        self.assertLess(len(text.encode()), len(json.dumps(result).encode()) * 2)

    def test_diversify_round_robins_sessions(self):
        entries = [{"provider": "codex", "session": s, "n": n} for n, s in enumerate("aaab")]
        self.assertEqual([e["n"] for e in sessiondb.diversify_sessions(entries, limit=2)], [0, 3, 1, 2])


class FewerStepsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "pilot.sqlite"
        self.db = sessiondb.connect(self.db_path)
        rows = [
            _codex("session_meta", "2026-09-03T05:31:04Z", id="thread-a", originator="Codex Desktop",
                   cli_version="0.150.0", cwd="/Users/example/Workspace"),
            _codex("turn_context", "2026-09-03T05:31:04Z", model="gpt-5.6-sol", cwd="/Users/example/Workspace"),
            _codex("response_item", "2026-09-03T05:31:06Z", type="message", role="user",
                   content=[{"type": "input_text", "text": "why did the push need approval?"}]),
            _codex("response_item", "2026-09-03T05:31:08Z", type="function_call", name="exec_command",
                   call_id="c1", arguments=json.dumps({"cmd": "git push"})),
            _codex("response_item", "2026-09-03T05:31:09Z", type="function_call_output", call_id="c1",
                   output="rejected: external destination " + "detail " * 300),
            _codex("response_item", "2026-09-03T05:31:10Z", type="message", role="assistant",
                   content=[{"type": "output_text", "text": "The reviewer saw an external destination."}]),
        ]
        path = self.root / "rollout-2026-09-03T12-31-04-thread-a.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        sessiondb.ingest(self.db, "codex", path)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def test_evidence_is_the_hit_with_its_request_and_reply(self):
        text = sessiondb.read_session_text(self.db_path, _read_args(line=5, evidence=True))
        body = text.splitlines()[1:]
        self.assertTrue(body[0].startswith("[L3 ") and "user] why did the push need approval?" in body[0])
        self.assertTrue(any("→ exec_command: git push" in line for line in body))
        self.assertTrue(any("← exec_command: rejected: external destination" in line for line in body))
        self.assertTrue(any(line.startswith("[L6 ") and "external destination" in line for line in body))
        self.assertTrue(body[-1].startswith("[coverage"))  # a standalone read keeps its coverage line

    def test_header_names_app_model_version_and_folder(self):
        header = sessiondb.read_session_text(self.db_path, _read_args(tail=1)).splitlines()[0]
        for value in ("app Codex Desktop", "model gpt-5.6-sol", "version 0.150.0", "cwd /Users/example/Workspace"):
            self.assertIn(value, header)

    def test_symbol_queries_become_exact_text_and_others_do_not(self):
        self.assertEqual(sessiondb.literal_query("Write(/Users/example/**)"), '"Write(/Users/example/**)"')
        self.assertEqual(sessiondb.literal_query("rule src/a.py drift"), 'rule "src/a.py" drift')
        self.assertEqual(sessiondb.literal_query("archiv* handoff"), "archiv* handoff")
        self.assertEqual(sessiondb.literal_query("<INSTRUCTIONS>"), '"<INSTRUCTIONS>"')
        self.assertEqual(sessiondb.literal_query("push approval"), "push approval")
        self.assertEqual(sessiondb.literal_query('"a/b" OR c'), '"a/b" OR c')

    def test_widening_only_when_all_words_found_nothing(self):
        empty = {"entries": [], "complete": True, "total": 0}
        self.assertEqual(sessiondb.widened_query("push zzqx approval", empty), "push OR zzqx OR approval")
        self.assertIsNone(sessiondb.widened_query("push approval", {"entries": [{}], "complete": True}))
        self.assertIsNone(sessiondb.widened_query("push", empty))
        self.assertIsNone(sessiondb.widened_query('"push approval" x', empty))
        self.assertIsNone(sessiondb.widened_query("push approval", {"entries": [], "complete": False}))
        # an empty preview of a non-empty result is not a zero result
        self.assertIsNone(sessiondb.widened_query("push approval", {"entries": [], "complete": True, "total": 5}))


class RoundTwoTracesTest(unittest.TestCase):
    def test_parenthesis_inside_a_word_is_part_of_the_word(self):
        atoms = [token for token in sessiondb.search_query_tokens("Edit( redundant") if not isinstance(token, str)]
        self.assertEqual([atom.value for atom in atoms], ["Edit(", "redundant"])
        tree = sessiondb.parse_search_query('"Write(" Edit( redundant OR duplicate')
        self.assertEqual(tree[0], "OR")
        self.assertEqual(sessiondb.search_query_tokens("Write(/x/**)")[0].value, "Write(/x/**)")

    def test_grouping_parentheses_still_group(self):
        tree = sessiondb.parse_search_query("(archive OR archived) handoff")
        self.assertEqual(tree[0], "AND")
        self.assertEqual(tree[1][0], "OR")
        tokens = sessiondb.search_query_tokens("(rule Edit(x)) drift")
        self.assertEqual([token if isinstance(token, str) else token.value for token in tokens],
                         ["(", "rule", "Edit(x)", ")", "drift"])

    def test_query_error_is_not_a_coverage_gap(self):
        with self.assertRaises(sessiondb.QuerySyntaxError):
            sessiondb.parse_search_query('"unterminated')
        text = sessiondb.render_search_text(
            {"complete": False, "reason": "query_error: unterminated quoted phrase", "entries": []})
        self.assertTrue(text.startswith("query error: unterminated quoted phrase"))
        self.assertNotIn("coverage incomplete", text)

    def test_time_filters_cover_whole_dates_and_reject_line_numbers(self):
        self.assertEqual(sessiondb._since_bound("2026-08-10"), "2026-08-10T00:00:00.000000Z")
        self.assertEqual(sessiondb._until_bound("2026-08-10"), "2026-08-10T23:59:59.999999Z")
        self.assertEqual(sessiondb._until_bound("2026-02"), "2026-02-28T23:59:59.999999Z")
        self.assertEqual(sessiondb._until_bound("2026-08-10T09:00"), "2026-08-10T09:00:00.000000Z")
        self.assertEqual(sessiondb._since_bound("2026-08-10T16:00+07:00"), "2026-08-10T09:00:00.000000Z")
        with self.assertRaises(argparse.ArgumentTypeError):
            sessiondb._until_bound("L154")


class AstraReviewCasesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "pilot.sqlite"
        self.db = sessiondb.connect(self.db_path)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def ingest(self, rows, name):
        path = self.root / name
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows))
        sessiondb.ingest(self.db, "codex", path)

    def test_prefix_tokens_are_latin_words_only(self):
        self.assertEqual(sessiondb.search_query_tokens("archiv*")[0].kind, "prefix")
        self.assertEqual(sessiondb.search_query_tokens("báo*")[0].kind, "prefix")
        self.assertNotEqual(sessiondb.search_query_tokens("東京*")[0].kind, "prefix")
        self.assertNotEqual(sessiondb.search_query_tokens("a*")[0].kind, "prefix")

    def test_evidence_window_keeps_the_part_that_matches(self):
        text = "intro " * 2000 + "THE-DECISION-IS-HERE " + "outro " * 50
        window = sessiondb._window_around_match(text, "decision", 600)
        self.assertIn("THE-DECISION-IS-HERE", window)
        self.assertTrue(window.startswith("[…"))

    def test_copy_is_hidden_only_next_to_its_original(self):
        rows = [
            _codex("session_meta", "2026-09-03T05:31:04Z", id="thread-d"),
            _codex("response_item", "2026-09-03T05:31:05Z", type="message", role="assistant",
                   content=[{"type": "output_text", "text": "first answer"}]),
            _codex("event_msg", "2026-09-03T05:31:05Z", type="agent_message", message="first answer"),
            _codex("event_msg", "2026-09-03T06:40:00Z", type="agent_message", message="unique later answer"),
        ]
        self.ingest(rows, "rollout-2026-09-03T12-31-04-thread-d.jsonl")
        text = sessiondb.read_session_text(self.db_path, _read_args(session="thread-d"))
        self.assertEqual(text.count("first answer"), 1)
        self.assertIn("unique later answer", text)
        # A session whose answer exists only as a copy still shows it.
        self.ingest([_codex("session_meta", "2026-09-03T05:31:04Z", id="thread-b"),
                     _codex("event_msg", "2026-09-03T05:31:06Z", type="agent_message", message="only answer")],
                    "rollout-2026-09-03T12-31-04-thread-b.jsonl")
        self.assertIn("only answer", sessiondb.read_session_text(self.db_path, _read_args(session="thread-b")))


class PastQueryCoverageTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def region(self, rows, until, start=0):
        path = self.root / "s.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        return sessiondb.region_after_until(path, start, argparse.Namespace(until=until))

    def test_only_a_fully_checked_later_region_is_excluded(self):
        later = {"timestamp": "2026-09-05T00:00:00Z"}
        older = {"timestamp": "2026-08-01T00:00:00Z"}
        self.assertTrue(self.region([later, later], "2026-09-01T00:00:00Z"))
        self.assertFalse(self.region([later, older], "2026-09-01T00:00:00Z"))  # out of order
        self.assertTrue(self.region([{"id": "x", "thread_name": "t", "updated_at": "2026-09-05T00:00:00Z"}],
                                    "2026-09-01T00:00:00Z"))
        # A restored or copied transcript is new on disk but holds old events.
        self.assertFalse(self.region([older], "2026-09-01T00:00:00Z"))
        # A last line without a newline is still a record once it parses; a partial one is unknown.
        path = self.root / "s.jsonl"
        args = argparse.Namespace(until="2026-09-01T00:00:00Z")
        path.write_text(json.dumps(later) + "\n" + json.dumps(older))
        self.assertFalse(sessiondb.region_after_until(path, 0, args))
        path.write_text(json.dumps(later) + "\n" + '{"timestamp": "2026-09-0')
        self.assertFalse(sessiondb.region_after_until(path, 0, args))

    def test_untimestamped_lines_take_the_time_of_the_record_before_them(self):
        later = {"timestamp": "2026-09-05T00:00:00Z"}
        self.assertTrue(self.region([later, {"type": "summary"}], "2026-09-01T00:00:00Z"))
        self.assertFalse(self.region([{"type": "summary"}, later], "2026-09-01T00:00:00Z"))
        path = self.root / "s.jsonl"
        args = argparse.Namespace(until="2026-09-01T00:00:00Z")
        self.assertTrue(sessiondb.region_after_until(path, 0, args, previous="2026-09-04T00:00:00Z"))

    def test_a_shared_title_index_tail_is_never_scoped_to_its_current_sessions(self):
        db = sessiondb.connect(self.root / "db.sqlite")
        try:
            path = self.root / sessiondb.CODEX_SESSION_INDEX_NAME
            path.write_text(json.dumps({"id": "session-a", "thread_name": "A", "updated_at": "2026-09-01T00:00:00Z"}) + "\n")
            sessiondb.ingest(db, "codex", path)
            source_id = db.execute("SELECT source_id FROM sources").fetchone()[0]
            args = argparse.Namespace(project=None, session="session-b", lineage=None, until=None)
            self.assertTrue(sessiondb.appended_tail_affects_query(db, source_id, args, path, 0))
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()


class ReviewFindingsTest(_ReadingFixture):
    """Problems found in the October review, each with the smallest reproducing case."""

    def search_args(self, **overrides):
        values = dict(text="handoff", mode="ranked", provider=None, project=None, session=None, role=None,
                      tool=None, lineage=None, since=None, until=None, diversify=True, order="relevance")
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_query_errors_are_reported_before_any_database_work(self):
        missing = self.root / "missing.sqlite"
        for text in ('"unterminated', "NOT foo", "(a OR", "   "):
            result = sessiondb.search_index_entrypoint(missing, self.search_args(
                text=text, manifest=self.root / "manifest.json"))
            self.assertTrue(result["reason"].startswith("query_error:"), (text, result.get("reason")))
            self.assertTrue(sessiondb.render_search_text(result).startswith("query error:"))

    def test_coverage_after_the_search_stays_inside_its_deadline(self):
        self.codex_session()
        self.db.commit()
        original = sessiondb.query_coverage
        seen = []
        def expired(*_args, **_kwargs):
            seen.append(sessiondb._QUERY_DEADLINE.get())
            raise sessiondb.QueryDeadlineExceeded("search deadline exceeded")
        sessiondb.query_coverage = expired
        try:
            result = sessiondb.search_index_entrypoint(self.db_path, self.search_args(deadline_seconds=30))
        finally:
            sessiondb.query_coverage = original
        self.assertEqual(len(seen), 1)
        self.assertIsNotNone(seen[0])
        self.assertEqual(result["coverage"]["reason"], "query_deadline_exceeded")

    def test_a_time_window_narrows_matches_by_index_with_the_same_results(self):
        self.codex_session()
        self.db.commit()
        self.assertIsNotNone(self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='events_time_idx'").fetchone())
        args = self.search_args(text="threads", since="2026-09-03T05:31:08Z", until="2026-09-03T05:31:09Z")
        with_index = [e["result_id"] for e in sessiondb.search_index(self.db, args)["entries"]]
        self.db.execute("DROP INDEX events_time_idx")
        without = [e["result_id"] for e in sessiondb.search_index(self.db, args)["entries"]]
        self.assertEqual(with_index, without)
        self.assertTrue(with_index)

    def test_a_search_that_ran_out_of_time_is_not_a_no_match(self):
        text = sessiondb.render_search_text(
            {"complete": False, "entries": [], "coverage": {"complete": False, "reason": "query_deadline_exceeded"}})
        self.assertTrue(text.startswith("search stopped at its time limit"))
        self.assertNotIn("no matches", text)

    def test_more_results_start_at_the_first_record_not_shown(self):
        entries = [{"session": "s1", "source": "f", "line": n, "passage": f"hit {n}"} for n in (1, 2, 3)]
        result = {"entries": entries, "total": 3, "complete": True, "bundle": {"path": "/tmp/a b/x.jsonl"},
                  "sessions": {"s1": {"provider": "codex", "session": "s1"}}, "sources": {}}
        text = sessiondb.render_search_text({**result, "coverage": "missing"}, None,
                                            self.search_args(until="2026-09-03T06:00:00.000000Z"))
        self.assertIn("--offset 2", text)
        self.assertIn("'/tmp/a b/x.jsonl'", text)
        self.assertIn("--until 2026-09-03T06:00:00.000000Z", text)

    def test_session_search_keeps_the_passage_of_a_record_it_cannot_open(self):
        self.codex_session()
        entries = [{"session": "thread-a", "source": "f", "line": 999, "passage": "tail passage"}]
        result = {"entries": entries, "total": 1, "complete": True,
                  "sessions": {"thread-a": {"provider": "codex", "session": "thread-a"}}, "sources": {}}
        text = sessiondb.render_search_text(result, self.db_path, self.search_args(session="thread-a"))
        self.assertIn("tail passage", text)

    def test_full_read_keeps_every_field_of_a_tool_input(self):
        self.ingest("claude", [
            {"type": "assistant", "sessionId": "full-c", "timestamp": "2026-09-01T00:00:01Z",
             "message": {"role": "assistant", "content": [
                 {"type": "tool_use", "id": "t1", "name": "Bash",
                  "input": {"command": "ls", "cwd": "/tmp/project-x"}}]}}], "full-c.jsonl")
        self.assertNotIn("project-x", self.read(session="full-c"))
        self.assertIn("/tmp/project-x", self.read(session="full-c", line=1, full=True))
        # A tool result's extra fields and a contentless metadata record are shown whole too.
        self.ingest("claude", [
            {"type": "user", "sessionId": "full-r", "timestamp": "2026-09-01T00:00:02Z",
             "message": {"role": "user", "content": [
                 {"type": "tool_result", "tool_use_id": "t1", "is_error": True,
                  "content": [{"type": "text", "text": "done"}, {"type": "image", "source": "img-ref-7"}]},
                 {"type": "text", "text": "and a note"}, {"type": "document", "title": "spec-doc-9"}]}},
            {"type": "cost-state", "sessionId": "full-r", "totalDuration": 1551}], "full-r.jsonl")
        full = self.read(session="full-r", line=1, full=True)
        for kept in ("img-ref-7", "is_error", "and a note", "spec-doc-9"):
            self.assertIn(kept, full)
        self.assertIn("totalDuration", self.read(session="full-r", line=2, full=True))

    def test_evidence_parts_of_one_record_share_the_byte_budget(self):
        self.ingest("claude", [
            {"type": "assistant", "sessionId": "many-c", "timestamp": "2026-09-01T00:00:01Z",
             "message": {"role": "assistant", "content": [
                 {"type": "tool_use", "id": f"t{n}", "name": "Bash", "input": {"command": "list files " * 80}}
                 for n in range(20)]}}], "many-c.jsonl")
        text = self.read(session="many-c", line=1, evidence=True, max_bytes=3000)
        self.assertLess(len(text.encode()), 6000)
        self.assertIn("more parts of L1: add --full", text)

    def test_canonical_copies_are_loaded_only_when_a_copy_needs_checking(self):
        self.ingest("claude", [{"type": "user", "sessionId": "plain-c", "timestamp": "2026-09-01T00:00:00Z",
                                "message": {"role": "user", "content": "hello"}}], "plain-c.jsonl")
        original = sessiondb._canonical_messages
        calls = []
        sessiondb._canonical_messages = lambda *a: calls.append(1) or original(*a)
        try:
            self.read(session="plain-c")
        finally:
            sessiondb._canonical_messages = original
        self.assertEqual(calls, [])

    def test_prefix_is_fts_verified_only_on_the_accent_keeping_index(self):
        self.assertFalse(sessiondb.fts_fully_verifies(sessiondb.SearchAtom("prefix", "bao")))

    def test_bundle_text_page_says_when_it_is_incomplete_and_keeps_its_filters(self):
        args = argparse.Namespace(offset=0, bundle=Path("/tmp/a b.jsonl"), until=None, provider="codex",
                                  project=None, session=None, role="user", tool=None)
        page = {"complete": False, "entries": [], "issues": [{"x": 1}], "has_more": False}
        self.assertIn("incomplete", sessiondb.render_bundle_page_text(page, args))
        page = {"complete": False, "entries": [], "coverage": "missing", "has_more": True}
        text = sessiondb.render_bundle_page_text(page, args)
        self.assertIn("incomplete: missing", text)
        self.assertIn("--bundle '/tmp/a b.jsonl' --offset 0 --provider codex --role user", text)

    def test_read_hints_are_quoted_and_keep_the_provider(self):
        hint = sessiondb._read_hint("s1", _read_args(provider="codex"), source="my file.jsonl", line=4)
        self.assertEqual(hint, "read --session s1 --provider codex --source 'my file.jsonl' --line 4")


class GrepAndRedirectTest(_ReadingFixture):
    """Agents reach for grep and scripts on transcripts; SessionDB answers the same needs."""

    def grep(self, pattern, **options):
        values = dict(pattern=pattern, fixed_strings=False, ignore_case=False, sessions_only=False,
                      context=40, max_count=40, reverse=False, provider=None, project=None,
                      session=None, role=None, since=None, until=None)
        values.update(options)
        return sessiondb.grep_history(self.db_path, argparse.Namespace(**values))

    def test_fixed_string_and_regex_find_records_with_context(self):
        self.codex_session()
        self.db.commit()
        exact = self.grep("102 threads handed", fixed_strings=True)
        self.assertIn("102 threads handed off", exact)
        self.assertIn("read: read --session thread-a", exact)
        self.assertIn("handed off", self.grep(r"threads\s+handed", ignore_case=True))
        listed = self.grep("Archived 102", fixed_strings=True, sessions_only=True)
        self.assertRegex(listed, r"codex thread-a · .* · 1 matches · first 2026-09-03T05:31Z")

    def test_secrets_are_matched_but_never_shown(self):
        self.codex_session()
        self.db.commit()
        self.assertNotIn("sk-live-secret", self.grep("list threads", fixed_strings=True))

    def test_a_pattern_without_words_needs_a_scope(self):
        self.codex_session()
        self.db.commit()
        self.assertTrue(self.grep(r"\d{3}").startswith("pattern error"))
        self.assertIn("102", self.grep(r"\d{3} threads", session="thread-a"))

    def test_literal_runs_follow_regex_alternatives(self):
        branches = sessiondb._literal_branches("escalat|tag me", False)
        self.assertEqual([[run[0] for run in branch] for branch in branches], [["escalat"], ["tag me"]])
        self.assertIsNone(sessiondb._literal_branches(r"\d+", False))


class SessionsListTest(_ReadingFixture):
    def test_sessions_by_time_window_and_by_title(self):
        self.codex_session()
        self.ingest("codex", [{"id": "thread-a", "thread_name": "Archive handed-off sessions",
                               "updated_at": "2026-09-03T05:31:35Z"}], sessiondb.CODEX_SESSION_INDEX_NAME)
        self.db.commit()
        args = dict(title=None, since="2026-09-03T00:00:00.000000Z", until="2026-09-03T23:59:59.999999Z",
                    provider=None, project=None, reverse=False, limit=60)
        listed = sessiondb.list_sessions(self.db_path, argparse.Namespace(**args))
        self.assertIn("codex thread-a · Archive handed-off sessions · 2026-09-03T05:31Z", listed)
        by_title = sessiondb.list_sessions(self.db_path, argparse.Namespace(
            **{**args, "since": None, "until": None, "title": "handed archive"}))
        self.assertTrue(by_title.startswith("1 sessions"))
        none = sessiondb.list_sessions(self.db_path, argparse.Namespace(**{**args, "title": "invoice"}))
        self.assertEqual(none, "0 sessions")

    def test_claude_sessions_are_found_by_title(self):
        # Claude title records keep the title in a field, not in message content.
        self.ingest("claude", [
            {"type": "user", "sessionId": "lead-c", "timestamp": "2026-09-03T05:31:05Z",
             "message": {"role": "user", "content": "check the clinic proposal"}},
            {"type": "custom-title", "sessionId": "lead-c", "customTitle": "Sakura lead: Line OA integration"},
            {"type": "agent-name", "sessionId": "lead-c", "agentName": "Sakura lead: Line OA integration"},
        ], "lead-c.jsonl")
        self.ingest("claude", [
            {"type": "user", "sessionId": "meme-c", "timestamp": "2026-09-03T06:00:00Z",
             "message": {"role": "user", "content": "make a meme"}},
            {"type": "ai-title", "sessionId": "meme-c", "aiTitle": "Karpathy frontier models meme"},
        ], "meme-c.jsonl")
        self.db.commit()
        args = dict(since=None, until=None, provider=None, project=None, reverse=False, limit=60)
        for title, session in (("Sakura Line OA", "lead-c"), ("karpathy meme", "meme-c")):
            listed = sessiondb.list_sessions(self.db_path, argparse.Namespace(**args, title=title))
            self.assertTrue(listed.startswith("1 sessions"), listed)
            self.assertIn(f"claude {session} ·", listed)
        self.assertEqual(sessiondb.list_sessions(self.db_path, argparse.Namespace(**args, title="clinic")),
                         "0 sessions")


class SearchTextErrorTest(unittest.TestCase):
    def test_an_unreadable_database_is_not_reported_as_a_no_match(self):
        result = {"complete": False, "entries": [], "total": None, "sessions": {}, "sources": {},
                  "reason": "database_error: database is locked"}
        text = sessiondb.render_search_text(result)
        self.assertIn("could not read the database (database is locked)", text)
        self.assertIn("not a no-match", text)
        self.assertNotIn("try other words", text)
