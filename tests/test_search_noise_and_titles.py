import argparse
import json
import tempfile
import unittest
from pathlib import Path

import sessiondb


def _search_args(**overrides):
    values = dict(text="", mode="literal", provider=None, project=None, session=None, role=None,
                  tool=None, lineage=None, since=None, until=None)
    values.update(overrides)
    return argparse.Namespace(**values)


class SearchNoiseAndTitlesTest(unittest.TestCase):
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
        result = sessiondb.ingest(self.db, provider, path)
        return path, result

    def search(self, **overrides):
        return sessiondb.bundle_if_oversized(
            self.db_path, sessiondb.search_index(self.db, _search_args(**overrides)), None)

    def test_user_role_filter_skips_tool_results_stored_as_user_records(self):
        self.ingest("claude", [
            {"type": "assistant", "sessionId": "c1", "message": {"role": "assistant", "content": [
                {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]}},
            {"type": "user", "sessionId": "c1", "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": "rolemarker from a tool"}]}},
            {"type": "user", "sessionId": "c1", "message": {"role": "user",
                                                            "content": "rolemarker typed by the user"}},
        ], "c1.jsonl")
        unfiltered = self.search(text="rolemarker")
        self.assertEqual(unfiltered["total"], 2)
        typed = self.search(text="rolemarker", role="user")
        self.assertEqual(typed["total"], 1)
        self.assertIn("typed by the user", typed["entries"][0]["passage"])
        rows, _ = sessiondb.matching_rows(self.db, argparse.Namespace(
            provider=None, session="c1", role="user", tool=None, lineage=None, project=None,
            since=None, until=None, text=None, raw=False, limit=10, offset=0))
        self.assertEqual([row["tool_event"] for row in rows], [0])

    def test_system_prompts_rank_after_conversation_unless_asked_for(self):
        prompt_rows = [
            {"type": "session_meta", "timestamp": "2026-09-01T00:00:00Z", "payload": {"id": f"x{n}"}}
            for n in range(1)
        ]
        rows = prompt_rows + [
            {"type": "response_item", "timestamp": "2026-09-01T00:00:01Z", "payload": {
                "type": "message", "role": "developer",
                "content": [{"type": "input_text", "text": "archive threads setupmarker"}]}}
            for _ in range(3)
        ] + [
            {"type": "event_msg", "timestamp": "2026-09-01T00:00:02Z", "payload": {
                "type": "user_message",
                "message": "please archive the handed off threads setupmarker and keep the rest of this "
                           "longer typed request so it is not the shortest record"}},
        ]
        self.ingest("codex", rows, "rollout-2026-09-01T00-00-00-x0.jsonl")
        ranked = self.search(text="setupmarker")
        self.assertEqual(ranked["entries"][0]["role"], "user")
        developer = self.search(text="setupmarker", role="developer")
        self.assertTrue(developer["entries"])
        self.assertTrue(all(entry["role"] == "developer" for entry in developer["entries"]))

    def test_setup_demotion_keeps_order_within_each_group(self):
        entries = [{"role": "developer", "category": "message", "n": 1},
                   {"role": "user", "category": "user", "n": 2},
                   {"role": None, "category": "compacted", "n": 3},
                   {"role": "assistant", "category": "assistant", "n": 4}]
        order = [entry["n"] for entry in sessiondb.demote_setup_records(entries, None)]
        self.assertEqual(order, [2, 4, 1, 3])
        self.assertEqual([e["n"] for e in sessiondb.demote_setup_records(entries, "developer")], [1, 2, 3, 4])

    def test_codex_session_index_rows_become_searchable_titles(self):
        self.ingest("codex", [
            {"type": "session_meta", "timestamp": "2026-09-03T05:31:04Z", "payload": {"id": "thread-a"}},
            {"type": "event_msg", "timestamp": "2026-09-03T05:31:05Z",
             "payload": {"type": "user_message", "message": "titleprobe archive all sessions"}},
        ], "rollout-2026-09-03T12-31-04-thread-a.jsonl")
        _, result = self.ingest("codex", [
            {"id": "thread-a", "thread_name": "Archive handed-off sessions",
             "updated_at": "2026-09-03T05:31:35Z"},
        ], sessiondb.CODEX_SESSION_INDEX_NAME)
        self.assertEqual(result["issues"], 0)
        found = self.search(text="titleprobe")
        title = found["sessions"][found["entries"][0]["session"]]["title"]
        self.assertEqual(title, {"status": "known", "value": "Archive handed-off sessions"})
        by_title = self.search(text="Archive handed-off sessions")
        self.assertEqual(by_title["total"], 1)
        self.assertEqual(found["sessions"][found["entries"][0]["session"]]["session"],
                         by_title["sessions"][by_title["entries"][0]["session"]]["session"])

    def test_title_shaped_rows_elsewhere_stay_unsupported(self):
        _, result = self.ingest("codex", [
            {"id": "thread-b", "thread_name": "Looks like a title", "updated_at": "2026-09-03T05:31:35Z"},
        ], "rollout-2026-09-03T12-31-04-thread-b.jsonl")
        self.assertEqual(result["issues"], 1)

    def test_newest_title_is_read_without_scanning_every_title_record(self):
        rows = [{"type": "ai-title", "sessionId": "many-c", "timestamp": f"2026-09-01T00:{n // 60:02d}:{n % 60:02d}Z",
                 "aiTitle": f"Title {n}"} for n in range(300)]
        rows.append({"type": "assistant", "sessionId": "many-c", "timestamp": "2026-09-02T00:00:00Z",
                     "message": {"role": "assistant", "content": "manytitlesprobe"}})
        self.ingest("claude", rows, "many-c.jsonl")
        found = self.search(text="manytitlesprobe")
        title = found["sessions"][found["entries"][0]["session"]]["title"]
        self.assertEqual(title, {"status": "known", "value": "Title 299"})
        plan = " ".join(row[3] for row in self.db.execute(
            "EXPLAIN QUERY PLAN SELECT e.* FROM sessions s JOIN sources src ON src.source_id=s.source_id "
            "JOIN events e ON e.source_key=src.source_key "
            "AND e.session_id=s.session_id WHERE s.provider='claude' AND s.session_id='many-c' "
            "AND e.category IN ('ai-title','custom-title','thread_name') "
            "ORDER BY e.order_timestamp DESC, e.line_number DESC LIMIT 8"))
        self.assertIn("events_title_idx", plan)

    def test_a_file_root_is_discovered_as_one_source(self):
        index = self.root / sessiondb.CODEX_SESSION_INDEX_NAME
        index.write_text('{"id": "a", "thread_name": "t", "updated_at": "2026-09-01T00:00:00Z"}\n')
        folder = self.root / "sessions"
        folder.mkdir()
        (folder / "rollout-x.jsonl").write_text('{"type": "session_meta"}\n')
        found = sessiondb.discover_transcript_sources({"codex": [folder, index]})
        self.assertEqual(sorted((p.name, kind) for _, p, kind in found),
                         [("rollout-x.jsonl", "active"), (sessiondb.CODEX_SESSION_INDEX_NAME, "index")])


if __name__ == "__main__":
    unittest.main()
