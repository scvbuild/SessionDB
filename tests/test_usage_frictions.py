"""Fixes for the frictions seen in real SessionDB use after the 2026-10-03 install."""
import argparse
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import full_corpus
import sessiondb

ORDER = "e.order_timestamp,e.provider,e.session_id,e.source_id,e.line_number,e.event_id"


def query_args(**changes):
    values = dict(
        text=None, provider=None, project=None, session=None, role=None, tool=None,
        lineage=None, since=None, until=None, raw=False, limit=1000, offset=0,
        before=0, after=0, include_page=True, include_hashes=False,
    )
    values.update(changes)
    return argparse.Namespace(**values)


class SessionSourcePrefilterTest(unittest.TestCase):
    PATTERNS = (
        "alpha_1", "alpha", "agent-7", "x9", "stem-only", "mixed-file", "inner-id", "%", "nothing",
    )

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = sessiondb.connect(self.root / "prefilter.sqlite")

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def write(self, name, lines):
        path = self.root / name
        path.write_text("".join(
            (line if isinstance(line, str) else json.dumps(line)) + "\n" for line in lines
        ), encoding="utf-8")
        return path

    def claude(self, session, text, at, **extra):
        return {"type": "user", "sessionId": session, "timestamp": at,
                "message": {"role": "user", "content": text}, **extra}

    def build(self):
        self.alpha = self.write("alpha.jsonl", [
            self.claude("alpha_1", "first", "2026-10-01T00:00:00Z"),
            self.claude("alpha_1", "second", "2026-10-01T00:00:02Z"),
            self.claude("alpha_1", "child", "2026-10-01T00:00:01Z", agentId="agent-7"),
        ])
        self.write("alphax.jsonl", [self.claude("alphaX1", "lookalike", "2026-10-01T00:00:03Z")])
        self.write("codex.jsonl", [
            {"type": "session_meta", "timestamp": "2026-10-01T00:00:04Z", "payload": {"id": "x9"}},
            {"type": "response_item", "timestamp": "2026-10-01T00:00:05Z",
             "payload": {"type": "message", "role": "user", "content": "codex text"}},
        ])
        self.write("stem-only.jsonl", ["not-json"])
        self.write("mixed-file.jsonl", [
            self.claude("inner-id", "mixed valid", "2026-10-01T00:00:06Z"), "not-json",
        ])
        for name in ("alpha", "alphax", "stem-only", "mixed-file"):
            sessiondb.ingest(self.db, "claude", self.root / f"{name}.jsonl")
        sessiondb.ingest(self.db, "codex", self.root / "codex.jsonl")
        self.db.commit()

    def reference(self, session):
        """The pre-change scan: the event-level clause alone, in query order."""
        escaped = session.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        return [row[0] for row in self.db.execute(
            "SELECT e.event_id FROM events e JOIN sources src USING(source_id) "
            f"WHERE e.session_id LIKE ? ESCAPE '\\' ORDER BY {ORDER}", (f"%{escaped}%",),
        )]

    def assert_paths_match_reference(self):
        for session in self.PATTERNS:
            with self.subTest(session=session):
                expected = self.reference(session)
                rows, _ = sessiondb.matching_rows(self.db, query_args(session=session))
                self.assertEqual([row["event_id"] for row in rows], expected)
                clauses, values = sessiondb._index_filter_clauses(query_args(session=session))
                indexed = [row[0] for row in self.db.execute(
                    "SELECT e.event_id FROM events e WHERE " + " AND ".join(clauses)
                    + f" ORDER BY {ORDER}", values,
                )]
                self.assertEqual(indexed, expected)

    def test_prefilter_returns_the_full_scan_rows_in_order(self):
        self.build()
        self.assertTrue(self.reference("stem-only"), "malformed-only source must have an event")
        self.assert_paths_match_reference()
        self.assertNotIn(
            self.reference("alphaX1")[0], self.reference("alpha_1"), "escaped _ must stay literal",
        )

    def test_prefilter_holds_after_replacement_and_desktop_deletion(self):
        self.build()
        self.write("alpha.jsonl", [self.claude("alpha_1", "rewritten", "2026-10-02T00:00:00Z")])
        sessiondb.ingest(self.db, "claude", self.alpha)
        self.db.commit()
        self.assert_paths_match_reference()
        source_id = self.db.execute(
            "SELECT source_id FROM sources WHERE path=?", (str(self.alpha.resolve()),),
        ).fetchone()[0]
        full_corpus._remove_source(self.db, source_id)
        self.db.commit()
        self.assertEqual(self.reference("alpha_1"), [])
        self.assert_paths_match_reference()

    def test_session_filter_reads_events_through_the_source_index(self):
        self.build()
        clauses, values = sessiondb._index_filter_clauses(query_args(session="alpha_1"))
        plan = " ".join(row[3] for row in self.db.execute(
            "EXPLAIN QUERY PLAN SELECT e.event_id FROM events e WHERE " + " AND ".join(clauses),
            values,
        ))
        self.assertIn("USING INDEX sqlite_autoindex_events_2", plan)
        self.assertNotIn("SCAN e", plan)


class ReceiptAvailabilityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "receipt.sqlite"
        self.db = sessiondb.connect(self.db_path)
        self.ingest("first", "needle first evidence")
        self.state_dir = self.root / "freshness"

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def ingest(self, name, text):
        path = self.root / f"{name}.jsonl"
        path.write_text(json.dumps({"type": "user", "sessionId": name, "message": {
            "role": "user", "content": text}}) + "\n", encoding="utf-8")
        sessiondb.ingest(self.db, "claude", path)
        self.db.commit()

    def report(self, **changes):
        value = {
            "complete": True, "complete_except_drift": True, "issues": [],
            "hard_failures": [], "sources_drifted_after_sync": [],
            "elapsed_seconds": 0.1, "status_counts": {}, "migrations": [],
        }
        value.update(changes)
        return value


    def receipt_exists(self):
        return sessiondb.projection_integrity_receipt_path(self.db_path).exists()





class CleanQueryPagingTest(unittest.TestCase):
    EVENTS = 130

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "paging.sqlite"
        self.db = sessiondb.connect(self.db_path)
        path = self.root / "paging.jsonl"
        # Even events match "evenhit", so one event of context per hit exceeds the event cap.
        path.write_text("".join(json.dumps({
            "type": "user", "sessionId": "paging",
            "timestamp": f"2026-10-01T00:{n // 60:02d}:{n % 60:02d}Z",
            "message": {"role": "user",
                        "content": f"page event {n}" + (" evenhit" if n % 2 == 0 else "")},
        }) + "\n" for n in range(self.EVENTS)), encoding="utf-8")
        sessiondb.ingest(self.db, "claude", path)
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def clean(self, **changes):
        return sessiondb.query_clean(self.db, query_args(
            session="paging", include_page=False, **{"limit": 50, **changes},
        ))

    def cli(self, *extra):
        with contextlib.redirect_stdout(io.StringIO()) as output, \
                contextlib.redirect_stderr(io.StringIO()) as error:
            code = sessiondb.main([
                "--db", str(self.db_path), "query", "--clean", "--session", "paging", *extra,
            ])
        self.assertEqual(code, 0)
        return json.loads(output.getvalue()), error.getvalue()

    def walk(self, limit):
        lines, offset, pages = [], 0, 0
        while offset is not None:
            result = self.clean(limit=limit, offset=offset)
            lines.extend(event["line"] for event in result["events"])
            offset = result.get("page", {}).get("next_offset")
            pages += 1
            self.assertLess(pages, 10)
        return lines

    def test_offsets_recover_every_event_exactly_once(self):
        expected = list(range(1, self.EVENTS + 1))
        for limit in (50, 150):
            with self.subTest(limit=limit):
                self.assertEqual(self.walk(limit), expected)

    def test_first_default_page_reports_paging(self):
        page = self.clean()["page"]
        self.assertTrue(page["has_more"])
        self.assertEqual(page["next_offset"], 50)

    def test_cap_notice_names_the_next_offset(self):
        result, notice = self.cli("--limit", "150")
        self.assertEqual(result["page"]["next_offset"], sessiondb.MAX_CLEAN_EVENTS)
        self.assertIn(f"--offset {sessiondb.MAX_CLEAN_EVENTS}", notice)
        self.assertIn("bundle holds only the returned events", notice)
        _, notice = self.cli("--limit", "150", "--offset", "100")
        self.assertEqual(notice, "")
        _, notice = self.cli("--limit", "50")
        self.assertEqual(notice, "")

    def test_capped_context_has_no_offset_and_says_to_narrow(self):
        result, notice = self.cli("--text", "evenhit", "--limit", "60", "--before", "1")
        self.assertTrue(result["page"]["response_truncated"])
        self.assertNotIn("next_offset", result["page"])
        self.assertIn("narrow --since/--until", notice)


class ReadableContinuationTest(unittest.TestCase):
    def test_whole_message_from_offset_zero_until_next_offset_is_null(self):
        text = "".join(f"word{n:05d} " for n in range(3000)) + "needle at the end"
        pieces, offset, pages = [], 0, 0
        while offset is not None:
            window = sessiondb.bounded_readable_text(text, argparse.Namespace(
                readable_query="needle", readable_offset=offset,
            ), 1_000_000)
            segment = window["readable_segments"][0]
            pieces.append(text[segment["start"]:segment["end"]])
            if window["readable_next_offset"] is None:
                self.assertTrue(window["readable_truncated"], "later pages stay flagged")
            offset = window["readable_next_offset"]
            pages += 1
        self.assertGreater(pages, 2)
        self.assertEqual("".join(pieces), text)


class VariableLaunchedAuditTest(unittest.TestCase):
    CLI = "/Users/example/.local/bin/sessiondb"
    DB = "/Users/example/.local/share/sessiondb/data/sessiondb.sqlite"








if __name__ == "__main__":
    unittest.main()
