import argparse
import json
import tempfile
import unittest
from pathlib import Path

import sessiondb


class PresentationRepairsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "pilot.sqlite"
        self.db = sessiondb.connect(self.db_path)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def ingest(self, provider, rows):
        path = self.root / f"{provider}.jsonl"
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
        self.assertEqual(sessiondb.ingest(self.db, provider, path)["issues"], 0)
        return path

    def delivered(self, text):
        args = argparse.Namespace(text=text, mode="literal", provider=None, project=None,
                                  session=None, role=None, tool=None, lineage=None,
                                  since=None, until=None)
        return sessiondb.bundle_if_oversized(self.db_path, sessiondb.search_index(self.db, args), None)

    def details(self, result, result_id, **options):
        follow = result["follow_up"]["details"]
        args = argparse.Namespace(snapshot_id=follow["snapshot_id"],
                                  result_ids=[result_id], bundle=Path(follow["bundle"]),
                                  before=0, after=0, readable=True, manifest=None,
                                  readable_query=follow.get("readable_query"), readable_offset=None)
        for key, value in options.items():
            setattr(args, key, value)
        return sessiondb.details_entrypoint(self.db_path, args)

    def test_short_claude_content_survives_metadata_and_redaction(self):
        content = "edgeprobe " + "word " * 225 + "final confirmation is 41%"
        self.assertLessEqual(len(content), sessiondb.PASSAGE_FULL_CHARS)
        self.ingest("claude", [{"type": "user", "sessionId": "short-c",
                                "message": {"role": "user", "content": content}}])
        result = self.delivered("edgeprobe")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["entries"][0]["passage"], content)
        self.assertNotIn("readable_query", result["entries"][0])
        self.assertTrue(result["follow_up"]["details"]["readable"])
        self.assertEqual(result["follow_up"]["details"]["readable_query"], "edgeprobe")

    def test_large_codex_readable_is_bounded_and_continuable(self):
        ending = "The approved limit is 23.75%."
        content = "boundaryprobe " + "nội dung dài " * 7500 + ending
        self.ingest("codex", [
            {"type": "session_meta", "timestamp": "2026-09-01T00:00:00Z",
             "payload": {"id": "long-x", "title": "Limit review"}},
            {"type": "event_msg", "timestamp": "2026-09-02T00:00:00Z",
             "payload": {"type": "agent_message", "message": content}},
        ])
        result = self.delivered("boundaryprobe")
        hit = result["entries"][0]
        self.assertTrue(hit["passage_truncated"])
        title = result["sessions"][hit["session"]]["title"]
        self.assertEqual(title["value"], "Limit review")
        self.assertNotIn("source", title)
        header, entries = sessiondb.read_result_bundle(Path(result["bundle"]["path"]))
        list(entries)
        self.assertEqual(header["sessions"][hit["session"]]["title"]["source"]["line"], 1)
        detail = self.details(result, hit["id"])
        event = detail["events"][0]
        self.assertIn(ending, event["text"])
        self.assertLessEqual(len(event["text"].encode("utf-8")), sessiondb.READABLE_EVENT_MAX_BYTES + 5)
        self.assertTrue(event["readable_truncated"])
        self.assertIsNotNone(event["readable_next_offset"])
        self.assertEqual(event["source"]["line"], 2)
        next_page = self.details(result, hit["id"], readable_offset=event["readable_next_offset"])
        self.assertGreater(next_page["events"][0]["readable_segments"][0]["start"], 0)
        self.assertLessEqual(len(next_page["events"][0]["text"].encode("utf-8")),
                             sessiondb.READABLE_EVENT_MAX_BYTES)
        exact = self.details(result, hit["id"], readable=False)
        self.assertIn("payload", exact["events"][0]["structure"])

    def test_title_scan_budget_reports_unknown_and_control_is_known(self):
        rows = [{"type": "session_meta", "timestamp": "2026-09-01T00:00:00Z",
                 "payload": {"id": "crowded-x", "title": "Old known title"}}]
        rows += [{"type": "session_meta", "timestamp": "2026-09-02T00:00:00Z",
                  "payload": {"id": "crowded-x", "title": {"invalid": i}}}
                 for i in range(140)]
        rows += [{"type": "event_msg", "payload": {"type": "agent_message",
                   "message": "crowdedprobe observed"}}]
        self.ingest("codex", rows)
        crowded = self.delivered("crowdedprobe")
        title = next(iter(crowded["sessions"].values()))["title"]
        self.assertEqual(title["status"], "unknown")
        self.assertEqual(title["reason"], "title_record_budget_exceeded")
        self.assertNotIn("value", title)

    def test_duplicate_locators_stale_reference_and_redaction(self):
        self.ingest("claude", [
            {"type": "custom-title", "sessionId": "repeat-c", "customTitle": "Review note"},
            {"type": "user", "sessionId": "repeat-c",
             "message": {"content": "repeatprobe api_key=hidden-value"}},
            {"type": "user", "sessionId": "repeat-c",
             "message": {"content": "repeatprobe api_key=hidden-value"}},
        ])
        result = self.delivered("repeatprobe")
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["entries"][0]["occurrence_count"], 2)
        self.assertNotIn("hidden-value", json.dumps(result))
        for occurrence in result["entries"][0]["occurrences"]:
            detail = self.details(result, occurrence["id"])
            self.assertEqual(detail["results"][0]["status"], "ok")
            self.assertIn("[REDACTED]", detail["events"][0]["text"])
            self.assertNotIn("hidden-value", json.dumps(detail))
        stale = self.details(result, result["entries"][0]["id"], snapshot_id="old-snapshot")
        self.assertTrue(stale["snapshot_advanced"])
        self.assertEqual(stale["results"][0]["status"], "ok")
        missing = self.details(result, result["entries"][0]["id"], result_ids=["missing-result"])
        self.assertEqual(missing["results"][0]["status"], "missing")

    def test_multiple_readable_events_share_text_budget(self):
        content = "multiquery " + "á" * 60000
        self.ingest("claude", [
            {"type": "user", "sessionId": "multi-c", "message": {"content": content}},
            {"type": "user", "sessionId": "multi-c", "message": {"content": content + " second"}},
        ])
        result = self.delivered("multiquery")
        result_ids = [item["id"] for entry in result["entries"] for item in entry["occurrences"]]
        self.assertEqual(len(result_ids), 2)
        follow = result["follow_up"]["details"]
        detail = sessiondb.details_entrypoint(self.db_path, argparse.Namespace(
            snapshot_id=result["snapshot_id"], result_ids=result_ids,
            bundle=Path(follow["bundle"]), before=0, after=0, readable=True,
            readable_query="multiquery", readable_offset=None, manifest=None,
        ))
        self.assertEqual(len(detail["events"]), 2)
        self.assertLessEqual(sum(len(item["text"].encode("utf-8")) for item in detail["events"]),
                             sessiondb.READABLE_RESPONSE_TEXT_MAX_BYTES)
        self.assertTrue(all(item["readable_truncated"] for item in detail["events"]))


if __name__ == "__main__":
    unittest.main()
