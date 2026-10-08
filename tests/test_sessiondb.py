import argparse
import base64
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import unittest
from pathlib import Path
from unittest import mock

import zstandard as zstd

import full_corpus
import sessiondb
import snapshot_export


class SessionDBTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "pilot.sqlite"
        self.db = sessiondb.connect(self.db_path)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def write(self, name, rows):
        path = self.root / name
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return path

    REPLAY_FIXTURE_AVAILABLE = {
        "relevance": 10, "chronological": 3, "filter_query": 6,
        "text_query": 40, "exact_session": 2, "record_stream": 1,
    }

    def replay_records(self, name="replay-label.jsonl", answer_time="2026-09-20T00:00:00Z"):
        source = self.write(name, [
            {"type": "user", "sessionId": "replay", "timestamp": "2026-09-19T00:00:00Z",
             "message": {"content": "where did we decide the invoice schedule?"}},
            {"type": "user", "sessionId": "replay", "timestamp": answer_time,
             "message": {"content": "gold answer"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.db.commit()
        rows = self.db.execute(
            "SELECT s.path,e.line_number,e.raw_hash FROM events e "
            "JOIN sources s ON s.source_id=e.source_id WHERE s.path=? ORDER BY e.line_number",
            (str(source.resolve()),),
        ).fetchall()
        request, answer = ({"path": row["path"], "line": row["line_number"],
                            "raw_hash": row["raw_hash"]} for row in rows)
        return request, answer

    def replay_label(self, request_ref, answer_ref, **changes):
        label = {
            "events": [answer_ref], "require": "any", "expected": "The schedule decision.",
            "found_by_original_search": False, "evidence": "grep invoice in replay-label.jsonl",
            "request": {**request_ref, "text": "where did we decide the invoice schedule?",
                        "speaker": "user"},
        }
        label.update(changes)
        return {"answer": label}

    def replay_fixture(self):
        request, answer = self.replay_records()
        chains, labels = [], {}
        for method, count in self.REPLAY_FIXTURE_AVAILABLE.items():
            for index in range(count):
                chain_id = f"{method}-{index}"
                chains.append({
                    "chain_id": chain_id, "initial_method": method,
                    "started_at": "2026-09-20T01:00:00Z",
                    "ended_at": "2026-09-20T01:00:01Z",
                    "steps": [{"method": method, "output": "[]",
                               "command": f"sessiondb --db /live.sqlite search-index --text {chain_id}"}],
                    "transitions": ["raw_targeted"] if index == 0 else [],
                })
                labels[chain_id] = self.replay_label(request, answer)
        return (request, answer), chains, labels






    def test_cross_provider_dedup_reconstruction_search_and_idempotency(self):
        shared = "standing instruction alpha"
        claude = self.write("claude.jsonl", [
            {"type": "user", "uuid": "u1", "sessionId": "c1", "timestamp": "2026-08-14T01:00:00Z", "message": {"role": "user", "content": shared}},
            {"type": "assistant", "uuid": "a1", "sessionId": "c1", "timestamp": "2026-08-14T01:00:01Z", "message": {"role": "assistant", "content": [{"type": "text", "text": "answer"}]}},
            {"type": "future-shape", "sessionId": "c1", "timestamp": "2026-08-14T01:00:02Z", "content": "visible unknown"},
        ])
        codex = self.write("codex.jsonl", [
            {"type": "session_meta", "timestamp": "2026-08-14T02:00:00Z", "payload": {"id": "x1", "cwd": "/project"}},
            {"type": "response_item", "timestamp": "2026-08-14T02:00:01Z", "payload": {"type": "message", "role": "user", "content": shared}},
            {"type": "event_msg", "timestamp": "2026-08-14T02:00:02Z", "payload": {"type": "agent_message", "message": "api_key=topsecret", "token": "opaquevalue"}},
        ])
        first = sessiondb.ingest(self.db, "claude", claude)
        sessiondb.ingest(self.db, "codex", codex)
        second = sessiondb.ingest(self.db, "claude", claude)
        self.assertEqual(first["events"], 3)
        self.assertEqual(second["status"], "unchanged")
        shared_hash = sessiondb.digest(sessiondb.canonical(shared))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM objects WHERE object_hash=?", (shared_hash,)).fetchone()[0], 1)
        expanded = sessiondb.reconstruct(self.db, "claude", "c1")
        self.assertEqual([item["content"] for item in expanded[:2]], [shared, [{"type": "text", "text": "answer"}]])
        args = argparse.Namespace(text="standing", provider=None, project=None, session=None, role=None, tool=None, lineage=None, since=None, until=None, raw=False, limit=50)
        self.assertEqual({row["provider"] for row in sessiondb.query(self.db, args)}, {"claude", "codex"})
        args.text = "topsecret"
        self.assertEqual(sessiondb.query(self.db, args), [])
        args.text = "REDACTED"
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)
        args.raw = True
        args.text = "topsecret"
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)
        args.text = "opaquevalue"
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)
        self.assertEqual(sessiondb.report(self.db, self.db_path)["issues"], 1)

    def test_malformed_complete_line_is_recorded(self):
        path = self.root / "broken.jsonl"
        path.write_text('{"type":"user"}\nnot-json\n', encoding="utf-8")
        result = sessiondb.ingest(self.db, "claude", path)
        self.assertEqual(result["events"], 2)
        self.assertEqual(result["issues"], 1)

    def test_jsonl_bare_carriage_return_does_not_split_a_record(self):
        path = self.root / "bare-cr.jsonl"
        path.write_bytes(
            b'{"type":"user",\r"sessionId":"cr","message":{"content":"first"}}\n'
            b'{"type":"user","sessionId":"cr","message":{"content":"second"}}\n'
        )
        result = sessiondb.ingest(self.db, "claude", path)
        self.assertEqual(result["events"], 2)
        self.assertEqual(result["issues"], 0)
        self.assertEqual(
            [event["content"] for event in sessiondb.reconstruct(self.db, "claude", "cr")],
            ["first", "second"],
        )

    def test_exact_search_preserves_provider_dictionary_order(self):
        content = [{"id": "toolu_123", "type": "tool_result", "content": "ordered payload text"}]
        source = self.write("ordered.jsonl", [
            {"type": "user", "sessionId": "ordered", "message": {"role": "user", "content": content}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        row = self.db.execute("SELECT * FROM events").fetchone()
        self.assertEqual(list(sessiondb.read_event_content(self.db, row)[0]), ["id", "type", "content"])
        args = argparse.Namespace(
            text="toolu_123 tool_result ordered payload", provider=None, project=None,
            session=None, role=None, tool=None, lineage=None, since=None, until=None,
            raw=False, limit=50, before=0, after=0,
        )
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)

    def test_unicode_word_search_uses_complete_terms(self):
        source = self.write("vietnamese.jsonl", [
            {"type": "user", "sessionId": "unicode-words", "message": {
                "role": "user", "content": [{"type": "text", "text": "Em sẽ đánh số các điểm khi cần"}],
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = argparse.Namespace(
            text="sẽ đánh số các điểm khi cần", provider=None, project=None,
            session=None, role=None, tool=None, lineage=None, since=None, until=None,
            raw=False, limit=50, before=0, after=0,
        )
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)

    def test_nested_payload_type_does_not_crash_tool_detection(self):
        codex = self.write("nested.jsonl", [
            {"type": "session_meta", "timestamp": "2026-08-14T02:00:00Z", "payload": {"id": "x2"}},
            {"type": "world_state", "timestamp": "2026-08-14T02:00:01Z", "payload": {"state": {"type": {"nested": True}}}},
        ])
        result = sessiondb.ingest(self.db, "codex", codex)
        self.assertEqual(result["status"], "imported")
        self.assertEqual(result["issues"], 0)

    def test_codex_token_count_structure_is_queryable_without_source_reads(self):
        row = {
            "type": "event_msg", "timestamp": "2026-08-27T16:25:55Z",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": 1234, "cached_input_tokens": 1024,
                        "output_tokens": 56, "reasoning_output_tokens": 12,
                        "total_tokens": 1290,
                    },
                },
                "rate_limits": {
                    "limit_id": "codex",
                    "primary": {
                        "used_percent": 8.0, "window_minutes": 10080,
                        "resets_at": 1788452755,
                    },
                    "credits": {"has_credits": False, "balance": "0"},
                },
            },
        }
        source = self.write("codex-token-count.jsonl", [
            {"type": "session_meta", "payload": {"id": "codex-token-count"}}, row,
        ])
        self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 0)
        args = argparse.Namespace(
            text=None, provider="codex", project=None, session="codex-token-count",
            role=None, tool=None, lineage=None, since=None, until=None, raw=False,
            limit=10, offset=0, before=0, after=0, include_structure=True,
        )
        with sessiondb.forbid_source_transcript_reads() as telemetry:
            result = sessiondb.query(self.db, args)
        self.assertEqual(telemetry["attempts"], 0)
        token_event = next(item for item in result if item["category"] == "token_count")
        self.assertEqual(token_event["structure"], row)

        args.include_structure = False
        token_event = next(
            item for item in sessiondb.query(self.db, args) if item["category"] == "token_count"
        )
        self.assertNotIn("structure", token_event)

    def test_codex_token_usage_record_auto_admission_preserves_structure_and_control(self):
        usage = {
            "type": "token_usage_record", "timestamp": "2026-09-03T09:51:02.563Z",
            "payload": {
                "response_id": "resp_token_usage",
                "root_turn_id": "turn_token_usage",
                "session_id": "codex-token-usage",
                "thread_id": "codex-token-usage",
                "turn_id": "turn_token_usage",
                "thread_token_usage": {
                    "cache_write_input_tokens": 0, "cached_input_tokens": 108672,
                    "input_tokens": 115661, "output_tokens": 274,
                    "reasoning_output_tokens": 150, "total_tokens": 115935,
                },
                "turn_token_usage": {
                    "cache_write_input_tokens": 0, "cached_input_tokens": 108672,
                    "input_tokens": 115661, "output_tokens": 274,
                    "reasoning_output_tokens": 150, "total_tokens": 115935,
                },
                "usage": {
                    "cache_write_input_tokens": 0, "cached_input_tokens": 108672,
                    "input_tokens": 115661, "output_tokens": 274,
                    "reasoning_output_tokens": 150, "total_tokens": 115935,
                },
            },
        }
        control = {
            "type": "event_msg", "timestamp": "2026-09-03T09:51:03Z",
            "payload": {"type": "token_count", "info": {"state": "control"}},
        }
        source = self.write("codex-token-usage-migration.jsonl", [
            {"type": "session_meta", "payload": {"id": "codex-token-usage"}}, usage, control,
        ])
        self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 0)
        self.db.execute(
            "UPDATE events SET supported=0,issue='legacy unsupported token usage' "
            "WHERE category='token_usage_record'",
        )
        self.db.commit()

        with sessiondb.forbid_source_transcript_reads() as telemetry:
            result = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(telemetry["attempts"], 0)
        self.assertEqual((result["admitted_groups"], result["admitted_events"]), (1, 1))
        self.assertEqual(
            [tuple(row) for row in self.db.execute(
                "SELECT category,supported,issue FROM events ORDER BY line_number"
            )],
            [("session_meta", 1, None), ("token_usage_record", 1, None), ("token_count", 1, None)],
        )
        event = self.db.execute(
            "SELECT * FROM events WHERE category='token_usage_record'"
        ).fetchone()
        self.assertEqual(sessiondb.read_event_structure(self.db, event), usage)
        args = argparse.Namespace(
            text="resp_token_usage", provider="codex", project=None,
            session="codex-token-usage", role=None, tool=None, lineage=None,
            since=None, until=None, raw=False, limit=10, offset=0, before=0, after=0,
        )
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)

    def test_codex_token_usage_record_shape_drift_remains_unsupported(self):
        row = {
            "type": "token_usage_record", "payload": {
                "response_id": "resp_token_usage",
                "root_turn_id": "turn_token_usage",
                "session_id": "shape-drift", "thread_id": "shape-drift",
                "turn_id": "turn_token_usage",
                "thread_token_usage": {
                    "cache_write_input_tokens": 0, "cached_input_tokens": 1,
                    "input_tokens": 2, "output_tokens": 3,
                    "reasoning_output_tokens": 4, "total_tokens": 5,
                },
                "turn_token_usage": {
                    "cache_write_input_tokens": 0, "cached_input_tokens": 1,
                    "input_tokens": 2, "output_tokens": 3,
                    "reasoning_output_tokens": 4, "total_tokens": 5,
                },
                "usage": {
                    "cache_write_input_tokens": 0, "cached_input_tokens": 1,
                    "input_tokens": 2, "output_tokens": 3,
                    "reasoning_output_tokens": 4, "total_tokens": 5,
                },
                "message": "must remain unsupported",
            },
        }
        source = self.write("codex-token-usage-drift.jsonl", [row])
        self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 1)
        event = self.db.execute("SELECT * FROM events").fetchone()
        self.assertEqual(event["supported"], 0)
        self.assertEqual(sessiondb.read_event_structure(self.db, event), row)

    def test_codex_compaction_preserves_exact_opaque_envelope_without_indexing_ciphertext(self):
        row = {
            "timestamp": "2026-09-12T05:57:33.000Z",
            "type": "response_item",
            "payload": {
                "type": "compaction",
                "id": "cmp_fixture",
                "encrypted_content": "opaque-compaction-ciphertext-marker",
                "internal_chat_message_metadata_passthrough": {"turn_id": "turn-fixture"},
            },
        }
        # A row-level integer ordinal is accepted the same way.
        for name, case in (("codex-compaction.jsonl", row), ("codex-compaction-ordinal.jsonl", {**row, "ordinal": 2})):
            with self.subTest(name):
                source = self.write(name, [case])
                self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 0)
                event = self.db.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 1").fetchone()
                self.assertEqual(
                    (event["category"], event["supported"], event["content_hash"]),
                    ("compaction", 1, None),
                )
                self.assertEqual(sessiondb.read_event_structure(self.db, event), case)
        args = argparse.Namespace(
            text="opaque-compaction-ciphertext-marker", provider="codex", project=None,
            session=None, role=None, tool=None, lineage=None, since=None, until=None,
            raw=False, limit=10, offset=0, before=0, after=0,
        )
        self.assertEqual(sessiondb.query(self.db, args), [])

    def test_codex_compaction_rejects_non_integer_ordinal(self):
        row = {
            "timestamp": "2026-09-12T05:57:33.000Z",
            "type": "response_item",
            "ordinal": "2",
            "payload": {
                "type": "compaction",
                "id": "cmp_fixture",
                "encrypted_content": "opaque",
                "internal_chat_message_metadata_passthrough": {"turn_id": "turn-fixture"},
            },
        }
        source = self.write("codex-compaction-bad-ordinal.jsonl", [row])
        self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 1)
        self.assertEqual(
            self.db.execute("SELECT supported FROM events").fetchone()["supported"], 0,
        )

    def test_codex_compaction_shape_drift_remains_unsupported(self):
        row = {
            "timestamp": "2026-09-12T05:57:33.000Z",
            "type": "response_item",
            "payload": {
                "type": "compaction",
                "id": "cmp_fixture",
                "encrypted_content": "opaque",
                "internal_chat_message_metadata_passthrough": {"turn_id": "turn-fixture"},
                "content": "must remain unsupported",
            },
        }
        source = self.write("codex-compaction-drift.jsonl", [row])
        self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 1)
        event = self.db.execute("SELECT * FROM events").fetchone()
        self.assertEqual(event["supported"], 0)
        self.assertEqual(sessiondb.read_event_structure(self.db, event), row)

    def test_codex_compaction_migration_rebuilds_only_affected_sources(self):
        transcripts = self.root / "codex-compaction-migration"
        transcripts.mkdir()
        compaction = {
            "timestamp": "2026-09-12T05:57:33.000Z",
            "type": "response_item",
            "payload": {
                "type": "compaction",
                "id": "cmp_migration",
                "encrypted_content": "opaque-migration-ciphertext",
                "internal_chat_message_metadata_passthrough": {"turn_id": "turn-migration"},
            },
        }
        affected = transcripts / "affected.jsonl"
        unaffected = transcripts / "unaffected.jsonl"
        affected.write_text(json.dumps(compaction) + "\n", encoding="utf-8")
        unaffected.write_text(json.dumps({
            "type": "response_item",
            "payload": {"type": "message", "role": "user", "content": "control"},
        }) + "\n", encoding="utf-8")
        db_path = self.root / "codex-compaction-migration.sqlite"
        db = sessiondb.connect(db_path)
        sessiondb.ingest(db, "codex", affected)
        sessiondb.ingest(db, "codex", unaffected)
        db.execute("UPDATE sources SET adapter_version='codex-v9'")
        db.execute(
            "UPDATE events SET supported=0,issue='unknown Codex shape: response_item/compaction' "
            "WHERE category='compaction'"
        )
        db.execute(
            "UPDATE sources SET issue_count=1 WHERE path=?", (str(affected.resolve()),),
        )
        db.commit()
        db.close()
        old_roots = full_corpus.ROOTS
        reads = []
        stable_snapshot = sessiondb.stable_source_snapshot

        def track_read(path, *args, **kwargs):
            reads.append(Path(path).name)
            return stable_snapshot(path, *args, **kwargs)

        try:
            full_corpus.ROOTS = {"codex": (transcripts,)}
            with mock.patch.object(sessiondb, "stable_source_snapshot", side_effect=track_read):
                report = full_corpus.fast_sync(
                    db_path, self.root / "codex-compaction-migration.json",
                )
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        self.assertEqual(reads, ["affected.jsonl"])
        self.assertEqual(report["migrations"][0]["mode"], "category-scoped")
        self.assertEqual(report["migrations"][0]["selection"], "unsupported-only")
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                {row[0] for row in db.execute("SELECT adapter_version FROM sources")},
                {"codex-v10"},
            )
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM events WHERE supported=0"
            ).fetchone()[0], 0)
            event = db.execute(
                "SELECT * FROM events WHERE category='compaction'"
            ).fetchone()
            self.assertEqual(sessiondb.read_event_structure(db, event), compaction)
        finally:
            db.close()

    def test_codex_realtime_items_preserve_text_metadata_and_unknown_control(self):
        rows = [
            {"type": "session_meta", "payload": {"id": "realtime-items"}},
            {"type": "realtime_item", "timestamp": "2026-09-06T15:29:47.177Z", "ordinal": 1,
             "payload": {"id": "segment-1", "realtime_session_id": "session-1",
                         "type": "transcript_segment", "turn_id": "turn-1", "role": "user",
                         "text": "Searchable realtime text api_key=topsecret"}},
            {"type": "realtime_item", "timestamp": "2026-09-06T15:29:48.177Z", "ordinal": 2,
             "payload": {"id": "promotion-1", "realtime_session_id": "session-1",
                         "type": "bem_item_promoted", "turn_id": "turn-1", "item_id": "item-1",
                         "presentation": {"type": "assistant"}}},
            {"type": "realtime_item", "timestamp": "2026-09-06T15:29:49.177Z", "ordinal": 3,
             "payload": {"id": "closed-1", "realtime_session_id": "session-1",
                         "type": "realtime_session_closed", "outcome": "ended"}},
            {"type": "realtime_item", "timestamp": "2026-09-06T15:29:50.177Z", "ordinal": 4,
             "payload": {"id": "future-1", "realtime_session_id": "session-1",
                         "type": "future_realtime_item", "text": "must remain unsupported"}},
        ]
        source = self.write("codex-realtime-items.jsonl", rows)
        self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 1)
        events = self.db.execute(
            "SELECT * FROM events WHERE category IN "
            "('transcript_segment','bem_item_promoted','realtime_session_closed','future_realtime_item') "
            "ORDER BY line_number"
        ).fetchall()
        self.assertEqual(
            [(event["category"], event["supported"], event["content_hash"] is not None) for event in events],
            [("transcript_segment", 1, True), ("bem_item_promoted", 1, False),
             ("realtime_session_closed", 1, False), ("future_realtime_item", 0, False)],
        )
        for event, row in zip(events, rows[1:]):
            self.assertEqual(sessiondb.read_event_structure(self.db, event), row)
        args = argparse.Namespace(
            text="Searchable realtime text", provider="codex", project=None,
            session="realtime-items", role=None, tool=None, lineage=None,
            since=None, until=None, raw=False, limit=10, offset=0, before=0, after=0,
        )
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)
        args.text = "topsecret"
        self.assertEqual(sessiondb.query(self.db, args), [])
        args.text = "REDACTED"
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)

    def test_codex_realtime_category_migration_rebuilds_only_affected_sources(self):
        transcripts = self.root / "codex-realtime-category-migration"
        transcripts.mkdir()
        affected = transcripts / "affected.jsonl"
        unaffected = transcripts / "unaffected.jsonl"
        affected.write_text("".join(json.dumps(row) + "\n" for row in [
            {"type": "session_meta", "payload": {"id": "realtime-affected"}},
            {"type": "realtime_item", "payload": {
                "id": "segment-1", "realtime_session_id": "session-1",
                "type": "transcript_segment", "turn_id": "turn-1", "role": "assistant",
                "text": "migrated realtime text",
            }},
        ]), encoding="utf-8")
        unaffected.write_text("".join(json.dumps(row) + "\n" for row in [
            {"type": "session_meta", "payload": {"id": "realtime-unaffected"}},
            {"type": "response_item", "payload": {
                "type": "message", "role": "user", "content": "control",
            }},
        ]), encoding="utf-8")
        db_path = self.root / "codex-realtime-category.sqlite"
        db = sessiondb.connect(db_path)
        sessiondb.ingest(db, "codex", affected)
        sessiondb.ingest(db, "codex", unaffected)
        db.execute("UPDATE sources SET adapter_version='codex-v7'")
        db.execute(
            "UPDATE events SET supported=0,issue='unknown Codex realtime shape' "
            "WHERE category='transcript_segment'"
        )
        db.commit()
        db.close()
        old_roots = full_corpus.ROOTS
        reads = []
        stable_snapshot = sessiondb.stable_source_snapshot

        def track_read(path, *args, **kwargs):
            reads.append(Path(path).name)
            return stable_snapshot(path, *args, **kwargs)

        try:
            full_corpus.ROOTS = {"codex": (transcripts,)}
            with mock.patch.object(sessiondb, "stable_source_snapshot", side_effect=track_read):
                report = full_corpus.fast_sync(db_path, self.root / "codex-realtime-category.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        self.assertEqual(reads, ["affected.jsonl"])
        self.assertEqual(report["migrations"][0]["mode"], "category-scoped")
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                {row[0] for row in db.execute("SELECT adapter_version FROM sources")},
                {"codex-v10"},
            )
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM events WHERE supported=0"
            ).fetchone()[0], 0)
            event = db.execute(
                "SELECT * FROM events WHERE category='transcript_segment'"
            ).fetchone()
            self.assertEqual(sessiondb.read_event_structure(db, event)["payload"]["text"],
                             "migrated realtime text")
        finally:
            db.close()

    def test_raw_session_cli_uses_read_only_database(self):
        source = self.write("read-only-raw-session.jsonl", [
            {"type": "session_meta", "payload": {"id": "read-only-raw-session"}},
            {"type": "event_msg", "payload": {
                "type": "token_count", "rate_limits": {"primary": {"used_percent": 8.0}},
            }},
        ])
        sessiondb.ingest(self.db, "codex", source)
        self.db.close()
        os.chmod(self.db_path, 0o444)
        output = io.StringIO()
        try:
            with contextlib.redirect_stdout(output):
                self.assertEqual(sessiondb.main([
                    "--db", str(self.db_path), "session", "--provider", "codex",
                    "read-only-raw-session", "--raw",
                ]), 0)
        finally:
            os.chmod(self.db_path, 0o600)
            self.db = sessiondb.connect(self.db_path)
        events = json.loads(output.getvalue())
        token_event = next(item for item in events if item["category"] == "token_count")
        self.assertEqual(token_event["content"]["payload"]["rate_limits"]["primary"]["used_percent"], 8.0)

    def _expanded_query_cli(self, *extra):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(sessiondb.main(["--db", str(self.db_path), "query", *extra]), 0)
        return json.loads(stdout.getvalue()), stderr.getvalue()

    def test_expanded_query_signals_truncation_without_changing_bare_list(self):
        rows = [{"type": "session_meta", "timestamp": "2026-08-14T02:00:00Z", "payload": {"id": "page-x"}}]
        rows += [
            {"type": "response_item", "timestamp": f"2026-08-14T02:00:0{index}Z", "payload": {
                "type": "message", "role": "user",
                "content": [{"type": "input_text", "text": f"pagingneedle {index}"}],
            }}
            for index in range(1, 6)
        ]
        sessiondb.ingest(self.db, "codex", self.write("paging.jsonl", rows))
        self.db.commit()
        base = ["--text", "pagingneedle", "--provider", "codex", "--include-structure"]

        events, notice = self._expanded_query_cli(*base, "--limit", "2")
        self.assertIsInstance(events, list)
        self.assertEqual([item["text"].split("\n")[-1] for item in events], ["pagingneedle 1", "pagingneedle 2"])
        self.assertIn("structure", events[0])
        self.assertEqual(len(notice.splitlines()), 1)
        self.assertIn("query output truncated: returned 2 of at least 3 matching hits from offset 0", notice)
        self.assertIn("--offset 2", notice)

        # Paging with the notice's offset reaches every record; the final page is silent.
        seen = [item["text"].split("\n")[-1] for item in events]
        offset = 2
        while True:
            page, notice = self._expanded_query_cli(*base, "--limit", "2", "--offset", str(offset))
            seen += [item["text"].split("\n")[-1] for item in page]
            if not notice:
                break
            offset = int(re.search(r"--offset (\d+)", notice).group(1))
        self.assertEqual(seen, [f"pagingneedle {index}" for index in range(1, 6)])

        # A page that returns exactly --limit records with nothing after it is not truncated.
        events, notice = self._expanded_query_cli(*base, "--limit", "5")
        self.assertEqual(len(events), 5)
        self.assertEqual(notice, "")
        events, notice = self._expanded_query_cli(*base, "--limit", "4")
        self.assertEqual(len(events), 4)
        self.assertIn("--offset 4", notice)

        # Filter-only queries also stop scanning one hit past the page.
        events, notice = self._expanded_query_cli("--session", "page-x", "--role", "user", "--limit", "3")
        self.assertEqual(len(events), 3)
        self.assertIn("returned 3 of at least 4 matching hits", notice)

        # Opt-in envelope: same events, machine-readable page, no stderr notice.
        envelope, notice = self._expanded_query_cli(*base, "--limit", "2", "--page-envelope")
        bare, _ = self._expanded_query_cli(*base, "--limit", "2")
        self.assertEqual(notice, "")
        self.assertEqual(envelope["events"], bare)
        self.assertTrue(envelope["page"]["has_more"])
        self.assertEqual(envelope["page"]["next_offset"], 2)
        self.assertTrue(envelope["page"]["total_hits_is_lower_bound"])
        envelope, _ = self._expanded_query_cli(*base, "--limit", "2", "--offset", "4", "--page-envelope")
        self.assertFalse(envelope["page"]["has_more"])
        self.assertIsNone(envelope["page"]["next_offset"])
        self.assertEqual(len(envelope["events"]), 1)

        # With --clean, the envelope request returns the full clean page instead of an error.
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(sessiondb.main([
                "--db", str(self.db_path), "query", *base[:2], "--clean", "--page-envelope",
            ]), 0)
        self.assertIn("next_offset", json.loads(output.getvalue())["page"])

    def test_codex_tool_outputs_inherit_tool_from_call_id(self):
        codex = self.write("tool-link.jsonl", [
            {"type": "session_meta", "payload": {"id": "tool-link"}},
            {"type": "response_item", "payload": {
                "type": "custom_tool_call", "call_id": "call-1", "name": "exec", "input": "pwd",
            }},
            {"type": "response_item", "payload": {
                "type": "custom_tool_call_output", "call_id": "call-1", "output": "done",
            }},
        ])
        sessiondb.ingest(self.db, "codex", codex)
        rows = self.db.execute(
            "SELECT category,tool FROM events WHERE category LIKE 'custom_tool_call%' ORDER BY line_number"
        ).fetchall()
        self.assertEqual([tuple(row) for row in rows], [
            ("custom_tool_call", "exec"), ("custom_tool_call_output", "exec"),
        ])

    def test_codex_item_completed_shapes_are_supported_without_duplicate_search_content(self):
        items = [
            {"type": "UserMessage", "id": "user-1", "content": [
                {"type": "text", "text": "unique user completion"},
                {"type": "local_image", "path": "/tmp/example.png"},
            ]},
            {"type": "AgentMessage", "id": "message-1", "content": [
                {"type": "Text", "text": "paired assistant completion"},
            ]},
            {"type": "Reasoning", "id": "reasoning-1", "summary_text": ["paired reasoning"]},
            {"type": "CommandExecution", "id": "exec-1", "command": ["pwd"],
             "stdout": "command completion output", "status": "completed"},
            {"type": "FileChange", "id": "exec-2", "changes": [{"path": "a.txt"}],
             "status": "completed"},
            {"type": "Extension", "id": "exec-3", "kind": "web.search",
             "results": [{"title": "result"}]},
            {"type": "DynamicToolCall", "id": "exec-4", "namespace": "codex_app",
             "tool": "list_threads", "content_items": []},
            {"type": "McpToolCall", "id": "exec-5", "server": "node_repl",
             "tool": "js", "result": {"ok": True}},
            {"type": "ContextCompaction", "id": "compact-1"},
            {"type": "SubAgentActivity", "id": "call-1", "kind": "started"},
            {"type": "CollabAgentToolCall", "id": "call-2", "tool": "wait"},
        ]
        rows = [{"type": "session_meta", "payload": {"id": "completed-items"}}]
        rows.extend(
            {"type": "event_msg", "payload": {"type": "item_completed", "item": item}}
            for item in items
        )
        rows.append({"type": "response_item", "payload": {
            "type": "message", "id": "message-1", "role": "assistant",
            "content": [{"type": "output_text", "text": "paired assistant completion"}],
        }})
        source = self.write("item-completed.jsonl", rows)
        result = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(result["issues"], 0)
        events = self.db.execute(
            "SELECT role,tool,tool_event,content_hash FROM events "
            "WHERE category='item_completed' ORDER BY line_number"
        ).fetchall()
        self.assertEqual(len(events), len(items))
        self.assertEqual((events[0]["role"], events[0]["tool_event"]), ("user", 0))
        self.assertEqual(
            [(events[index]["tool"], events[index]["tool_event"]) for index in range(3, 8)],
            [("exec_command", 1), ("apply_patch", 1), ("web.search", 1),
             ("list_threads", 1), ("js", 1)],
        )
        for index in (1, 2, 8, 9, 10):
            self.assertIsNone(events[index]["content_hash"])
        args = argparse.Namespace(
            text="paired assistant completion", provider="codex", project=None,
            session=None, role=None, tool=None, lineage=None, since=None, until=None,
            raw=False, limit=50, before=0, after=0,
        )
        self.assertEqual(len(sessiondb.query(self.db, args)), 1)
        complete = sessiondb.search_index(self.db, argparse.Namespace(
            text="paired assistant completion", mode="literal", provider="codex",
            project=None, session=None, role=None, tool=None, lineage=None, since=None, until=None,
        ))
        self.assertEqual(complete["total"], 2)
        self.assertEqual(len({item["result_id"] for item in complete["entries"]}), 2)
        raw = sessiondb.reconstruct(self.db, "codex", "completed-items", raw=True)
        self.assertEqual(raw[1]["content"]["payload"]["item"], items[0])

    def test_codex_function_call_output_completion_preserves_content_and_keeps_unknown_control_unsupported(self):
        function_output = {
            "type": "FunctionCallOutput", "id": "fco-1",
            "name": "send_message_to_thread", "namespace": "codex_app",
            "output": "delegated result with api_key=topsecret",
        }
        unknown = {
            "type": "FutureMessage", "id": "future-1",
            "content": "must remain unsupported",
        }
        rows = [
            {"type": "session_meta", "payload": {"id": "function-output-completion"}},
            {"type": "event_msg", "payload": {"type": "item_completed", "item": function_output}},
            {"type": "event_msg", "payload": {"type": "item_completed", "item": unknown}},
        ]
        source = self.write("function-output-completion.jsonl", rows)
        result = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(result["issues"], 1)
        events = self.db.execute(
            "SELECT category,tool,tool_event,supported,content_hash FROM events "
            "WHERE category='item_completed' ORDER BY line_number"
        ).fetchall()
        self.assertEqual(
            [
                (event["category"], event["tool"], event["tool_event"],
                 event["supported"], event["content_hash"] is not None)
                for event in events
            ],
            [("item_completed", "send_message_to_thread", 1, 1, True),
             ("item_completed", None, 0, 0, False)],
        )
        raw = sessiondb.reconstruct(self.db, "codex", "function-output-completion", raw=True)
        self.assertEqual([event["content"] for event in raw], rows)

        args = argparse.Namespace(
            text="delegated result", provider="codex", project=None,
            session="function-output-completion", role=None, tool=None,
            lineage=None, since=None, until=None, raw=False, limit=50,
            before=0, after=0,
        )
        result = sessiondb.query(self.db, args)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["tool"], "send_message_to_thread")
        self.assertIn("[REDACTED]", result[0]["text"])
        args.text = "topsecret"
        self.assertEqual(sessiondb.query(self.db, args), [])

    def test_codex_web_search_completion_preserves_results_and_redacts_search(self):
        web_search = {
            "type": "WebSearch", "id": "web-1",
            "action": {"type": "search", "query": "find api_key=topsecret"},
            "query": "find api_key=topsecret",
            "results": [{
                "type": "web_result", "title": "Safe searchable result",
                "url": "https://example.test/result", "domain": "example.test",
                "snippet": "safe web result text", "ref_id": "ref-1",
            }],
        }
        unknown = {
            "type": "FutureWebSearch", "id": "future-web-1",
            "query": "must remain unsupported",
        }
        rows = [
            {"type": "session_meta", "payload": {"id": "web-search-completion"}},
            {"type": "event_msg", "payload": {"type": "item_completed", "item": web_search}},
            {"type": "event_msg", "payload": {"type": "item_completed", "item": unknown}},
        ]
        source = self.write("web-search-completion.jsonl", rows)
        result = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(result["issues"], 1)
        events = self.db.execute(
            "SELECT tool,tool_event,supported,content_hash FROM events "
            "WHERE category='item_completed' ORDER BY line_number"
        ).fetchall()
        self.assertEqual(
            [(event["tool"], event["tool_event"], event["supported"], event["content_hash"] is not None)
             for event in events],
            [("web.search", 1, 1, True), (None, 0, 0, False)],
        )
        raw = sessiondb.reconstruct(self.db, "codex", "web-search-completion", raw=True)
        self.assertEqual([event["content"] for event in raw], rows)

        args = argparse.Namespace(
            text="Safe searchable result", provider="codex", project=None,
            session="web-search-completion", role=None, tool=None,
            lineage=None, since=None, until=None, raw=False, limit=50,
            before=0, after=0,
        )
        result = sessiondb.query(self.db, args)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["tool"], "web.search")
        self.assertIn("[REDACTED]", result[0]["text"])
        args.text = "topsecret"
        self.assertEqual(sessiondb.query(self.db, args), [])

    def test_unknown_codex_completed_content_is_blocking_and_not_auto_admitted(self):
        source = self.write("future-item-completed.jsonl", [{
            "type": "event_msg", "payload": {
                "type": "item_completed", "item": {
                    "type": "FutureMessage", "id": "future-1", "content": "must not be hidden",
                },
            },
        }])
        result = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(result["issues"], 1)
        admission = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(admission["admitted_events"], 0)
        issue = full_corpus.issue_groups(self.db)[0]
        self.assertEqual((issue["category"], issue["noncritical_adapter_gap"]),
                         ("item_completed", 0))

    def test_claude_artifact_records_preserve_searchable_artifact_state(self):
        rows = [
            {
                "type": "artifact-comment-monitor", "v": 1,
                "sessionId": "artifact-session",
                "artifacts": {
                    "artifact-1": {
                        "state": "armed", "writtenAtMs": 1,
                        "title": "The Shaking Foundation",
                    },
                },
            },
            {
                "type": "artifact-autoreact-ledger", "v": 1,
                "sessionId": "artifact-session", "accountUuid": "account-1",
                "artifacts": {
                    "artifact-1": {
                        "savedAt": 2, "everBaselined": True,
                        "everHadThreads": True, "stampHighWater": None,
                        "turnTimestamps": [],
                        "threads": [{"comment": "Keep the worker audit current"}],
                    },
                },
            },
        ]
        source = self.write("artifact-records.jsonl", rows)
        result = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual((result["issues"], result["events"]), (0, 2))
        events = self.db.execute(
            "SELECT category,supported,content_hash FROM events ORDER BY line_number"
        ).fetchall()
        self.assertEqual(
            [(row["category"], row["supported"], row["content_hash"] is not None) for row in events],
            [
                ("artifact-comment-monitor", 1, True),
                ("artifact-autoreact-ledger", 1, True),
            ],
        )
        raw = sessiondb.reconstruct(self.db, "claude", "artifact-session", raw=True)
        self.assertEqual([item["content"] for item in raw], rows)
        for text in ("Shaking Foundation", "worker audit current"):
            args = argparse.Namespace(
                text=text, provider="claude", project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None,
                raw=False, limit=50, before=0, after=0,
            )
            self.assertEqual(len(sessiondb.query(self.db, args)), 1)

    def test_malformed_claude_artifact_record_remains_blocking(self):
        source = self.write("malformed-artifact-record.jsonl", [{
            "type": "artifact-comment-monitor", "v": 1,
            "sessionId": "artifact-malformed", "artifacts": "opaque",
        }])
        result = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(result["issues"], 1)
        event = self.db.execute("SELECT supported,issue FROM events").fetchone()
        self.assertEqual(event["supported"], 0)
        self.assertIn("unknown Claude row type", event["issue"])

    def test_passive_codex_completed_item_is_auto_admitted_and_revalidated(self):
        source = self.write("image-view-completed.jsonl", [{
            "type": "event_msg", "payload": {
                "type": "item_completed", "thread_id": "thread-1", "turn_id": "turn-1",
                "item": {
                    "type": "ImageView", "id": "image-1",
                    "path": "file:///private/tmp/example.png",
                },
                "started_at_ms": 1, "completed_at_ms": 2,
            },
        }])
        result = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(result["issues"], 1)
        issue = full_corpus.issue_groups(self.db)[0]
        self.assertEqual((issue["category"], issue["noncritical_adapter_gap"]),
                         ("item_completed", 1))
        admission = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual((admission["admitted_groups"], admission["admitted_events"]), (1, 1))
        event = self.db.execute("SELECT supported,issue FROM events").fetchone()
        self.assertEqual(tuple(event), (1, None))

        with source.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "type": "event_msg", "payload": {
                    "type": "item_completed", "thread_id": "thread-1", "turn_id": "turn-2",
                    "item": {
                        "type": "ImageView", "id": "image-2",
                        "path": "file:///private/tmp/other.png",
                    },
                    "started_at_ms": 3, "completed_at_ms": 4,
                },
            }) + "\n")
        second = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(second["issues"], 0)
        self.assertEqual(
            [row[0] for row in self.db.execute("SELECT supported FROM events ORDER BY line_number")],
            [1, 1],
        )

    def test_base64_canary_is_redacted(self):
        canary = "A" * 200
        self.assertNotIn(canary, sessiondb.redact("attachment=" + canary))
        self.assertEqual(sessiondb.search_projection(canary), "[BASE64_REDACTED]")

    def test_content_pack_uses_zstandard_without_copying_source(self):
        shared = "compressible text block " * 500
        source = self.write("packed.jsonl", [
            {"type": "user", "sessionId": "packed", "message": {"role": "user", "content": shared}},
            {"type": "user", "sessionId": "packed", "message": {"role": "user", "content": shared}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        row = self.db.execute(
            "SELECT kind,raw_bytes,compressed_bytes,compression FROM objects"
        ).fetchone()
        self.assertEqual(row["kind"], "content")
        self.assertEqual(row["compression"], "zstd")
        self.assertLess(row["compressed_bytes"], row["raw_bytes"])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM objects").fetchone()[0], 1)
        source.unlink()
        expanded = sessiondb.reconstruct(self.db, "claude", "packed")
        self.assertEqual([item["content"] for item in expanded], [shared, shared])
        with sessiondb.forbid_source_transcript_reads() as telemetry:
            exact = sessiondb.reconstruct(self.db, "claude", "packed", raw=True)
        self.assertEqual(telemetry["attempts"], 0)
        self.assertEqual(exact[0]["content"]["message"]["content"], shared)

    def test_unreadable_record_is_retained_without_source_recovery(self):
        path = self.root / "broken.jsonl"
        path.write_text("not-json\n", encoding="utf-8")
        sessiondb.ingest(self.db, "claude", path)
        row = self.db.execute("SELECT * FROM events").fetchone()
        path.unlink()
        with sessiondb.forbid_source_transcript_reads() as telemetry:
            self.assertEqual(sessiondb.read_raw_event(self.db, row), "not-json\n")
        self.assertEqual(telemetry["attempts"], 0)
        self.assertEqual(telemetry["sources"], [])
        self.assertFalse(sessiondb.source_read_audit_path(self.db).exists())

    def test_embedded_global_instructions_are_factored_once(self):
        instructions = "# Global Instructions\n\n## Behavior rules\n- Keep exact evidence."
        first = "first wrapper\n<INSTRUCTIONS>\n" + instructions + "\n</INSTRUCTIONS>\nfirst tail"
        second = "different wrapper\n<INSTRUCTIONS>\n" + instructions + "\n</INSTRUCTIONS>\ndifferent tail"
        source = self.write("instructions.jsonl", [
            {"type": "user", "sessionId": "instructions", "message": {"role": "user", "content": first}},
            {"type": "user", "sessionId": "instructions", "message": {"role": "user", "content": second}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        expected_hash = sessiondb.digest(sessiondb.canonical(instructions))
        row = self.db.execute("SELECT kind,compression FROM objects WHERE object_hash=?", (expected_hash,)).fetchone()
        self.assertEqual(tuple(row), ("known-instruction", "zstd"))
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM object_references WHERE child_hash=?", (expected_hash,)).fetchone()[0],
            2,
        )
        self.assertEqual(
            [item["content"] for item in sessiondb.reconstruct(self.db, "claude", "instructions")],
            [first, second],
        )
        args = argparse.Namespace(
            text="Global Instructions", provider="claude", project=None, session=None, role="user",
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
            include_hashes=True,
        )
        clean = sessiondb.query_clean(self.db, args)
        self.assertEqual(len(clean["events"]), 2)
        self.assertEqual(clean["object_hashes"].count(expected_hash), 1)
        self.assertEqual(len(clean["sources"]), 1)

    def test_clean_query_redacts_objects(self):
        source = self.write("secret.jsonl", [
            {"type": "user", "sessionId": "secret", "message": {"role": "user", "content": "api_key=topsecret"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = argparse.Namespace(
            text="REDACTED", provider="claude", project=None, session=None, role="user",
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
        )
        clean = sessiondb.query_clean(self.db, args)
        rendered = json.dumps(clean)
        self.assertIn("[REDACTED]", rendered)
        self.assertNotIn("topsecret", rendered)
        stored_hash = self.db.execute("SELECT object_hash FROM event_content").fetchone()[0]
        args.include_hashes = True
        clean_with_hashes = sessiondb.query_clean(self.db, args)
        self.assertIn(stored_hash, clean_with_hashes["object_hashes"])
        self.assertIn(stored_hash, clean_with_hashes["events"][0]["content_hashes"])
        self.assertNotEqual(clean_with_hashes["object_hashes"], clean_with_hashes["response_object_hashes"])

    def test_clean_query_reports_live_coverage_even_when_result_is_cached(self):
        source = self.write("coverage.jsonl", [
            {"type": "user", "sessionId": "coverage", "message": {"role": "user", "content": "present"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = argparse.Namespace(
            text="missing", provider=None, project=None, session=None, role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
        )
        first = sessiondb.query_clean(self.db, args)
        self.assertEqual(first["coverage"], "missing")

        status_dir = self.root / "freshness"
        status_dir.mkdir()
        (status_dir / "watcher_status.json").write_text(json.dumps({
            "version": 1,
            "pass": True,
            "coverage_complete": True,
            "generated_at": "2026-08-16T12:00:00+00:00",
            "reason": "filesystem_event",
            "issues": [],
            "hard_failures": [],
            "sources_drifted_after_sync": [],
        }))
        second = sessiondb.query_clean(self.db, args)
        self.assertTrue(second["coverage"]["complete"])
        self.assertEqual(second["coverage"]["status"], "complete")
        self.assertEqual(second["coverage"]["reason"], "filesystem_event")
        self.assertIsInstance(second["coverage"]["age_seconds"], float)

    def test_clean_query_scopes_active_claude_and_codex_tails(self):
        claude = self.write("claude-live.jsonl", [
            {"type": "user", "sessionId": "claude-live", "timestamp": "2026-08-21T10:00:00Z",
             "message": {"role": "user", "content": "claude indexed"}},
        ])
        codex = self.write("codex-live.jsonl", [
            {"timestamp": "2026-08-21T10:00:00Z", "type": "event_msg",
             "payload": {"type": "user_message", "message": "codex indexed"}},
        ])
        sessiondb.ingest(self.db, "claude", claude)
        sessiondb.ingest(self.db, "codex", codex)
        claude_offset = claude.stat().st_size
        codex_offset = codex.stat().st_size
        with claude.open("a") as handle:
            handle.write(json.dumps({
                "type": "assistant", "sessionId": "claude-live",
                "timestamp": "2026-08-21T10:01:00Z",
                "message": {"role": "assistant", "content": "claude tail"},
            }) + "\n")
        with codex.open("a") as handle:
            handle.write(json.dumps({
                "timestamp": "2026-08-21T10:01:00Z", "type": "event_msg",
                "payload": {"type": "agent_message", "message": "codex tail"},
            }) + "\n")
        status_dir = self.root / "freshness"
        status_dir.mkdir()
        (status_dir / "watcher_status.json").write_text(json.dumps({
            "version": 1, "pass": False, "coverage_complete": False,
            "generated_at": "2026-08-21T10:01:00Z", "reason": "filesystem_event",
            "issues": [], "hard_failures": [],
            "sources_drifted_after_sync": [str(claude.resolve()), str(codex.resolve())],
        }))
        args = argparse.Namespace(
            text="missing", provider="claude", project=None, session=None, role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
        )
        live = sessiondb.query_clean(self.db, args)["coverage"]
        self.assertEqual(live["status"], "catching_up")
        self.assertFalse(live["complete"])
        self.assertEqual(live["fallback"]["mode"], "raw_source_tail")
        self.assertEqual(
            [(item["provider"], item["indexed_bytes"]) for item in live["affected_sources"]],
            [("claude", claude_offset)],
        )

        args.until = "2026-08-20T23:59:59Z"
        historical = sessiondb.query_clean(self.db, args)["coverage"]
        self.assertTrue(historical["complete"])
        self.assertEqual(historical["status"], "complete")
        self.assertEqual(historical["reason"], "active_sources_outside_query")

        args.provider = "codex"
        args.until = None
        codex_live = sessiondb.query_clean(self.db, args)["coverage"]
        self.assertEqual(codex_live["status"], "catching_up")
        self.assertEqual(codex_live["affected_sources"][0]["indexed_bytes"], codex_offset)

    def test_clean_query_keeps_hard_failure_incomplete_during_active_drift(self):
        source = self.write("failure.jsonl", [
            {"type": "user", "sessionId": "failure", "message": {"role": "user", "content": "indexed"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        status_dir = self.root / "freshness"
        status_dir.mkdir()
        (status_dir / "watcher_status.json").write_text(json.dumps({
            "version": 1, "pass": False, "coverage_complete": False,
            "generated_at": "2026-08-21T10:01:00Z", "reason": "filesystem_event",
            "issues": [], "hard_failures": ["database integrity failed"],
            "sources_drifted_after_sync": [str(source.resolve())],
        }))
        args = argparse.Namespace(
            text="indexed", provider="claude", project=None, session=None, role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
        )
        coverage = sessiondb.query_clean(self.db, args)["coverage"]
        self.assertFalse(coverage["complete"])
        self.assertEqual(coverage["status"], "incomplete")
        self.assertIn("database integrity failed", coverage["issues"])

    def test_clean_query_ignores_structured_issue_outside_provider_scope(self):
        source = self.write("scoped-issue.jsonl", [
            {"type": "user", "sessionId": "scoped-issue",
             "message": {"role": "user", "content": "indexed"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        status_dir = self.root / "freshness"
        status_dir.mkdir()
        (status_dir / "watcher_status.json").write_text(json.dumps({
            "version": 1, "pass": False, "coverage_complete": True,
            "generated_at": "2026-08-21T10:01:00Z", "reason": "filesystem_event",
            "issues": [{"provider": "codex", "issue": "unsupported codex event"}],
            "hard_failures": [], "sources_drifted_after_sync": [],
        }))
        args = argparse.Namespace(
            text="indexed", provider="claude", project=None, session=None, role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
        )
        coverage = sessiondb.query_clean(self.db, args)["coverage"]
        self.assertTrue(coverage["complete"])
        self.assertEqual(coverage["status"], "complete")
        self.assertEqual(coverage.get("issues", []), [])
        args.provider = "codex"
        codex_coverage = sessiondb.query_clean(self.db, args)["coverage"]
        self.assertFalse(codex_coverage["complete"])
        self.assertEqual(codex_coverage["status"], "incomplete")

    def test_clean_query_treats_quiet_unindexed_tail_as_persistent_drift(self):
        source = self.write("persistent.jsonl", [
            {"type": "user", "sessionId": "persistent", "message": {"role": "user", "content": "indexed"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        with source.open("a") as handle:
            handle.write(json.dumps({
                "type": "assistant", "sessionId": "persistent",
                "message": {"role": "assistant", "content": "quiet tail"},
            }) + "\n")
        old = time.time() - sessiondb.ACTIVE_DRIFT_GRACE_SECONDS - 10
        os.utime(source, (old, old))
        status_dir = self.root / "freshness"
        status_dir.mkdir()
        (status_dir / "watcher_status.json").write_text(json.dumps({
            "version": 1, "pass": False, "coverage_complete": False,
            "generated_at": "2026-08-21T10:01:00Z", "reason": "filesystem_event",
            "issues": [], "hard_failures": [],
            "sources_drifted_after_sync": [str(source.resolve())],
        }))
        args = argparse.Namespace(
            text="indexed", provider="claude", project=None, session=None, role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
        )
        coverage = sessiondb.query_clean(self.db, args)["coverage"]
        self.assertFalse(coverage["complete"])
        self.assertEqual(coverage["status"], "incomplete")
        self.assertEqual(coverage["affected_sources"][0]["reason"], "persistent_source_drift")
        self.assertIn("persistent_source_drift", coverage["issues"][0])

    def test_clean_query_caps_events_and_reports_truncation(self):
        source = self.write("many.jsonl", [
            {"type": "user", "sessionId": "many", "message": {
                "role": "user", "content": f"common anchor {index}",
            }}
            for index in range(sessiondb.MAX_CLEAN_EVENTS + 1)
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = argparse.Namespace(
            text="common anchor", provider="claude", project=None, session="many", role=None,
            tool=None, lineage=None, since=None, until=None, raw=False,
            limit=sessiondb.MAX_CLEAN_EVENTS + 1, offset=0, before=0, after=0,
        )
        clean = sessiondb.query_clean(self.db, args)
        self.assertEqual(len(clean["events"]), sessiondb.MAX_CLEAN_EVENTS)
        self.assertTrue(clean["page"]["response_truncated"])
        self.assertTrue(clean["page"]["has_more"])
        self.assertEqual(clean["page"]["context"]["returned_events"], sessiondb.MAX_CLEAN_EVENTS)
        args.offset = sessiondb.MAX_CLEAN_EVENTS
        remainder = sessiondb.query_clean(self.db, args)
        self.assertEqual(len(remainder["events"]), 1)
        self.assertFalse(remainder["page"]["response_truncated"])
        self.assertFalse(remainder["page"]["has_more"])

    def test_query_context_expands_beyond_fts_hit_and_initial_role_filter(self):
        source = self.write("context.jsonl", [
            {"type": "user", "sessionId": "context", "message": {"role": "user", "content": "unique anchor"}},
            {"type": "assistant", "sessionId": "context", "message": {"role": "assistant", "content": "answer outside the indexed hit"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = argparse.Namespace(
            text="unique anchor", provider="claude", project=None, session="context", role="user",
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50, before=0, after=1,
        )
        expanded = sessiondb.query(self.db, args)
        self.assertEqual([row["role"] for row in expanded], ["user", "assistant"])
        self.assertIn("answer outside the indexed hit", expanded[1]["text"])
        clean = sessiondb.query_clean(self.db, args)
        self.assertIn("answer outside the indexed hit", json.dumps(clean))
        self.assertEqual(clean["page"]["context"]["returned_events"], 2)

    def test_unicode_literal_fallback_and_pagination_metadata(self):
        source = self.write("unicode.jsonl", [
            {"type": "user", "sessionId": "unicode", "message": {"role": "user", "content": "中文检索 🚀 first"}},
            {"type": "user", "sessionId": "unicode", "message": {"role": "user", "content": "中文检索 🚀 second"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = argparse.Namespace(
            text="中文检索", provider="claude", project=None, session="unicode", role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=1, offset=0,
            before=10, after=10,
        )
        first = sessiondb.query_clean(self.db, args)
        self.assertTrue(first["page"]["has_more"])
        self.assertTrue(first["page"]["context"]["before_clipped"])
        self.assertTrue(first["page"]["context"]["after_clipped"])
        args.offset = 1
        second = sessiondb.query_clean(self.db, args)
        self.assertEqual(second["page"]["offset"], 1)
        args.text = "🚀"
        args.offset = 0
        self.assertEqual(sessiondb.query_clean(self.db, args)["page"]["total_hits"], 2)

    def test_large_structured_values_and_tagged_prompts_are_shared(self):
        shared_description = "tool definition detail " * 80
        project_rules = "# Project Instructions\n" + ("- Preserve project evidence exactly.\n" * 30)
        nested_rules = "<skills_instructions>\n<skill>\n" + project_rules + "\n</skill>\n</skills_instructions>"
        first_prompt = "first prefix\n" + nested_rules + "\nfirst tail"
        second_prompt = "second prefix\n" + nested_rules + "\nsecond tail"
        contents = [
            {"type": "tool_definition", "name": "first", "description": shared_description},
            {"type": "tool_definition", "name": "second", "description": shared_description},
            {"type": "text", "text": first_prompt},
            {"type": "text", "text": second_prompt},
        ]
        source = self.write("shared-values.jsonl", [
            {"type": "user", "sessionId": "shared-values", "message": {"role": "user", "content": contents}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        description_hash = sessiondb.digest(sessiondb.canonical(shared_description))
        rules_hash = sessiondb.digest(sessiondb.canonical(project_rules))
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM object_references WHERE child_hash=?", (description_hash,)).fetchone()[0],
            2,
        )
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM object_references WHERE child_hash=? AND purpose='tagged-block'", (rules_hash,)).fetchone()[0],
            2,
        )
        self.assertEqual(sessiondb.reconstruct(self.db, "claude", "shared-values")[0]["content"], contents)

    def test_schema_v9_preserves_exact_parsed_provider_structure(self):
        row = {
            "timestamp": "2026-08-15T00:00:00Z", "type": "event_msg",
            "payload": {
                "type": "mcp_tool_call_end", "call_id": "call-1",
                "invocation": {"server": "demo", "arguments": {"text": "large argument " * 100}},
                "result": {"Ok": {"content": [{"type": "text", "text": "large result " * 100}]}},
            },
        }
        source = self.write("structure.jsonl", [row])
        result = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(result["issues"], 0)
        event = self.db.execute("SELECT * FROM events").fetchone()
        bundle = self.db.execute("SELECT structure_zstd FROM sources").fetchone()[0]
        self.assertNotIn(b"large argument", bundle)
        self.assertEqual(sessiondb.read_event_structure(self.db, event), row)

    def test_new_metadata_shapes_are_supported(self):
        claude = self.write("claude-meta.jsonl", [
            {"type": "file-history-delta", "sessionId": "meta", "trackingPath": "CLAUDE.md"},
            {"type": "frame-link", "sessionId": "meta", "path": "/tmp/frame", "frameUrl": "https://example.test"},
        ])
        codex = self.write("codex-meta.jsonl", [
            {"type": "inter_agent_communication_metadata", "payload": {"trigger_turn": False}},
            {"type": "event_msg", "payload": {"type": "sub_agent_activity", "kind": "started"}},
            {"type": "response_item", "payload": {"type": "tool_search_output", "tools": [{"name": "demo"}]}},
        ])
        self.assertEqual(sessiondb.ingest(self.db, "claude", claude)["issues"], 0)
        self.assertEqual(sessiondb.ingest(self.db, "codex", codex)["issues"], 0)

    def test_bridge_session_and_cost_state_metadata_are_supported_and_contentless(self):
        rows = [
            {
                "type": "bridge-session",
                "sessionId": "bridge-root",
                "bridgeSessionId": "bridge-child",
                "lastSequenceNum": 0,
                "ownerAccountUuid": "11111111-1111-1111-1111-111111111111",
                "ownerOrganizationUuid": "22222222-2222-2222-2222-222222222222",
            },
            {
                "type": "cost-state",
                "sessionId": "cost-root",
                "totalCostUSD": 0,
                "totalAPIDuration": 0,
                "totalAPIDurationWithoutRetries": 0,
                "totalToolDuration": 0,
                "totalLinesAdded": 0,
                "totalLinesRemoved": 0,
                "totalDuration": 1551,
                "startTime": 1790126150220,
                "modelUsage": {},
                "hasUnknownModelCost": False,
            },
        ]
        for row in rows:
            with self.subTest(row["type"]):
                source = self.write(f"{row['type']}.jsonl", [row])
                result = sessiondb.ingest(self.db, "claude", source)
                self.assertEqual(result["issues"], 0)
                event = self.db.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 1").fetchone()
                self.assertEqual(event["category"], row["type"])
                self.assertEqual(event["supported"], 1)
                self.assertIsNone(event["content_hash"])
                self.assertEqual(sessiondb.read_event_structure(self.db, event), row)
                self.assertIsNone(sessiondb.reconstruct(self.db, "claude", row["sessionId"])[0]["content"])

    def test_worktree_lifecycle_metadata_is_supported_and_contentless(self):
        rows = [
            {
                "type": "relocated",
                "sessionId": "worktree-root",
                "relocatedCwd": "/tmp/example-worker",
            },
            {
                "type": "worktree-state",
                "sessionId": "worktree-root",
                "worktreeSession": {
                    "originalCwd": "/tmp/example-main",
                    "worktreePath": "/tmp/example-worker",
                    "worktreeName": "example-worker",
                    "worktreeBranch": "example-worker-20260820",
                    "sessionId": "worktree-root",
                    "enteredExisting": True,
                },
            },
        ]
        source = self.write("worktree-lifecycle.jsonl", rows)
        result = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(result["issues"], 0)
        events = self.db.execute("SELECT * FROM events ORDER BY line_number").fetchall()
        self.assertEqual([event["category"] for event in events], ["relocated", "worktree-state"])
        self.assertEqual([event["supported"] for event in events], [1, 1])
        self.assertEqual([sessiondb.read_event_structure(self.db, event) for event in events], rows)
        self.assertEqual(
            [event["content"] for event in sessiondb.reconstruct(self.db, "claude", "worktree-root")],
            [None, None],
        )

    def test_atis_latch_is_strict_contentless_metadata(self):
        rows = [
            {"type": "atis-latch", "sessionId": "atis", "atis": ""},
            {"type": "atis-latch", "sessionId": "atis", "atis": "experiment-name"},
        ]
        source = self.write("atis-latch.jsonl", rows)
        result = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(result["issues"], 0)
        events = self.db.execute("SELECT * FROM events ORDER BY line_number").fetchall()
        self.assertEqual([event["supported"] for event in events], [1, 1])
        self.assertEqual([event["content_hash"] for event in events], [None, None])
        self.assertEqual([sessiondb.read_event_structure(self.db, event) for event in events], rows)

    def test_atis_latch_rejects_shape_drift(self):
        rows = [
            {"type": "atis-latch", "sessionId": "atis", "atis": 1},
            {"type": "atis-latch", "sessionId": "atis", "atis": "", "future": True},
        ]
        source = self.write("atis-latch-invalid.jsonl", rows)
        result = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(result["issues"], 2)
        self.assertEqual(
            [row[0] for row in self.db.execute("SELECT supported FROM events ORDER BY line_number")],
            [0, 0],
        )





















    def test_issue_group_marks_only_auto_repairable_unknowns_as_noncritical(self):
        source = self.write("issue-classes.jsonl", [
            {"type": "safe-marker", "sessionId": "classes", "phase": "start"},
            {"type": "opaque-marker", "sessionId": "classes", "note": "unclear prose"},
            {"type": "content-marker", "sessionId": "classes", "content": "real content"},
        ])
        sessiondb.ingest(self.db, "claude", source)
        groups = {item["category"]: item for item in full_corpus.issue_groups(self.db)}
        self.assertEqual(groups["safe-marker"]["noncritical_adapter_gap"], 1)
        self.assertEqual(groups["opaque-marker"]["noncritical_adapter_gap"], 0)
        self.assertEqual(groups["content-marker"]["noncritical_adapter_gap"], 0)
        evidence_groups = {
            item["category"]: item
            for item in full_corpus.issue_groups(self.db, include_repair_evidence=True)
        }
        safe_evidence = evidence_groups["safe-marker"]
        self.assertEqual(safe_evidence["repair_evidence_complete"], 1)
        self.assertEqual(len(safe_evidence["_repair_events"]), 1)
        self.assertEqual(
            safe_evidence["_repair_events"][0]["structure"],
            {"type": "safe-marker", "sessionId": "classes", "phase": "start"},
        )
        self.assertEqual(safe_evidence["_repair_events"][0]["evidence_source"], "sessiondb")
        source.unlink()
        unavailable = {item["category"]: item for item in full_corpus.issue_groups(self.db)}
        self.assertEqual(unavailable["safe-marker"]["fallback_readable"], 0)
        self.assertEqual(unavailable["safe-marker"]["noncritical_adapter_gap"], 0)

    def test_old_prototype_schema_fails_visibly(self):
        path = self.root / "old.sqlite"
        old = sqlite3.connect(path)
        old.execute("CREATE TABLE events(event_id TEXT)")
        old.commit()
        old.close()
        with self.assertRaisesRegex(ValueError, "pre-v9"):
            sessiondb.connect(path)

    def test_unsupported_schema_versions_fail_visibly(self):
        for version in range(2, 9):
            with self.subTest(version=version):
                path = self.root / f"v{version}.sqlite"
                old = sqlite3.connect(path)
                old.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                old.execute("INSERT INTO metadata VALUES('schema_version',?)", (str(version),))
                old.commit()
                old.close()
                with self.assertRaisesRegex(ValueError, f"schema version: {version}"):
                    sessiondb.connect(path)

    def test_schema_v9_projection_storage_migrates_without_rebuilding_pack(self):
        source = self.write("v9-migration.jsonl", [{
            "type": "user", "sessionId": "v9-migration",
            "message": {"role": "user", "content": "khách hàng proposal"},
        }])
        sessiondb.ingest(self.db, "claude", source)
        pack = sessiondb.projection_pack_path(self.db)
        pack_identity = (pack.name, pack.stat().st_size, sessiondb.digest(pack.read_bytes()))
        fts_rowids = [row[0] for row in self.db.execute(
            "SELECT rowid FROM positional_projection_fts ORDER BY rowid"
        )]
        self.db.execute("PRAGMA foreign_keys=OFF")
        self.db.executescript("""
            CREATE TABLE search_projection_blocks_v9 (
              block_id TEXT PRIMARY KEY,pack_offset INTEGER NOT NULL,
              compressed_bytes INTEGER NOT NULL,raw_bytes INTEGER NOT NULL,
              block_hash TEXT NOT NULL
            ) WITHOUT ROWID;
            INSERT INTO search_projection_blocks_v9
              SELECT hex(block_digest),pack_offset,compressed_bytes,raw_bytes,
                     hex(block_digest)
              FROM search_projection_blocks;
            CREATE TABLE search_projection_refs_v9 (
              event_rowid INTEGER PRIMARY KEY,block_id TEXT NOT NULL,
              projection_offset INTEGER NOT NULL,projection_bytes INTEGER NOT NULL,
              projection_hash TEXT NOT NULL
            ) WITHOUT ROWID;
            INSERT INTO search_projection_refs_v9
              SELECT r.event_rowid,hex(b.block_digest),r.projection_offset,r.projection_bytes,
                     hex(r.projection_hash)
              FROM search_projection_refs r JOIN search_projection_blocks b USING(block_key);
            DROP TABLE search_projection_refs;
            DROP TABLE search_projection_blocks;
            ALTER TABLE search_projection_blocks_v9 RENAME TO search_projection_blocks;
            ALTER TABLE search_projection_refs_v9 RENAME TO search_projection_refs;
            UPDATE metadata SET value='9' WHERE key='schema_version';
        """)
        self.db.commit()
        self.db.close()
        with self.assertRaisesRegex(ValueError, "writer migration"):
            sessiondb.connect(self.db_path, read_only=True)
        self.db = sessiondb.connect(self.db_path)
        sessiondb.migrate_projection_storage_v9_to_v10(self.db)
        self.assertEqual(
            self.db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0],
            "10",
        )
        self.assertEqual(
            self.db.execute("SELECT typeof(block_digest) FROM search_projection_blocks").fetchone()[0],
            "blob",
        )
        self.assertEqual(
            self.db.execute("SELECT typeof(reference_hash) FROM search_projection_refs").fetchone()[0],
            "blob",
        )
        pack_report = sessiondb.verify_projection_pack(self.db)
        self.assertTrue(pack_report["pass"], pack_report)
        self.assertEqual(
            [row[0] for row in self.db.execute(
                "SELECT rowid FROM positional_projection_fts ORDER BY rowid"
            )],
            fts_rowids,
        )
        result = sessiondb.search_index(
            self.db, self._search_args(text="khách hàng", mode="literal"),
        )
        self.assertEqual(result["total"], 1)
        self.assertEqual(
            (pack.name, pack.stat().st_size, sessiondb.digest(pack.read_bytes())), pack_identity,
        )
        source.write_text(json.dumps({
            "type": "user", "sessionId": "v9-migration",
            "message": {"role": "user", "content": "khách hàng updated proposal"},
        }) + "\n", encoding="utf-8")
        sessiondb.ingest(self.db, "claude", source)
        updated = sessiondb.search_index(
            self.db, self._search_args(text="updated proposal", mode="literal"),
        )
        self.assertEqual(updated["total"], 1)

    def test_schema_v9_without_projection_tables_can_initialize_v10(self):
        path = self.root / "v9-no-projection.sqlite"
        old = sqlite3.connect(path)
        old.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        old.execute("INSERT INTO metadata VALUES('schema_version','9')")
        old.commit()
        old.close()
        migrated = sessiondb.connect(path)
        try:
            self.assertEqual(
                migrated.execute(
                    "SELECT value FROM metadata WHERE key='schema_version'"
                ).fetchone()[0],
                "10",
            )
            self.assertTrue(migrated.execute(
                "SELECT 1 FROM sqlite_master WHERE name='search_projection_refs'"
            ).fetchone())
        finally:
            migrated.close()

    def test_schema_v9_projection_migration_rolls_back_on_invalid_digest(self):
        path = self.root / "v9-invalid.sqlite"
        old = sqlite3.connect(path)
        old.executescript("""
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO metadata VALUES('schema_version','9');
            CREATE TABLE search_projection_blocks (
              block_id TEXT PRIMARY KEY,pack_offset INTEGER NOT NULL,
              compressed_bytes INTEGER NOT NULL,raw_bytes INTEGER NOT NULL,
              block_hash TEXT NOT NULL,compressed_hash TEXT
            ) WITHOUT ROWID;
            CREATE TABLE search_projection_refs (
              event_rowid INTEGER PRIMARY KEY,block_id TEXT NOT NULL,
              projection_offset INTEGER NOT NULL,projection_bytes INTEGER NOT NULL,
              projection_hash TEXT NOT NULL,reference_hash TEXT
            ) WITHOUT ROWID;
            INSERT INTO search_projection_blocks VALUES('00',0,0,0,'00',
              '0000000000000000000000000000000000000000000000000000000000000000');
        """)
        old.commit()
        old.close()
        with self.assertRaises((sqlite3.OperationalError, ValueError)):
            sessiondb.connect(path)
        unchanged = sqlite3.connect(path)
        try:
            self.assertEqual(
                unchanged.execute(
                    "SELECT value FROM metadata WHERE key='schema_version'"
                ).fetchone()[0],
                "9",
            )
            self.assertIn(
                "block_id",
                {row[1] for row in unchanged.execute(
                    "PRAGMA table_info(search_projection_blocks)"
                )},
            )
        finally:
            unchanged.close()

    def test_v9_redaction_rule_table_catches_real_secret_classes(self):
        secrets = [
            "token=plain-token-value", "Authorization: Bearer abc.def.ghi12345",
            "ghp_123456789012345678901234567890123456", "sk-1234567890abcdefghijklmnop",
            "AKIA1234567890ABCDEF",
            "-----BEGIN PRIVATE KEY-----\nprivatevalue\n-----END PRIVATE KEY-----",
            "https://alice:hunter2@example.test/path",
        ]
        for secret in secrets:
            with self.subTest(secret=secret[:20]):
                self.assertNotEqual(sessiondb.redact(secret), secret)
        self.assertNotIn("abc.def.ghi12345", sessiondb.redact("Authorization: Bearer abc.def.ghi12345"))
        self.assertNotIn("dXNlcjpwYXNz", sessiondb.redact("Authorization: Basic dXNlcjpwYXNz"))
        self.assertNotIn("horse battery staple", sessiondb.redact("password: correct horse battery staple"))
        self.assertEqual(sessiondb.redact("public token count: 12"), "public token count: 12")
        self.assertEqual(sessiondb.redact("sk-short"), "sk-short")

    def test_secret_containers_are_redacted_from_clean_output_and_fts(self):
        source = self.write("nested-secret.jsonl", [
            {"type": "user", "sessionId": "nested-secret", "message": {"role": "user", "content": {
                "api_key": ["nested-secret-canary"],
                "authorization": {"value": "uniquecanaryvalue"},
            }}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        rendered = json.dumps(sessiondb.query_clean(self.db, argparse.Namespace(
            text=None, provider=None, project=None, session="nested-secret", role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
            offset=0, before=0, after=0, include_page=False, include_hashes=False,
        )))
        self.assertNotIn("nested-secret-canary", rendered)
        self.assertNotIn("uniquecanaryvalue", rendered)
        self.assertEqual(
            self.db.execute(
                "SELECT COUNT(*) FROM positional_projection_fts WHERE positional_projection_fts MATCH ?",
                ("uniquecanaryvalue",),
            ).fetchone()[0],
            0,
        )

    def test_content_identity_preserves_provider_dictionary_order(self):
        first = {"id": "x", "type": "text"}
        second = {"type": "text", "id": "x"}
        source = self.write("ordered-content.jsonl", [
            {"type": "user", "sessionId": "ordered-content", "message": {"role": "user", "content": first}},
            {"type": "user", "sessionId": "ordered-content", "message": {"role": "user", "content": second}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        rebuilt = sessiondb.reconstruct(self.db, "claude", "ordered-content")
        self.assertEqual([list(event["content"]) for event in rebuilt], [["id", "type"], ["type", "id"]])

    def test_same_connection_reingest_invalidates_clean_query_cache(self):
        first = self.write("cache-a.jsonl", [
            {"type": "user", "sessionId": "cache-a", "message": {"role": "user", "content": "alpha"}},
        ])
        second = self.write("cache-b.jsonl", [
            {"type": "user", "sessionId": "cache-b", "message": {"role": "user", "content": "bravo"}},
        ])
        sessiondb.ingest(self.db, "claude", first)
        sessiondb.ingest(self.db, "claude", second)
        args = argparse.Namespace(
            text=None, provider=None, project=None, session="cache-a", role=None, tool=None,
            lineage=None, since=None, until=None, raw=False, limit=50, offset=0,
            before=0, after=0, include_page=False, include_hashes=False,
        )
        self.assertIn("alpha", json.dumps(sessiondb.query_clean(self.db, args)))
        self.write("cache-a.jsonl", [
            {"type": "user", "sessionId": "cache-a", "message": {"role": "user", "content": "bravo"}},
        ])
        sessiondb.ingest(self.db, "claude", first)
        refreshed = json.dumps(sessiondb.query_clean(self.db, args))
        self.assertNotIn("alpha", refreshed)
        self.assertIn("bravo", refreshed)

    def test_text_pagination_reaches_beyond_previous_candidate_ceiling(self):
        source = self.write("many-matches.jsonl", [
            {"type": "user", "sessionId": "many-matches", "message": {"role": "user", "content": f"common {index}"}}
            for index in range(10_001)
        ])
        sessiondb.ingest(self.db, "claude", source)
        rows, page = sessiondb.result_window(self.db, argparse.Namespace(
            text="common", provider=None, project=None, session="many-matches", role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=1,
            offset=10_000, before=0, after=0, include_page=True,
        ))
        self.assertEqual(len(rows), 1)
        self.assertFalse(page["candidate_scan_capped"])
        index_args = argparse.Namespace(
            text="common", mode="literal", provider=None, project=None, session="many-matches",
            role=None, tool=None, lineage=None, since=None, until=None,
        )
        with mock.patch.object(sessiondb, "read_event_structure", side_effect=AssertionError("reconstructed")):
            index = sessiondb.search_index(self.db, index_args)
        self.assertTrue(index["complete"])
        self.assertEqual(index["total"], 10_001)
        self.assertEqual(index["telemetry"]["event_reconstructions"], 0)

    def test_context_counts_adjacent_events_across_blank_lines(self):
        path = self.root / "blank-context.jsonl"
        path.write_text(
            json.dumps({"type": "user", "sessionId": "blank-context", "message": {"role": "user", "content": "hit"}})
            + "\n\n" + json.dumps({"type": "assistant", "sessionId": "blank-context", "message": {"role": "assistant", "content": "next"}}) + "\n"
        )
        sessiondb.ingest(self.db, "claude", path)
        rows, page = sessiondb.result_window(self.db, argparse.Namespace(
            text="hit", provider=None, project=None, session="blank-context", role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=1,
            offset=0, before=0, after=1, include_page=True,
        ))
        self.assertEqual([row["line_number"] for row in rows], [1, 3])
        self.assertFalse(page["context"]["after_clipped"])

    def test_raw_retrieval_preserves_malformed_utf8_evidence(self):
        path = self.root / "invalid-utf8.jsonl"
        path.write_bytes(b"\xff invalid-utf8\n")
        sessiondb.ingest(self.db, "claude", path)
        rebuilt = sessiondb.reconstruct(self.db, "claude", "invalid-utf8", raw=True)
        self.assertEqual(rebuilt[0]["content"], "� invalid-utf8\n")
        rows = sessiondb.query(self.db, argparse.Namespace(
            text="invalid-utf8", provider=None, project=None, session=None, role=None,
            tool=None, lineage=None, since=None, until=None, raw=True, limit=1,
            offset=0, before=0, after=0,
        ))
        self.assertEqual(len(rows), 1)

    def test_raw_search_finds_values_omitted_from_redacted_projection(self):
        source = self.write("raw-excluded.jsonl", [
            {"type": "user", "sessionId": "raw-excluded", "message": {
                "role": "user", "content": {
                    "data": "rawonlycanary", "flag": True, "missing": None,
                },
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.assertIn("rawonlycanary", json.dumps(sessiondb.reconstruct(self.db, "claude", "raw-excluded", raw=True)))
        for needle in ("rawonlycanary", "data", "flag", "true", "missing", "null"):
            rows = sessiondb.query(self.db, argparse.Namespace(
                text=needle, provider=None, project=None, session=None, role=None,
                tool=None, lineage=None, since=None, until=None, raw=True, limit=1,
                offset=0, before=0, after=0,
            ))
            with self.subTest(needle=needle):
                self.assertEqual(len(rows), 1)
        self.assertEqual(
            self.db.execute(
                "SELECT COUNT(*) FROM positional_projection_fts WHERE positional_projection_fts MATCH ?",
                ("rawonlycanary",),
            ).fetchone()[0],
            0,
        )
        for needle in (None, "REDACTED"):
            clean = sessiondb.query_clean(self.db, argparse.Namespace(
                text=needle, provider=None, project=None, session="raw-excluded", role=None,
                tool=None, lineage=None, since=None, until=None, raw=False, limit=1,
                offset=0, before=0, after=0, include_page=False, include_hashes=False,
            ))
            self.assertNotIn("rawonlycanary", json.dumps(clean))

    def test_clean_context_includes_malformed_neighbor_without_structure_crash(self):
        path = self.root / "malformed-context.jsonl"
        path.write_text(
            "not-json\n" + json.dumps({
                "type": "user", "sessionId": "malformed-context",
                "message": {"role": "user", "content": "context-hit"},
            }) + "\n"
        )
        sessiondb.ingest(self.db, "claude", path)
        clean = sessiondb.query_clean(self.db, argparse.Namespace(
            text="context-hit", provider=None, project=None, session="malformed-context",
            role=None, tool=None, lineage=None, since=None, until=None, raw=False,
            limit=1, offset=0, before=1, after=0, include_page=True, include_hashes=False,
        ))
        self.assertEqual(len(clean["events"]), 2)
        malformed = next(event for event in clean["events"] if event.get("ok") is False)
        self.assertIn("Expecting value", malformed["issue"])

    def test_clean_query_rejects_same_length_valid_json_substitution(self):
        source = self.write("substitution.jsonl", [
            {"type": "user", "sessionId": "substitution", "message": {"role": "user", "content": "alpha"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        obj = self.db.execute("SELECT * FROM objects WHERE kind='content'").fetchone()
        args = argparse.Namespace(
            text=None, provider="claude", project=None, session="substitution", role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
            offset=0, before=0, after=0,
        )
        sessiondb.query_clean(self.db, args)
        replacement = sessiondb.stored_json("bravo")
        compressed = zstd.compress(replacement, level=sessiondb.ZSTD_LEVEL)
        self.assertEqual(len(compressed), obj["compressed_bytes"])
        with sessiondb.pack_path(self.db).open("r+b") as handle:
            handle.seek(obj["pack_offset"])
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        with self.assertRaisesRegex(ValueError, "verification failed"):
            sessiondb.query_clean(self.db, args)

    def test_pack_verifier_detects_truncation(self):
        source = self.write("truncated.jsonl", [
            {"type": "user", "sessionId": "truncated", "message": {"content": "packed value"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        pack = sessiondb.pack_path(self.db)
        with pack.open("r+b") as handle:
            handle.truncate(max(0, pack.stat().st_size - 1))
        verification = sessiondb.verify_pack(self.db)
        self.assertFalse(verification["pass"])
        self.assertTrue(verification["failures"])

    def test_online_backup_during_active_ingest_verifies_pack_snapshot(self):
        source = self.write("backup.jsonl", [
            {"type": "user", "sessionId": "backup", "message": {"content": "committed"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        active = threading.Event()

        def write_concurrently():
            writer = sessiondb.connect(self.db_path)
            writer.execute("BEGIN IMMEDIATE")
            sessiondb.put_packed_object(writer, sessiondb.stored_json("concurrent object"), "content")
            active.set()
            time.sleep(0.2)
            writer.commit()
            writer.close()

        thread = threading.Thread(target=write_concurrently)
        thread.start()
        self.assertTrue(active.wait(2))
        result = full_corpus.backup(self.db_path, self.root / "backup-copy")
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(result["complete"])
        self.assertTrue(result["report"]["pack_verification"]["pass"])
        self.assertTrue(result["report"]["retrieval_pack_verification"]["pass"])
        restored = sessiondb.connect(Path(result["database"]))
        try:
            self.assertTrue(sessiondb.projection_status(restored)["complete"])
            self.assertNotEqual(
                sessiondb.projection_pack_path(restored), sessiondb.projection_pack_path(self.db),
            )
            found = sessiondb.search_index(restored, argparse.Namespace(
                text="committed", mode="literal", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None,
            ))
            self.assertEqual(found["total"], 1)
        finally:
            restored.close()

    def test_interrupted_repeat_backup_preserves_previous_retrieval_generation(self):
        source = self.write("repeat-backup.jsonl", [
            {"type": "user", "sessionId": "repeat", "message": {"content": "first"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        target_dir = self.root / "repeat-copy"
        first = full_corpus.backup(self.db_path, target_dir)
        self.assertTrue(first["complete"])
        copied_db = Path(first["database"])
        prior = sessiondb.connect(copied_db, read_only=True)
        try:
            prior_pack = sessiondb.projection_pack_path(prior)
            prior_bytes = prior_pack.read_bytes()
            before = sessiondb.search_index(prior, argparse.Namespace(
                text="first", mode="literal", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None,
            ))
            self.assertTrue(before["complete"])
            self.assertEqual(before["total"], 1)
        finally:
            prior.close()
        source.write_text(json.dumps(
            {"type": "user", "sessionId": "repeat", "message": {"content": "second changed"}}
        ) + "\n", encoding="utf-8")
        sessiondb.ingest(self.db, "claude", source)
        real_replace = os.replace

        def fail_database_publication(source_path, destination_path):
            if Path(destination_path) == copied_db:
                raise OSError("simulated publication interruption")
            return real_replace(source_path, destination_path)

        with mock.patch.object(full_corpus.os, "replace", side_effect=fail_database_publication):
            with self.assertRaisesRegex(OSError, "publication interruption"):
                full_corpus.backup(self.db_path, target_dir)
        preserved = sessiondb.connect(copied_db, read_only=True)
        try:
            self.assertEqual(sessiondb.projection_pack_path(preserved), prior_pack)
            self.assertEqual(prior_pack.read_bytes(), prior_bytes)
            self.assertTrue(sessiondb.verify_projection_pack(preserved)["pass"])
        finally:
            preserved.close()
        stale = sessiondb.connect(copied_db, read_only=True)
        try:
            repeated = full_corpus.backup(self.db_path, target_dir)
            self.assertTrue(repeated["complete"])
            self.assertFalse(prior_pack.exists())
            stale_result = sessiondb.search_index(stale, argparse.Namespace(
                text="first", mode="literal", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None,
            ))
            self.assertFalse(stale_result["complete"])
            self.assertEqual(stale_result["coverage"]["reason"], "database_inode_replaced")
            self.assertEqual(stale_result["entries"], [])
        finally:
            stale.close()
        reopened = sessiondb.connect(copied_db, read_only=True)
        try:
            found = sessiondb.search_index(reopened, argparse.Namespace(
                text="second", mode="literal", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None,
            ))
            self.assertTrue(found["complete"])
            self.assertEqual(found["total"], 1)
        finally:
            reopened.close()

    def test_pack_verifier_allows_and_reports_interior_orphans(self):
        self.db.execute("BEGIN IMMEDIATE")
        sessiondb.put_packed_object(self.db, sessiondb.stored_json("rolled back"), "content")
        self.db.rollback()
        self.db.execute("BEGIN IMMEDIATE")
        sessiondb.put_packed_object(self.db, sessiondb.stored_json("committed after gap"), "content")
        self.db.commit()
        verification = sessiondb.verify_pack(self.db)
        self.assertTrue(verification["pass"])
        self.assertGreater(verification["orphan_bytes"], 0)

    def test_compact_copy_removes_orphans_and_unreferenced_objects(self):
        source = self.write("compact.jsonl", [
            {"type": "user", "sessionId": "compact", "message": {"content": "reachable"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.db.execute("BEGIN IMMEDIATE")
        sessiondb.put_packed_object(self.db, sessiondb.stored_json("rolled back orphan"), "content")
        self.db.rollback()
        self.db.execute("BEGIN IMMEDIATE")
        sessiondb.put_packed_object(self.db, sessiondb.stored_json("unreferenced committed"), "content")
        self.db.commit()
        before = sessiondb.pack_path(self.db).stat().st_size
        source_database_bytes = self.db_path.read_bytes()
        source_pack_bytes = sessiondb.pack_path(self.db).read_bytes()
        source_retrieval = sessiondb.projection_pack_path(self.db)
        source_retrieval_bytes = source_retrieval.read_bytes()
        target = self.root / "compacted.sqlite"
        result = full_corpus.compact_copy(self.db_path, target)
        self.assertTrue(result["complete"])
        self.assertEqual(result["foreign_key_failures"], [])
        self.assertEqual(result["freelist_bytes"], 0)
        self.assertEqual(result["report"]["pack_verification"]["orphan_bytes"], 0)
        self.assertLess(Path(str(target) + ".objects").stat().st_size, before)
        self.assertEqual(self.db_path.read_bytes(), source_database_bytes)
        self.assertEqual(sessiondb.pack_path(self.db).read_bytes(), source_pack_bytes)
        self.assertEqual(source_retrieval.read_bytes(), source_retrieval_bytes)
        compacted = sessiondb.connect(target)
        try:
            self.assertEqual(sessiondb.reconstruct(compacted, "claude", "compact")[0]["content"], "reachable")
            self.assertTrue(sessiondb.projection_status(compacted)["complete"])
            self.assertNotEqual(sessiondb.projection_pack_path(compacted), source_retrieval)
            found = sessiondb.search_index(compacted, argparse.Namespace(
                text="reachable", mode="literal", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None,
            ))
            self.assertEqual(found["total"], 1)
            sessiondb.build_search_projections(compacted)
            self.assertEqual(source_retrieval.read_bytes(), source_retrieval_bytes)
        finally:
            compacted.close()

    def test_failed_compact_copy_publishes_nothing_and_cleans_temporary_files(self):
        source = self.write("compact-failure.jsonl", [
            {"type": "user", "sessionId": "compact-failure", "message": {"content": "must remain safe"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        source_database_bytes = self.db_path.read_bytes()
        pack = sessiondb.pack_path(self.db)
        pack_bytes = pack.read_bytes()
        pack.write_bytes(pack_bytes[:-1])
        target = self.root / "failed-candidate.sqlite"
        with self.assertRaisesRegex(ValueError, "source pack extent missing"):
            full_corpus.compact_copy(self.db_path, target)
        self.assertEqual(self.db_path.read_bytes(), source_database_bytes)
        self.assertEqual(pack.read_bytes(), pack_bytes[:-1])
        self.assertFalse(target.exists())
        self.assertFalse(Path(str(target) + ".objects").exists())
        self.assertEqual(list(self.root.glob(".failed-candidate.sqlite.*.tmp*")), [])

    def test_compaction_policy_reports_not_due_without_creating_a_copy(self):
        source = self.write("compact-policy.jsonl", [
            {"type": "user", "sessionId": "compact-policy", "message": {"content": "keep current"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        analysis = full_corpus.compaction_analysis(self.db_path, self.root)
        self.assertFalse(analysis["threshold_met"])
        self.assertFalse(analysis["due"])
        self.assertEqual(analysis["minimum_reclaim_bytes"], 256 * 1024 * 1024)
        self.assertEqual(analysis["minimum_reclaim_percent"], 15.0)
        target = self.root / "not-due.sqlite"
        result = full_corpus.compact_if_due(self.db_path, target)
        self.assertTrue(result["complete"])
        self.assertEqual(result["status"], "not_due")
        self.assertFalse(result["created"])
        self.assertFalse(target.exists())
        self.assertFalse(Path(str(target) + ".objects").exists())

    def test_compaction_promotion_prioritizes_correctness_tokens_then_speed(self):
        baseline = {
            "pass": True,
            "database_events": 12,
            "correctness": {"pass": True, "passed": 33, "total": 33},
            "beyond_index": {"pass": True, "expected_beyond_direct_hits": 3, "recovered_with_context": 3},
            "tokens": {"full_corpus_clean_tokens": 100, "matched_raw_transcript_tokens": 300},
            "latency_ms": {"bounded": {"p95": 10.0}},
        }
        candidate = json.loads(json.dumps(baseline))
        candidate["latency_ms"]["bounded"]["p95"] = 10.9
        baseline_path = self.root / "baseline.json"
        candidate_path = self.root / "candidate.json"
        baseline_path.write_text(json.dumps(baseline))
        candidate_path.write_text(json.dumps(candidate))
        accepted = full_corpus.validate_promotion(baseline_path, candidate_path)
        self.assertTrue(accepted["promotion_allowed"])
        candidate["tokens"]["full_corpus_clean_tokens"] += 1
        candidate_path.write_text(json.dumps(candidate))
        token_rejected = full_corpus.validate_promotion(baseline_path, candidate_path)
        self.assertFalse(token_rejected["tokens_equal"])
        self.assertFalse(token_rejected["promotion_allowed"])
        candidate = json.loads(json.dumps(baseline))
        candidate["latency_ms"]["bounded"]["p95"] = 11.1
        candidate_path.write_text(json.dumps(candidate))
        speed_rejected = full_corpus.validate_promotion(baseline_path, candidate_path)
        self.assertFalse(speed_rejected["speed_pass"])
        self.assertFalse(speed_rejected["promotion_allowed"])

    def test_two_process_writers_are_serialized_and_verified(self):
        first = self.write("writer-one.jsonl", [
            {"type": "user", "sessionId": "writer-one", "message": {"content": f"one-{index}"}}
            for index in range(100)
        ])
        second = self.write("writer-two.jsonl", [
            {"type": "user", "sessionId": "writer-two", "message": {"content": f"two-{index}"}}
            for index in range(100)
        ])
        command = [sys.executable, str(Path(sessiondb.__file__)), "--db", str(self.db_path), "ingest", "--provider", "claude"]
        processes = [
            subprocess.Popen(command + [str(first)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True),
            subprocess.Popen(command + [str(second)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True),
        ]
        results = [process.communicate(timeout=30) + (process.returncode,) for process in processes]
        self.assertEqual([result[2] for result in results], [0, 0], results)
        self.db.close()
        self.db = sessiondb.connect(self.db_path)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 200)
        self.assertTrue(sessiondb.verify_pack(self.db)["pass"])

    def test_sigkill_leaves_recoverable_orphan_and_next_write_succeeds(self):
        marker = self.root / "writer-active"
        script = (
            "import pathlib,sys,time,sessiondb;"
            "db=sessiondb.connect(pathlib.Path(sys.argv[1]));"
            "db.execute('BEGIN IMMEDIATE');"
            "sessiondb.put_packed_object(db,sessiondb.stored_json('killed object'),'content');"
            "pathlib.Path(sys.argv[2]).write_text('ready');"
            "time.sleep(60)"
        )
        child = subprocess.Popen([sys.executable, "-c", script, str(self.db_path), str(marker)])
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(marker.exists())
        child.send_signal(signal.SIGKILL)
        child.wait(timeout=5)
        self.db.execute("BEGIN IMMEDIATE")
        sessiondb.put_packed_object(self.db, sessiondb.stored_json("after kill"), "content")
        self.db.commit()
        verification = sessiondb.verify_pack(self.db)
        self.assertTrue(verification["pass"])
        self.assertGreater(verification["orphan_bytes"], 0)

    def test_whole_row_structure_and_punctuation_are_searchable(self):
        source = self.write("structure-search.jsonl", [
            {"type": "user", "sessionId": "structure-search", "message": {"role": "user", "content": "ordinary"},
             "toolUseResult": {"nested": {"value": "needle --> 中文 🚀"}}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        for needle in ("needle", "-->", "中文", "🚀"):
            args = argparse.Namespace(
                text=needle, provider="claude", project=None, session="structure-search", role=None,
                tool=None, lineage=None, since=None, until=None, raw=False, limit=1,
                offset=0, before=0, after=0, include_page=True,
            )
            with self.subTest(needle=needle):
                result = sessiondb.query_clean(self.db, args)
                self.assertEqual(len(result["events"]), 1)
                self.assertIn(needle, result["events"][0]["match"])
                self.assertFalse(result["page"]["candidate_scan_capped"])

    def test_reconstruction_includes_contentless_events_in_timestamp_order(self):
        first = self.write("first.jsonl", [
            {"type": "user", "sessionId": "merged", "timestamp": "2026-08-14T09:00:00+07:00", "message": {"content": "later"}},
            {"type": "mode", "sessionId": "merged"},
        ])
        second = self.write("second.jsonl", [
            {"type": "user", "sessionId": "merged", "timestamp": "2026-08-14T01:00:00Z", "message": {"content": "earlier"}},
        ])
        sessiondb.ingest(self.db, "claude", first)
        sessiondb.ingest(self.db, "claude", second)
        events = sessiondb.reconstruct(self.db, "claude", "merged")
        self.assertEqual(len(events), 3)
        self.assertEqual([event["content"] for event in events], ["earlier", "later", None])
        self.assertEqual(events[-1]["order_kind"], "forward_fill")
        self.assertEqual(events[1]["order_timestamp"], "2026-08-14T02:00:00.000000Z")

    def test_claude_tool_results_have_nonzero_denominator_and_attribution(self):
        source = self.write("claude-tools.jsonl", [
            {"type": "assistant", "sessionId": "tools", "message": {"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu-1", "name": "Read", "input": {"file": "x"}},
            ]}},
            {"type": "user", "sessionId": "tools", "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu-1", "content": "done"},
            ]}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        rows = self.db.execute("SELECT tool_event,tool FROM events ORDER BY line_number").fetchall()
        self.assertEqual(sum(row["tool_event"] for row in rows), 2)
        self.assertEqual([row["tool"] for row in rows], ["Read", "Read"])

    def test_portable_snapshot_is_complete_deduplicated_redacted_and_resolvable(self):
        source = self.write("daily-tools.jsonl", [
            {"type": "assistant", "sessionId": "daily", "timestamp": "2026-08-14T01:00:00Z",
             "message": {"role": "assistant", "content": [
                 {"type": "tool_use", "id": "toolu-daily", "name": "Read", "input": {"token": "secret-value"}},
             ]}},
            {"type": "user", "sessionId": "daily", "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu-daily", "content": "done"},
            ]}},
            {"type": "future-shape", "sessionId": "daily", "timestamp": "2026-08-14T01:00:02Z",
             "content": "unknown but retained"},
        ])
        sessiondb.ingest(self.db, "claude", source)
        sessiondb.source_read_audit_path(self.db).write_text(json.dumps({
            "timestamp": "2026-08-14T01:00:03Z", "source_id": "example",
            "source_path": str(source), "reason": "test fallback",
        }) + "\n")
        output = self.root / "snapshot"
        manifest = snapshot_export.export_snapshot(
            self.db_path, output, "2026-08-14T00:00:00Z", "2026-08-15T00:00:00Z", {"complete": True},
        )
        self.assertTrue(manifest["export_complete"])
        self.assertEqual(manifest["coverage"], "partial")
        self.assertTrue(manifest["attention_required"])
        self.assertEqual(manifest["counts"]["unsupported_events"], 1)
        self.assertEqual(manifest["counts"]["raw_source_reads"], 1)
        self.assertEqual(manifest["counts"]["tool_attribution_events_by_provider"]["claude"], 2)
        self.assertEqual(manifest["counts"]["unresolved_tool_attribution_events"], 0)
        self.assertEqual(manifest["counts"]["events_with_inferred_timestamp"], 1)
        self.assertEqual(manifest["fallback"]["scope"], "sources")
        self.assertEqual(manifest["fallback"]["providers"], ["claude"])
        self.assertEqual(manifest["fallback"]["sources"][0]["path"], str(source.resolve()))
        self.assertEqual(manifest["fallback"]["sources"][0]["provider"], "claude")
        self.assertEqual(manifest["format"], "sessiondb-portable-snapshot")
        self.assertEqual(len((output / "events.jsonl").read_text().splitlines()), 3)
        self.assertGreater(len((output / "objects.jsonl").read_text().splitlines()), 0)
        event_rows = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
        object_rows = [json.loads(line) for line in (output / "objects.jsonl").read_text().splitlines()]
        object_ids = {row["id"] for row in object_rows}
        self.assertTrue(all(ref in object_ids for row in event_rows for ref in row["content_refs"]))
        self.assertEqual(output.stat().st_mode & 0o777, 0o700)
        for path in output.rglob("*"):
            if path.is_file():
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                self.assertNotIn("secret-value", path.read_text())

    def test_stat_precheck_skips_unchanged_source_without_reading(self):
        source = self.write("stat-skip.jsonl", [
            {"type": "user", "sessionId": "stat-skip", "message": {"content": "value"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        os.chmod(source, 0)
        try:
            self.assertEqual(sessiondb.ingest(self.db, "claude", source)["status"], "unchanged")
        finally:
            os.chmod(source, 0o600)

    def test_read_only_connection_serves_clean_query(self):
        source = self.write("read-only.jsonl", [
            {"type": "user", "sessionId": "read-only", "timestamp": "2026-08-14T01:00:00Z",
             "message": {"content": "read only retrieval marker"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        reader = sessiondb.connect(self.db_path, read_only=True)
        try:
            args = argparse.Namespace(
                text="read only retrieval marker", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None, raw=False, clean=True,
                before=0, after=0, limit=10, offset=0, include_page=False, include_hashes=False,
            )
            result = sessiondb.query_clean(reader, args)
            self.assertEqual(len(result["events"]), 1)
        finally:
            reader.close()

    def test_append_ingest_parses_only_new_lines_and_preserves_tool_state(self):
        source = self.write("append.jsonl", [
            {"type": "response_item", "timestamp": "2026-08-16T01:00:00Z",
             "payload": {"type": "function_call", "call_id": "call-1", "name": "exec", "arguments": "{}"}},
        ])
        first = sessiondb.ingest(self.db, "codex", source)
        first_event = self.db.execute("SELECT event_id FROM events").fetchone()[0]
        with source.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "type": "response_item", "timestamp": "2026-08-16T01:00:01Z",
                "payload": {"type": "function_call_output", "call_id": "call-1", "output": "done"},
            }) + "\n")
        second = sessiondb.ingest(self.db, "codex", source)
        self.assertEqual(first["status"], "imported")
        self.assertEqual(second["status"], "appended")
        self.assertEqual(second["events"], 1)
        self.assertEqual(second["nonblank_lines"], 2)
        self.assertEqual(self.db.execute("SELECT event_id FROM events ORDER BY line_number").fetchone()[0], first_event)
        self.assertEqual(
            self.db.execute("SELECT tool FROM events ORDER BY line_number DESC LIMIT 1").fetchone()[0], "exec",
        )
        state = sessiondb.load_adapter_state(
            self.db, self.db.execute("SELECT source_id FROM sources").fetchone()[0],
        )
        self.assertNotIn("__sessiondb_tool_calls", state)

    def test_persisted_codex_append_state_omits_unused_session_payload(self):
        source = self.write("state.jsonl", [{
            "type": "session_meta", "payload": {
                "id": "state", "cwd": "/tmp/project",
                "base_instructions": "large instruction " * 100,
                "dynamic_tools": [{"name": "tool", "description": "large " * 100}],
            },
        }])
        sessiondb.ingest(self.db, "codex", source)
        state = sessiondb.load_adapter_state(
            self.db, self.db.execute("SELECT source_id FROM sources").fetchone()[0],
        )
        self.assertEqual(state, {"id": "state", "cwd": "/tmp/project"})

    def test_append_ingest_falls_back_to_full_rebuild_after_prefix_rewrite(self):
        source = self.write("rewrite.jsonl", [
            {"type": "user", "sessionId": "rewrite", "message": {"content": "first"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        source.write_text(
            json.dumps({"type": "user", "sessionId": "rewrite", "message": {"content": "changed"}}) + "\n"
            + json.dumps({"type": "assistant", "sessionId": "rewrite", "message": {"content": "tail"}}) + "\n",
            encoding="utf-8",
        )
        result = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(result["status"], "imported")
        self.assertEqual(
            [item["content"] for item in sessiondb.reconstruct(self.db, "claude", "rewrite")],
            ["changed", "tail"],
        )

    def test_foreign_unversioned_database_is_not_modified(self):
        path = self.root / "foreign.sqlite"
        foreign = sqlite3.connect(path)
        foreign.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        foreign.execute("CREATE TABLE business_data(value TEXT)")
        foreign.commit()
        foreign.close()
        before = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "unversioned"):
            sessiondb.connect(path)
        self.assertEqual(path.read_bytes(), before)

    def test_fast_sync_ignores_source_that_moves_after_discovery(self):
        source = self.write("moving.jsonl", [{
            "type": "session_meta", "payload": {"id": "moving"},
        }])
        discovered = [("codex", source.resolve(), "active")]

        def discovery():
            if source.exists():
                source.unlink()
                return discovered
            return []

        with mock.patch.object(full_corpus, "discover", side_effect=discovery):
            report = full_corpus.fast_sync(
                self.root / "moving.sqlite", self.root / "moving-report.json",
            )
        self.assertTrue(report["complete"])
        self.assertEqual(report["hard_failures"], [])
        self.assertEqual(report["issues"], [])

    def test_sync_recognizes_source_relocation_by_hash(self):
        active = self.root / "active"
        archived = self.root / "archived"
        active.mkdir()
        archived.mkdir()
        source = active / "relocate.jsonl"
        source.write_text(json.dumps({
            "type": "session_meta", "payload": {"id": "relocate"},
        }) + "\n")
        db_path = self.root / "relocation.sqlite"
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"codex": (active, archived)}
            first = full_corpus.sync(
                db_path, self.root / "manifest-one.json", self.root / "report-one.json", 10,
            )
            moved = archived / source.name
            source.rename(moved)
            second = full_corpus.sync(
                db_path, self.root / "manifest-two.json", self.root / "report-two.json", 10,
            )
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(first["complete"])
        self.assertTrue(second["complete"])
        self.assertEqual(second["relocated_sources"][0]["to"], str(moved.resolve()))
        self.assertFalse(second["stale_indexed_sources"])

    def _claude_transcript(self, path, session, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "type": "user", "uuid": f"{session}-u1", "sessionId": session,
            "timestamp": "2026-09-26T16:33:18Z", "message": {"role": "user", "content": text},
        }) + "\n")
        return path

    def _delete_marker(self, project, session):
        (project / f"{session}.desktop-released.json").write_text(json.dumps(
            {"v": 1, "releasedAt": "2026-09-26T16:35:35.864Z", "reason": "delete"},
        ))

    def test_sync_forgets_desktop_deleted_sessions_and_keeps_moved_or_unmarked_ones(self):
        projects = self.root / "projects"
        project, other = projects / "-work", projects / "-other"
        deleted = self._claude_transcript(project / "sess-del.jsonl", "sess-del", "deletedneedle root")
        subagent = self._claude_transcript(
            project / "sess-del" / "subagents" / "agent-1.jsonl", "sess-del", "deletedneedle child",
        )
        moved = self._claude_transcript(project / "sess-move.jsonl", "sess-move", "movedneedle")
        vanished = self._claude_transcript(project / "sess-gone.jsonl", "sess-gone", "vanishedneedle")
        kept = self._claude_transcript(project / "sess-kept.jsonl", "sess-kept", "keptneedle")
        db_path = self.root / "desktop-delete.sqlite"
        results = self.root / "retrieval-results"
        results.mkdir()
        (results / "query-a.jsonl").write_text('{"session_id": "sess-del"}\n')
        (results / "query-b.jsonl").write_text('{"session_id": "sess-kept"}\n')
        snapshots = self.root / "shadow" / "snapshots"
        (snapshots / "2026-09-26").mkdir(parents=True)
        (snapshots / "2026-09-26" / "events.jsonl").write_text('{"session_id": "sess-del"}\n')
        (snapshots / "2026-09-25").mkdir()
        (snapshots / "2026-09-25" / "events.jsonl").write_text('{"session_id": "sess-kept"}\n')
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"claude": (projects,)}
            first = full_corpus.sync(db_path, self.root / "m1.json", self.root / "r1.json", 10)
            # The 2026-09-26 case: the app removed the transcript and left a delete marker.
            deleted.unlink()
            shutil.rmtree(project / "sess-del")
            self._delete_marker(project, "sess-del")
            moved.rename(other / moved.name) if other.exists() else (other.mkdir(), moved.rename(other / moved.name))
            vanished.unlink()
            self._delete_marker(project, "sess-kept")
            second = full_corpus.sync(db_path, self.root / "m2.json", self.root / "r2.json", 10)
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(first["complete"])
        removed = second["desktop_deleted_sources"]
        self.assertEqual(sorted(item["path"] for item in removed),
                         sorted([str(deleted.resolve()), str(subagent.resolve())]))
        self.assertEqual({tuple(item["sessions"]) for item in removed}, {("sess-del",)})
        self.assertEqual(second["stale_indexed_sources"], [str(vanished.resolve())])
        self.assertEqual(second["relocated_sources"][0]["to"], str((other / moved.name).resolve()))
        self.assertEqual(sorted(removed[0]["derived_copies_removed"]),
                         sorted([str((results / "query-a.jsonl").resolve()),
                                 str((snapshots / "2026-09-26").resolve())]))
        self.assertTrue((results / "query-b.jsonl").exists())
        self.assertTrue((snapshots / "2026-09-25").exists())
        db = sessiondb.connect(db_path)
        try:
            def hits(text):
                args = argparse.Namespace(
                    text=text, provider=None, project=None, session=None, role=None, tool=None,
                    lineage=None, since=None, until=None, raw=False, limit=50,
                )
                return sessiondb.query(db, args)
            self.assertEqual(hits("deletedneedle"), [])
            for text in ("movedneedle", "vanishedneedle", "keptneedle"):
                self.assertEqual(len(hits(text)), 1, text)
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM events WHERE session_id='sess-del'").fetchone()[0], 0)
            self.assertIsNone(db.execute(
                "SELECT 1 FROM metadata WHERE key LIKE 'adapter_state:%' AND key NOT IN "
                "(SELECT 'adapter_state:' || source_id FROM sources)").fetchone())
            self.assertTrue(sessiondb.projection_status(db)["complete"])
        finally:
            db.close()


    def test_watcher_sync_forgets_desktop_deleted_sessions(self):
        projects = self.root / "projects"
        project = projects / "-work"
        deleted = self._claude_transcript(project / "sess-del.jsonl", "sess-del", "deletedneedle")
        self._claude_transcript(project / "sess-live.jsonl", "sess-live", "liveneedle")
        db_path = self.root / "watcher-delete.sqlite"
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"claude": (projects,)}
            full_corpus.fast_sync(db_path, self.root / "fast-1.json")
            deleted.unlink()
            # Without a marker the vanished transcript stays indexed.
            unmarked = full_corpus.fast_sync(db_path, self.root / "fast-2.json")
            self._delete_marker(project, "sess-del")
            marked = full_corpus.fast_sync(db_path, self.root / "fast-3.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertEqual(unmarked["desktop_deleted_sources"], [])
        self.assertEqual([item["sessions"] for item in marked["desktop_deleted_sources"]], [["sess-del"]])
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                [row[0] for row in db.execute("SELECT DISTINCT session_id FROM events ORDER BY 1")],
                ["sess-live"],
            )
            self.assertTrue(sessiondb.projection_status(db)["complete"])
        finally:
            db.close()

    def test_sync_recognizes_source_relocation_after_append(self):
        active = self.root / "active-appended"
        archived = self.root / "archived-appended"
        active.mkdir()
        archived.mkdir()
        source = active / "relocate.jsonl"
        source.write_text(json.dumps({
            "type": "session_meta", "payload": {"id": "relocate-appended"},
        }) + "\n")
        db_path = self.root / "relocation-appended.sqlite"
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"codex": (active, archived)}
            first = full_corpus.sync(
                db_path, self.root / "manifest-appended-one.json",
                self.root / "report-appended-one.json", 10,
            )
            moved = archived / source.name
            source.rename(moved)
            with moved.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "type": "event_msg", "payload": {
                        "type": "agent_message", "message": "after move",
                    },
                }) + "\n")
            second = full_corpus.sync(
                db_path, self.root / "manifest-appended-two.json",
                self.root / "report-appended-two.json", 10,
            )
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(first["complete"])
        self.assertTrue(second["complete"])
        self.assertEqual(second["status_counts"], {"appended": 1})
        self.assertEqual(second["relocated_sources"][0]["to"], str(moved.resolve()))
        self.assertEqual(second["relocated_sources"][0]["mode"], "relocated")

    def test_sync_repairs_duplicate_created_by_an_appended_move(self):
        active = self.root / "active-duplicate"
        archived = self.root / "archived-duplicate"
        active.mkdir()
        archived.mkdir()
        source = active / "duplicate.jsonl"
        source.write_text(json.dumps({
            "type": "session_meta", "payload": {"id": "duplicate"},
        }) + "\n")
        db_path = self.root / "relocation-duplicate.sqlite"
        old_roots = full_corpus.ROOTS
        connection = None
        try:
            full_corpus.ROOTS = {"codex": (active, archived)}
            full_corpus.sync(
                db_path, self.root / "manifest-duplicate-one.json",
                self.root / "report-duplicate-one.json", 10,
            )
            moved = archived / source.name
            source.rename(moved)
            with moved.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "type": "event_msg", "payload": {
                        "type": "agent_message", "message": "after move",
                    },
                }) + "\n")
            connection = sessiondb.connect(db_path)
            sessiondb.ingest(connection, "codex", moved)
            connection.close()
            connection = None
            repaired = full_corpus.sync(
                db_path, self.root / "manifest-duplicate-two.json",
                self.root / "report-duplicate-two.json", 10,
            )
        finally:
            if connection is not None:
                connection.close()
            full_corpus.ROOTS = old_roots
        self.assertTrue(repaired["complete"])
        self.assertEqual(repaired["relocated_sources"][0]["mode"], "deduplicated_appended_move")
        connection = sessiondb.connect(db_path)
        try:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM sources").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0], 2)
        finally:
            connection.close()

    def test_sync_does_not_relocate_same_name_with_different_content(self):
        active = self.root / "active-unrelated"
        archived = self.root / "archived-unrelated"
        active.mkdir()
        archived.mkdir()
        source = active / "same-name.jsonl"
        source.write_text(json.dumps({
            "type": "session_meta", "payload": {"id": "original"},
        }) + "\n")
        db_path = self.root / "relocation-unrelated.sqlite"
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"codex": (active, archived)}
            full_corpus.sync(
                db_path, self.root / "manifest-unrelated-one.json",
                self.root / "report-unrelated-one.json", 10,
            )
            source.unlink()
            replacement = archived / source.name
            replacement.write_text(json.dumps({
                "type": "session_meta", "payload": {"id": "replacement"},
            }) + "\n")
            second = full_corpus.sync(
                db_path, self.root / "manifest-unrelated-two.json",
                self.root / "report-unrelated-two.json", 10,
            )
        finally:
            full_corpus.ROOTS = old_roots
        self.assertFalse(second["complete"])
        self.assertFalse(second["relocated_sources"])
        self.assertEqual(len(second["stale_indexed_sources"]), 1)

    def test_fast_sync_recognizes_source_relocation_after_append(self):
        active = self.root / "fast-active-appended"
        archived = self.root / "fast-archived-appended"
        active.mkdir()
        archived.mkdir()
        source = active / "fast-relocate.jsonl"
        source.write_text(json.dumps({
            "type": "user", "sessionId": "fast-relocate", "message": {"content": "first"},
        }) + "\n")
        db_path = self.root / "fast-relocation-appended.sqlite"
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"claude": (active, archived)}
            full_corpus.fast_sync(db_path, self.root / "fast-relocate-one.json")
            moved = archived / source.name
            source.rename(moved)
            with moved.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "type": "assistant", "sessionId": "fast-relocate",
                    "message": {"content": "second"},
                }) + "\n")
            second = full_corpus.fast_sync(db_path, self.root / "fast-relocate-two.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(second["complete"])
        self.assertEqual(second["status_counts"], {"appended": 1})
        self.assertEqual(second["relocated_sources"][0]["to"], str(moved.resolve()))

    def test_fast_sync_catches_appended_events_and_reports_complete_coverage(self):
        transcripts = self.root / "transcripts"
        transcripts.mkdir()
        source = transcripts / "active.jsonl"
        source.write_text(json.dumps({
            "type": "user", "sessionId": "active", "message": {"content": "first"},
        }) + "\n")
        db_path = self.root / "fresh.sqlite"
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"claude": (transcripts,)}
            first = full_corpus.fast_sync(db_path, self.root / "fresh-one.json")
            with source.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "type": "assistant", "sessionId": "active", "message": {"content": "second"},
                }) + "\n")
            second = full_corpus.fast_sync(db_path, self.root / "fresh-two.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(first["complete"])
        self.assertTrue(second["complete"])
        self.assertEqual(second["status_counts"], {"appended": 1})
        connection = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                [item["content"] for item in sessiondb.reconstruct(connection, "claude", "active")],
                ["first", "second"],
            )
        finally:
            connection.close()

    def test_fast_sync_defers_tail_appended_after_run_watermark(self):
        transcripts = self.root / "watermarked-transcripts"
        transcripts.mkdir()
        source = transcripts / "active.jsonl"
        first_row = {"type": "user", "sessionId": "watermark", "message": {"content": "first"}}
        second_row = {"type": "assistant", "sessionId": "watermark", "message": {"content": "second"}}
        source.write_text(json.dumps(first_row) + "\n", encoding="utf-8")
        db_path = self.root / "watermarked.sqlite"
        old_roots = full_corpus.ROOTS
        original_snapshot = sessiondb.stable_source_snapshot
        appended = False

        def append_after_boundary(path, *args, **kwargs):
            nonlocal appended
            if kwargs.get("watermark") is not None and not appended:
                with source.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(second_row) + "\n")
                appended = True
            return original_snapshot(path, *args, **kwargs)

        try:
            full_corpus.ROOTS = {"claude": (transcripts,)}
            with mock.patch.object(
                sessiondb, "stable_source_snapshot", side_effect=append_after_boundary,
            ):
                first = full_corpus.fast_sync(db_path, self.root / "watermark-one.json")
            second = full_corpus.fast_sync(db_path, self.root / "watermark-two.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertEqual(first["rounds"], 1)
        self.assertEqual(first["source_boundary"], "run-start-watermark")
        self.assertFalse(first["complete"])
        self.assertEqual(first["sources_drifted_after_sync"], [str(source.resolve())])
        self.assertTrue(second["complete"])
        connection = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                [item["content"] for item in sessiondb.reconstruct(connection, "claude", "watermark")],
                ["first", "second"],
            )
        finally:
            connection.close()

    def test_fast_sync_defers_rewrite_after_watermark_without_retry_or_failure(self):
        transcripts = self.root / "rewritten-watermark-transcripts"
        transcripts.mkdir()
        source = transcripts / "active.jsonl"
        first_row = {"type": "user", "sessionId": "rewrite-watermark", "message": {"content": "first"}}
        second_row = {"type": "user", "sessionId": "rewrite-watermark", "message": {"content": "other"}}
        source.write_text(json.dumps(first_row) + "\n", encoding="utf-8")
        db_path = self.root / "rewritten-watermark.sqlite"
        old_roots = full_corpus.ROOTS
        original_snapshot = sessiondb.stable_source_snapshot
        calls = 0

        def rewrite_after_boundary(path, *args, **kwargs):
            nonlocal calls
            watermark = kwargs.get("watermark")
            calls += 1
            source.write_text(json.dumps(second_row) + "\n", encoding="utf-8")
            os.utime(source, ns=(watermark.st_atime_ns, watermark.st_mtime_ns + 1_000_000))
            return original_snapshot(path, *args, **kwargs)

        try:
            full_corpus.ROOTS = {"claude": (transcripts,)}
            with mock.patch.object(
                sessiondb, "stable_source_snapshot", side_effect=rewrite_after_boundary,
            ):
                first = full_corpus.fast_sync(db_path, self.root / "rewrite-watermark-one.json")
            second = full_corpus.fast_sync(db_path, self.root / "rewrite-watermark-two.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertEqual(calls, 1)
        self.assertFalse(first["complete"])
        self.assertEqual(first["hard_failures"], [])
        self.assertEqual(first["missing_discovered_sources"], [str(source.resolve())])
        self.assertTrue(second["complete"])
        connection = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                sessiondb.reconstruct(connection, "claude", "rewrite-watermark")[0]["content"],
                "other",
            )
        finally:
            connection.close()

    def test_metadata_migration_matches_full_rebuild_without_source_reads(self):
        transcripts = self.root / "metadata-migration"
        transcripts.mkdir()
        source = transcripts / "events.jsonl"
        rows = [
            {"type": "user", "sessionId": "migration", "message": {"content": "kept"}},
            {"type": "atis-latch", "sessionId": "migration", "atis": ""},
        ]
        source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        fast_path = self.root / "metadata-fast.sqlite"
        full_path = self.root / "metadata-full.sqlite"
        for path in (fast_path, full_path):
            db = sessiondb.connect(path)
            sessiondb.ingest(db, "claude", source)
            db.execute("UPDATE sources SET adapter_version='claude-v4',issue_count=1")
            db.execute(
                "UPDATE events SET supported=0,issue='legacy unsupported metadata' "
                "WHERE category='atis-latch'"
            )
            db.commit()
            db.close()
        old_roots = full_corpus.ROOTS
        old_claude_version = sessiondb.ADAPTER_VERSION["claude"]
        transition = ("claude", "claude-v4", "claude-v5")
        plan = sessiondb.ADAPTER_MIGRATIONS[transition]
        try:
            sessiondb.ADAPTER_VERSION["claude"] = "claude-v5"
            full_corpus.ROOTS = {"claude": (transcripts,)}
            with mock.patch.object(
                sessiondb, "stable_source_snapshot",
                side_effect=AssertionError("metadata migration read a source transcript"),
            ):
                fast = full_corpus.fast_sync(fast_path, self.root / "metadata-fast.json")
            sessiondb.ADAPTER_MIGRATIONS.pop(transition)
            full = full_corpus.fast_sync(full_path, self.root / "metadata-full.json")
        finally:
            sessiondb.ADAPTER_MIGRATIONS[transition] = plan
            sessiondb.ADAPTER_VERSION["claude"] = old_claude_version
            full_corpus.ROOTS = old_roots
        self.assertTrue(fast["complete"])
        self.assertEqual(fast["migrations"][0]["mode"], "metadata-only")
        self.assertEqual(fast["migrations"][0]["migrated_events"], 1)
        self.assertEqual(full["migrations"][0]["mode"], "provider-wide")
        fast_db = sessiondb.connect(fast_path)
        full_db = sessiondb.connect(full_path)
        try:
            columns = (
                "category,role,tool,tool_event,raw_hash,content_hash,content_shape,"
                "supported,issue,line_number"
            )
            fast_events = [tuple(row) for row in fast_db.execute(
                f"SELECT {columns} FROM events ORDER BY line_number"
            )]
            full_events = [tuple(row) for row in full_db.execute(
                f"SELECT {columns} FROM events ORDER BY line_number"
            )]
            self.assertEqual(fast_events, full_events)
            self.assertEqual(
                sessiondb.reconstruct(fast_db, "claude", "migration", raw=True),
                sessiondb.reconstruct(full_db, "claude", "migration", raw=True),
            )
        finally:
            fast_db.close()
            full_db.close()

    def test_cost_state_metadata_migration_skips_transcript_reads(self):
        transcripts = self.root / "cost-state-migration"
        transcripts.mkdir()
        source = transcripts / "cost.jsonl"
        source.write_text("".join(json.dumps(row) + "\n" for row in (
            {"type": "user", "sessionId": "cost-migration", "message": {"content": "keep me"}},
            {
                "type": "cost-state", "sessionId": "cost-migration",
                "totalCostUSD": 0, "totalDuration": 1551, "modelUsage": {},
                "hasUnknownModelCost": False,
            },
            {
                "type": "still-unknown", "sessionId": "cost-migration",
                "totalCostUSD": 0, "totalDuration": 1551, "modelUsage": {},
                "hasUnknownModelCost": False,
            },
        )), encoding="utf-8")
        db_path = self.root / "cost-migration.sqlite"
        db = sessiondb.connect(db_path)
        sessiondb.ingest(db, "claude", source)
        db.execute("UPDATE sources SET adapter_version='claude-v6',issue_count=2")
        db.execute(
            "UPDATE events SET supported=0,issue='unknown Claude row type: cost-state' "
            "WHERE category='cost-state'"
        )
        db.commit()
        db.close()
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"claude": (transcripts,)}
            with mock.patch.object(
                sessiondb, "stable_source_snapshot",
                side_effect=AssertionError("metadata migration read a source transcript"),
            ):
                report = full_corpus.fast_sync(db_path, self.root / "cost-migration.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        self.assertEqual(report["migrations"][0]["mode"], "metadata-only")
        self.assertEqual(report["migrations"][0]["migrated_events"], 1)
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual([tuple(row) for row in db.execute(
                "SELECT category,supported,issue FROM events ORDER BY line_number"
            )], [
                ("user", 1, None),
                ("cost-state", 1, None),
                ("still-unknown", 0, "unknown Claude row type: still-unknown"),
            ])
            self.assertEqual(tuple(db.execute(
                "SELECT adapter_version,issue_count FROM sources"
            ).fetchone()), ("claude-v7", 1))
        finally:
            db.close()

    def test_invalid_metadata_migration_rolls_back_then_falls_back(self):
        transcripts = self.root / "migration-fallback"
        transcripts.mkdir()
        source = transcripts / "user.jsonl"
        source.write_text(json.dumps({
            "type": "user", "sessionId": "fallback", "message": {"content": "preserve me"},
        }) + "\n", encoding="utf-8")
        db_path = self.root / "fallback.sqlite"
        db = sessiondb.connect(db_path)
        sessiondb.ingest(db, "claude", source)
        db.execute("UPDATE sources SET adapter_version='claude-v4'")
        db.commit()
        source_id = db.execute("SELECT source_id FROM sources").fetchone()[0]
        before = tuple(db.execute(
            "SELECT adapter_version,issue_count FROM sources WHERE source_id=?", (source_id,),
        ).fetchone())
        invalid = sessiondb.AdapterMigration("metadata-only", frozenset({"user"}))
        with self.assertRaisesRegex(ValueError, "changed event semantics"):
            sessiondb.apply_metadata_migration(
                db, "claude", "claude-v4", "claude-v5", [source_id], invalid,
            )
        self.assertEqual(tuple(db.execute(
            "SELECT adapter_version,issue_count FROM sources WHERE source_id=?", (source_id,),
        ).fetchone()), before)
        db.close()
        old_roots = full_corpus.ROOTS
        old_claude_version = sessiondb.ADAPTER_VERSION["claude"]
        transition = ("claude", "claude-v4", "claude-v5")
        original = sessiondb.ADAPTER_MIGRATIONS[transition]
        try:
            sessiondb.ADAPTER_VERSION["claude"] = "claude-v5"
            full_corpus.ROOTS = {"claude": (transcripts,)}
            sessiondb.ADAPTER_MIGRATIONS[transition] = invalid
            report = full_corpus.fast_sync(db_path, self.root / "fallback.json")
        finally:
            sessiondb.ADAPTER_MIGRATIONS[transition] = original
            sessiondb.ADAPTER_VERSION["claude"] = old_claude_version
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        self.assertEqual(report["migrations"][0]["mode"], "provider-wide")
        self.assertIn("changed event semantics", report["migrations"][0]["fallback_reason"])

    def test_category_migration_rebuilds_only_affected_sources(self):
        transcripts = self.root / "category-migration"
        transcripts.mkdir()
        affected = transcripts / "affected.jsonl"
        unaffected = transcripts / "unaffected.jsonl"
        affected.write_text(json.dumps({
            "type": "artifact-comment-monitor", "v": 1,
            "sessionId": "affected",
            "artifacts": {"artifact-1": {
                "state": "armed", "writtenAtMs": 1,
                "title": "The Shaking Foundation",
            }},
        }) + "\n", encoding="utf-8")
        unaffected.write_text(json.dumps({
            "type": "user", "sessionId": "unaffected", "message": {"content": "unchanged"},
        }) + "\n", encoding="utf-8")
        db_path = self.root / "category.sqlite"
        db = sessiondb.connect(db_path)
        sessiondb.ingest(db, "claude", affected)
        sessiondb.ingest(db, "claude", unaffected)
        db.execute("UPDATE sources SET adapter_version='claude-v5'")
        db.commit()
        db.close()
        old_roots = full_corpus.ROOTS
        reads = []
        stable_snapshot = sessiondb.stable_source_snapshot

        def track_read(path, *args, **kwargs):
            reads.append(Path(path).name)
            return stable_snapshot(path, *args, **kwargs)

        try:
            full_corpus.ROOTS = {"claude": (transcripts,)}
            with mock.patch.object(sessiondb, "stable_source_snapshot", side_effect=track_read), \
                    mock.patch.dict(sessiondb.ADAPTER_VERSION, {"claude": "claude-v6"}):
                report = full_corpus.fast_sync(db_path, self.root / "category.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        self.assertEqual(reads, ["affected.jsonl"])
        self.assertEqual(report["migrations"][0]["mode"], "category-scoped")
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                {row[0] for row in db.execute("SELECT adapter_version FROM sources")},
                {"claude-v6"},
            )
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM events WHERE supported=0"
            ).fetchone()[0], 0)
        finally:
            db.close()

    def test_codex_web_search_migration_rebuilds_only_affected_sources(self):
        transcripts = self.root / "codex-category-migration"
        transcripts.mkdir()
        affected = transcripts / "affected.jsonl"
        unaffected = transcripts / "unaffected.jsonl"
        affected.write_text("".join(json.dumps(row) + "\n" for row in [
            {"type": "session_meta", "payload": {"id": "affected"}},
            {"type": "event_msg", "payload": {"type": "item_completed", "item": {
                "type": "WebSearch", "id": "web-1",
                "action": {"type": "search", "query": "migrated web search"},
                "query": "migrated web search",
                "results": [{"type": "web_result", "title": "migrated result"}],
            }}},
        ]), encoding="utf-8")
        unaffected.write_text("".join(json.dumps(row) + "\n" for row in [
            {"type": "session_meta", "payload": {"id": "unaffected"}},
            {"type": "event_msg", "payload": {"type": "item_completed", "item": {
                "type": "CommandExecution", "id": "cmd-1", "command": "pwd",
                "aggregated_output": "/tmp", "exit_code": 0,
            }}},
            {"type": "response_item", "payload": {
                "type": "message", "role": "user", "content": "control",
            }},
        ]), encoding="utf-8")
        db_path = self.root / "codex-category.sqlite"
        db = sessiondb.connect(db_path)
        sessiondb.ingest(db, "codex", affected)
        sessiondb.ingest(db, "codex", unaffected)
        db.execute("UPDATE sources SET adapter_version='codex-v8'")
        db.execute(
            "UPDATE events SET supported=0,issue='unknown Codex completion shape' "
            "WHERE category='item_completed' AND tool='web.search'"
        )
        db.commit()
        db.close()
        old_roots = full_corpus.ROOTS
        reads = []
        stable_snapshot = sessiondb.stable_source_snapshot

        def track_read(path, *args, **kwargs):
            reads.append(Path(path).name)
            return stable_snapshot(path, *args, **kwargs)

        try:
            full_corpus.ROOTS = {"codex": (transcripts,)}
            with mock.patch.object(sessiondb, "stable_source_snapshot", side_effect=track_read):
                report = full_corpus.fast_sync(db_path, self.root / "codex-category.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        self.assertEqual(reads, ["affected.jsonl"])
        self.assertEqual(report["migrations"][0]["mode"], "category-scoped")
        self.assertEqual(report["migrations"][0]["selection"], "unsupported-only")
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual(
                {row[0] for row in db.execute("SELECT adapter_version FROM sources")},
                {"codex-v10"},
            )
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM events WHERE supported=0"
            ).fetchone()[0], 0)
            self.assertEqual(
                sessiondb.reconstruct(db, "codex", "affected", raw=True)[1]
                ["content"]["payload"]["item"]["query"],
                "migrated web search",
            )
        finally:
            db.close()

    def test_provider_migration_resumes_idempotently_after_interruption(self):
        transcripts = self.root / "migration-resume"
        transcripts.mkdir()
        for index in range(3):
            (transcripts / f"{index}.jsonl").write_text(json.dumps({
                "type": "user", "sessionId": f"resume-{index}",
                "message": {"content": f"event {index}"},
            }) + "\n", encoding="utf-8")
        db_path = self.root / "resume.sqlite"
        db = sessiondb.connect(db_path)
        for source in sorted(transcripts.glob("*.jsonl")):
            sessiondb.ingest(db, "claude", source)
        db.execute("UPDATE sources SET adapter_version='claude-v4'")
        db.commit()
        db.close()
        old_roots = full_corpus.ROOTS
        old_claude_version = sessiondb.ADAPTER_VERSION["claude"]
        transition = ("claude", "claude-v4", "claude-v5")
        original = sessiondb.ADAPTER_MIGRATIONS.pop(transition)

        def interrupt(progress):
            if progress["state"] == "running" and progress["completed_sources"] == 1:
                raise RuntimeError("synthetic interruption")

        try:
            sessiondb.ADAPTER_VERSION["claude"] = "claude-v5"
            full_corpus.ROOTS = {"claude": (transcripts,)}
            with self.assertRaisesRegex(RuntimeError, "synthetic interruption"):
                full_corpus.fast_sync(
                    db_path, self.root / "resume-interrupted.json", progress_callback=interrupt,
                )
            db = sessiondb.connect(db_path)
            try:
                versions = [row[0] for row in db.execute(
                    "SELECT adapter_version FROM sources ORDER BY path"
                )]
                self.assertEqual(versions.count("claude-v5"), 1)
            finally:
                db.close()
            resumed = full_corpus.fast_sync(db_path, self.root / "resume-complete.json")
        finally:
            sessiondb.ADAPTER_MIGRATIONS[transition] = original
            sessiondb.ADAPTER_VERSION["claude"] = old_claude_version
            full_corpus.ROOTS = old_roots
        self.assertTrue(resumed["complete"])
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM sources").fetchone()[0], 3)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 3)
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM sources WHERE adapter_version='claude-v5'"
            ).fetchone()[0], 3)
        finally:
            db.close()


    def test_safe_metadata_is_admitted_without_source_reads_and_persists(self):
        source = self.write("auto-metadata.jsonl", [{
            "type": "workspace-marker", "sessionId": "auto-metadata",
            "timestamp": "2026-08-20T12:00:00Z", "cwd": "/tmp/worker",
            "sequence": 1, "active": True,
        }])
        first = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(first["issues"], 1)
        with sessiondb.forbid_source_transcript_reads() as telemetry:
            admission = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(telemetry["attempts"], 0)
        self.assertEqual((admission["admitted_groups"], admission["admitted_events"]), (1, 1))
        self.assertEqual(self.db.execute("SELECT supported FROM events").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT issue_count FROM sources").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM adapter_extensions").fetchone()[0], 1)
        columns = "category,role,tool,tool_event,raw_hash,content_hash,supported,issue,line_number"
        promoted = tuple(self.db.execute(f"SELECT {columns} FROM events").fetchone())
        promoted_raw = sessiondb.reconstruct(self.db, "claude", "auto-metadata", raw=True)
        self.db.execute("UPDATE sources SET adapter_version='claude-v3'")
        self.db.commit()
        rebuilt = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(rebuilt["issues"], 0)
        self.assertEqual(tuple(self.db.execute(f"SELECT {columns} FROM events").fetchone()), promoted)
        self.assertEqual(
            sessiondb.reconstruct(self.db, "claude", "auto-metadata", raw=True), promoted_raw,
        )

        with source.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "type": "workspace-marker", "sessionId": "auto-metadata",
                "timestamp": "2026-08-20T12:00:01Z", "cwd": "/tmp/other",
                "sequence": 2, "active": False,
            }) + "\n")
        self.db.close()
        self.db = sessiondb.connect(self.db_path)
        second = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(second["issues"], 0)
        self.assertEqual(
            [row[0] for row in self.db.execute("SELECT supported FROM events ORDER BY line_number")],
            [1, 1],
        )

    def test_passive_thread_goal_update_is_silently_admitted_and_exportable(self):
        row = {
            "type": "event_msg", "timestamp": "2026-08-24T00:00:00Z",
            "payload": {
                "type": "thread_goal_updated", "threadId": "thread-1",
                "goal": {
                    "threadId": "thread-1", "objective": "Keep SessionDB silently current",
                    "status": "active", "tokensUsed": 1200, "timeUsedSeconds": 42.5,
                    "createdAt": 1787529600, "updatedAt": 1787529642,
                },
            },
        }
        source = self.write("thread-goal.jsonl", [row])
        self.assertEqual(sessiondb.ingest(self.db, "codex", source)["issues"], 1)

        with sessiondb.forbid_source_transcript_reads() as telemetry:
            admission = sessiondb.apply_auto_metadata_admissions(self.db)

        self.assertEqual(telemetry["attempts"], 0)
        self.assertEqual((admission["admitted_groups"], admission["admitted_events"]), (1, 1))
        event = self.db.execute("SELECT * FROM events").fetchone()
        self.assertEqual((event["supported"], event["issue"]), (1, None))
        self.assertEqual(sessiondb.read_event_structure(self.db, event), row)
        args = argparse.Namespace(
            text="silently current", provider="codex", project=None, session=None,
            role=None, tool=None, lineage=None, since=None, until=None, raw=False,
            limit=10, offset=0, before=0, after=0, include_page=False,
            include_hashes=False,
        )
        self.assertEqual(len(sessiondb.result_window(self.db, args)[0]), 1)

        manifest = snapshot_export.export_snapshot(
            self.db_path, self.root / "thread-goal-snapshot",
            "2026-08-23T00:00:00Z", "2026-08-25T00:00:00Z",
            {"complete": True, "issues": [], "hard_failures": []},
        )
        self.assertEqual(manifest["coverage"], "complete")
        self.assertEqual(manifest["counts"]["unsupported_events"], 0)

    def test_unknown_identifier_metadata_field_is_auto_admitted(self):
        source = self.write("auto-identifier.jsonl", [{
            "type": "future-latch", "sessionId": "auto-identifier",
            "latch": "experiment-name",
        }])
        self.assertEqual(sessiondb.ingest(self.db, "claude", source)["issues"], 1)
        admission = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(admission["admitted_events"], 1)
        event = self.db.execute("SELECT * FROM events").fetchone()
        self.assertEqual((event["supported"], event["content_hash"]), (1, None))

    def test_admitted_category_revalidates_every_future_shape(self):
        source = self.write("auto-revalidate.jsonl", [{
            "type": "workspace-marker", "sessionId": "auto-revalidate", "sequence": 1,
        }])
        sessiondb.ingest(self.db, "claude", source)
        sessiondb.apply_auto_metadata_admissions(self.db)
        with source.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "type": "workspace-marker", "sessionId": "auto-revalidate",
                "message": {"content": "must remain unsupported"},
            }) + "\n")
        result = sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(result["issues"], 1)
        events = self.db.execute(
            "SELECT supported,issue FROM events ORDER BY line_number"
        ).fetchall()
        self.assertEqual([row["supported"] for row in events], [1, 0])
        self.assertIn("unknown Claude row type", events[1]["issue"])
        admission = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(admission["admitted_events"], 0)
        self.assertEqual(admission["rejected_groups"], 1)

    def test_mixed_safe_and_unsafe_category_is_rejected_as_a_group(self):
        source = self.write("auto-mixed.jsonl", [
            {"type": "mixed-marker", "sessionId": "mixed", "sequence": 1},
            {"type": "mixed-marker", "sessionId": "mixed", "content": "unsafe"},
        ])
        sessiondb.ingest(self.db, "claude", source)
        admission = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(admission["admitted_groups"], 0)
        self.assertEqual(admission["rejected_groups"], 1)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM events WHERE supported=0"
        ).fetchone()[0], 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM adapter_extensions").fetchone()[0], 0)

    def test_content_tool_secret_and_oversized_metadata_are_rejected(self):
        source = self.write("auto-denied.jsonl", [
            {"type": "content-marker", "sessionId": "denied", "message": "unsafe"},
            {"type": "tool-marker", "sessionId": "denied", "toolName": "exec"},
            {"type": "secret-marker", "sessionId": "denied", "apiKey": "hidden"},
            {"type": "large-marker", "sessionId": "denied", "label": "x" * 513},
            {"type": "ambiguous-marker", "sessionId": "denied", "note": "unclear prose"},
            {
                "type": "secret-value-marker", "sessionId": "denied",
                "latch": "ghp_12345678901234567890",
            },
        ])
        sessiondb.ingest(self.db, "claude", source)
        admission = sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(admission["admitted_events"], 0)
        self.assertEqual(admission["rejected_groups"], 6)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM adapter_extensions").fetchone()[0], 0)

    def test_auto_metadata_admission_rolls_back_on_registry_failure(self):
        source = self.write("auto-rollback.jsonl", [{
            "type": "rollback-marker", "sessionId": "rollback", "sequence": 1,
        }])
        sessiondb.ingest(self.db, "claude", source)
        self.db.executescript("""
            CREATE TRIGGER fail_auto_admission BEFORE INSERT ON adapter_extensions
            BEGIN SELECT RAISE(ABORT, 'synthetic registry failure'); END;
        """)
        self.db.commit()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "synthetic registry failure"):
            sessiondb.apply_auto_metadata_admissions(self.db)
        self.assertEqual(self.db.execute("SELECT supported FROM events").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT issue_count FROM sources").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM adapter_extensions").fetchone()[0], 0)

    def test_fast_sync_auto_admits_claude_and_codex_metadata(self):
        claude_root = self.root / "auto-claude"
        codex_root = self.root / "auto-codex"
        claude_root.mkdir()
        codex_root.mkdir()
        (claude_root / "event.jsonl").write_text(json.dumps({
            "type": "future-marker", "sessionId": "claude-auto", "phase": "start",
        }) + "\n", encoding="utf-8")
        (codex_root / "event.jsonl").write_text(json.dumps({
            "type": "event_msg", "payload": {
                "type": "future_marker", "phase": "start", "sequence": 1,
            },
        }) + "\n", encoding="utf-8")
        db_path = self.root / "auto-fast.sqlite"
        old_roots = full_corpus.ROOTS
        try:
            full_corpus.ROOTS = {"claude": (claude_root,), "codex": (codex_root,)}
            report = full_corpus.fast_sync(db_path, self.root / "auto-fast.json")
        finally:
            full_corpus.ROOTS = old_roots
        self.assertTrue(report["complete"])
        self.assertEqual(report["issues"], [])
        self.assertEqual(report["auto_adapter_updates"]["admitted_groups"], 2)
        self.assertEqual(report["auto_adapter_updates"]["admitted_events"], 2)
        db = sessiondb.connect(db_path)
        try:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM events WHERE supported=1"
            ).fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM adapter_extensions").fetchone()[0], 2)
        finally:
            db.close()

    def test_manifest_cycle_fails_cleanly(self):
        object_hash = "f" * 64
        sessiondb.put_packed_object(
            self.db, sessiondb.stored_json({sessiondb.REF_KEY: object_hash}),
            "manifest", object_hash=object_hash,
        )
        self.db.commit()
        with self.assertRaisesRegex(ValueError, "cycle"):
            sessiondb.read_content_object(self.db, object_hash)

    def test_complete_search_index_modes_stable_ids_and_truthful_passages(self):
        claude = self.write("index-claude.jsonl", [
            {"type": "user", "sessionId": "index-c", "timestamp": "2026-09-15T01:00:00Z",
             "message": {"role": "user", "content": "Straße alpha beta"}},
        ])
        codex = self.write("index-codex.jsonl", [
            {"type": "session_meta", "timestamp": "2026-09-15T01:00:01Z", "payload": {"id": "index-x", "cwd": "/project"}},
            {"type": "event_msg", "timestamp": "2026-09-15T01:00:02Z",
             "payload": {"type": "agent_message", "message": "beta then alpha"}},
        ])
        sessiondb.ingest(self.db, "claude", claude)
        sessiondb.ingest(self.db, "codex", codex)
        base = dict(provider=None, project=None, session=None, role=None, tool=None,
                    lineage=None, since=None, until=None)
        literal = sessiondb.search_index(self.db, argparse.Namespace(
            text="straße alpha", mode="literal", **base,
        ))
        words = sessiondb.search_index(self.db, argparse.Namespace(
            text="alpha beta", mode="all_words", **base,
        ))
        self.assertEqual(literal["total"], 1)
        self.assertIn("Straße alpha", literal["entries"][0]["passage"])
        self.assertEqual(words["total"], 2)
        self.assertEqual(len({item["result_id"] for item in words["entries"]}), 2)
        codex_passage = next(item["passage"] for item in words["entries"] if item["provider"] == "codex")
        self.assertIn("beta then alpha", codex_passage)
        self.assertNotIn("alpha beta", codex_passage)
        self.assertEqual(words["telemetry"]["event_reconstructions"], 0)
        self.assertEqual(words["telemetry"]["source_reads"], 0)

    def test_answer_useful_previews_titles_grouping_and_follow_up(self):
        queue_id = "queue-item-" + "7" * 24
        filename = "Báo giá khách hàng tháng chín.md"
        claude_rows = [
            {"type": "ai-title", "sessionId": "answer-c", "timestamp": "2026-09-01T00:00:00Z",
             "aiTitle": "Old queue discussion"},
            {"type": "custom-title", "sessionId": "answer-c", "timestamp": "2026-09-02T00:00:00Z",
             "customTitle": "Current queue discussion"},
            {"type": "queue-operation", "sessionId": "answer-c", "timestamp": "2026-09-02T00:01:00Z",
             "content": "queue lookup " + ("context " * 67) + queue_id + " final status"},
            {"type": "user", "sessionId": "repeat-c", "message": {"content": "repeat marker exact note"}},
            {"type": "user", "sessionId": "repeat-c", "message": {"content": "repeat marker exact note"}},
            {"type": "user", "sessionId": "untitled-c", "message": {"content": "untitled marker"}},
            {"type": "user", "sessionId": "secret-c", "message": {"content": "secret marker api_key=topsecret"}},
        ]
        codex_rows = [
            {"type": "session_meta", "timestamp": "2026-09-01T00:00:00Z",
             "payload": {"id": "answer-x"}},
            {"type": "event_msg", "timestamp": "2026-09-02T00:01:00Z",
             "payload": {"type": "agent_message", "message": "proposal lookup " +
                         ("details " * 22) + filename + " at 12% rate " + ("more " * 250)}},
            {"type": "session_meta", "payload": {"id": "untitled-x"}},
            {"type": "event_msg", "payload": {"type": "user_message", "message": "untitled marker"}},
        ]
        # Codex keeps thread titles only in its session index; a rename appends a newer row.
        index_rows = [
            {"id": "answer-x", "thread_name": "Old proposal", "updated_at": "2026-09-01T00:00:00Z"},
            {"id": "answer-x", "thread_name": "Current proposal", "updated_at": "2026-09-02T00:00:00Z"},
        ]
        claude_path = self.write("answer-claude.jsonl", claude_rows)
        codex_path = self.write("answer-codex.jsonl", codex_rows)
        index_path = self.write(sessiondb.CODEX_SESSION_INDEX_NAME, index_rows)
        sessiondb.ingest(self.db, "claude", claude_path)
        sessiondb.ingest(self.db, "codex", codex_path)
        sessiondb.ingest(self.db, "codex", index_path)

        def delivered(term):
            result = sessiondb.search_index(self.db, self._search_args(text=term, mode="literal"))
            return sessiondb.bundle_if_oversized(self.db_path, result, None)

        queue = delivered("queue lookup")
        self.assertEqual(queue["total"], 1)
        self.assertIn(queue_id, queue["entries"][0]["passage"])
        self.assertIn("final status", queue["entries"][0]["passage"])
        title = queue["sessions"][queue["entries"][0]["session"]]["title"]
        self.assertEqual(title["value"], "Current queue discussion")
        self.assertNotIn("source", title)
        proposal = delivered("proposal lookup")
        hit = proposal["entries"][0]
        self.assertIn(filename, hit["passage"])
        self.assertIn("12% rate", hit["passage"])
        codex_title = proposal["sessions"][hit["session"]]["title"]
        self.assertEqual(codex_title["value"], "Current proposal")
        self.assertNotIn("source", codex_title)
        bundle_header, bundle_entries = sessiondb.read_result_bundle(Path(proposal["bundle"]["path"]))
        bundled = list(bundle_entries)
        self.assertIn("telemetry", bundle_header)
        self.assertIn("rank", bundled[0])
        self.assertEqual(
            bundle_header["sessions"][hit["session"]]["title"]["source"]["line"], 2,
        )
        self.assertLessEqual(len(hit["passage"]), 900)
        accented = delivered("Báo giá khách hàng")
        self.assertEqual(accented["total"], 1)
        self.assertIn(filename, accented["entries"][0]["passage"])
        absent = delivered("unrelated missing phrase")
        self.assertEqual(absent["total"], 0)
        self.assertEqual(absent["entries"], [])
        follow = proposal["follow_up"]["details"]
        self.assertEqual(set(follow), {"command", "snapshot_id", "bundle", "db", "readable", "readable_query"})
        self.assertTrue(follow["readable"])
        detail = sessiondb.details_entrypoint(self.db_path, argparse.Namespace(
            snapshot_id=follow["snapshot_id"], result_ids=[hit["id"]],
            bundle=Path(follow["bundle"]), before=0, after=0, readable=True, manifest=None,
        ))
        self.assertTrue(detail["complete"])
        self.assertIn(filename, detail["events"][0]["text"])
        self.assertNotIn("structure", detail["events"][0])
        self.assertEqual(detail["events"][0]["source"]["path"], str(codex_path.resolve()))
        self.assertEqual(detail["events"][0]["source"]["line"], 2)
        exact = sessiondb.details_entrypoint(self.db_path, argparse.Namespace(
            snapshot_id=follow["snapshot_id"], result_ids=[hit["id"]],
            bundle=Path(follow["bundle"]), before=0, after=0, readable=False, manifest=None,
        ))
        self.assertIn("payload", exact["events"][0]["structure"])
        stale = sessiondb.detail_events(self.db, argparse.Namespace(
            snapshot_id="old-snapshot", result_ids=["missing-result"], before=0, after=0,
        ))
        self.assertFalse(stale["complete"])
        self.assertTrue(stale["snapshot_advanced"])
        self.assertEqual(stale["results"][0]["status"], "missing")

        repeat = delivered("repeat marker")
        self.assertEqual(repeat["total"], 2)
        self.assertFalse(repeat["has_more"])
        self.assertEqual(len(repeat["entries"]), 1)
        group = repeat["entries"][0]
        self.assertEqual(group["occurrence_count"], 2)
        self.assertEqual({item["id"] for item in group["occurrences"]}, {"r1", "r2"})
        self.assertNotIn("follow_up", group)
        self.assertTrue(all("follow_up" not in item for item in group["occurrences"]))
        page = sessiondb.result_bundle_page(Path(repeat["bundle"]["path"]), argparse.Namespace(
            offset=1, limit=1, provider=None, project=None, session=None, role=None, tool=None,
        ), self.db_path)
        self.assertEqual(page["total"], 2)
        self.assertEqual(len(page["entries"]), 1)
        unknown = delivered("untitled marker")
        self.assertEqual(unknown["total"], 2)
        self.assertTrue(all(value["title"]["status"] == "unknown"
                            for value in unknown["sessions"].values()))
        secret = delivered("secret marker")
        self.assertNotIn("topsecret", json.dumps(secret))
        self.assertIn("[REDACTED]", secret["entries"][0]["passage"])
        secret_follow = secret["follow_up"]["details"]
        secret_detail = sessiondb.details_entrypoint(self.db_path, argparse.Namespace(
            snapshot_id=secret_follow["snapshot_id"], result_ids=[secret["entries"][0]["id"]],
            bundle=Path(secret_follow["bundle"]), before=0, after=0,
            readable=True, manifest=None,
        ))
        self.assertNotIn("topsecret", json.dumps(secret_detail))
        self.assertIn("[REDACTED]", secret_detail["events"][0]["text"])

        # The same frozen records supply CLI byte, time, and follow-up counts.
        metrics = {}
        for label, term, answer, expected_title in (
            ("queue", "queue lookup", queue_id, "Current queue discussion"),
            ("proposal", "proposal lookup", filename, "Current proposal"),
        ):
            started = time.monotonic()
            command = [sys.executable, str(Path(sessiondb.__file__)), "--db", str(self.db_path),
                       "search-index", "--text", term, "--mode", "literal"]
            process = subprocess.run(command, capture_output=True, text=True, check=True)
            elapsed = time.monotonic() - started
            response = json.loads(process.stdout)
            self.assertEqual(response["indexed_total"], 1)
            self.assertIsNone(response["total"])
            self.assertFalse(response["complete"])
            self.assertIn(answer, response["entries"][0]["passage"])
            self.assertEqual(response["follow_up"]["details"]["command"], "details")
            self.assertEqual(next(iter(response["sessions"].values()))["title"]["value"],
                             expected_title)
            if label == "proposal":
                self.assertIn("12% rate", response["entries"][0]["passage"])
            metrics[label] = {
                "response_bytes": len(process.stdout.encode()),
                "cli_seconds": round(elapsed, 4), "follow_ups_to_answer": 0,
            }
        print("answer_usefulness_metrics", json.dumps(metrics))

    def test_stage3_fills_ten_distinct_passages_from_one_ranked_prefix(self):
        rows = [
            {"type": "user", "sessionId": f"duplicate-{index}",
             "message": {"role": "user", "content": "stage3needle shared passage"}}
            for index in range(3)
        ] + [
            {"type": "user", "sessionId": f"distinct-{index}",
             "message": {"role": "user", "content": f"stage3needle distinct passage {index}"}}
            for index in range(12)
        ]
        sessiondb.ingest(self.db, "claude", self.write("stage3-fill.jsonl", rows))
        result = sessiondb.search_index(
            self.db, self._search_args(text="stage3needle", mode="literal"),
        )
        delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)

        self.assertEqual(len(delivered["entries"]), 10)
        self.assertEqual(delivered["entries"][0]["occurrence_count"], 3)
        self.assertEqual(delivered["shown_through"], 12)
        self.assertEqual(delivered["display"]["distinct_passages"], 10)
        self.assertEqual(delivered["display"]["stop_reason"], "distinct_target")
        self.assertTrue(delivered["has_more"])
        self.assertEqual(delivered["follow_up"]["bundle_page"]["offset"], 12)
        self.assertEqual(delivered["follow_up"]["bundle_page"]["db"], str(self.db_path))
        visible_ids = [
            occurrence["id"] if "occurrences" in entry else entry["id"]
            for entry in delivered["entries"]
            for occurrence in (entry.get("occurrences") or [entry])
        ]
        self.assertEqual(visible_ids, [f"r{index}" for index in range(1, 13)])

    def test_stage3_byte_budget_stops_before_entry_and_bundle_keeps_all(self):
        entries = []
        for index in range(10):
            entries.append({
                "result_id": f"result-{index}", "provider": "claude",
                "session": f"session-{index}", "timestamp": None,
                "category": "user", "role": "user", "tool": None, "project": None,
                "source": {"path": f"/source/{index}", "line": index + 1, "hash": str(index)},
                "passage": "\x1b" * (sessiondb.PASSAGE_FULL_CHARS - 1) + str(index),
                "match_origin": "native", "rank": {"document_bytes": 1200},
            })
        result = {
            "protocol_version": sessiondb.RETRIEVAL_PROTOCOL_VERSION,
            "complete": True, "backend": "database", "snapshot_id": "snapshot",
            "projection_version": sessiondb.PROJECTION_VERSION,
            "mode": "literal", "order": "relevance", "query": "needle",
            "total": len(entries), "entries": entries,
            "telemetry": {"diagnostic": True},
        }
        delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)

        self.assertLessEqual(sessiondb._encoded_size(delivered), sessiondb.QUERY_INLINE_MAX_BYTES)
        emitted = json.dumps(delivered, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        self.assertLessEqual(len(emitted.encode()), sessiondb.QUERY_INLINE_MAX_BYTES)
        self.assertEqual(delivered["display"]["stop_reason"], "byte_budget")
        self.assertLess(delivered["shown_through"], len(entries))
        header, bundled_entries = sessiondb.read_result_bundle(Path(delivered["bundle"]["path"]))
        self.assertEqual(len(list(bundled_entries)), len(entries))
        self.assertEqual(header["telemetry"], {"diagnostic": True})

    def test_stage3_same_text_different_origins_remain_distinct(self):
        base = {
            "provider": "claude", "session": "origin", "timestamp": None,
            "category": "user", "role": "user", "tool": None, "project": None,
            "source": {"path": "/source", "line": 1, "hash": "hash"},
            "passage": "same displayed passage", "rank": {},
        }
        entries = [
            {**base, "result_id": "native-result", "match_origin": "native"},
            {**base, "result_id": "copied-result", "match_origin": "copied"},
        ]
        result = {
            "protocol_version": sessiondb.RETRIEVAL_PROTOCOL_VERSION,
            "complete": True, "backend": "database", "snapshot_id": "snapshot",
            "projection_version": sessiondb.PROJECTION_VERSION,
            "mode": "literal", "order": "relevance", "query": "same",
            "total": 2, "entries": entries, "telemetry": {},
        }
        delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)
        self.assertEqual(len(delivered["entries"]), 2)
        self.assertEqual(
            [entry["match_origin"] for entry in delivered["entries"]], ["native", "copied"],
        )

    def test_stage3_prehydrated_duplicates_stop_at_scan_cap(self):
        base = {
            "provider": "claude", "timestamp": None, "category": "user",
            "role": "user", "tool": None, "project": None,
            "passage": "same fallback passage", "match_origin": "native", "rank": {},
        }
        entries = [
            {
                **base, "result_id": f"fallback-{index}", "session": f"session-{index}",
                "source": {"path": f"/source/{index}", "line": 1, "hash": str(index)},
            }
            for index in range(60)
        ]
        result = {
            "protocol_version": sessiondb.RETRIEVAL_PROTOCOL_VERSION,
            "complete": True, "backend": "raw", "snapshot_id": "snapshot",
            "projection_version": sessiondb.PROJECTION_VERSION,
            "mode": "literal", "order": "relevance", "query": "fallback",
            "total": len(entries), "entries": entries, "telemetry": {},
        }
        delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)
        self.assertEqual(delivered["shown_through"], sessiondb.INLINE_PASSAGE_SCAN_LIMIT)
        self.assertEqual(delivered["entries"][0]["occurrence_count"], 50)
        self.assertEqual(delivered["display"]["stop_reason"], "scan_limit")
        self.assertTrue(delivered["has_more"])

    def test_search_index_requires_current_projection_and_explicit_build_repairs_it(self):
        source = self.write("stale-index.jsonl", [
            {"type": "user", "sessionId": "stale-index", "message": {"role": "user", "content": "needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.db.execute("DELETE FROM positional_projection_fts")
        self.db.commit()
        args = argparse.Namespace(
            text="needle", mode="literal", provider=None, project=None, session=None,
            role=None, tool=None, lineage=None, since=None, until=None,
        )
        self.assertFalse(sessiondb.projection_status(self.db)["complete"])
        rebuilt = sessiondb.build_search_projections(self.db)
        self.assertTrue(rebuilt["complete"])
        self.assertEqual(rebuilt["source_reads"], 0)
        self.assertEqual(sessiondb.search_index(self.db, args)["total"], 1)

    def test_search_index_rejects_removed_assisted_strategy(self):
        command = [
            sys.executable, str(Path(sessiondb.__file__)), "--db", str(self.db_path),
            "search-index", "--text", "needle", "--strategy", "assisted",
        ]
        process = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(process.returncode, 2)
        self.assertIn("unrecognized arguments: --strategy assisted", process.stderr)

    def test_old_projection_shape_is_retired_without_blocking_ingestion(self):
        database = self.root / "old-projection.sqlite"
        first = self.write("old-projection-first.jsonl", [{
            "type": "user", "sessionId": "old-projection",
            "message": {"role": "user", "content": "first needle"},
        }])
        db = sessiondb.connect(database)
        sessiondb.ingest(db, "claude", first)
        db.execute("DROP TABLE positional_projection_fts")
        db.execute(
            "CREATE VIRTUAL TABLE positional_projection_fts USING fts5("
            "projection,content='',contentless_delete=1,detail=full)"
        )
        db.execute("INSERT INTO positional_projection_fts(rowid,projection) VALUES(1,'first')")
        db.commit()
        db.close()

        migrated = sessiondb.connect(database)
        try:
            columns = {
                row[1] for row in migrated.execute("PRAGMA table_info(positional_projection_fts)")
            }
            self.assertTrue(set(sessiondb.PROJECTION_ORIGINS) <= columns)
            second = self.write("old-projection-second.jsonl", [{
                "type": "user", "sessionId": "old-projection-2",
                "message": {"role": "user", "content": "second needle"},
            }])
            sessiondb.ingest(migrated, "claude", second)
            self.assertEqual(migrated.execute("SELECT COUNT(*) FROM events").fetchone()[0], 2)
            self.assertFalse(sessiondb.projection_status(migrated)["complete"])
            self.assertTrue(sessiondb.build_search_projections(migrated)["complete"])
        finally:
            migrated.close()

    def _search_args(self, **overrides):
        values = dict(
            text="needle", mode="literal", provider=None, project=None, session=None,
            role=None, tool=None, lineage=None, since=None, until=None,
        )
        values.update(overrides)
        return argparse.Namespace(**values)











    def test_unprojected_events_outside_the_query_do_not_disable_search(self):
        for name in ("scoped-a", "scoped-b"):
            sessiondb.ingest(self.db, "claude", self.write(f"{name}.jsonl", [
                {"type": "user", "sessionId": name, "message": {"role": "user", "content": "needle"}},
            ]))
        rowid = self.db.execute("SELECT rowid FROM events WHERE session_id='scoped-b'").fetchone()[0]
        for table, column in (
            ("positional_projection_fts", "rowid"), ("search_projection_refs", "event_rowid"),
        ):
            self.db.execute(f"DELETE FROM {table} WHERE {column}=?", (rowid,))
        self.db.commit()
        self.assertFalse(sessiondb.projection_status(self.db)["complete"])

        scoped = sessiondb.search_index(self.db, self._search_args(session="scoped-a"))
        self.assertTrue(scoped["complete"])
        self.assertEqual(scoped["total"], 1)

        self.assertFalse(sessiondb.projection_status(self.db)["complete"])

    def test_projection_integrity_failures_stay_corpus_wide(self):
        sessiondb.ingest(self.db, "claude", self.write("integrity.jsonl", [
            {"type": "user", "sessionId": "integrity", "message": {"role": "user", "content": "needle"}},
        ]))
        self.db.execute("UPDATE metadata SET value='old-projection' WHERE key='projection_version'")
        self.db.commit()
        result = sessiondb.search_index(self.db, self._search_args(session="integrity"))
        self.assertFalse(result["complete"])
        self.assertEqual(result["coverage"]["reason"], "projection_missing_or_stale")

    def test_unsupported_events_count_only_when_they_could_match_the_query(self):
        for name in ("issue-a", "issue-b"):
            sessiondb.ingest(self.db, "claude", self.write(f"{name}.jsonl", [
                {"type": "user", "sessionId": name, "timestamp": "2026-09-10T01:00:00Z",
                 "message": {"role": "user", "content": "needle"}},
            ]))
        self.db.execute("UPDATE events SET supported=0,issue='test' WHERE session_id='issue-b'")
        self.db.execute(
            "UPDATE sources SET issue_count=1 WHERE source_id IN "
            "(SELECT source_id FROM events WHERE session_id='issue-b')"
        )
        self.db.commit()
        self.assertEqual(
            sessiondb.unsupported_events_in_scope(self.db, self._search_args(session="issue-a")), (0, 0),
        )
        self.assertEqual(sessiondb.unsupported_events_in_scope(self.db, self._search_args()), (1, 1))
        self.assertEqual(
            sessiondb.unsupported_events_in_scope(self.db, self._search_args(until="2026-09-01")), (0, 0),
        )
        # Role and tool filters are never applied to unsupported events.
        self.assertEqual(
            sessiondb.unsupported_events_in_scope(self.db, self._search_args(role="assistant")), (1, 1),
        )
        # A missing dimension on an unsupported event keeps it in scope.
        self.db.execute("UPDATE events SET order_timestamp=NULL WHERE session_id='issue-b'")
        self.db.commit()
        self.assertEqual(
            sessiondb.unsupported_events_in_scope(self.db, self._search_args(until="2026-09-01")), (1, 1),
        )


    def test_batch_details_preserve_structure_context_and_snapshot_identity(self):
        source = self.write("details.jsonl", [
            {"type": "user", "sessionId": "details", "message": {"role": "user", "content": "before"}},
            {"type": "assistant", "sessionId": "details", "message": {"role": "assistant", "content": "selected needle"}},
            {"type": "user", "sessionId": "details", "message": {"role": "user", "content": "after"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        search = sessiondb.search_index(self.db, argparse.Namespace(
            text="needle", mode="literal", provider=None, project=None, session=None,
            role=None, tool=None, lineage=None, since=None, until=None,
        ))
        result_id = search["entries"][0]["result_id"]
        details = sessiondb.detail_events(self.db, argparse.Namespace(
            snapshot_id=search["snapshot_id"], result_ids=[result_id, result_id, "missing"],
            before=1, after=1,
        ))
        self.assertFalse(details["complete"])
        self.assertEqual([item["status"] for item in details["results"]], ["ok", "missing"])
        self.assertEqual(len(details["events"]), 3)
        selected = next(item for item in details["events"] if item["selected"])
        self.assertEqual(selected["structure"]["message"]["content"], "selected needle")
        self.assertEqual(details["telemetry"]["source_reads"], 0)
        stale = sessiondb.detail_events(self.db, argparse.Namespace(
            snapshot_id="wrong", result_ids=[result_id], before=0, after=0,
        ))
        self.assertTrue(stale["complete"])
        self.assertTrue(stale["snapshot_advanced"])

    def test_bound_continuations_reject_cross_case_and_nonmember_details(self):
        source = self.write("bound-details.jsonl", [{
            "type": "user", "sessionId": "bound-details",
            "message": {"role": "user", "content": "continuation needle " + "word " * 10000},
        }])
        sessiondb.ingest(self.db, "claude", source)
        search_args = self._search_args(
            text="continuation needle", case_id="case-alpha", provider="claude",
        )
        search = sessiondb.search_index(self.db, search_args)
        sessiondb.ensure_retrieval_origin(search, search_args)
        delivered = sessiondb.bundle_if_oversized(self.db_path, search, None)
        origin = delivered["origin"]
        self.assertEqual(origin["binding_status"], "bound")
        self.assertEqual(origin["case_id"], "case-alpha")
        self.assertEqual(origin["filters"], {"provider": "claude"})
        self.assertEqual(delivered["telemetry"]["command"], "search-index")
        self.assertIn("coverage", delivered["telemetry"]["phases_seconds"])
        self.assertNotIn("continuation needle", json.dumps(delivered["telemetry"]))
        self.assertNotIn(str(source), json.dumps(delivered["telemetry"]))

        follow = delivered["follow_up"]["details"]
        common = dict(
            snapshot_id=follow["snapshot_id"], result_ids=[delivered["entries"][0]["id"]],
            bundle=Path(follow["bundle"]), before=0, after=0, readable=True,
            readable_query=follow["readable_query"], readable_offset=None, manifest=None,
            case_id=follow["case_id"], continuation_id=follow["continuation_id"],
            previous_detail_fingerprint=None,
        )
        detail = sessiondb.details_entrypoint(self.db_path, argparse.Namespace(**common))
        self.assertTrue(detail["complete"])
        self.assertEqual(detail["origin"], origin)
        self.assertEqual(detail["telemetry"]["command"], "details")
        self.assertIn("bundle_read", detail["telemetry"]["phases_seconds"])
        fingerprint = detail["continuation"]["detail_fingerprint"]
        repeated = sessiondb.details_entrypoint(
            self.db_path,
            argparse.Namespace(**{**common, "previous_detail_fingerprint": fingerprint}),
        )
        self.assertTrue(repeated["continuation"]["unchanged_from_previous"])
        self.assertEqual(repeated["continuation"]["retry_status"], "unchanged")
        next_offset = next(iter(detail["continuation"]["next_readable_offsets"].values()))
        advanced = sessiondb.details_entrypoint(
            self.db_path,
            argparse.Namespace(**{
                **common, "readable_offset": next_offset,
                "previous_detail_fingerprint": fingerprint,
            }),
        )
        self.assertFalse(advanced["continuation"]["unchanged_from_previous"])
        self.assertEqual(advanced["continuation"]["retry_status"], "changed")

        wrong_case = sessiondb.details_entrypoint(
            self.db_path, argparse.Namespace(**{**common, "case_id": "case-beta"}),
        )
        self.assertFalse(wrong_case["complete"])
        self.assertEqual(wrong_case["reason"], "continuation_case_mismatch")
        missing_context = sessiondb.details_entrypoint(
            self.db_path,
            argparse.Namespace(**{**common, "case_id": None, "continuation_id": None}),
        )
        self.assertEqual(missing_context["reason"], "continuation_context_required")
        with (
            mock.patch.object(sessiondb, "connect", side_effect=AssertionError("database accessed")),
            mock.patch.object(
                sessiondb, "raw_manifest_details", side_effect=AssertionError("fallback accessed"),
            ),
        ):
            no_bundle = sessiondb.details_entrypoint(
                self.db_path,
                argparse.Namespace(**{
                    **common, "bundle": None, "manifest": self.root / "private-manifest.json",
                }),
            )
        self.assertEqual(no_bundle["reason"], "continuation_bundle_required")

        legacy_bundle = self.root / "legacy-result.jsonl"
        sessiondb.write_result_bundle(
            {key: value for key, value in search.items() if key != "origin"}, legacy_bundle,
        )
        with mock.patch.object(
            sessiondb, "connect", side_effect=AssertionError("database accessed"),
        ):
            origin_missing = sessiondb.details_entrypoint(
                self.db_path,
                argparse.Namespace(**{**common, "bundle": legacy_bundle}),
            )
        self.assertEqual(origin_missing["reason"], "continuation_origin_missing")

        other = self.write("outside-details.jsonl", [{
            "type": "user", "sessionId": "outside-details",
            "message": {"role": "user", "content": "outside evidence"},
        }])
        sessiondb.ingest(self.db, "claude", other)
        outside = sessiondb.search_index(self.db, self._search_args(text="outside evidence"))
        bypass = sessiondb.details_entrypoint(
            self.db_path,
            argparse.Namespace(**{
                **common, "result_ids": [outside["entries"][0]["result_id"]],
            }),
        )
        self.assertEqual(bypass["reason"], "continuation_result_not_in_bundle")

        page_args = argparse.Namespace(
            offset=0, limit=1, provider=None, project=None, session=None, role=None, tool=None,
            snapshot_id=origin["snapshot_id"], case_id="case-beta",
            continuation_id=origin["continuation_id"],
        )
        page = sessiondb.result_bundle_page(Path(follow["bundle"]), page_args, self.db_path)
        self.assertEqual(page["reason"], "continuation_case_mismatch")
        self.assertEqual(page["telemetry"]["command"], "bundle-page")

    def test_retrieval_timing_preserves_query_contract_and_excludes_error_text(self):
        source = self.write("query-contract.jsonl", [{
            "type": "user", "sessionId": "query-contract",
            "message": {"role": "user", "content": "query contract needle"},
        }])
        sessiondb.ingest(self.db, "claude", source)
        script = str(Path(sessiondb.__file__))
        base = [sys.executable, script, "--db", str(self.db_path), "query", "--text", "needle"]
        ordinary = subprocess.run(base, capture_output=True, text=True, check=True)
        self.assertIsInstance(json.loads(ordinary.stdout), list)
        clean = subprocess.run([*base, "--clean"], capture_output=True, text=True, check=True)
        clean_result = json.loads(clean.stdout)
        self.assertEqual(clean_result["telemetry"]["command"], "query")
        self.assertIn("connection_snapshot", clean_result["telemetry"]["phases_seconds"])

        private_path = self.root / "private" / "missing-bundle.jsonl"
        bundle_error = sessiondb.details_entrypoint(self.db_path, argparse.Namespace(
            snapshot_id="snapshot", result_ids=["r1"], bundle=private_path,
            before=0, after=0, readable=False, manifest=None,
        ))
        self.assertIn(str(private_path), bundle_error["reason"])
        self.assertEqual(bundle_error["telemetry"]["outcome"], "error")
        self.assertNotIn(str(private_path), json.dumps(bundle_error["telemetry"]))

        with mock.patch.object(sessiondb, "connect", side_effect=OSError("/private/secret-db")):
            database_error = sessiondb.details_entrypoint(self.db_path, argparse.Namespace(
                snapshot_id="snapshot", result_ids=["missing"], bundle=None,
                before=0, after=0, readable=False, manifest=None,
            ))
        self.assertEqual(database_error["telemetry"]["outcome"], "error")
        self.assertNotIn("/private/secret-db", json.dumps(database_error["telemetry"]))

        with mock.patch.object(sessiondb.time, "monotonic", return_value=13.0):
            timed = sessiondb.attach_command_timing(
                {"complete": False}, "details", 10.0, {"database_read": 2.0},
            )
        self.assertEqual(timed["telemetry"]["total_seconds"], 3.0)
        self.assertEqual(timed["telemetry"]["phases_seconds"], {"database_read": 2.0})
        self.assertEqual(timed["telemetry"]["unattributed_seconds"], 1.0)

    def test_private_result_bundle_is_pageable_and_not_overwritten(self):
        source = self.write("bundle.jsonl", [
            {"type": "user", "sessionId": "bundle", "message": {"role": "user", "content": "bundle needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        result = sessiondb.search_index(self.db, argparse.Namespace(
            text="needle", mode="literal", provider=None, project=None, session=None,
            role=None, tool=None, lineage=None, since=None, until=None,
        ))
        bundle = self.root / "result.jsonl"
        manifest = sessiondb.write_result_bundle(result, bundle)
        self.assertEqual(bundle.stat().st_mode & 0o777, 0o600)
        self.assertEqual(manifest["bundle"]["entries"], 1)
        page = sessiondb.result_bundle_page(bundle, argparse.Namespace(
            offset=0, limit=10, provider="claude", project=None, session=None, role=None, tool=None,
        ))
        self.assertEqual(page["total"], 1)
        with self.assertRaises(FileExistsError):
            sessiondb.write_result_bundle(result, bundle)

    def test_expired_bundle_cleanup_preserves_active_readers_and_foreign_files(self):
        result = {
            "protocol_version": 1, "complete": True, "backend": "database",
            "snapshot_id": "snapshot", "projection_version": sessiondb.PROJECTION_VERSION,
            "query": "needle", "total": 1, "entries": [{"result_id": "result-1"}],
            "bundle_lifecycle": {"automatic": True},
        }
        directory = self.root / "retrieval-results"
        expired = directory / "search-expired.jsonl"
        active = directory / "search-active.jsonl"
        foreign = directory / "search-foreign.jsonl"
        header_list = directory / "search-header-list.jsonl"
        lifecycle_list = directory / "search-lifecycle-list.jsonl"
        invalid_utf = directory / "search-invalid-utf.jsonl"
        oversized = directory / "search-oversized-header.jsonl"
        fifo = directory / "search-fifo.jsonl"
        sessiondb.write_result_bundle(result, expired)
        sessiondb.write_result_bundle(result, active)
        foreign.write_text("foreign\n", encoding="utf-8")
        header_list.write_text('{"header":[]}\n', encoding="utf-8")
        lifecycle_list.write_text(json.dumps({"header": {
            "format": "sessiondb-result-bundle-v1", "bundle_lifecycle": [],
        }}) + "\n", encoding="utf-8")
        invalid_utf.write_bytes(b"\xff\xfe\n")
        oversized.write_text("x" * (sessiondb.RESULT_BUNDLE_HEADER_MAX_BYTES + 1), encoding="utf-8")
        os.mkfifo(fifo)
        old = time.time() - 100
        for path in (
            expired, active, foreign, header_list, lifecycle_list, invalid_utf, oversized,
        ):
            os.utime(path, (old, old))
        _header, entries = sessiondb.read_result_bundle(active)
        cleanup = sessiondb.prune_result_bundles(
            self.db_path, retention_seconds=10, now=time.time(),
        )
        self.assertFalse(expired.exists())
        self.assertTrue(active.exists())
        self.assertTrue(foreign.exists())
        self.assertTrue(header_list.exists())
        self.assertTrue(lifecycle_list.exists())
        self.assertTrue(invalid_utf.exists())
        self.assertTrue(oversized.exists())
        self.assertTrue(fifo.exists())
        self.assertEqual(cleanup["deleted"], 1)
        self.assertEqual(cleanup["skipped_active"], 1)
        list(entries)
        cleanup = sessiondb.prune_result_bundles(
            self.db_path, retention_seconds=10, now=time.time(),
        )
        self.assertFalse(active.exists())
        self.assertEqual(cleanup["deleted"], 1)

    def test_bundle_pages_hydrate_evidence_after_the_inline_previews(self):
        source = self.write("bundle-pages.jsonl", [
            {"type": "user", "sessionId": "bundle-pages", "message": {
                "role": "user", "content": f"pageable needle evidence {index}",
            }}
            for index in range(12)
        ])
        sessiondb.ingest(self.db, "claude", source)
        result = sessiondb.search_index(
            self.db, self._search_args(text="needle", mode="literal"),
        )
        delivered = sessiondb.bundle_if_oversized(self.db_path, result, self.root / "pages.jsonl")
        page = sessiondb.result_bundle_page(
            Path(delivered["bundle"]["path"]),
            argparse.Namespace(
                offset=10, limit=2, provider=None, project=None, session=None, role=None, tool=None,
            ),
            self.db_path,
        )
        self.assertTrue(page["complete"])
        self.assertEqual(len(page["entries"]), 2)
        self.assertTrue(all("needle" in entry["passage"] for entry in page["entries"]))

    def test_fast_search_verifies_projection_blocks_after_the_tenth_result(self):
        source = self.write("integrity-after-preview.jsonl", [
            {"type": "user", "sessionId": "integrity-after-preview", "message": {
                "role": "user", "content": f"needle {index} " + (str(index) * 300_000),
            }}
            for index in range(12)
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = self._search_args(text="needle", mode="literal")
        result = sessiondb.search_index(self.db, args)
        self.assertEqual(result["total"], 12)
        target_id = sessiondb.parse_retrieval_result_id(result["entries"][10]["result_id"])[0]
        target = self.db.execute(
            "SELECT b.* FROM events e JOIN search_projection_refs r ON r.event_rowid=e.rowid "
            "JOIN search_projection_blocks b USING(block_key) WHERE e.event_id=?",
            (target_id,),
        ).fetchone()
        pack = sessiondb.projection_pack_path(self.db)
        with pack.open("r+b") as handle:
            handle.seek(target["pack_offset"])
            original = handle.read(1)
            handle.seek(target["pack_offset"])
            handle.write(bytes([original[0] ^ 1]))
        with self.assertRaisesRegex(ValueError, "compressed projection block integrity"):
            sessiondb.search_index(self.db, args)

    def test_manifest_fallback_is_bounded_attributed_and_supports_details(self):
        source = self.write("fallback.jsonl", [
            {"type": "user", "sessionId": "fallback", "timestamp": "2026-09-15T01:00:00Z",
             "message": {"role": "user", "content": "fallback needle"}},
        ])
        data = source.read_bytes()
        logical = str(source.resolve())
        source_id = sessiondb.digest(("claude\0" + logical).encode())
        manifest_path = self.root / "sources.json"
        manifest = {"version": 1, "sources": [{
            "provider": "claude", "path": str(source), "logical_path": logical,
            "source_id": source_id, "size_bytes": len(data), "mtime_ns": source.stat().st_mtime_ns,
            "sha256": sessiondb.digest(data), "adapter_version": sessiondb.ADAPTER_VERSION["claude"],
            "status": "complete", "issue_count": 0,
        }]}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        args = argparse.Namespace(
            text="needle", mode="literal", provider=None, project=None, session=None,
            role=None, tool=None, lineage=None, since=None, until=None, manifest=manifest_path,
        )
        result = sessiondb.search_index_entrypoint(self.root / "absent.sqlite", args)
        self.assertTrue(result["complete"])
        self.assertEqual(result["backend"], "raw_fallback")
        self.assertIn("database_error", result["recovery"]["reason"])
        ranked = sessiondb.raw_manifest_search(
            manifest_path,
            argparse.Namespace(
                text='"fallback needle" NOT absent', mode="ranked", order="relevance",
                provider=None, project=None, session=None, role=None, tool=None,
                lineage=None, since=None, until=None,
            ),
            "test",
        )
        self.assertEqual(ranked["total"], 1)
        self.assertEqual(ranked["entries"][0]["match_origin"], "native")
        details = sessiondb.details_entrypoint(self.root / "absent.sqlite", argparse.Namespace(
            snapshot_id=result["snapshot_id"], result_ids=[result["entries"][0]["result_id"]],
            before=0, after=0, manifest=manifest_path,
        ))
        self.assertTrue(details["complete"])
        self.assertEqual(details["backend"], "raw_fallback")
        self.assertEqual(details["events"][0]["structure"]["message"]["content"], "fallback needle")
        readable = sessiondb.details_entrypoint(self.root / "absent.sqlite", argparse.Namespace(
            snapshot_id=result["snapshot_id"], result_ids=[result["entries"][0]["result_id"]],
            before=0, after=0, manifest=manifest_path, readable=True,
        ))
        self.assertTrue(readable["complete"])
        self.assertEqual(readable["events"][0]["text"], "fallback needle")
        self.assertNotIn("structure", readable["events"][0])

    def test_search_index_verifies_accent_and_mixed_script_candidates(self):
        source = self.write("predicate-parity.jsonl", [
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "café only"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "a\u0301b combined"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "中文 presentword"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "中文 alpha beta"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "中文 x 文检 y 检索"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "ộ"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "độ"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "ắ"}},
            {"type": "user", "sessionId": "predicate", "message": {"role": "user", "content": "한"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        base = dict(provider=None, project=None, session=None, role=None, tool=None,
                    lineage=None, since=None, until=None)
        expected = {
            "cafe": 1, "ab": 1, "中文 absentword": 0, "中文检索": 0,
            "ộ": 1, "độ": 1, "ắ": 1, "한": 1,
        }
        for text, count in expected.items():
            result = sessiondb.search_index(self.db, argparse.Namespace(text=text, mode="literal", **base))
            self.assertEqual(result["total"], count, text)
        mixed_words = sessiondb.search_index(
            self.db, argparse.Namespace(text="中文 alpha-beta", mode="all_words", **base),
        )
        self.assertEqual(mixed_words["total"], 1)
        data = source.read_bytes()
        manifest_path = self.root / "predicate-manifest.json"
        manifest_path.write_text(json.dumps({"version": 1, "sources": [{
            "provider": "claude", "path": str(source), "logical_path": str(source.resolve()),
            "size_bytes": len(data), "mtime_ns": source.stat().st_mtime_ns,
            "sha256": sessiondb.digest(data), "adapter_version": sessiondb.ADAPTER_VERSION["claude"],
            "status": "complete", "issue_count": 0,
        }]}), encoding="utf-8")
        for text, count in expected.items():
            result = sessiondb.raw_manifest_search(
                manifest_path, argparse.Namespace(text=text, mode="literal", **base), "test",
            )
            self.assertTrue(result["complete"])
            self.assertEqual(result["total"], count, text)
        raw_words = sessiondb.raw_manifest_search(
            manifest_path,
            argparse.Namespace(text="中文 alpha-beta", mode="all_words", **base),
            "test",
        )
        self.assertEqual(raw_words["total"], 1)

    def test_ranked_predicates_normalization_repetition_and_anchor_namespaces(self):
        source = self.write("predicate-equivalence.jsonl", [
            {"type": "user", "sessionId": "predicate", "message": {
                "role": "user", "content": "alpha beta alpha gamma",
            }},
            {"type": "user", "sessionId": "predicate", "message": {
                "role": "user", "content": "khách hàng some_error_code",
            }},
            {"type": "user", "sessionId": "predicate", "message": {
                "role": "user", "content": "!",
            }},
            {"type": "user", "sessionId": "predicate", "message": {
                "role": "user", "content": "u21",
            }},
            {"type": "user", "sessionId": "predicate", "message": {
                "role": "user", "content": "a\u0301 a\u0301 alpha beta",
            }},
            {"type": "user", "sessionId": "predicate", "message": {
                "role": "user", "content": "concatenate",
            }},
            {"type": "user", "sessionId": "predicate", "message": {
                "role": "user", "content": {
                    "left": "splitnativeone", "right": "splitnativetwo",
                },
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        cases = (
            ("alpha beta alpha gamma", "literal", 1),
            ('"alpha beta alpha gamma"', "ranked", 1),
            ('"khách hang"', "ranked", 1),
            ("kha\u0301ch ha\u0300ng", "all_words", 1),
            ("error", "ranked", 1),
            ("alpha NEAR/0 beta", "ranked", 2),
            ("u21", "ranked", 1),
            ("!", "literal", 1),
            ('"cat"', "ranked", 0),
            ("splitnativeone AND splitnativetwo", "ranked", 1),
            ("alpha gamma", "all_words", 1),
            ("sessiondbregionboundary", "literal", 0),
        )
        for text, mode, expected in cases:
            result = sessiondb.search_index(self.db, self._search_args(text=text, mode=mode))
            self.assertTrue(result["complete"], text)
            self.assertEqual(result["total"], expected, text)

        regions = {"native": ["alpha", "beta"], "copied": [], "unclassified": []}
        tree = sessiondb.parse_search_query("alpha AND beta")
        self.assertTrue(sessiondb.matching_projection_regions(regions, "alpha AND beta", "ranked", tree))

    def test_search_rejects_stale_projection_posting_before_reporting_absence(self):
        source = self.write("stale-posting.jsonl", [{
            "type": "user", "sessionId": "stale", "message": {
                "role": "user", "content": "onlymatchingneedle",
            },
        }])
        sessiondb.ingest(self.db, "claude", source)
        rowid = self.db.execute(
            "SELECT rowid FROM events WHERE session_id='stale'"
        ).fetchone()[0]
        self.db.execute("DELETE FROM positional_projection_fts WHERE rowid=?", (rowid,))
        self.db.commit()
        result = sessiondb.search_index(
            self.db, self._search_args(text="onlymatchingneedle", mode="ranked"),
        )
        self.assertFalse(result["complete"])
        self.assertIsNone(result["total"])

    def test_all_words_candidate_gate_matches_shared_punctuation_and_accent_predicate(self):
        source = self.write("all-words-parity.jsonl", [
            {"type": "user", "sessionId": "parity", "message": {"role": "user", "content": "alpha beta"}},
            {"type": "user", "sessionId": "parity", "message": {"role": "user", "content": "cafe"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        base = dict(provider=None, project=None, session=None, role=None, tool=None,
                    lineage=None, since=None, until=None)
        for text, expected in (("alpha-beta", 1), ("café", 0)):
            result = sessiondb.search_index(
                self.db, argparse.Namespace(text=text, mode="all_words", **base),
            )
            self.assertTrue(result["complete"])
            self.assertEqual(result["total"], expected, text)

    def test_projection_passage_uses_original_unicode_offsets(self):
        projection = "ß" * 250 + " needle tail"
        passage = sessiondb.projection_passage(projection, "needle", "literal")
        self.assertIn("needle", passage)

    def test_compiled_phrase_verifier_preserves_boundaries_accents_and_large_inputs(self):
        cases = (
            ("alpha alpha tail", "alpha alpha", True),
            ("alpha---beta_harness", "alpha beta harness", True),
            ("alpha\0beta harness", "alpha beta harness", False),
            ("khách hàng", "khach hang", True),
            (unicodedata.normalize("NFD", "khách hàng"), "khach hang", True),
            ("đặt hàng", "dat hang", False),
            ("đặt hàng", "đat hang", True),
            ("Zoho Books hóa đơn", "Zoho Books hóa đơn", True),
            ("Zóho Books hóa đơn", "Zoho Books hóa đơn", True),
            ("Zoho Books hoa don", "Zoho Books hóa đơn", False),
            ("Zoho Books hòa đơn", "Zoho Books hóa đơn", False),
        )
        for projection, query, expected in cases:
            with self.subTest(projection=projection, query=query):
                spans = sessiondb._atom_spans(
                    projection, sessiondb.SearchAtom("phrase", query),
                )
                self.assertEqual(bool(spans), expected)
        large = "x " * 1_000_000
        self.assertFalse(sessiondb._atom_spans(
            large, sessiondb.SearchAtom("phrase", "test pilot harness"),
            allow_accent_fallback=False,
        ))
        self.assertTrue(sessiondb._atom_spans(
            large + "test pilot harness", sessiondb.SearchAtom("phrase", "test pilot harness"),
            allow_accent_fallback=False,
        ))
        with mock.patch.object(
            sessiondb, "_mixed_accent_phrase_spans_fast",
            wraps=sessiondb._mixed_accent_phrase_spans_fast,
        ) as fast_mixed:
            self.assertTrue(sessiondb._atom_spans(
                large + "Zoho Books hóa đơn",
                sessiondb.SearchAtom("phrase", "Zoho Books hóa đơn"),
            ))
        fast_mixed.assert_called_once()
        for projection, query in (
            ('alpha beta far far alpha beta target', '"alpha beta" NEAR/0 target'),
            ('khách hàng xa xa khách hàng target', '"khach hang" NEAR/0 target'),
        ):
            with self.subTest(projection=projection, query=query):
                matched, spans = sessiondb.evaluate_search_query(
                    sessiondb.parse_search_query(query), projection,
                )
                self.assertTrue(matched)
                self.assertGreaterEqual(len(spans), 2)
        for projection, query in (
            ("Zürich trip", "zurich trip"),
            ("Noël test", "noel test"),
            ("über test", "uber test"),
            ("İstanbul trip", "istanbul trip"),
        ):
            for normalized in (projection, unicodedata.normalize("NFD", projection)):
                with self.subTest(projection=normalized, query=query):
                    matches = sessiondb.matching_projection_regions(
                        {"native": [normalized], "copied": [], "unclassified": []},
                        query, "literal", None, allow_accent_fallback=False,
                    )
                    self.assertEqual(len(matches), 1)

    def test_ascii_fast_predicate_respects_long_combining_boundary(self):
        projection = "x" + "\u0301" * 17 + "needle"
        self.assertFalse(sessiondb.projection_query_matches(projection, "needle", "literal"))

    def test_search_projection_hash_rejects_substitution(self):
        source = self.write("passage-integrity.jsonl", [
            {"type": "user", "sessionId": "integrity", "message": {"role": "user", "content": "needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.db.execute("UPDATE search_projection_refs SET projection_hash=?", (bytes(32),))
        self.db.commit()
        self.assertFalse(sessiondb.verify_projection_pack(self.db)["pass"])
        args = argparse.Namespace(
            text="needle", mode="literal", provider=None, project=None, session=None,
            role=None, tool=None, lineage=None, since=None, until=None,
        )
        with self.assertRaisesRegex(ValueError, "integrity failed"):
            sessiondb.search_index(self.db, args)

    def test_projection_status_rejects_dangling_block_reference(self):
        source = self.write("dangling-projection.jsonl", [
            {"type": "user", "sessionId": "dangling", "message": {"role": "user", "content": "needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.db.execute("PRAGMA foreign_keys=OFF")
        self.db.execute("DELETE FROM search_projection_blocks")
        self.db.commit()
        self.assertFalse(sessiondb.projection_status(self.db)["complete"])

    def test_search_reader_pins_old_pack_across_rebuild(self):
        source = self.write("pinned-reader.jsonl", [
            {"type": "user", "sessionId": "pinned", "message": {"role": "user", "content": "needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.db.execute("PRAGMA journal_mode=WAL")
        reader = sessiondb.connect(self.db_path)
        reader.execute("BEGIN")
        old_pack = sessiondb.projection_pack_path(reader)
        handle = old_pack.open("rb")
        try:
            sessiondb.build_search_projections(self.db)
            self.assertFalse(old_pack.exists())
            result = sessiondb._search_index(reader, argparse.Namespace(
                text="needle", mode="literal", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None,
            ), handle)
            self.assertEqual(result["total"], 1)
        finally:
            handle.close()
            reader.close()

    def test_public_search_entrypoint_holds_lease_across_snapshot_and_pack_open(self):
        source = self.write("public-pinned-reader.jsonl", [
            {"type": "user", "sessionId": "public-pinned", "message": {"role": "user", "content": "needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        shared_ready = threading.Event()
        proceed = threading.Event()
        reader_result = {}
        writer_result = {}
        real_acquire = sessiondb.acquire_projection_pack_lease

        def delayed_acquire(db, *, exclusive, timeout=60.0):
            handle = real_acquire(db, exclusive=exclusive, timeout=timeout)
            if not exclusive:
                shared_ready.set()
                self.assertTrue(proceed.wait(2))
            return handle

        def read_entrypoint():
            reader_result["value"] = sessiondb.search_index_entrypoint(
                self.db_path, argparse.Namespace(
                    text="needle", mode="literal", provider=None, project=None, session=None,
                    role=None, tool=None, lineage=None, since=None, until=None, manifest=None,
                ),
            )

        def rebuild():
            writer = sessiondb.connect(self.db_path)
            try:
                writer_result["value"] = sessiondb.build_search_projections(writer)
            finally:
                writer.close()

        with (
            mock.patch.object(sessiondb, "acquire_projection_pack_lease", side_effect=delayed_acquire),
            mock.patch.object(sessiondb, "query_coverage", return_value={"complete": True, "age_seconds": 0}),
        ):
            reader_thread = threading.Thread(target=read_entrypoint)
            writer_thread = threading.Thread(target=rebuild)
            reader_thread.start()
            self.assertTrue(shared_ready.wait(2))
            writer_thread.start()
            time.sleep(0.05)
            self.assertTrue(writer_thread.is_alive())
            proceed.set()
            reader_thread.join(3)
            writer_thread.join(3)
        self.assertFalse(reader_thread.is_alive())
        self.assertFalse(writer_thread.is_alive())
        self.assertEqual(reader_result["value"]["total"], 1)
        self.assertTrue(writer_result["value"]["complete"])

    def test_projection_build_lock_failure_leaves_no_generation(self):
        blocker = sessiondb.connect(self.db_path)
        contender = sessiondb.connect(self.db_path, timeout=0.01)
        before = {
            path for path in self.root.glob(self.db_path.name + ".retrieval.*")
            if path.name != self.db_path.name + ".retrieval.lock"
        }
        blocker.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(sqlite3.OperationalError):
                sessiondb.build_search_projections(contender)
        finally:
            blocker.rollback()
            contender.close()
            blocker.close()
        after = {
            path for path in self.root.glob(self.db_path.name + ".retrieval.*")
            if path.name != self.db_path.name + ".retrieval.lock"
        }
        self.assertEqual(after, before)

    def test_full_rebuild_requires_and_consumes_current_readiness_receipt(self):
        source = self.write("readiness.jsonl", [{
            "type": "user", "sessionId": "readiness",
            "message": {"role": "user", "content": "readiness sample"},
        }])
        sessiondb.ingest(self.db, "claude", source)
        receipt_path = self.root / "readiness-receipt.json"
        receipt = {
            "schema": sessiondb.REBUILD_READINESS_SCHEMA,
            "created_at": sessiondb.datetime.now(sessiondb.timezone.utc).isoformat(),
            "revision": "reviewed-revision",
            "module_sha256": sessiondb.digest(Path(sessiondb.__file__).read_bytes()),
            "projection_version": sessiondb.PROJECTION_VERSION,
            "plan": {"sha256": "plan-digest"},
            "material_review": {
                "reviewer": "Astra", "outcome": "approved",
                "material_changes_reviewed": True,
                "reviewed_revision": "reviewed-revision", "plan_sha256": "plan-digest",
            },
            "unit_tests": {"pass": True, "tests": 1},
            "representative_samples": {"pass": True},
            "tokenizer": {"pass": True},
            "corpus": {
                "events": 1, "snapshot_id": sessiondb.projection_snapshot_id(self.db),
                "schema_version": sessiondb.SCHEMA_VERSION,
            },
            "capacity": {"pass": True, "required_free_bytes": 0},
        }
        receipt["receipt_sha256"] = sessiondb.rebuild_receipt_digest(receipt)
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        ledger = self.root / "receipt-ledger"
        original_consume = sessiondb.consume_rebuild_readiness_receipt

        def consume_inside_transaction(db, path, event_count):
            self.assertTrue(db.in_transaction)
            writer_errors = []

            def contend_for_write():
                contender = sqlite3.connect(self.db_path, timeout=0.01)
                try:
                    contender.execute("BEGIN IMMEDIATE")
                except sqlite3.OperationalError as exc:
                    writer_errors.append(exc)
                finally:
                    contender.close()

            writer = threading.Thread(target=contend_for_write)
            writer.start()
            writer.join(timeout=1)
            self.assertFalse(writer.is_alive())
            self.assertEqual(len(writer_errors), 1)
            return original_consume(db, path, event_count)

        with (
            mock.patch.object(sessiondb, "FULL_REBUILD_RECEIPT_MIN_EVENTS", 1),
            mock.patch.object(sessiondb, "validate_frozen_rebuild_inputs"),
            mock.patch.object(sessiondb, "rebuild_receipt_ledger_dir", return_value=ledger),
            mock.patch.object(
                sessiondb, "consume_rebuild_readiness_receipt",
                side_effect=consume_inside_transaction,
            ),
        ):
            with self.assertRaisesRegex(ValueError, "one-use readiness receipt"):
                sessiondb.build_search_projections(self.db)
            built = sessiondb.build_search_projections(
                self.db, readiness_receipt=receipt_path,
            )
            self.assertTrue(built["complete"])
            consumed = Path(built["readiness_receipt"])
            self.assertFalse(receipt_path.exists())
            self.assertTrue(consumed.exists())
            replay_path = self.root / "renamed-copy.json"
            replay_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "already consumed"):
                sessiondb.preflight_full_rebuild_readiness(self.db, replay_path)
            with self.assertRaisesRegex(ValueError, "already consumed"):
                sessiondb.build_search_projections(
                    self.db, readiness_receipt=replay_path,
                )

    def test_rebuild_readiness_receipt_rejects_code_drift(self):
        source = self.write("readiness-drift.jsonl", [{
            "type": "user", "sessionId": "readiness-drift",
            "message": {"role": "user", "content": "readiness drift"},
        }])
        sessiondb.ingest(self.db, "claude", source)
        receipt_path = self.root / "drift-receipt.json"
        receipt = {
            "schema": sessiondb.REBUILD_READINESS_SCHEMA,
            "created_at": sessiondb.datetime.now(sessiondb.timezone.utc).isoformat(),
            "revision": "reviewed-revision", "module_sha256": "wrong",
            "projection_version": sessiondb.PROJECTION_VERSION,
            "plan": {"sha256": "plan-digest"},
            "material_review": {
                "outcome": "approved", "material_changes_reviewed": True,
                "reviewed_revision": "reviewed-revision", "plan_sha256": "plan-digest",
            },
            "unit_tests": {"pass": True}, "representative_samples": {"pass": True},
            "tokenizer": {"pass": True},
            "corpus": {
                "events": 1, "snapshot_id": sessiondb.projection_snapshot_id(self.db),
                "schema_version": sessiondb.SCHEMA_VERSION,
            },
            "capacity": {"pass": True, "required_free_bytes": 0},
        }
        receipt["receipt_sha256"] = sessiondb.rebuild_receipt_digest(receipt)
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "does not match SessionDB code"):
            sessiondb.validate_rebuild_readiness_receipt(self.db, receipt_path, 1)

    def test_rebuild_readiness_rechecks_revision_and_plan(self):
        repository = self.root / "repository"
        repository.mkdir()
        plan = repository / "Search implementation plan.md"
        plan.write_text("frozen plan", encoding="utf-8")
        receipt = {
            "revision": "frozen-revision",
            "repository": {"root": str(repository)},
            "plan": {
                "path": str(plan), "relative_path": plan.name,
                "sha256": sessiondb.digest(plan.read_bytes()),
            },
        }
        clean = [mock.Mock(stdout="frozen-revision\n"), mock.Mock(stdout="")]
        with mock.patch.object(sessiondb.subprocess, "run", side_effect=clean):
            sessiondb.validate_frozen_rebuild_inputs(receipt)
        plan.write_text("changed plan", encoding="utf-8")
        changed = [mock.Mock(stdout="frozen-revision\n"), mock.Mock(stdout="")]
        with (
            mock.patch.object(sessiondb.subprocess, "run", side_effect=changed),
            self.assertRaisesRegex(ValueError, "plan has changed"),
        ):
            sessiondb.validate_frozen_rebuild_inputs(receipt)

    def test_compact_copy_rejects_before_creating_candidate(self):
        target = self.root / "should-not-exist.sqlite"
        with (
            mock.patch.object(
                sessiondb, "preflight_full_rebuild_readiness",
                side_effect=ValueError("missing readiness"),
            ),
            self.assertRaisesRegex(ValueError, "missing readiness"),
        ):
            full_corpus.compact_copy(self.db_path, target)
        self.assertFalse(target.exists())

    def test_contended_projection_lease_reaches_manifest_fallback_within_deadline(self):
        source = self.write("lease-fallback.jsonl", [
            {"type": "user", "sessionId": "lease", "message": {"role": "user", "content": "lease needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        data = source.read_bytes()
        manifest_path = self.root / "lease-manifest.json"
        manifest_path.write_text(json.dumps({"version": 1, "sources": [{
            "provider": "claude", "path": str(source), "logical_path": str(source.resolve()),
            "size_bytes": len(data), "mtime_ns": source.stat().st_mtime_ns,
            "sha256": sessiondb.digest(data), "adapter_version": sessiondb.ADAPTER_VERSION["claude"],
            "status": "complete", "issue_count": 0,
        }]}), encoding="utf-8")
        lease = sessiondb.acquire_projection_pack_lease(self.db, exclusive=True)
        started = time.monotonic()
        try:
            result = sessiondb.search_index_entrypoint(self.db_path, argparse.Namespace(
                text="needle", mode="literal", provider=None, project=None, session=None,
                role=None, tool=None, lineage=None, since=None, until=None, manifest=manifest_path,
            ))
        finally:
            sessiondb.release_projection_pack_lease(lease)
        self.assertLess(time.monotonic() - started, 2.5)
        self.assertTrue(result["complete"])
        self.assertEqual(result["backend"], "raw_fallback")

    def test_replacement_orphans_are_disclosed_and_compaction_reclaims_them(self):
        source = self.write("replace-projection.jsonl", [
            {"type": "user", "sessionId": "replace", "message": {"role": "user", "content": "first value"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        source.write_text(json.dumps(
            {"type": "user", "sessionId": "replace", "message": {"role": "user", "content": "second value"}}
        ) + "\n", encoding="utf-8")
        sessiondb.ingest(self.db, "claude", source)
        verification = sessiondb.verify_projection_pack(self.db)
        self.assertGreater(verification["unreachable_block_bytes"], 0)
        target = self.root / "replace-compact.sqlite"
        compacted = full_corpus.compact_copy(self.db_path, target)
        self.assertTrue(compacted["complete"])
        self.assertEqual(compacted["report"]["retrieval_pack_verification"]["orphan_bytes"], 0)

    def test_existing_v9_database_can_ingest_before_explicit_projection_build(self):
        first = self.write("pre-index.jsonl", [
            {"type": "user", "sessionId": "pre-index", "message": {"role": "user", "content": "old"}},
        ])
        sessiondb.ingest(self.db, "claude", first)
        self.db.execute("DROP TRIGGER events_projection_bd")
        self.db.execute("DROP TABLE positional_projection_fts")
        self.db.execute("DROP TABLE search_projection_refs")
        self.db.execute("DROP TABLE search_projection_blocks")
        self.db.execute("DELETE FROM metadata WHERE key LIKE 'projection_%'")
        self.db.commit()
        self.db.close()
        self.db = sessiondb.connect(self.db_path)
        second = self.write("post-index.jsonl", [
            {"type": "user", "sessionId": "post-index", "message": {"role": "user", "content": "new"}},
        ])
        result = sessiondb.ingest(self.db, "claude", second)
        self.assertEqual(result["status"], "imported")
        self.assertFalse(sessiondb.projection_status(self.db)["complete"])
        self.assertTrue(sessiondb.build_search_projections(self.db)["complete"])

    def test_result_bundle_rejects_record_boundary_truncation(self):
        result = {
            "complete": True, "total": 2, "entries": [
                {"result_id": "one"}, {"result_id": "two"},
            ],
        }
        bundle = self.root / "truncated.jsonl"
        sessiondb.write_result_bundle(result, bundle)
        lines = bundle.read_text(encoding="utf-8").splitlines()
        bundle.write_text("\n".join(lines[:-2] + lines[-1:]) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "integrity"):
            sessiondb.result_bundle_page(bundle, argparse.Namespace(
                offset=0, limit=10, provider=None, project=None, session=None, role=None, tool=None,
            ))

    def test_detail_fallback_reads_only_result_locator_source(self):
        source = self.write("located.jsonl", [
            {"type": "user", "sessionId": "located", "message": {"role": "user", "content": "selected"}},
        ])
        data = source.read_bytes()
        logical = str(source.resolve())
        source_id = sessiondb.digest(("claude\0" + logical).encode())
        row = json.loads(data)
        raw_hash = sessiondb.digest(sessiondb.stored_json(row))
        event_id = sessiondb.digest(f"{source_id}:1:{raw_hash}".encode())
        result_id = sessiondb.retrieval_result_id(event_id, source_id, 1)
        missing = self.root / "unrelated-missing.jsonl"
        missing_logical = str(missing.resolve())
        manifest_path = self.root / "located-manifest.json"
        manifest_path.write_text(json.dumps({"version": 1, "sources": [
            {
                "provider": "claude", "path": str(source), "logical_path": logical,
                "size_bytes": len(data), "mtime_ns": source.stat().st_mtime_ns,
                "sha256": sessiondb.digest(data), "adapter_version": sessiondb.ADAPTER_VERSION["claude"],
                "status": "complete", "issue_count": 0,
            },
            {
                "provider": "claude", "path": str(missing), "logical_path": missing_logical,
                "size_bytes": 10, "mtime_ns": 1, "sha256": "0" * 64,
                "adapter_version": sessiondb.ADAPTER_VERSION["claude"],
                "status": "complete", "issue_count": 0,
            },
        ]}), encoding="utf-8")
        manifest = sessiondb.load_source_manifest(manifest_path)
        details = sessiondb.raw_manifest_details(
            manifest_path, argparse.Namespace(
                snapshot_id=manifest["snapshot_id"], result_ids=[result_id], before=0, after=0,
            ), "test",
        )
        self.assertTrue(details["complete"])
        self.assertEqual(details["telemetry"]["source_reads"], 1)
        self.assertEqual(details["events"][0]["structure"]["message"]["content"], "selected")

    def test_ranked_query_grammar_accents_and_compact_aliases(self):
        source = self.write("ranked.jsonl", [
            {"type": "user", "sessionId": "ranked", "message": {"role": "user", "content": "bán hàng alpha beta"}},
            {"type": "user", "sessionId": "ranked", "message": {"role": "user", "content": "ban hang alpha gamma"}},
            {"type": "user", "sessionId": "ranked", "message": {"role": "user", "content": "bàn bạc delta"}},
            {"type": "user", "sessionId": "ranked", "message": {"role": "user", "content": "alpha one two three beta"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        base = dict(provider=None, project=None, session=None, role=None, tool=None,
                    lineage=None, since=None, until=None)
        exact = sessiondb.search_index(
            self.db, argparse.Namespace(text='"bán hàng"', mode="ranked", **base),
        )
        self.assertEqual(exact["total"], 1)
        folded = sessiondb.search_index(
            self.db, argparse.Namespace(text="ban AND (beta OR gamma) NOT delta", mode="ranked", **base),
        )
        self.assertEqual(folded["total"], 2)
        quoted_boolean = sessiondb.search_index(
            self.db, argparse.Namespace(
                text='"ban" AND ("beta" OR "gamma")', mode="ranked", **base,
            ),
        )
        self.assertEqual(quoted_boolean["total"], 2)
        self.assertEqual(
            quoted_boolean["telemetry"]["verified_by"],
            "equivalent_positional_fts_predicate",
        )
        self.assertFalse(sessiondb.fts_fully_verifies(
            sessiondb.parse_search_query('"bán hàng"'),
        ))
        chronological = sessiondb.search_index(
            self.db, argparse.Namespace(
                text="ban AND (beta OR gamma) NOT delta", mode="ranked",
                order="chronological", **base,
            ),
        )
        self.assertEqual(chronological["order"], "chronological")
        self.assertIn("bán hàng alpha beta", chronological["entries"][0]["passage"])
        near = sessiondb.search_index(
            self.db, argparse.Namespace(text="alpha NEAR/2 beta", mode="ranked", **base),
        )
        self.assertEqual(near["total"], 1)
        wider_near = sessiondb.search_index(
            self.db, argparse.Namespace(text="alpha NEAR/3 beta", mode="ranked", **base),
        )
        self.assertEqual(wider_near["total"], 2)
        minus = sessiondb.search_index(
            self.db, argparse.Namespace(text="alpha -gamma", mode="ranked", **base),
        )
        self.assertEqual(minus["total"], 2)
        with self.assertRaisesRegex(ValueError, "positive term"):
            sessiondb.search_index(
                self.db, argparse.Namespace(text="NOT delta", mode="ranked", **base),
            )
        delivered = sessiondb.bundle_if_oversized(self.db_path, folded, None)
        self.assertEqual([item["id"] for item in delivered["entries"]], ["r1", "r2"])
        resolved = sessiondb.resolve_result_aliases(Path(delivered["bundle"]["path"]), ["r2"])
        self.assertEqual(resolved, [folded["entries"][1]["result_id"]])

        many = {**folded, "total": 12, "entries": [
            {
                **folded["entries"][0], "result_id": f"result-{index}",
                "session": f"session-{index}",
                "source": {"path": f"/source/{index}", "line": index, "hash": str(index)},
                "passage": f"distinct passage {index}",
            }
            for index in range(12)
        ]}
        bounded = sessiondb.bundle_if_oversized(self.db_path, many, None)
        self.assertEqual(len(bounded["entries"]), 10)
        self.assertEqual(len(bounded["sessions"]), 10)
        self.assertEqual(len(bounded["sources"]), 10)
        self.assertTrue(all("occurrences" not in item for item in bounded["entries"]))
        self.assertEqual(bounded["telemetry"]["command"], "search-index")
        self.assertIn("bundle_write", bounded["telemetry"]["phases_seconds"])
        self.assertTrue(all("rank" not in item for item in bounded["entries"]))

    def test_search_deadline_returns_no_partial_results(self):
        source = self.write("deadline.jsonl", [{
            "type": "user", "sessionId": "deadline",
            "message": {"role": "user", "content": "deadline needle"},
        }])
        sessiondb.ingest(self.db, "claude", source)
        args = self._search_args(text="needle", mode="literal")
        args.deadline_seconds = 1.0
        calls = 0

        def clock():
            nonlocal calls
            calls += 1
            return 0.0 if calls <= 3 else 2.0

        with mock.patch.object(sessiondb.time, "monotonic", side_effect=clock):
            result = sessiondb.search_index(self.db, args)
        self.assertFalse(result["complete"])
        self.assertIsNone(result["total"])
        self.assertEqual(result["entries"], [])
        self.assertEqual(result["coverage"]["reason"], "query_deadline_exceeded")
        self.assertGreater(calls, 3)
        args.deadline_seconds = 1e-12
        entrypoint = sessiondb.search_index_entrypoint(self.db_path, args)
        self.assertFalse(entrypoint["complete"])
        self.assertEqual(entrypoint["backend"], "database")
        self.assertEqual(entrypoint["coverage"]["reason"], "query_deadline_exceeded")
        args.deadline_seconds = float("nan")
        with self.assertRaisesRegex(ValueError, "finite"):
            sessiondb.search_index(self.db, args)

    def test_ascii_phrase_fast_path_returns_the_python_verifier_results(self):
        rows = [
            "Rule-Writing Checklist for reviewers", "rule writing checklist without the hyphen",
            "the checklist of rule-writing", "Rule-Writing   Checklist spaced", "unrelated words",
            "checklist Rule-Writing reversed",
        ]
        source = self.write("phrase-fast.jsonl", [
            {"type": "user", "sessionId": "phrase", "message": {"role": "user", "content": text}}
            for text in rows
        ])
        sessiondb.ingest(self.db, "claude", source)
        sessiondb.rebuild_search_fts(self.db, "full")
        for query in ('"Rule-Writing Checklist"', '"rule writing checklist"', '"alpha beta" OR "checklist for"'):
            args = self._search_args(text=query, mode="ranked")
            fast = sessiondb.search_index(self.db, args)
            with mock.patch.object(sessiondb, "fts_fully_verifies", return_value=False):
                slow = sessiondb.search_index(self.db, args)
            self.assertEqual(
                fast["telemetry"]["verified_by"], "equivalent_positional_fts_predicate", query,
            )
            self.assertNotEqual(slow["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate")
            self.assertEqual(
                sorted(entry["result_id"] for entry in fast["entries"]),
                sorted(entry["result_id"] for entry in slow["entries"]), query,
            )
            self.assertEqual(fast["total"], slow["total"], query)
        self.assertTrue(sessiondb.fts_fully_verifies(sessiondb.parse_search_query('"a b"'), True))
        self.assertFalse(sessiondb.fts_fully_verifies(sessiondb.parse_search_query('"a b"'), False))
        self.assertFalse(sessiondb.fts_fully_verifies(sessiondb.parse_search_query('"bán hàng"'), True))
        self.assertFalse(sessiondb.fts_fully_verifies(sessiondb.parse_search_query('"привет мир"'), True))

    ACCENT_ROWS = [
        "báo giá cho khách hàng", "Báo Giá mới gửi", "bao gia khong dau", "bảo gia đình",
        "thanh toán xong", "thành toàn tập", "thanh toan chua", "café au lait", "cafe noir",
        "gửi báo giá khách hàng hôm nay nhé", "báo, giá tách", "giá báo đảo",
    ]

    def _accent_fixture(self, keep_accents=True):
        source = self.write("accents.jsonl", [
            {"type": "user", "sessionId": "accents", "message": {"role": "user", "content": text}}
            for text in self.ACCENT_ROWS
        ])
        sessiondb.ingest(self.db, "claude", source)
        sessiondb.rebuild_search_fts(self.db, "full", keep_accents=keep_accents)

    def _accent_matches(self, query, mode="ranked"):
        result = sessiondb.search_index(self.db, self._search_args(text=query, mode=mode))
        self.assertTrue(result["complete"], query)
        return result, sorted(entry["passage"] or "" for entry in result["entries"])

    ACCENT_EXPECTED = {
        '"báo giá"': ["báo giá cho khách hàng", "Báo Giá mới gửi", "gửi báo giá khách hàng hôm nay nhé",
                      "báo, giá tách"],
        "báo": ["báo giá cho khách hàng", "Báo Giá mới gửi", "gửi báo giá khách hàng hôm nay nhé",
                "báo, giá tách", "giá báo đảo"],
        '"bao gia"': ["báo giá cho khách hàng", "Báo Giá mới gửi", "bao gia khong dau", "bảo gia đình",
                      "gửi báo giá khách hàng hôm nay nhé", "báo, giá tách"],
        '"thanh toán"': ["thanh toán xong"],
        '"thanh toan"': ["thanh toán xong", "thành toàn tập", "thanh toan chua"],
        "cafe": ["café au lait", "cafe noir"],
        '"báo giá khách hàng hôm nay"': ["gửi báo giá khách hàng hôm nay nhé"],
        '"bao gia khach hang hom nay"': ["gửi báo giá khách hàng hôm nay nhé"],
        '"thanh toán" OR "cafe noir"': ["thanh toán xong", "cafe noir"],
    }

    def test_accent_keeping_index_answers_words_and_phrases_from_positions(self):
        self._accent_fixture()
        self.assertTrue(sessiondb.fts_index_keeps_accents(self.db))
        variants = {tuple(row) for row in self.db.execute("SELECT folded,term FROM search_term_variants")}
        self.assertIn(("bao", "báo"), variants)
        self.assertIn(("cafe", "café"), variants)
        self.assertNotIn(("bao", "bao"), variants)
        for query, expected in self.ACCENT_EXPECTED.items():
            fast, found = self._accent_matches(query)
            with mock.patch.object(sessiondb, "fts_fully_verifies", return_value=False):
                slow, verified = self._accent_matches(query)
            with self.subTest(query=query):
                self.assertEqual(fast["telemetry"]["verified_by"], "equivalent_positional_fts_predicate")
                if query != "cafe":  # a single plain word has its own fast path
                    self.assertNotEqual(
                        slow["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate",
                    )
                self.assertEqual(found, sorted(expected))
                self.assertEqual(verified, sorted(expected))
        # Control: without the spelling lookup an unaccented phrase misses its accented forms.
        with mock.patch.object(sessiondb.IndexSpellings, "spellings", lambda self, term: [term]):
            _result, unexpanded = self._accent_matches('"bao gia"')
        self.assertEqual(unexpanded, ["bao gia khong dau"])

    def test_literal_mode_answers_accented_phrases_from_positions(self):
        self._accent_fixture()
        for query, expected in (
            ("báo giá", self.ACCENT_EXPECTED['"báo giá"']),
            ("bao gia", self.ACCENT_EXPECTED['"bao gia"']),
            ("thanh toán", ["thanh toán xong"]),
        ):
            result, found = self._accent_matches(query, mode="literal")
            with self.subTest(query=query):
                self.assertEqual(result["telemetry"]["verified_by"], "equivalent_positional_fts_predicate")
                self.assertEqual(found, sorted(expected))
        # Control: the older accent-folding index keeps the Python re-check for the same query.
        self.db.close()
        self.db = sessiondb.connect(self.db_path)
        sessiondb.rebuild_search_fts(self.db, "full", keep_accents=False)
        result, found = self._accent_matches("bao gia", mode="literal")
        self.assertNotEqual(result["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate")
        self.assertEqual(found, sorted(self.ACCENT_EXPECTED['"bao gia"']))

    def test_unaccented_phrase_prunes_spellings_or_keeps_the_python_recheck(self):
        self._accent_fixture()
        # bao x3 spellings, gia x2: 6 combinations; three adjacent pairs occur.
        with mock.patch.object(sessiondb, "PHRASE_SPELLING_CAP", 4):
            pruned, found = self._accent_matches('"bao gia"')
        self.assertEqual(pruned["telemetry"]["verified_by"], "equivalent_positional_fts_predicate")
        self.assertEqual(found, sorted(self.ACCENT_EXPECTED['"bao gia"']))
        with mock.patch.object(sessiondb, "PHRASE_SPELLING_CAP", 2):
            capped, found = self._accent_matches('"bao gia"')
        self.assertNotEqual(capped["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate")
        self.assertEqual(found, sorted(self.ACCENT_EXPECTED['"bao gia"']))

    def test_accent_folding_index_gives_the_same_results_with_current_code(self):
        self._accent_fixture(keep_accents=False)
        self.assertFalse(sessiondb.fts_index_keeps_accents(self.db))
        for query, expected in self.ACCENT_EXPECTED.items():
            _result, found = self._accent_matches(query)
            with self.subTest(query=query):
                self.assertEqual(found, sorted(expected))

    def test_accent_folding_index_upgrades_online_and_keeps_later_ingest(self):
        source = self.write("accent-legacy.jsonl", [
            {"type": "user", "sessionId": "legacy", "message": {"role": "user", "content": text}}
            for text in ("thanh toán đầy đủ", "viết thành toàn bộ")
        ])
        sessiondb.ingest(self.db, "claude", source)
        sessiondb.rebuild_search_fts(self.db, "full", keep_accents=False)
        legacy = sessiondb.search_index(self.db, self._search_args(text='"thanh toán"', mode="ranked"))
        self.assertEqual(legacy["total"], 1)
        self.assertNotEqual(legacy["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate")
        sessiondb.rebuild_search_fts_online(self.db, "full")
        self.assertTrue(sessiondb.fts_index_keeps_accents(self.db))
        upgraded = sessiondb.search_index(self.db, self._search_args(text='"thanh toán"', mode="ranked"))
        self.assertEqual(
            [entry["result_id"] for entry in upgraded["entries"]],
            [entry["result_id"] for entry in legacy["entries"]],
        )
        self.assertEqual(upgraded["telemetry"]["verified_by"], "equivalent_positional_fts_predicate")
        later = self.write("accent-later.jsonl", [
            {"type": "user", "sessionId": "later", "message": {"role": "user", "content": "thanh toán sau"}},
        ])
        sessiondb.ingest(self.db, "claude", later)
        after = sessiondb.search_index(self.db, self._search_args(text='"thanh toán"', mode="ranked"))
        self.assertEqual(after["total"], 2)

    def test_accent_keeping_results_equal_the_folding_index_results(self):
        rows = [
            "thanh toán đầy đủ", "viết thành toàn bộ", "thanh toan plain", "báo giá khách hàng",
            "bao gia plain", "Thanh   Toán, xong", "thành toán riêng", "khách hàng mới",
            "khach hang cu", "đồng ý", "dong y",
        ]
        source = self.write("accent-baseline.jsonl", [
            {"type": "user", "sessionId": "baseline", "message": {"role": "user", "content": text}}
            for text in rows
        ])
        sessiondb.ingest(self.db, "claude", source)
        queries = (
            '"báo giá"', '"khách hàng"', '"đồng ý"', '"thanh toán"', '"thành toán"', "toán",
            '"bao gia"', '"khach hang"', '"dong y"', '"báo giá" OR "khách hàng"',
        )

        def found(query):
            return {
                entry["result_id"] for entry in sessiondb.search_index(
                    self.db, self._search_args(text=query, mode="ranked"),
                )["entries"]
            }

        sessiondb.rebuild_search_fts(self.db, "full", keep_accents=False)
        folding = {query: found(query) for query in queries}
        sessiondb.rebuild_search_fts(self.db, "full")
        for query in queries:
            with self.subTest(query=query):
                self.assertEqual(found(query), folding[query])
        # "thanh" is unaccented, so it also matches "thành" beside the exact "toán".
        self.assertEqual(len(folding['"thanh toán"']), 3)

    def test_accent_layout_change_survives_reconnect_concurrent_ingest_and_rollback(self):
        first = self.write("layout-first.jsonl", [
            {"type": "user", "sessionId": "layout", "message": {"role": "user", "content": "thanh toán một"}},
        ])
        sessiondb.ingest(self.db, "claude", first)
        sessiondb.rebuild_search_fts(self.db, "full", keep_accents=False)
        later = self.write("layout-later.jsonl", [
            {"type": "user", "sessionId": "layout2", "message": {"role": "user", "content": "thanh toán hai"}},
        ])
        sessiondb.rebuild_search_fts_online(
            self.db, "full", between_chunks=lambda: sessiondb.ingest(self.db, "claude", later),
        )
        reopened = sessiondb.connect(self.db_path)
        try:
            self.assertTrue(sessiondb.fts_index_keeps_accents(reopened))
            for query in ('"thanh toán"', '"thanh toan"'):
                self.assertEqual(sessiondb.search_index(
                    reopened, self._search_args(text=query, mode="ranked"),
                )["total"], 2, query)
            sessiondb.rebuild_search_fts_online(reopened, "full", keep_accents=False)
            self.assertFalse(sessiondb.fts_index_keeps_accents(reopened))
            rolled_back = sessiondb.search_index(reopened, self._search_args(text='"thanh toán"', mode="ranked"))
            self.assertEqual(rolled_back["total"], 2)
            self.assertNotEqual(
                rolled_back["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate",
            )
        finally:
            reopened.close()

    def test_query_text_reads_the_positional_index(self):
        self._accent_fixture()
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name IN ('search_fts','literal_fts')"
        ).fetchone())
        base = dict(provider=None, project=None, session=None, role=None, tool=None, lineage=None,
                    since=None, until=None, raw=False, limit=50, offset=0, before=0, after=0)
        for needle, expected in (
            ("báo giá", ["báo giá cho khách hàng", "Báo Giá mới gửi", "gửi báo giá khách hàng hôm nay nhé"]),
            ("bao gia", ["bao gia khong dau"]),
            ("café", ["café au lait"]),
            ("toán xong", ["thanh toán xong"]),
        ):
            rows = sessiondb.query(self.db, argparse.Namespace(text=needle, **base))
            with self.subTest(needle=needle):
                self.assertEqual(sorted(row["text"] for row in rows), sorted(expected))
        expression = sessiondb.query_text_candidate_expression(self.db, "báo giá", False)
        self.assertIn('"báo"', expression)

    def _query_text_args(self, text, **overrides):
        values = dict(text=text, provider=None, project=None, session=None, role=None, tool=None,
                      lineage=None, since=None, until=None, raw=False, limit=50, offset=0,
                      before=0, after=0)
        values.update(overrides)
        return argparse.Namespace(**values)

    def _query_text_fixture(self):
        contents = [
            "Could not verify worktree state today",
            "we cannot verify worktree statement; the state is not clear",
            "verify the worktree first, not the state",
            ["could not verify", "worktree state after the split"],
        ]
        source = self.write("query-text.jsonl", [
            {"type": "user", "sessionId": "query-text", "message": {"role": "user", "content": content}}
            for content in contents
        ])
        sessiondb.ingest(self.db, "claude", source)
        sessiondb.rebuild_search_fts(self.db, "full")
        self.assertTrue(sessiondb.fts_index_is_positional(self.db))

    def _query_text_candidates(self, text):
        expression = sessiondb.query_text_candidate_expression(self.db, text, False)
        return self.db.execute(
            "SELECT COUNT(*) FROM positional_projection_fts WHERE positional_projection_fts MATCH ?",
            (expression,),
        ).fetchone()[0]

    def test_query_text_candidates_need_the_words_close_together(self):
        self._query_text_fixture()
        needle = "not verify worktree state"
        expected = [
            "Could not verify worktree state today",
            "could not verify\nworktree state after the split",
            "we cannot verify worktree statement; the state is not clear",
        ]
        found = sorted(row["text"] for row in sessiondb.query(self.db, self._query_text_args(needle)))
        self.assertEqual(found, expected)
        self.assertEqual(self._query_text_candidates(needle), 3)
        # Control: without the closeness group the scattered words are a candidate again.
        with mock.patch.object(sessiondb, "query_text_near_group", return_value=None):
            self.assertEqual(self._query_text_candidates(needle), 4)
            unfiltered = sessiondb.query(self.db, self._query_text_args(needle))
        self.assertEqual(sorted(row["text"] for row in unfiltered), expected)
        # The first word may end a longer word and the last may begin one, unless bounded.
        self.assertIn('NEAR("verify" "worktree" "state"*, 2)',
                      sessiondb.query_text_candidate_expression(self.db, needle, False))
        self.assertIn('NEAR("could" "not" "verify", 2)',
                      sessiondb.query_text_candidate_expression(self.db, "(could not verify.", False))

    def test_query_text_checks_stored_search_text_without_rebuilding_records(self):
        self._query_text_fixture()
        needle = "not verify worktree state"
        with mock.patch.object(
            sessiondb, "normalized_event_search_text",
            side_effect=AssertionError("query --text rebuilt a record"),
        ):
            self.assertEqual(len(sessiondb.query(self.db, self._query_text_args(needle))), 3)
        # Control: without stored text every candidate is rebuilt, with the same answer.
        rebuilt = mock.Mock(wraps=sessiondb.normalized_event_search_text)
        with mock.patch.object(sessiondb, "normalized_event_search_text", rebuilt), \
                mock.patch.object(sessiondb, "read_projection_for_event", side_effect=ValueError("off")):
            self.assertEqual(len(sessiondb.query(self.db, self._query_text_args(needle))), 3)
        self.assertEqual(rebuilt.call_count, 3)
        # Raw search keeps rebuilding the raw record for every event in scope.
        rebuilt.reset_mock()
        with mock.patch.object(sessiondb, "normalized_event_search_text", rebuilt):
            sessiondb.query(self.db, self._query_text_args(needle, raw=True))
        self.assertEqual(rebuilt.call_count, 4)

    def test_search_query_words_stay_words_after_sentence_punctuation_and_lower_case_operators(self):
        atom = sessiondb.SearchAtom
        self.assertEqual(
            sessiondb.parse_search_query("Inconclusive not reproduced in isolation"),
            ("AND", atom("term", "Inconclusive"), atom("term", "not"), atom("term", "reproduced"),
             atom("term", "in"), atom("term", "isolation")),
        )
        self.assertEqual(sessiondb.parse_search_query("alpha NOT beta"),
                         ("AND", atom("term", "alpha"), ("NOT", atom("term", "beta"))))
        self.assertEqual(sessiondb.parse_search_query("alpha or beta"),
                         ("AND", atom("term", "alpha"), atom("term", "or"), atom("term", "beta")))
        self.assertEqual(sessiondb.parse_search_query("implementation, done."),
                         ("AND", atom("term", "implementation"), atom("term", "done")))
        self.assertEqual(sessiondb.parse_search_query("run --stage -draft"),
                         ("AND", atom("term", "run"), atom("term", "--stage"),
                          ("NOT", atom("term", "draft"))))
        self.assertEqual(sessiondb.parse_search_query("zoho.com. ..."),
                         ("AND", atom("term", "zoho.com"), atom("term", "...")))

    def test_punctuation_joined_words_are_answered_from_their_anchor(self):
        source = self.write("structured-terms.jsonl", [
            {"type": "user", "sessionId": "structured-terms", "message": {"role": "user", "content": content}}
            for content in (
                "the cost-state report", "cost state apart", "see x.cost-state here",
                "open run_trials.py now", "zoho.com agents list",
            )
        ])
        sessiondb.ingest(self.db, "claude", source)
        sessiondb.rebuild_search_fts(self.db, "full")
        for query, expected in (
            ("cost-state", ["the cost-state report"]),
            ("run_trials.py", ["open run_trials.py now"]),
            ("zoho.com agents", ["zoho.com agents list"]),
        ):
            fast, found = self._accent_matches(query)
            with mock.patch.object(sessiondb, "fts_fully_verifies", return_value=False):
                slow, verified = self._accent_matches(query)
            with self.subTest(query=query):
                self.assertEqual(fast["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate")
                # Control: the re-checked path gives the same answer.
                self.assertNotEqual(slow["telemetry"].get("verified_by"), "equivalent_positional_fts_predicate")
                self.assertEqual(found, expected)
                self.assertEqual(verified, expected)
        with sessiondb.index_spellings(self.db):
            self.assertFalse(sessiondb.fts_fully_verifies(sessiondb.parse_search_query("cöst-state"), True))

    def test_deadline_result_keeps_finished_phase_timings(self):
        source = self.write("timings.jsonl", [{
            "type": "user", "sessionId": "timings",
            "message": {"role": "user", "content": "timing needle"},
        }])
        sessiondb.ingest(self.db, "claude", source)
        with mock.patch.object(
            sessiondb, "verify_candidate_projection_integrity",
            side_effect=sessiondb.QueryDeadlineExceeded("search deadline exceeded"),
        ):
            result = sessiondb.search_index(self.db, self._search_args(text="needle", mode="ranked"))
        self.assertEqual(result["coverage"]["reason"], "query_deadline_exceeded")
        telemetry = result["telemetry"]
        self.assertEqual(telemetry["interrupted_phase"], "candidate_integrity")
        self.assertIn("coverage_seconds", telemetry)
        self.assertIn("candidate_selection_seconds", telemetry)
        self.assertEqual(telemetry["fts_candidates"], 1)
        timed = sessiondb.attach_command_timing(result, "search-index", time.monotonic() - 1.0)
        self.assertIn("candidate_integrity", timed["telemetry"]["phases_seconds"])
        self.assertLess(timed["telemetry"]["unattributed_seconds"], timed["telemetry"]["total_seconds"])
        self.assertEqual(timed["telemetry"]["outcome"], "timeout")
        with mock.patch.object(
            sessiondb, "hydrate_distinct_passages",
            side_effect=sessiondb.QueryDeadlineExceeded("search deadline exceeded"),
        ):
            late = sessiondb.search_index(self.db, self._search_args(text="needle", mode="ranked"))
        self.assertEqual(late["telemetry"]["interrupted_phase"], "hydration")
        self.assertIn("candidate_integrity_seconds", late["telemetry"])
        self.assertIn("candidate_selection_seconds", late["telemetry"])

    def test_search_index_limit_sets_the_inline_passage_count_and_must_be_positive(self):
        seen = {}

        def capture(db_path, args):
            seen["limit"] = args.passage_limit
            return {"complete": True, "total": 0, "entries": []}

        with mock.patch.object(sessiondb, "search_index_entrypoint", side_effect=capture), \
                contextlib.redirect_stdout(io.StringIO()):
            sessiondb.main(["--db", str(self.db_path), "search-index", "--text", "x", "--limit", "3"])
            self.assertEqual(seen["limit"], 3)
            sessiondb.main(["--db", str(self.db_path), "search-index", "--text", "x"])
            self.assertEqual(seen["limit"], sessiondb.INLINE_RESULT_LIMIT)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            sessiondb.main(["--db", str(self.db_path), "search-index", "--text", "x", "--limit", "0"])

    def test_embedded_transcript_matches_are_origin_labelled_and_region_bounded(self):
        copied = json.dumps({
            "timestamp": "2026-09-15T01:00:00Z", "type": "event_msg",
            "payload": {"type": "agent_message", "message": "copiedneedle bridgeright"},
        })
        source = self.write("embedded-copy.jsonl", [{
            "type": "user", "sessionId": "embedded-copy",
            "message": {"role": "user", "content": "bridgeleft\n" + copied},
        }])
        sessiondb.ingest(self.db, "claude", source)
        copied_result = sessiondb.search_index(
            self.db, self._search_args(text="copiedneedle", mode="ranked"),
        )
        self.assertEqual(copied_result["total"], 1)
        self.assertEqual(copied_result["entries"][0]["match_origin"], "copied")
        self.assertIn("copiedneedle", copied_result["entries"][0]["passage"])
        crossed = sessiondb.search_index(
            self.db, self._search_args(text='"bridgeleft bridgeright"', mode="ranked"),
        )
        self.assertEqual(crossed["total"], 0)

    def test_plaintext_positional_vocabulary_contains_only_redacted_terms(self):
        source = self.write("positional-redaction.jsonl", [{
            "type": "user", "sessionId": "positional-redaction",
            "message": {"role": "user", "content": "visibleword api_key=topsecret"},
        }])
        sessiondb.ingest(self.db, "claude", source)
        self.db.execute(
            "CREATE VIRTUAL TABLE projection_vocab USING "
            "fts5vocab(positional_projection_fts, 'row')"
        )
        terms = {row[0] for row in self.db.execute("SELECT term FROM projection_vocab")}
        definition = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE name='positional_projection_fts'"
        ).fetchone()[0]
        self.assertIn("detail=column", definition)
        self.assertIn("visibleword", terms)
        self.assertIn("redacted", terms)
        self.assertNotIn("topsecret", terms)

    def test_literal_identifier_anchor_excludes_separated_words(self):
        source = self.write("identifier-anchor.jsonl", [
            {"type": "user", "sessionId": "identifier-anchor", "message": {
                "role": "user", "content": "projection_pack_generation",
            }},
            {"type": "user", "sessionId": "identifier-anchor", "message": {
                "role": "user", "content": "projection pack generation",
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        result = sessiondb.search_index(
            self.db, self._search_args(text="projection_pack_generation", mode="literal"),
        )
        self.assertEqual(result["total"], 1)
        self.assertIn("projection_pack_generation", result["entries"][0]["passage"])
        self.assertTrue(sessiondb.projection_query_matches(
            "projection_pack_generation", "projection_pack_generation", "literal",
        ))
        self.assertFalse(sessiondb.projection_query_matches(
            "projection pack generation", "projection_pack_generation", "literal",
        ))

    def test_literal_punctuated_anchor_preserves_punctuation(self):
        source = self.write("punctuated-anchor.jsonl", [
            {"type": "user", "sessionId": "punctuated-anchor", "message": {
                "role": "user", "content": "Use --dangerously-skip-permissions once",
            }},
            {"type": "user", "sessionId": "punctuated-anchor", "message": {
                "role": "user", "content": "dangerously skip permissions",
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        result = sessiondb.search_index(
            self.db, self._search_args(text="--dangerously-skip-permissions", mode="literal"),
        )
        self.assertEqual(result["total"], 1)
        self.assertTrue(sessiondb.projection_query_matches(
            "đơn hàng Make.com scenario", "đơn hang Make.com scenario", "literal",
        ))
        self.assertFalse(sessiondb.projection_query_matches(
            "đơn hàng MakeXcom scenario", "đơn hang Make.com scenario", "literal",
        ))
        for query, projection in (
            ("foo_bar", "foo_bar_baz"),
            ("foo-bar", "foo-bar-baz"),
            ("foo.bar", "foo.bar.baz"),
            ("--foo", "x--foo"),
            ("foo_bar", "x.foo_bar"),
        ):
            self.assertFalse(sessiondb.projection_query_matches(projection, query, "literal"))
        self.assertTrue(sessiondb.projection_query_matches(
            "khách_hàng", "khách_hang", "literal",
        ))
        self.assertTrue(sessiondb.projection_query_matches(
            "use khách_hàng now", "use khách_hang now", "literal",
        ))

    def test_structured_candidates_match_the_exact_token_predicate(self):
        source = self.write("structured-candidates.jsonl", [
            {"type": "user", "sessionId": "structured-candidates", "message": {
                "role": "user", "content": "foo_bar_baz",
            }},
            {"type": "user", "sessionId": "structured-candidates", "message": {
                "role": "user", "content": "khách_hàng",
            }},
            {"type": "user", "sessionId": "structured-candidates", "message": {
                "role": "user", "content": "use khách_hàng now",
            }},
            {"type": "user", "sessionId": "structured-candidates", "message": {
                "role": "user", "content": "foo_bar-baz",
            }},
            {"type": "user", "sessionId": "structured-candidates", "message": {
                "role": "user", "content": "khach_hang khách",
            }},
            {"type": "user", "sessionId": "structured-candidates", "message": {
                "role": "user", "content": "khach_hang khách hàng",
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        self.assertEqual(sessiondb.search_index(
            self.db, self._search_args(text="foo_bar", mode="literal"),
        )["total"], 0)
        self.assertEqual(sessiondb.search_index(
            self.db, self._search_args(text="khách_hang", mode="literal"),
        )["total"], 2)
        self.assertEqual(sessiondb.search_index(
            self.db, self._search_args(text="khách_hàng", mode="literal"),
        )["total"], 2)
        self.assertEqual(sessiondb.search_index(
            self.db, self._search_args(text="use khách_hang now", mode="literal"),
        )["total"], 1)
        self.assertEqual(sessiondb.search_index(
            self.db, self._search_args(text="foo_bar-baz", mode="literal"),
        )["total"], 1)

    def test_structured_candidates_normalize_nfc_and_nfd(self):
        source = self.write("structured-normalization.jsonl", [
            {"type": "user", "sessionId": "structured-normalization", "message": {
                "role": "user", "content": unicodedata.normalize("NFD", "khách_hàng"),
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        nfc = sessiondb.search_index(
            self.db, self._search_args(text="khách_hàng", mode="literal"),
        )
        nfd = sessiondb.search_index(
            self.db, self._search_args(
                text=unicodedata.normalize("NFD", "khách_hàng"), mode="literal",
            ),
        )
        self.assertEqual(nfc["total"], 1)
        self.assertEqual(nfd["total"], 1)
        self.assertEqual(nfc["entries"][0]["result_id"], nfd["entries"][0]["result_id"])

    def test_structured_literal_fast_path_skips_full_projection_scan(self):
        source = self.write("structured-fast-path.jsonl", [
            {"type": "user", "sessionId": "structured-fast-path", "message": {
                "role": "user",
                "content": "--dangerously-skip-permissions " + (str(index) * 50_000),
            }}
            for index in range(12)
        ])
        sessiondb.ingest(self.db, "claude", source)
        with mock.patch.object(
            sessiondb, "matching_projection_regions",
            side_effect=AssertionError("structured fast path scanned complete projections"),
        ):
            result = sessiondb.search_index(
                self.db,
                self._search_args(text="--dangerously-skip-permissions", mode="literal"),
            )
        self.assertEqual(result["total"], 12)
        self.assertEqual(result["telemetry"]["source_reads"], 0)
        self.assertEqual(result["telemetry"]["candidate_projections"], 12)

    def test_structured_literal_anchor_applies_per_term_vietnamese_accents(self):
        source = self.write("structured-accents.jsonl", [
            {"type": "user", "sessionId": "structured-accents", "message": {
                "role": "user", "content": "báo-giá",
            }},
            {"type": "user", "sessionId": "structured-accents", "message": {
                "role": "user", "content": "bao-gia",
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        exact = sessiondb.search_index(
            self.db, self._search_args(text="báo-giá", mode="literal"),
        )
        folded = sessiondb.search_index(
            self.db, self._search_args(text="bao-gia", mode="literal"),
        )
        self.assertEqual(exact["total"], 1)
        self.assertEqual(folded["total"], 2)

    def test_compact_relevance_prefers_concise_and_tight_matches_without_bm25(self):
        source = self.write("compact-ranking.jsonl", [
            {"type": "user", "sessionId": "compact-ranking", "message": {
                "role": "user", "content": "needle " + ("noise " * 100),
            }},
            {"type": "user", "sessionId": "compact-ranking", "message": {
                "role": "user", "content": "needle needle needle needle",
            }},
            {"type": "user", "sessionId": "compact-ranking", "message": {
                "role": "user", "content": "needle",
            }},
            {"type": "user", "sessionId": "compact-ranking", "message": {
                "role": "user", "content": "alpha " + ("distant " * 20) + "beta",
            }},
            {"type": "user", "sessionId": "compact-ranking", "message": {
                "role": "user", "content": "alpha beta",
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        single = sessiondb.search_index(
            self.db, self._search_args(text="needle", mode="literal"),
        )
        self.assertTrue(single["entries"][0]["passage"].endswith("needle"))
        self.assertTrue(all(entry["rank"]["fts"] == 0 for entry in single["entries"]))
        near = sessiondb.search_index(
            self.db, self._search_args(text="alpha NEAR/50 beta", mode="ranked"),
        )
        self.assertIn("alpha beta", near["entries"][0]["passage"])
        self.assertLess(
            near["entries"][0]["rank"]["term_span"],
            near["entries"][1]["rank"]["term_span"],
        )

    def test_ranked_relevance_moves_same_session_repeats_after_distinct_results(self):
        rows = [
            {"type": "agent-name", "agentName": "alpha beta tiếng việt",
             "sessionId": "repeat-ranking"},
        ] * 5 + [
            {"type": "user", "sessionId": "repeat-ranking", "message": {
                "role": "user", "content": "alpha beta tiếng việt repeated note",
            }},
        ] * 3 + [
            {"type": "user", "sessionId": "repeat-ranking", "message": {
                "role": "user",
                "content": "alpha beta tiếng việt distinct discussion " + ("detail " * 10),
            }},
        ]
        sessiondb.ingest(self.db, "claude", self.write("repeat-ranking.jsonl", rows))
        # The ASCII query takes the fast path; the accented one takes the verified path.
        for text in ("alpha beta", "tiếng việt"):
            with self.subTest(text=text):
                result = sessiondb.search_index(
                    self.db, self._search_args(text=text, mode="ranked"),
                )
                self.assertEqual(result["total"], 9)
                head = result["entries"][:3]
                self.assertEqual([entry["category"] for entry in head].count("agent-name"), 1)
                self.assertTrue(any("distinct" in entry["passage"] for entry in head))
                self.assertEqual(
                    sum("repeated note" in entry["passage"] for entry in head), 1,
                )

    def test_earlier_sessiondb_searches_rank_after_what_they_found(self):
        rows = [
            {"type": "assistant", "sessionId": "echo", "message": {"role": "assistant", "content": [
                {"type": "tool_use", "id": "t1", "name": "Bash",
                 "input": {"command": "sessiondb --db x search-index --text 'invoice email'"}}]}},
            {"type": "user", "sessionId": "real", "message": {"role": "user", "content": (
                "The monthly invoice email went out on the sixth with the paid receipt attached for review")}},
            {"type": "user", "sessionId": "real-vn", "message": {"role": "user", "content": (
                "Gửi invoice email cho khách hàng mỗi tháng kèm hóa đơn đã thanh toán để đối chiếu")}},
        ]
        sessiondb.ingest(self.db, "claude", self.write("echo.jsonl", rows))
        for text in ("invoice email", "invoice email khách"):
            with self.subTest(text=text):
                entries = sessiondb.search_index(self.db, self._search_args(text=text, mode="ranked"))["entries"]
                self.assertNotEqual(entries[0]["session"], "echo")
                self.assertTrue(all("_event_rowid" not in entry for entry in entries))
        plain = sessiondb.search_index(self.db, self._search_args(text="invoice email", mode="ranked"))["entries"]
        self.assertEqual(plain[-1]["session"], "echo")
        about = sessiondb.search_index(self.db, self._search_args(text="sessiondb invoice", mode="ranked"))
        self.assertEqual([entry["session"] for entry in about["entries"]], ["echo"])

    def test_online_index_rebuild_keeps_serving_and_catches_concurrent_changes(self):
        def rows(session, words, count):
            return [{"type": "user", "sessionId": session, "message": {"role": "user",
                     "content": f"{words} needle item {index} " + "pad " * index}} for index in range(count)]
        kept = self.write("kept.jsonl", rows("kept", "alpha beta", 40))
        rewritten = self.write("rewritten.jsonl", rows("rewritten", "stale words", 20))
        sessiondb.ingest(self.db, "claude", kept)
        sessiondb.ingest(self.db, "claude", rewritten)
        writer = sessiondb.connect(self.db_path)
        calls = []
        def concurrent_writer():
            calls.append(1)
            # Mid-build, a watcher-like writer adds a source and rewrites another.
            if len(calls) == 1:
                sessiondb.ingest(writer, "claude", self.write("added.jsonl", rows("added", "gamma delta", 10)))
            if len(calls) == 2:
                rewritten.write_text("".join(json.dumps(row) + "\n" for row in rows("rewritten", "fresh words", 5)))
                sessiondb.ingest(writer, "claude", rewritten)
            # Searches keep answering from the serving index during the build.
            self.assertTrue(sessiondb.search_index(writer, self._search_args(text="needle", mode="ranked"))["complete"])
        result = sessiondb.rebuild_search_fts_online(
            self.db, "full", chunk_seconds=0.0, pause_seconds=0.0, between_chunks=concurrent_writer,
        )
        self.assertGreaterEqual(len(calls), 3)
        self.assertGreater(result["changed_during_build"], 0)
        self.assertEqual(result["source_reads"], 0)
        self.assertTrue(sessiondb.fts_index_is_positional(self.db))
        leftovers = self.db.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'search_fts_rebuild%' OR name=?",
            (sessiondb.SEARCH_FTS_NEXT,),
        ).fetchall()
        self.assertEqual(leftovers, [])
        online = {text: sorted(entry["result_id"] for entry in sessiondb.search_index(
            self.db, self._search_args(text=text, mode="ranked"))["entries"])
            for text in ("needle", "stale words", "fresh words", "gamma delta")}
        self.assertEqual(len(online["stale words"]), 0)
        self.assertEqual((len(online["fresh words"]), len(online["gamma delta"])), (5, 10))
        sessiondb.rebuild_search_fts(self.db, "full")
        offline = {text: sorted(entry["result_id"] for entry in sessiondb.search_index(
            self.db, self._search_args(text=text, mode="ranked"))["entries"]) for text in online}
        self.assertEqual(online, offline)
        # Deleting an event still removes it from the swapped-in index.
        sessiondb.ingest(self.db, "claude", self.write("added.jsonl", rows("added", "gamma delta", 3)))
        self.assertEqual(len(sessiondb.search_index(
            self.db, self._search_args(text="gamma delta", mode="ranked"))["entries"]), 3)
        writer.close()

    def test_failed_online_swap_leaves_the_serving_index_and_can_rerun(self):
        sessiondb.ingest(self.db, "claude", self.write("safe.jsonl", [
            {"type": "user", "sessionId": "safe", "message": {"role": "user", "content": "safe needle"}},
        ]))
        with mock.patch.object(sessiondb, "refresh_projection_metadata", side_effect=ValueError("boom")):
            with self.assertRaises(ValueError):
                sessiondb.rebuild_search_fts_online(self.db, "full")
        self.assertFalse(sessiondb.fts_index_is_positional(self.db))
        self.assertEqual(sessiondb.search_index(self.db, self._search_args(text="needle", mode="ranked"))["total"], 1)
        sessiondb.rebuild_search_fts_online(self.db, "full")
        self.assertTrue(sessiondb.fts_index_is_positional(self.db))
        self.assertEqual(sessiondb.search_index(self.db, self._search_args(text="needle", mode="ranked"))["total"], 1)
        sessiondb.build_search_projections(self.db)
        self.assertTrue(sessiondb.fts_index_is_positional(self.db))

    def test_fast_accent_fold_matches_the_reference(self):
        samples = [
            "", "Plain ASCII Text 123", "Tiếng Việt có dấu: báo giá khách hàng", "ĐẶNG đặng Ðđ",
            unicodedata.normalize("NFD", "Tiếng Việt"), "Straße ǅ ﬁ İstanbul", "cafe\u0301 x\u0308y",
            "\ue200cite\ue202101\u2020DESIGN.md\ue201", "日本語 한국어 العربية", "emoji 👍🏽 ok",
            "".join(chr(code) for code in range(0x0300, 0x0370)),
        ]
        samples += ["".join(chr(code) for code in range(start, start + 400)) for start in range(0, 0x3000, 400)]
        for value in samples:
            self.assertEqual(sessiondb._accent_fold_slow(value), sessiondb._accent_fold_reference(value), repr(value[:40]))

    def test_positional_index_ranks_closer_words_first_with_identical_results(self):
        texts = [
            "alpha " + "spread " * 30 + "beta " + "spread " * 30 + "gamma",
            "alpha ttt uuu aaa beta eee uuu gamma",
            "alpha ttt beta gamma",
            "alpha beta gamma",
            "gamma beta alpha reversed order",
            "only alpha and beta here",
            # Codex web citations wrap names in private-use characters.
            "L180: \ue200cite\ue202101\u2020DESIGN.md\ue201 |",
        ]
        rows = [
            {"type": "user", "sessionId": f"close-{index}", "message": {"role": "user", "content": value}}
            for index, value in enumerate(texts)
        ]
        sessiondb.ingest(self.db, "claude", self.write("closeness.jsonl", rows))
        queries = ("alpha beta gamma", '"alpha beta" gamma', '"beta gamma"', '"Design.md"', "md")
        column = {query: sessiondb.search_index(self.db, self._search_args(text=query, mode="ranked"))
                  for query in queries}
        before_snapshot = sessiondb.projection_snapshot_id(self.db)
        rebuilt = sessiondb.rebuild_search_fts(self.db, "full")
        self.assertEqual((rebuilt["events"], rebuilt["source_reads"]), (len(texts), 0))
        self.assertTrue(sessiondb.fts_index_is_positional(self.db))
        self.assertEqual(sessiondb.projection_snapshot_id(self.db), before_snapshot)
        full = {query: sessiondb.search_index(self.db, self._search_args(text=query, mode="ranked"))
                for query in queries}
        self.assertEqual([entry["session"] for entry in column['"Design.md"']["entries"]], ["close-6"])
        self.assertEqual([entry["session"] for entry in column["md"]["entries"]], ["close-6"])
        for query in queries:
            self.assertTrue(full[query]["complete"], query)
            self.assertEqual(
                sorted(entry["result_id"] for entry in full[query]["entries"]),
                sorted(entry["result_id"] for entry in column[query]["entries"]), query,
            )
        order = [int(entry["session"].split("-")[1]) for entry in full["alpha beta gamma"]["entries"]]
        # Exact phrase, then adjacent words in any order, then a wider gap, then far apart.
        self.assertEqual((order[0], set(order[1:3]), order[3], order[-1]), (3, {2, 4}, 1, 0))
        self.assertEqual(
            [entry["rank"]["closeness_tier"] for entry in full["alpha beta gamma"]["entries"]],
            [0, 1, 1, 2, sessiondb.CLOSENESS_UNRANKED],
        )
        sessiondb.rebuild_search_fts(self.db, "column")
        self.assertFalse(sessiondb.fts_index_is_positional(self.db))
        restored = sessiondb.search_index(self.db, self._search_args(text="alpha beta gamma", mode="ranked"))
        self.assertEqual(
            [entry["result_id"] for entry in restored["entries"]],
            [entry["result_id"] for entry in column["alpha beta gamma"]["entries"]],
        )

    def test_compact_relevance_uses_smallest_near_window_and_nfc_coordinates(self):
        source = self.write("compact-ranking-windows.jsonl", [
            {"type": "user", "sessionId": "compact-ranking-windows", "message": {
                "role": "user",
                "content": "alpha beta " + ("filler " * 20) + "alpha beta",
            }},
            {"type": "user", "sessionId": "compact-ranking-windows", "message": {
                "role": "user", "content": "alpha x beta",
            }},
            {"type": "user", "sessionId": "compact-ranking-windows", "message": {
                "role": "user",
                "content": unicodedata.normalize("NFD", "Tiếng Việt ") + "alpha beta",
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        near = sessiondb.search_index(
            self.db, self._search_args(text="alpha NEAR/1 beta", mode="ranked"),
        )
        self.assertNotIn("alpha x beta", near["entries"][0]["passage"])
        self.assertLess(
            near["entries"][0]["rank"]["term_span"],
            next(entry for entry in near["entries"] if "alpha x beta" in entry["passage"])["rank"]["term_span"],
        )
        normalized = unicodedata.normalize("NFC", "Tiếng Việt ") + "alpha beta"
        spans = sessiondb.ranking_search_spans(
            sessiondb.parse_search_query("alpha NEAR/1 beta"), normalized,
        )
        self.assertEqual(normalized[spans[0][0]:spans[-1][1]], "alpha beta")
        or_spans = sessiondb.ranking_search_spans(
            sessiondb.parse_search_query('"alpha beta" OR beta'), normalized,
        )
        self.assertEqual(normalized[or_spans[0][0]:or_spans[-1][1]], "beta")

    def test_record_stream_is_source_independent_redacted_and_resumable(self):
        source = self.write("stream.jsonl", [
            {"type": "user", "sessionId": "stream", "message": {
                "role": "user", "content": "visible", "api_key": "secret-value",
            }},
        ])
        sessiondb.ingest(self.db, "claude", source)
        source.unlink()
        output = self.root / "records.jsonl"
        args = argparse.Namespace(
            provider=None, project=None, session=None, role=None, tool=None, lineage=None,
            since=None, until=None, category=None, after_rowid=0, snapshot_id=None,
            snapshot_only=True, exact=False,
        )
        result = sessiondb.write_record_stream(self.db, output, args)
        self.assertTrue(result["complete"])
        self.assertEqual(result["source_reads"], 0)
        rebuilt = sessiondb.build_search_projections(self.db)
        self.assertEqual(rebuilt["source_reads"], 0)
        searched = sessiondb.search_index(
            self.db, self._search_args(text="visible", mode="ranked"),
        )
        self.assertEqual(searched["total"], 1)
        values = [json.loads(line) for line in output.read_text().splitlines()]
        record = next(item["record"] for item in values if "record" in item)
        self.assertNotIn("secret-value", json.dumps(record))
        live_args = argparse.Namespace(**vars(args))
        live_args.snapshot_only = False
        live_result = sessiondb.write_record_stream(
            self.db, self.root / "records-live-coverage.jsonl", live_args,
        )
        self.assertFalse(live_result["complete"])
        self.assertEqual(live_result["coverage"]["scope"], "live_corpus")
        resumed = self.root / "records-resumed.jsonl"
        args.after_rowid = result["last_rowid"]
        with self.assertRaisesRegex(ValueError, "snapshot-id"):
            sessiondb.write_record_stream(self.db, resumed, args)
        args.snapshot_id = result["snapshot_id"]
        self.assertEqual(sessiondb.write_record_stream(self.db, resumed, args)["records"], 0)

    def test_record_stream_discloses_unreadable_records_and_preserves_exact_bytes(self):
        source = self.root / "stream-unreadable.jsonl"
        source.write_bytes(b"\xff invalid\n")
        sessiondb.ingest(self.db, "claude", source)
        source.unlink()
        output = self.root / "unreadable-records.jsonl"
        args = argparse.Namespace(
            provider=None, project=None, session=None, role=None, tool=None, lineage=None,
            since=None, until=None, category=None, after_rowid=0, snapshot_id=None,
            snapshot_only=True, exact=True,
        )
        result = sessiondb.write_record_stream(self.db, output, args)
        self.assertFalse(result["complete"])
        self.assertEqual(result["coverage"]["unreadable_records"], 1)
        values = [json.loads(line) for line in output.read_text().splitlines()]
        record = next(item["record"] for item in values if "record" in item)
        retained = base64.b64decode(record["structure"]["unreadable_bytes_base64"])
        self.assertEqual(retained, b"\xff invalid\n")

    def _live_status(self, roots, **overrides):
        status = {
            "version": 1, "pass": True, "coverage_complete": True,
            "coverage_complete_except_drift": True,
            "discovery_roots": {"claude": [str(root) for root in roots]},
            "generated_at": "2026-08-21T10:01:00Z", "reason": "filesystem_event",
            "issues": [], "hard_failures": [], "sources_drifted_after_sync": [],
        }
        status.update(overrides)
        status_dir = self.root / "freshness"
        status_dir.mkdir(exist_ok=True)
        (status_dir / "watcher_status.json").write_text(json.dumps(status))

    def _live_source(self, name, rows):
        transcripts = self.root / "live-transcripts"
        transcripts.mkdir(exist_ok=True)
        path = transcripts / name
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        sessiondb.ingest(self.db, "claude", path)
        return transcripts, path

    def test_idle_live_checked_status_keeps_ranked_search_complete(self):
        transcripts, _source = self._live_source("idle.jsonl", [
            {"type": "user", "sessionId": "idle", "message": {"role": "user", "content": "idle needle"}},
        ])
        self._live_status([transcripts])
        result = sessiondb.search_index_entrypoint(self.db_path, self._search_args())
        self.assertTrue(result["complete"])
        self.assertEqual(result["total"], 1)
        coverage = sessiondb.query_coverage(self.db, self._search_args())
        self.assertTrue(coverage["live_checked"])
        self.assertEqual(coverage["reason"], "live_sources_current")
        self.assertGreater(coverage["age_seconds"], sessiondb.ACTIVE_DRIFT_GRACE_SECONDS)
        (self.root / "freshness" / "watcher_status.json").write_text(json.dumps({
            "version": 1, "pass": True, "coverage_complete": True,
            "generated_at": "2026-08-21T10:01:00Z", "reason": "filesystem_event",
            "issues": [], "hard_failures": [], "sources_drifted_after_sync": [],
        }))
        legacy = sessiondb.search_index_entrypoint(self.db_path, self._search_args())
        self.assertFalse(legacy["complete"])
        self.assertEqual(legacy["reason"], "watcher_status_stale")

    def test_active_tail_is_served_inside_the_answer(self):
        transcripts, source = self._live_source("active.jsonl", [
            {"type": "user", "sessionId": "active", "timestamp": "2026-08-21T10:00:00Z",
             "message": {"role": "user", "content": "tail needle"}},
        ])
        indexed_bytes = source.stat().st_size
        with source.open("a") as handle:
            handle.write(json.dumps({
                "type": "assistant", "sessionId": "active", "timestamp": "2026-08-21T10:01:00Z",
                "message": {"role": "assistant", "content": "unindexed needle"},
            }) + "\n")
            # A line still being written is not an event yet.
            handle.write('{"type": "assistant", "sessionId": "active", "message": {"content": "half needle')
        self._live_status([transcripts])
        result = sessiondb.search_index_entrypoint(self.db_path, self._search_args())
        self.assertTrue(result["complete"], result.get("coverage"))
        self.assertEqual(result["total"], 2)
        tail = [entry for entry in result["entries"] if entry.get("live_tail")]
        self.assertEqual(len(tail), 1)
        self.assertIn("unindexed needle", tail[0]["passage"])
        self.assertEqual((tail[0]["session"], tail[0]["source"]["line"]), ("active", 2))
        coverage = result["coverage"]
        self.assertEqual((coverage["status"], coverage["reason"]), ("complete", "live_tails_served"))
        self.assertNotIn("affected_sources", coverage)
        complete_size = source.read_bytes().rfind(b"\n") + 1
        self.assertEqual(coverage["tails_served"], [{
            "path": str(source.resolve()), "provider": "claude", "from_byte": indexed_bytes,
            "to_byte": complete_size, "events": 1, "reason": "active_source_tail",
        }])
        delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)
        self.assertTrue(delivered["complete"])
        self.assertEqual(delivered["coverage"]["reason"], "live_tails_served")
        self.assertEqual(sum(1 for entry in delivered["entries"] if entry.get("live_tail")), 1)
        ordered = sessiondb.search_index_entrypoint(
            self.db_path, self._search_args(order="chronological"),
        )
        self.assertEqual([entry["source"]["line"] for entry in ordered["entries"]], [1, 2])
        other = sessiondb.search_index_entrypoint(self.db_path, self._search_args(session="elsewhere"))
        self.assertTrue(other["complete"])
        self.assertEqual(other["total"], 0)
        # Any other watcher gap, such as an unfinished migration, still withholds completeness.
        self._live_status([transcripts], coverage_complete_except_drift=False)
        gap = sessiondb.search_index_entrypoint(self.db_path, self._search_args())
        self.assertFalse(gap["complete"])
        self.assertEqual(gap["indexed_total"], 1)

    def test_tail_merge_keeps_the_indexed_order_and_its_passages(self):
        def entry(line, size, passage, path="/indexed.jsonl", **extra):
            return {"result_id": f"r{line}", "provider": "claude", "session": "s", "order_timestamp": None,
                    "source": {"path": path, "line": line, "hash": str(line)}, "match_origin": "native",
                    "rank": {"phrase_hits": 0, "term_span": 0, "document_bytes": size, "fts": 0.0},
                    "passage": passage, **extra}
        # A demoted echo (size 10) sits last and was never given a passage.
        indexed = [entry(1, 50, "first"), entry(2, 90, "second"), entry(3, 10, None)]
        tail = [entry(9, 60, "tail", path="/active.jsonl", live_tail=True)]
        merged = sessiondb.merge_live_tail_entries(indexed, tail, set(), "relevance", False)
        self.assertEqual([item["result_id"] for item in merged], ["r1", "r9", "r2", "r3"])
        rewritten = sessiondb.merge_live_tail_entries(indexed, tail, {"/indexed.jsonl"}, "relevance", False)
        self.assertEqual([item["result_id"] for item in rewritten], ["r9"])

    def _clean_args(self, **overrides):
        values = dict(text="needle", provider=None, project=None, session=None, role=None, tool=None,
                      lineage=None, since=None, until=None, raw=False, limit=50, offset=0,
                      before=0, after=0, include_page=False, include_hashes=False)
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_clean_query_serves_active_tails_exactly_as_indexed(self):
        transcripts, source = self._live_source("clean.jsonl", [
            {"type": "user", "sessionId": "clean", "timestamp": "2026-08-21T10:00:00Z",
             "message": {"role": "user", "content": "indexed needle"}},
        ])
        appended = {"type": "assistant", "sessionId": "clean", "timestamp": "2026-08-21T10:01:00Z",
                    "message": {"role": "assistant", "content": [{"type": "text", "text": "tail needle here"}]}}
        with source.open("a") as handle:
            handle.write(json.dumps(appended) + "\n")
            handle.write('{"type": "user", "sessionId": "clean", "message": {"content": "half needle')
        self._live_status([transcripts])
        served = sessiondb.query_clean(self.db, self._clean_args())
        self.assertTrue(served["coverage"]["complete"], served["coverage"])
        self.assertEqual(served["coverage"]["reason"], "live_tails_served")
        self.assertEqual(len(served["events"]), 2)
        self.assertEqual([event.get("live_tail") for event in served["events"]], [None, True])
        # Once indexed, the same line yields the same clean event.
        source.write_text("".join(
            line + "\n" for line in source.read_text().splitlines()[:2]
        ))
        sessiondb.ingest(self.db, "claude", source)
        indexed = sessiondb.query_clean(self.db, self._clean_args())
        self.assertTrue(indexed["coverage"]["complete"])
        strip = lambda result: [{k: v for k, v in event.items() if k != "live_tail"} for event in result["events"]]
        self.assertEqual(strip(served), strip(indexed))
        self.assertEqual(served["objects"], indexed["objects"])
        self.assertEqual(served["sources"], indexed["sources"])

    def test_clean_query_reports_tails_it_cannot_fit(self):
        transcripts, source = self._live_source("fit.jsonl", [
            {"type": "user", "sessionId": "fit", "message": {"role": "user", "content": "first needle"}},
        ])
        with source.open("a") as handle:
            for index in range(3):
                handle.write(json.dumps({"type": "user", "sessionId": "fit",
                                         "message": {"role": "user", "content": f"more needle {index}"}}) + "\n")
        self._live_status([transcripts])
        for overrides in ({"limit": 2}, {"before": 1}, {"offset": 1}):
            with self.subTest(**overrides):
                result = sessiondb.query_clean(self.db, self._clean_args(**overrides))
                self.assertFalse(result["coverage"]["complete"])
                self.assertEqual(result["coverage"]["status"], "catching_up")
                self.assertTrue(all(not event.get("live_tail") for event in result["events"]))
        whole = sessiondb.query_clean(self.db, self._clean_args())
        self.assertTrue(whole["coverage"]["complete"])
        self.assertEqual(len(whole["events"]), 4)

    def test_unservable_active_tail_keeps_verified_results_and_names_the_tail(self):
        transcripts, source = self._live_source("active.jsonl", [
            {"type": "user", "sessionId": "active", "message": {"role": "user", "content": "tail needle"}},
        ])
        indexed_bytes = source.stat().st_size
        with source.open("a") as handle:
            handle.write(json.dumps({
                "type": "assistant", "sessionId": "active",
                "message": {"role": "assistant", "content": "unindexed needle"},
            }) + "\n")
        self._live_status([transcripts])
        with mock.patch.object(sessiondb, "LIVE_TAIL_MAX_BYTES", 10):
            result = sessiondb.search_index_entrypoint(self.db_path, self._search_args())
        self.assertFalse(result["complete"])
        self.assertIsNone(result["total"])
        self.assertEqual(result["indexed_total"], 1)
        self.assertEqual(len(result["entries"]), 1)
        coverage = result["coverage"]
        self.assertEqual(coverage["status"], "catching_up")
        self.assertEqual(coverage["affected_sources"], [{
            "path": str(source.resolve()), "provider": "claude",
            "indexed_bytes": indexed_bytes, "reason": "active_source_tail",
        }])
        with mock.patch.object(sessiondb, "LIVE_TAIL_MAX_BYTES", 10):
            delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)
        self.assertFalse(delivered["complete"])
        self.assertEqual(delivered["indexed_total"], 1)
        self.assertEqual(delivered["coverage"], coverage)
        page = sessiondb.result_bundle_page(Path(delivered["bundle"]["path"]), argparse.Namespace(
            offset=0, limit=10, provider=None, project=None, session=None, role=None, tool=None,
        ))
        self.assertFalse(page["complete"])
        self.assertEqual(page["coverage"], coverage)
        # A changed indexed prefix is a rewrite the tail cannot be trusted for.
        with mock.patch.object(sessiondb, "digest", side_effect=lambda value: "changed"):
            entries, served, failures = sessiondb.serve_live_tails(
                self.db, coverage["affected_sources"], self._search_args(), time.monotonic() + 30,
            )
        self.assertEqual((entries, served), ([], []))
        self.assertEqual(failures[0]["serve_failure"], "indexed_prefix_changed")

    def test_rewritten_and_new_sources_are_served_whole(self):
        transcripts, source = self._live_source("rewrite.jsonl", [
            {"type": "user", "sessionId": "rewrite", "message": {"role": "user", "content": "stale needle"},
             "padding": "x" * 200},
        ])
        source.write_text(json.dumps(
            {"type": "user", "sessionId": "rewrite", "message": {"role": "user", "content": "fresh needle"}},
        ) + "\n", encoding="utf-8")
        (transcripts / "new.jsonl").write_text(json.dumps(
            {"type": "user", "sessionId": "new", "message": {"role": "user", "content": "new needle"}},
        ) + "\n", encoding="utf-8")
        self._live_status([transcripts])
        result = sessiondb.search_index_entrypoint(self.db_path, self._search_args())
        self.assertTrue(result["complete"], result.get("coverage"))
        passages = sorted(entry["passage"] for entry in result["entries"])
        self.assertEqual(len(passages), 2)
        self.assertTrue(any("fresh needle" in value for value in passages))
        self.assertTrue(any("new needle" in value for value in passages))
        self.assertFalse(any("stale needle" in value for value in passages))

    def test_codex_tail_resumes_saved_parser_state(self):
        transcripts = self.root / "codex-live"
        transcripts.mkdir()
        source = transcripts / "rollout.jsonl"
        source.write_text("".join(json.dumps(row) + "\n" for row in [
            {"timestamp": "2026-08-21T10:00:00Z", "type": "session_meta",
             "payload": {"id": "codex-live-session", "cwd": "/Users/example/project"}},
            {"timestamp": "2026-08-21T10:00:01Z", "type": "event_msg",
             "payload": {"type": "user_message", "message": "codex indexed needle"}},
        ]), encoding="utf-8")
        sessiondb.ingest(self.db, "codex", source)
        with source.open("a") as handle:
            handle.write(json.dumps({
                "timestamp": "2026-08-21T10:01:00Z", "type": "event_msg",
                "payload": {"type": "agent_message", "message": "codex tail needle"},
            }) + "\n")
        self._live_status([], discovery_roots={"codex": [str(transcripts)]})
        result = sessiondb.search_index_entrypoint(self.db_path, self._search_args())
        self.assertTrue(result["complete"], result.get("coverage"))
        indexed = next(entry for entry in result["entries"] if not entry.get("live_tail"))
        tail = next(entry for entry in result["entries"] if entry.get("live_tail"))
        self.assertEqual(tail["session"], indexed["session"])
        self.assertEqual(tail["project"], indexed["project"])

    def test_live_check_flags_new_rewritten_persistent_and_incomplete_watcher_state(self):
        transcripts, source = self._live_source("rewrite.jsonl", [
            {"type": "user", "sessionId": "rewrite", "message": {"role": "user", "content": "first needle"},
             "padding": "x" * 200},
        ])
        fresh = transcripts / "fresh.jsonl"
        fresh.write_text(json.dumps({"type": "user", "sessionId": "fresh"}) + "\n", encoding="utf-8")
        self._live_status([transcripts])
        coverage = sessiondb.query_coverage(self.db, self._search_args(session="rewrite"))
        self.assertEqual(coverage["status"], "catching_up")
        self.assertEqual(
            [(item["path"], item["indexed_bytes"], item["reason"]) for item in coverage["affected_sources"]],
            [(str(fresh.resolve()), 0, "active_unindexed_source")],
        )
        source.write_text(json.dumps({"type": "user", "sessionId": "other"}) + "\n", encoding="utf-8")
        coverage = sessiondb.query_coverage(self.db, self._search_args(session="rewrite"))
        rewritten = [item for item in coverage["affected_sources"] if item["path"] == str(source.resolve())]
        self.assertEqual(rewritten[0]["indexed_bytes"], 0)
        self.assertEqual(rewritten[0]["reason"], "active_source_rewrite")
        old = time.time() - sessiondb.ACTIVE_DRIFT_GRACE_SECONDS - 10
        os.utime(source, (old, old))
        coverage = sessiondb.query_coverage(self.db, self._search_args())
        self.assertEqual(coverage["status"], "incomplete")
        self.assertIn("persistent_source_rewrite", " ".join(coverage["issues"]))
        self.assertEqual(
            sessiondb.query_coverage(self.db, self._search_args(provider="codex"))["status"],
            "complete",
        )
        self._live_status([transcripts], coverage_complete_except_drift=False, reason="startup")
        coverage = sessiondb.query_coverage(self.db, self._search_args(provider="codex"))
        self.assertEqual(coverage["status"], "incomplete")
        self.assertIn("watcher sync incomplete: startup", coverage["issues"])

    def test_watcher_publishes_live_check_inputs(self):
        report_path = self.root / "live-report.json"
        old_roots = full_corpus.ROOTS
        transcripts, _source = self._live_source("published.jsonl", [
            {"type": "user", "sessionId": "published", "message": {"role": "user", "content": "x"}},
        ])
        try:
            full_corpus.ROOTS = {"claude": (transcripts,)}
            report = full_corpus.fast_sync(self.db_path, report_path)
        finally:
            full_corpus.ROOTS = old_roots
        self.assertEqual(report["discovery_roots"], {"claude": [str(transcripts)]})
        self.assertTrue(report["complete_except_drift"])


    def test_folded_span_mapper_matches_single_span_mapping(self):
        def reference(text: str, start: int, end: int) -> tuple[int, int]:
            folded_offset = 0
            original_start = 0
            for index, char in enumerate(text):
                folded_char = "".join(
                    value for value in unicodedata.normalize("NFD", char.casefold())
                    if not unicodedata.combining(value)
                )
                next_offset = folded_offset + len(folded_char)
                if folded_offset <= start < next_offset:
                    original_start = index
                if next_offset >= end:
                    return original_start, index + 1
                folded_offset = next_offset
            return original_start, len(text)

        texts = ["", "API disconnect", "Đã ĐI qua ﬁle straße", "é café API", "日本 ǅ İ x"]
        for text in texts:
            mapper = sessiondb._folded_span_mapper(text)
            folded = sum(len(sessiondb._accent_fold(char)) for char in text)
            for start in range(folded + 2):
                for end in range(start, folded + 3):
                    self.assertEqual(
                        mapper(start, end), reference(text, start, end),
                        (text, start, end),
                    )

    def test_result_bundle_directory_skips_chmod_when_already_private(self):
        directory = self.db_path.parent / "retrieval-results"
        directory.mkdir(mode=0o700)
        os.chmod(directory, 0o700)
        with mock.patch.object(sessiondb.os, "chmod", side_effect=PermissionError("sandbox")) as chmod:
            path = sessiondb.automatic_result_bundle_path(self.db_path, {"query": "q"})
        chmod.assert_not_called()
        self.assertEqual(path.parent, directory.resolve())
        os.chmod(directory, 0o755)
        sessiondb.automatic_result_bundle_path(self.db_path, {"query": "q"})
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)

    def test_oversized_clean_query_returns_bounded_preview_and_exact_bundle(self):
        source = self.write("large.jsonl", [
            {"type": "user", "sessionId": "large", "message": {
                "role": "user", "content": f"large needle {index} " + ("word " * 12000),
            }}
            for index in range(3)
        ])
        sessiondb.ingest(self.db, "claude", source)
        args = argparse.Namespace(
            text="large needle", provider=None, project=None, session=None, role=None,
            tool=None, lineage=None, since=None, until=None, raw=False, limit=50,
        )
        full = sessiondb.query_clean(self.db, args)
        self.assertGreater(sessiondb._encoded_size(full), sessiondb.QUERY_INLINE_MAX_BYTES)
        delivered = sessiondb.deliver_clean_query(self.db_path, full)
        self.assertLessEqual(sessiondb._encoded_size(delivered), sessiondb.QUERY_INLINE_MAX_BYTES)
        self.assertTrue(delivered["preview"]["lossy"])
        self.assertEqual(delivered["coverage"], full["coverage"])
        self.assertTrue(all(len(item["text"]) <= sessiondb.QUERY_PREVIEW_OBJECT_CHARS
                            for item in delivered["objects"] if item))
        lines = Path(delivered["bundle"]["path"]).read_text(encoding="utf-8").splitlines()
        self.assertEqual(json.loads(lines[0])["header"]["format"], "sessiondb-query-bundle-v1")
        self.assertEqual(json.loads(lines[1])["payload"], json.loads(json.dumps(full)))
        small = {"sources": [], "events": [], "coverage": "missing"}
        self.assertIs(sessiondb.deliver_clean_query(self.db_path, small), small)
        with mock.patch.object(sessiondb, "write_query_bundle", side_effect=OSError("read-only")):
            failed = sessiondb.deliver_clean_query(self.db_path, full)
        self.assertFalse(failed["complete"])
        self.assertIn("read-only", failed["reason"])
        self.assertLessEqual(sessiondb._encoded_size(failed), sessiondb.QUERY_INLINE_MAX_BYTES)

    def test_ranked_bundle_write_failure_returns_previews_instead_of_crashing(self):
        source = self.write("bundle-failure.jsonl", [
            {"type": "user", "sessionId": "bundle", "message": {"role": "user", "content": "bundle needle"}},
        ])
        sessiondb.ingest(self.db, "claude", source)
        result = sessiondb.search_index(self.db, self._search_args())
        with mock.patch.object(sessiondb, "write_result_bundle", side_effect=PermissionError("sandbox")):
            delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)
        self.assertFalse(delivered["complete"])
        self.assertIn("sandbox", delivered["delivery_error"])
        self.assertEqual(len(delivered["entries"]), 1)

    def test_live_check_treats_larger_replacement_and_unreadable_source_as_whole_file(self):
        transcripts, source = self._live_source("replaced.jsonl", [
            {"type": "user", "sessionId": "replaced", "message": {"role": "user", "content": "old"}},
        ])
        source.write_text(
            json.dumps({"type": "user", "sessionId": "intruder", "message": {"content": "x" * 400}}) + "\n",
            encoding="utf-8",
        )
        self._live_status([transcripts])
        coverage = sessiondb.query_coverage(self.db, self._search_args(session="replaced"))
        self.assertEqual(
            [(item["indexed_bytes"], item["reason"]) for item in coverage["affected_sources"]],
            [(0, "active_source_rewrite")],
        )
        unreadable = transcripts / "unreadable.jsonl"
        unreadable.write_text("{}\n", encoding="utf-8")
        real_stat = Path.stat

        def stat_denied(path, *args, **kwargs):
            if path == unreadable.resolve() and not kwargs and not args:
                raise PermissionError("denied")
            return real_stat(path, *args, **kwargs)

        with mock.patch.object(sessiondb, "discover_transcript_sources", return_value=[
            ("claude", unreadable.resolve(), "root"),
        ]), mock.patch.object(Path, "stat", stat_denied):
            blocked = sessiondb.query_coverage(self.db, self._search_args())
        self.assertEqual(blocked["status"], "incomplete")
        self.assertEqual(blocked["affected_sources"][0]["reason"], "source_unreadable")

    def test_inline_delivery_caps_long_coverage_lists(self):
        coverage = {
            "complete": False, "status": "catching_up", "reason": "active_sources_affect_query",
            "affected_sources": [
                {"path": "/tmp/" + ("p" * 400) + str(index), "provider": "claude",
                 "indexed_bytes": index, "reason": "active_source_tail"}
                for index in range(400)
            ],
        }
        result = {"sources": [], "events": [], "coverage": coverage, "objects": ["x" * 70000]}
        delivered = sessiondb.deliver_clean_query(self.db_path, result)
        self.assertLessEqual(sessiondb._encoded_size(delivered), sessiondb.QUERY_INLINE_MAX_BYTES)
        self.assertEqual(delivered["coverage"]["affected_sources_total"], 400)
        payload = json.loads(Path(delivered["bundle"]["path"]).read_text().splitlines()[1])["payload"]
        self.assertEqual(len(payload["coverage"]["affected_sources"]), 400)


    def _fake_cli(self, body):
        cli = self.root / "fake-sessiondb"
        cli.write_text("#!/bin/sh\n" + body, encoding="utf-8")
        cli.chmod(0o755)
        return cli










































    def test_accent_fold_reuse_folds_each_large_projection_once_with_identical_results(self):
        rows = []
        for index in range(12):
            words = ["subagent delivery"] if index % 3 == 0 else (["subagent"] if index % 3 == 1 else ["delivery"])
            rows.append({"type": "user", "sessionId": f"fold-{index}", "message": {
                "role": "user", "content": " ".join(words) + f" marker{index} " + ("filler " * 900),
            }})
        source = self.write("fold.jsonl", rows)
        sessiondb.ingest(self.db, "claude", source)
        args = self._search_args(text='"subagent" NEAR/1 "delivery"',
                                 mode="ranked")
        slow = sessiondb._accent_fold_slow
        large = sessiondb.ACCENT_FOLD_CACHE_MIN_CHARS

        def run():
            sessiondb._ACCENT_FOLD_CACHE.clear()
            counter = {"large": 0}

            def counted(value):
                if len(value) >= large:
                    counter["large"] += 1
                return slow(value)

            with mock.patch.object(sessiondb, "_accent_fold_slow", side_effect=counted):
                result = sessiondb.search_index(self.db, args)
            return result, counter["large"]

        reused, reused_folds = run()
        with mock.patch.object(sessiondb, "ACCENT_FOLD_CACHE_MIN_CHARS", 10**12):
            control, control_folds = run()
        self.assertEqual(
            [entry["result_id"] for entry in reused["entries"]],
            [entry["result_id"] for entry in control["entries"]],
        )
        self.assertEqual(reused["total"], 4)
        self.assertLessEqual(reused_folds, 12)
        self.assertGreater(control_folds, 3 * reused_folds)
        sessiondb._ACCENT_FOLD_CACHE.clear()
        texts = ["x" * 4095, "y" * 4096]
        self.assertEqual([sessiondb._accent_fold(text) for text in texts], [slow(text) for text in texts])
        self.assertEqual(list(sessiondb._ACCENT_FOLD_CACHE), ["y" * 4096])
        for index in range(sessiondb.ACCENT_FOLD_CACHE_ENTRIES + 2):
            sessiondb._accent_fold(str(index) * 5000)
        self.assertEqual(len(sessiondb._ACCENT_FOLD_CACHE), sessiondb.ACCENT_FOLD_CACHE_ENTRIES)

    def test_ranked_search_builds_snippets_for_inline_results_and_pages_hydrate_the_rest(self):
        rows = [{"type": "user", "sessionId": f"lazy-{index}", "message": {
            "role": "user", "content": f"lazy needle number {index} " + (
                "context " * (400 if index == 10 else 40)
            ),
        }} for index in range(14)]
        sessiondb.ingest(self.db, "claude", self.write("lazy.jsonl", rows))
        query = '"lazy needle" AND number'
        lazy = sessiondb.search_index(self.db, self._search_args(text=query, mode="ranked"))
        self.assertNotIn("verified_by", lazy["telemetry"])
        eager_args = self._search_args(text=query, mode="ranked")
        eager_args.passage_limit = None
        eager = sessiondb.search_index(self.db, eager_args)
        self.assertEqual([entry["result_id"] for entry in lazy["entries"]],
                         [entry["result_id"] for entry in eager["entries"]])
        shown = sessiondb.INLINE_RESULT_LIMIT
        self.assertTrue(all(entry["passage"] for entry in lazy["entries"][:shown]))
        self.assertTrue(all(entry["passage"] is None for entry in lazy["entries"][shown:]))
        self.assertEqual([entry["passage"] for entry in lazy["entries"][:shown]],
                         [entry["passage"] for entry in eager["entries"][:shown]])
        delivered = sessiondb.bundle_if_oversized(self.db_path, lazy, None)
        page = sessiondb.result_bundle_page(Path(delivered["bundle"]["path"]), argparse.Namespace(
            offset=0, limit=50, provider=None, project=None, session=None, role=None, tool=None,
        ), self.db_path)
        self.assertEqual([entry["passage"] for entry in page["entries"]],
                         [entry["passage"] for entry in eager["entries"]])
        self.assertTrue(page["entries"][10]["passage_truncated"])

    def test_folded_span_matches_the_per_call_implementation(self):
        def reference(text, needle):
            folded, positions = [], []
            for index, char in enumerate(text):
                part = "".join(value for value in unicodedata.normalize("NFD", char.casefold())
                               if not unicodedata.combining(value))
                folded.append(part)
                positions.extend([index] * len(part))
            normalized = "".join(value for value in unicodedata.normalize("NFD", needle.casefold())
                                 if not unicodedata.combining(value))
            if not normalized:
                return None
            found = "".join(folded).find(normalized)
            if found < 0:
                return None
            return positions[found], positions[found + len(normalized) - 1] + 1

        texts = ["Báo GIÁ cho khách", "straße STRASSE ﬁle", "e\u0301 café API disconnect", "日本 ǅ İ", "x" * 5000 + "Báo giá"]
        needles = ["báo giá", "bao gia", "strasse", "fi", "cafe", "api", "ǆ", "i̇", "", "\u0301", "missing"]
        for text in texts:
            for needle in needles:
                self.assertEqual(sessiondb._folded_span(text, needle), reference(text, needle), (text[:20], needle))

    def test_projection_metadata_repair_requires_a_consistent_projection(self):
        sessiondb.ingest(self.db, "claude", self.write("repair.jsonl", [
            {"type": "user", "sessionId": "repair", "message": {"role": "user", "content": "repair needle"}},
        ]))
        self.db.execute("UPDATE metadata SET value='stale' WHERE key='projection_snapshot_id'")
        self.db.commit()
        self.assertFalse(sessiondb.projection_status(self.db)["complete"])
        self.assertTrue(sessiondb.repair_projection_metadata(self.db))
        self.assertTrue(sessiondb.projection_status(self.db)["complete"])
        self.assertFalse(sessiondb.repair_projection_metadata(self.db))
        rowid = self.db.execute("SELECT rowid FROM events WHERE session_id='repair'").fetchone()[0]
        self.db.execute("UPDATE metadata SET value='stale' WHERE key='projection_snapshot_id'")
        self.db.execute("DELETE FROM positional_projection_fts WHERE rowid=?", (rowid,))
        self.db.commit()
        self.assertFalse(sessiondb.repair_projection_metadata(self.db))
        self.assertFalse(sessiondb.projection_status(self.db)["complete"])

    def test_projection_metadata_repair_rejects_a_missing_reference_balanced_by_an_orphan(self):
        sessiondb.ingest(self.db, "claude", self.write("orphan.jsonl", [
            {"type": "user", "sessionId": "orphan", "message": {"role": "user", "content": "orphan needle"}},
        ]))
        self.db.execute("UPDATE metadata SET value='stale' WHERE key='projection_snapshot_id'")
        self.db.execute("UPDATE search_projection_refs SET event_rowid=event_rowid+100000")
        self.db.commit()
        status = sessiondb.projection_status(self.db)
        self.assertEqual(status["events"], status["passages"])
        self.assertEqual(status["invalid_references"], 0)
        self.assertFalse(sessiondb.repair_projection_metadata(self.db))
        self.assertFalse(sessiondb.projection_status(self.db)["complete"])

    def test_integrity_counts_use_the_narrow_indexes(self):
        for query, index in (("SELECT COUNT(*) FROM events", "events_count_idx"),
                             ("SELECT COUNT(*) FROM search_projection_refs", "search_projection_refs_block_idx")):
            plan = " ".join(row[3] for row in self.db.execute("EXPLAIN QUERY PLAN " + query))
            self.assertIn(index, plan)

    def _unwritable_primary(self, original):
        primary = sessiondb.result_bundle_directories(self.db_path)[0]

        def writer(result, path, *args, **kwargs):
            if Path(path).parent == primary:
                raise PermissionError("sandbox denies the SessionDB data directory")
            return original(result, path, *args, **kwargs)
        return writer

    def test_result_bundles_fall_back_to_a_private_temp_directory(self):
        sessiondb.ingest(self.db, "claude", self.write("fallback.jsonl", [
            {"type": "user", "sessionId": "fallback", "message": {"role": "user", "content": "fallback needle"}},
        ]))
        result = sessiondb.search_index(self.db, self._search_args(text="fallback needle"))
        temp_root = self.root / "tmp"
        temp_root.mkdir()
        with mock.patch.object(sessiondb.tempfile, "gettempdir", return_value=str(temp_root)), \
                mock.patch.object(sessiondb, "write_result_bundle",
                                  side_effect=self._unwritable_primary(sessiondb.write_result_bundle)):
            delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)
            fallback = sessiondb.result_bundle_directories(self.db_path)[1]
        self.assertNotIn("delivery_error", delivered)
        bundle = Path(delivered["bundle"]["path"])
        self.assertEqual(bundle.parent, fallback.resolve())
        self.assertEqual(fallback.stat().st_mode & 0o777, 0o700)
        self.assertEqual(bundle.stat().st_mode & 0o777, 0o600)
        page = sessiondb.result_bundle_page(bundle, argparse.Namespace(
            offset=0, limit=10, provider=None, project=None, session=None, role=None, tool=None,
        ), self.db_path)
        self.assertEqual(len(page["entries"]), 1)

    def test_result_bundle_fallback_rejects_a_linked_directory_and_reports_delivery_failure(self):
        sessiondb.ingest(self.db, "claude", self.write("linked.jsonl", [
            {"type": "user", "sessionId": "linked", "message": {"role": "user", "content": "linked needle"}},
        ]))
        result = sessiondb.search_index(self.db, self._search_args(text="linked needle"))
        temp_root = self.root / "tmp-linked"
        temp_root.mkdir()
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        (temp_root / f"sessiondb-{os.getuid()}").symlink_to(elsewhere)
        with mock.patch.object(sessiondb.tempfile, "gettempdir", return_value=str(temp_root)), \
                mock.patch.object(sessiondb, "write_result_bundle",
                                  side_effect=self._unwritable_primary(sessiondb.write_result_bundle)):
            delivered = sessiondb.bundle_if_oversized(self.db_path, result, None)
        self.assertFalse(delivered["complete"])
        self.assertIn("not a private directory", delivered["delivery_error"])
        self.assertEqual(list(elsewhere.iterdir()), [])
        self.assertEqual(len(delivered["entries"]), 1)


    def test_primary_bundle_writes_still_prune_expired_fallback_bundles(self):
        temp_root = self.root / "tmp-prune"
        temp_root.mkdir()
        with mock.patch.object(sessiondb.tempfile, "gettempdir", return_value=str(temp_root)):
            fallback = sessiondb.result_bundle_directories(self.db_path)[1]
            sessiondb._private_directory(fallback, parents_from=temp_root)
            old = fallback / "search-expired.jsonl"
            old.write_text(json.dumps({"header": {
                "format": "sessiondb-result-bundle-v1", "bundle_lifecycle": {"automatic": True},
            }}) + "\n")
            stamp = time.time() - sessiondb.RESULT_BUNDLE_RETENTION_SECONDS - 60
            os.utime(old, (stamp, stamp))
            sessiondb.automatic_result_bundle_path(self.db_path, {"query": "q"})
        self.assertFalse(old.exists())

if __name__ == "__main__":
    unittest.main()
