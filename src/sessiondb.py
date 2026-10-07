#!/usr/bin/env python3
"""SessionDB proof: lossless, provider-neutral transcript ingestion and retrieval."""

from __future__ import annotations

import argparse
import base64
import bisect
import contextvars
import fcntl
import hashlib
import itertools
import io
import json
import math
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import unicodedata
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import zstandard as zstd


ADAPTER_VERSION = {"claude": "claude-v7", "codex": "codex-v10"}
SCHEMA_VERSION = "10"
RETRIEVAL_PROTOCOL_VERSION = 1
PROJECTION_VERSION = "redacted-column-fts-v15"
PROJECTION_INTEGRITY_RECEIPT_SCHEMA = "sessiondb-projection-integrity-receipt-v1"
PROJECTION_INTEGRITY_RECEIPT_NAME = "projection-integrity-receipt.json"
COMPRESSION = "zstd"
ZSTD_LEVEL = 10
MIN_SHARED_TEXT_BYTES = 512
REF_KEY = "$sessiondb_ref"
CONCAT_KEY = "$sessiondb_concat"
EVENT_CONTENT_KEY = "$sessiondb_event_content"
EVENT_OBJECT_KEY = "$sessiondb_event_object"
TAG_OPEN_RE = re.compile(r"<[A-Za-z][A-Za-z0-9 _-]{0,63}>")
STRUCTURED_TEXT_PATTERNS = (
    (0, "global-claude-md", re.compile(r"(# Global Instructions\n.*?)(?=\n</INSTRUCTIONS>)", re.DOTALL)),
    (0, "global-claude-md", re.compile(r"(# Global Instructions\n.*?)(?=\n\n---\n## SECTION 4\b)", re.DOTALL)),
    (1, "tagged-block", re.compile(
        r"<(?P<tag>[A-Za-z][A-Za-z0-9 _-]{0,63})>\n(?P<body>.*?)(?=\n</(?P=tag)>)", re.DOTALL,
    )),
    (2, "section-block", re.compile(
        r"(^## SECTION [0-9]+[^\n]*\n\n.*?)(?=\n---\n## SECTION [0-9]+|\Z)", re.DOTALL | re.MULTILINE,
    )),
)
CLAUDE_TYPES = {
    "assistant", "user", "system", "summary", "queue-operation", "ai-title",
    "last-prompt", "file-history-snapshot", "progress", "tool_use", "tool_result",
    "custom-title", "attachment", "mode", "file-history-delta", "frame-link",
    "bridge-session", "relocated", "worktree-state", "cost-state",
}
CLAUDE_ARTIFACT_TYPES = {
    "artifact-comment-monitor", "artifact-autoreact-ledger",
}
CODEX_TYPES = {
    "session_meta", "event_msg", "response_item", "turn_context", "world_state",
    "compacted", "ghost_snapshot", "thread_rolled_back", "inter_agent_communication_metadata",
    "token_usage_record", "realtime_item",
}
CODEX_PAYLOAD_TYPES = {
    "message", "reasoning", "function_call", "function_call_output", "custom_tool_call",
    "custom_tool_call_output", "web_search_call", "user_message", "agent_message",
    "task_started", "task_complete", "token_count", "context_compacted", "compacted",
    "context_compaction", "plan_update", "turn_aborted", "warning", "error",
    "mcp_tool_call", "mcp_tool_call_output", "image_generation_call",
    "agent_reasoning", "thread_settings_applied", "web_search_end", "patch_apply_end",
    "sub_agent_activity", "mcp_tool_call_end", "tool_search_call", "tool_search_output",
    "image_generation_end", "item_completed", "compaction",
}
CODEX_REALTIME_PAYLOAD_TYPES = frozenset({
    "transcript_segment", "bem_item_promoted", "realtime_session_closed",
})
CODEX_COMPLETED_DUPLICATE_TYPES = frozenset({
    "AgentMessage", "Reasoning", "SubAgentActivity", "CollabAgentToolCall",
})
CODEX_COMPLETED_TOOL_TYPES = {
    "CommandExecution": "exec_command",
    "FileChange": "apply_patch",
    "WebSearch": "web.search",
}
CODEX_COMPLETED_UNIQUE_TOOL_TYPES = frozenset({
    "Extension", "DynamicToolCall", "McpToolCall", "FunctionCallOutput",
})
CODEX_TOKEN_USAGE_ID_KEYS = frozenset({"response_id", "root_turn_id", "session_id", "thread_id", "turn_id"})
CODEX_TOKEN_USAGE_COUNTER_KEYS = frozenset({
    "cache_write_input_tokens", "cached_input_tokens", "input_tokens",
    "output_tokens", "reasoning_output_tokens", "total_tokens",
})


@dataclass(frozen=True)
class AdapterMigration:
    mode: str
    categories: frozenset[str] = frozenset()
    unsupported_only: bool = False


ADAPTER_MIGRATIONS = {
    ("claude", "claude-v3", "claude-v4"): AdapterMigration(
        "metadata-only", frozenset({"relocated", "worktree-state"}),
    ),
    ("claude", "claude-v4", "claude-v5"): AdapterMigration(
        "metadata-only", frozenset({"atis-latch"}),
    ),
    ("claude", "claude-v5", "claude-v6"): AdapterMigration(
        "category-scoped", frozenset(CLAUDE_ARTIFACT_TYPES),
    ),
    ("claude", "claude-v6", "claude-v7"): AdapterMigration(
        "metadata-only", frozenset({"cost-state"}),
    ),
    ("codex", "codex-v4", "codex-v5"): AdapterMigration(
        "category-scoped", frozenset({"item_completed"}),
    ),
    ("codex", "codex-v5", "codex-v6"): AdapterMigration(
        "category-scoped", frozenset({"item_completed"}),
    ),
    ("codex", "codex-v6", "codex-v7"): AdapterMigration(
        "category-scoped", frozenset({"item_completed"}),
    ),
    ("codex", "codex-v6", "codex-v8"): AdapterMigration(
        "category-scoped", frozenset({"item_completed"}) | CODEX_REALTIME_PAYLOAD_TYPES,
    ),
    ("codex", "codex-v7", "codex-v8"): AdapterMigration(
        "category-scoped", CODEX_REALTIME_PAYLOAD_TYPES,
    ),
    ("codex", "codex-v6", "codex-v9"): AdapterMigration(
        "category-scoped", frozenset({"item_completed"}) | CODEX_REALTIME_PAYLOAD_TYPES,
        unsupported_only=True,
    ),
    ("codex", "codex-v7", "codex-v9"): AdapterMigration(
        "category-scoped", frozenset({"item_completed"}) | CODEX_REALTIME_PAYLOAD_TYPES,
        unsupported_only=True,
    ),
    ("codex", "codex-v8", "codex-v9"): AdapterMigration(
        "category-scoped", frozenset({"item_completed"}), unsupported_only=True,
    ),
    ("codex", "codex-v6", "codex-v10"): AdapterMigration(
        "category-scoped",
        frozenset({"item_completed", "compaction"}) | CODEX_REALTIME_PAYLOAD_TYPES,
        unsupported_only=True,
    ),
    ("codex", "codex-v7", "codex-v10"): AdapterMigration(
        "category-scoped",
        frozenset({"item_completed", "compaction"}) | CODEX_REALTIME_PAYLOAD_TYPES,
        unsupported_only=True,
    ),
    ("codex", "codex-v8", "codex-v10"): AdapterMigration(
        "category-scoped", frozenset({"item_completed", "compaction"}), unsupported_only=True,
    ),
    ("codex", "codex-v9", "codex-v10"): AdapterMigration(
        "category-scoped", frozenset({"compaction"}), unsupported_only=True,
    ),
}
REDACTION_RULES = (
    ("assignment", re.compile(
        r"(api[_-]?key|key|token|secret|password|authorization)"
        r"(\s*[:=]\s*)(\"[^\"]+\"|'[^']+'|[^\n,;]+)"
    )),
    ("bearer", re.compile(r"\b(bearer)(\s+)([a-z0-9._~+/=-]{8,})")),
    ("provider-key", re.compile(
        r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{16,}|AKIA[A-Z0-9]{16})\b"
    )),
    ("pem", re.compile(
        r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
        re.DOTALL,
    )),
    ("url-credentials", re.compile(r"(https?://)([^\s/@:]+):([^\s/@]+)@")),
)
ASCII_LOWER_TABLE = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")
SECRET_FIELD_RE = re.compile(
    r"(?i)(?:^|[_-])(?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|secret|password|authorization|private[_-]?key)$"
)
BASE64_RE = re.compile(r"(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{160,}={0,2}(?![A-Za-z0-9+/=])")
LITERAL_SEARCH_RE = re.compile(r"[\u3400-\u9fff\U0001F300-\U0001FAFF]")
LITERAL_RUN_RE = re.compile(r"[^\x00-\x7f]+|[^\w\s]+", re.UNICODE)
STRUCTURED_TOKEN_RE = re.compile(
    r"(?<![\w.-])(?:--)?[^\W_]+(?:[_.-][^\W_]+)+(?![\w.-])", re.UNICODE,
)
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
PREFIXED_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]*_[A-Za-z0-9-]{16,}$")
SEARCH_EXCLUDED_LEAVES = frozenset({
    "encrypted_content", "data", "blob", "images", "local_images", "audio", "local_audio",
})
CONTENT_VALUE_KEYS = frozenset({
    "content", "message", "summary", "arguments", "output", "input", "last_agent_message",
    "text", "result", "toolUseResult", "attachment", "snapshot", "compactMetadata",
})
MAX_OBJECT_DEPTH = 128
MAX_OBJECT_BYTES = 256 * 1024 * 1024
MAX_CLEAN_EVENTS = 100
CACHE_LIMIT = 8192
INLINE_RESULT_MAX_BYTES = 256 * 1024
INLINE_RESULT_LIMIT = 10
INLINE_PASSAGE_SCAN_LIMIT = 50
PROJECTION_BLOCK_TARGET_BYTES = 256 * 1024
SEARCH_DEADLINE_SECONDS = 30.0
# Readers wait out a watcher commit instead of failing after one second; the wait
# is charged to the caller's search deadline.
READER_LOCK_WAIT_SECONDS = 10.0
# Agents read inline output directly; larger clean-query answers go to a private file.
QUERY_INLINE_MAX_BYTES = 64 * 1024
QUERY_PREVIEW_OBJECT_CHARS = 2000
RESULT_BUNDLE_RETENTION_SECONDS = 30 * 24 * 60 * 60
RESULT_BUNDLE_HEADER_MAX_BYTES = 64 * 1024
REBUILD_READINESS_SCHEMA = "sessiondb-rebuild-readiness/v1"
FULL_REBUILD_RECEIPT_MIN_EVENTS = 100_000
REBUILD_READINESS_MAX_AGE_SECONDS = 24 * 60 * 60
FALLBACK_MAX_SOURCE_BYTES = 8 * 1024 * 1024 * 1024
FALLBACK_DEADLINE_SECONDS = 300.0
ACTIVE_DRIFT_GRACE_SECONDS = 300
# Unindexed bytes a query may parse itself from actively written transcripts.
LIVE_TAIL_MAX_BYTES = 64 * 1024 * 1024
AUTO_METADATA_MAX_BYTES = 16 * 1024
AUTO_METADATA_MAX_DEPTH = 8
AUTO_METADATA_MAX_ITEMS = 64
AUTO_METADATA_MAX_STRING = 512
AUTO_METADATA_UNKNOWN_SCALAR_MAX_STRING = 128
AUTO_METADATA_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:/@+\-]*$")
AUTO_METADATA_DENIED_KEYS = frozenset({
    "message", "content", "summary", "lastprompt", "aititle", "text", "prompt",
    "input", "output", "result", "arguments", "response", "request", "reasoning",
    "encryptedcontent", "attachment", "attachments", "snapshot", "compactmetadata",
    "image", "images", "localimages", "audio", "localaudio", "blob", "data", "body",
    "role", "tool", "toolname", "tooluse", "toolresult", "tooluseresult", "invocation",
    "command", "callid", "function", "apikey", "accesstoken", "refreshtoken", "token",
    "secret", "password", "authorization", "privatekey",
})
AUTO_METADATA_DENIED_VALUES = frozenset({
    "tooluse", "toolresult", "functioncall", "functioncalloutput", "customtoolcall",
    "customtoolcalloutput", "mcptoolcall", "mcptoolcalloutput", "user", "assistant",
})
AUTO_METADATA_ALLOWED_KEYS = frozenset({
    "type", "subtype", "payload", "metadata", "timestamp", "sessionid", "bridgesessionid",
    "agentid", "cwd", "relocatedcwd", "originalcwd", "worktreepath", "worktreename",
    "worktreebranch", "worktreesession", "enteredexisting", "trackingpath", "frameurl",
    "owneraccountuuid", "ownerorganizationuuid", "lastsequencenum",
    "sequence", "phase", "state", "status", "kind", "active", "enabled", "mode",
    "triggerturn", "version", "id", "uuid", "name", "path", "url", "count", "index",
    "goal", "objective", "tokensused", "timeusedseconds", "createdat", "updatedat",
    "startedatms", "completedatms",
})
AUTO_METADATA_ALLOWED_SUFFIXES = (
    "id", "uuid", "path", "url", "timestamp", "time", "count", "index",
    "number", "num", "version", "state", "status", "kind", "type", "name", "phase",
    "enabled", "active", "cwd", "branch",
)


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS objects (
  object_hash TEXT PRIMARY KEY, kind TEXT NOT NULL, raw_bytes INTEGER NOT NULL,
  stored_bytes INTEGER NOT NULL, stored_hash TEXT NOT NULL,
  pack_offset INTEGER NOT NULL, compressed_bytes INTEGER NOT NULL,
  compression TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS object_references (
  parent_hash TEXT NOT NULL REFERENCES objects(object_hash) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL, child_hash TEXT NOT NULL REFERENCES objects(object_hash),
  purpose TEXT NOT NULL,
  PRIMARY KEY(parent_hash, ordinal)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sources (
  source_id TEXT PRIMARY KEY, provider TEXT NOT NULL, path TEXT NOT NULL UNIQUE,
  size_bytes INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, source_hash TEXT NOT NULL,
  adapter_version TEXT NOT NULL, status TEXT NOT NULL, issue_count INTEGER NOT NULL DEFAULT 0,
  structure_raw_bytes INTEGER NOT NULL, structure_zstd BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT NOT NULL, provider TEXT NOT NULL, project TEXT, parent_session_id TEXT,
  root_session_id TEXT, source_id TEXT NOT NULL REFERENCES sources(source_id),
  PRIMARY KEY(provider, session_id, source_id)
);
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
  provider TEXT NOT NULL, session_id TEXT NOT NULL, project TEXT, sequence INTEGER NOT NULL,
  timestamp TEXT, timestamp_utc TEXT, order_timestamp TEXT, order_kind TEXT NOT NULL,
  category TEXT NOT NULL, role TEXT, tool TEXT, tool_event INTEGER NOT NULL,
  parent_session_id TEXT,
  raw_hash TEXT NOT NULL, content_hash TEXT, content_shape TEXT, supported INTEGER NOT NULL,
  structure_offset INTEGER NOT NULL, structure_bytes INTEGER NOT NULL,
  issue TEXT, line_number INTEGER NOT NULL,
  UNIQUE(source_id, line_number)
);
CREATE TABLE IF NOT EXISTS event_content (
  event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL, object_hash TEXT NOT NULL REFERENCES objects(object_hash),
  PRIMARY KEY(event_id, ordinal)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS adapter_extensions (
  provider TEXT NOT NULL, category TEXT NOT NULL, structural_signature TEXT NOT NULL,
  admitted_at TEXT NOT NULL, sample_event_id TEXT NOT NULL,
  PRIMARY KEY(provider, category, structural_signature)
) WITHOUT ROWID;
CREATE VIRTUAL TABLE IF NOT EXISTS positional_projection_fts USING fts5(
  native, copied, unclassified, native_literal, copied_literal, unclassified_literal,
  content='', contentless_delete=1, detail=column,
  tokenize='unicode61 remove_diacritics 0'
);
-- Accented Latin spellings that occur in the accent-keeping index, keyed by their unaccented
-- form, so an unaccented query word can expand to them. Stale rows are harmless.
CREATE TABLE IF NOT EXISTS search_term_variants (
  folded TEXT NOT NULL, term TEXT NOT NULL, PRIMARY KEY(folded, term)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS search_projection_blocks (
  block_key INTEGER PRIMARY KEY,
  block_digest BLOB NOT NULL UNIQUE CHECK(length(block_digest)=32),
  pack_offset INTEGER NOT NULL,
  compressed_bytes INTEGER NOT NULL, raw_bytes INTEGER NOT NULL,
  compressed_hash BLOB NOT NULL CHECK(length(compressed_hash)=32)
);
CREATE TABLE IF NOT EXISTS search_projection_refs (
  event_rowid INTEGER PRIMARY KEY,
  block_key INTEGER NOT NULL REFERENCES search_projection_blocks(block_key),
  projection_offset INTEGER NOT NULL, projection_bytes INTEGER NOT NULL,
  projection_hash BLOB NOT NULL CHECK(length(projection_hash)=32),
  reference_hash BLOB NOT NULL CHECK(length(reference_hash)=32)
) WITHOUT ROWID;
-- Narrow covering indexes let every search's integrity counts avoid scanning wide rows.
CREATE INDEX IF NOT EXISTS events_count_idx ON events(supported);
CREATE INDEX IF NOT EXISTS search_projection_refs_block_idx ON search_projection_refs(block_key);
CREATE TRIGGER IF NOT EXISTS events_projection_bd BEFORE DELETE ON events BEGIN
  DELETE FROM positional_projection_fts WHERE rowid=old.rowid;
  DELETE FROM search_projection_refs WHERE event_rowid=old.rowid;
END;
"""


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _seconds_since(started: float) -> float:
    return round(max(0.0, time.monotonic() - started), 6)


def _command_outcome(result: dict[str, Any]) -> str:
    if result.get("complete") is True:
        return "complete"
    reason = result.get("reason")
    if reason is None and isinstance(result.get("coverage"), dict):
        reason = result["coverage"].get("reason")
    reason = str(reason or "")
    if reason == "query_deadline_exceeded":
        return "timeout"
    if reason.startswith("continuation_"):
        return "rejected"
    if reason.startswith(("bundle_error:", "database_error:", "delivery_error:")):
        return "error"
    return "incomplete"


def attach_command_timing(
    result: dict[str, Any], command: str, started: float,
    phases: dict[str, float] | None = None, *, extend: bool = False,
) -> dict[str, Any]:
    """Attach monotonic, content-free command timings without changing evidence."""
    telemetry = dict(result.get("telemetry") or {})
    combined = dict(telemetry.get("phases_seconds") or {})
    for source, target in (
        ("coverage_seconds", "coverage"),
        ("candidate_selection_seconds", "candidate_selection"),
        ("candidate_integrity_seconds", "integrity"),
        ("projection_matching_seconds", "projection_matching"),
        ("hydration_seconds", "hydration"),
    ):
        if target not in combined and telemetry.get(source) is not None:
            combined[target] = round(max(0.0, float(telemetry[source])), 6)
    for name, value in (phases or {}).items():
        combined[name] = round(combined.get(name, 0.0) + max(0.0, value), 6)
    elapsed = _seconds_since(started)
    prior = float(telemetry.get("total_seconds", 0.0)) if extend else 0.0
    total = round(prior + elapsed, 6)
    attributed = round(sum(combined.values()), 6)
    telemetry.update({
        "command": command,
        "total_seconds": total,
        "phases_seconds": combined,
        "unattributed_seconds": round(max(0.0, total - attributed), 6),
        "outcome": _command_outcome(result),
    })
    result["telemetry"] = telemetry
    return result


def retrieval_origin(args: argparse.Namespace | None, result: dict[str, Any]) -> dict[str, Any]:
    case_id = getattr(args, "case_id", None) if args is not None else None
    if case_id is not None:
        if not isinstance(case_id, str) or not case_id.strip() or len(case_id) > 128:
            raise ValueError("case id must contain 1 to 128 characters")
        case_id = case_id.strip()
    filters = {}
    for name in ("provider", "project", "session", "role", "tool", "lineage", "since", "until"):
        value = getattr(args, name, None) if args is not None else None
        if value is not None:
            filters[name] = value
    return {
        "version": 1,
        "command": "search-index",
        "binding_status": "bound" if case_id is not None else "unbound",
        "case_id": case_id,
        "continuation_id": digest(os.urandom(32))[:32],
        "query": result.get("query") or (getattr(args, "text", None) if args is not None else None),
        "mode": result.get("mode") or (getattr(args, "mode", "ranked") if args is not None else "ranked"),
        "order": result.get("order") or (getattr(args, "order", "relevance") if args is not None else "relevance"),
        "filters": filters,
        "snapshot_id": result.get("snapshot_id"),
    }


def ensure_retrieval_origin(
    result: dict[str, Any], args: argparse.Namespace | None = None,
) -> dict[str, Any]:
    if not isinstance(result.get("origin"), dict):
        result["origin"] = retrieval_origin(args, result)
    return result


def continuation_context_error(
    header: dict[str, Any], args: argparse.Namespace,
) -> str | None:
    origin = header.get("origin")
    if not isinstance(origin, dict):
        if getattr(args, "case_id", None) is not None or getattr(args, "continuation_id", None) is not None:
            return "continuation_origin_missing"
        return None
    expected_case = origin.get("case_id")
    actual_case = getattr(args, "case_id", None)
    expected_continuation = origin.get("continuation_id")
    actual_continuation = getattr(args, "continuation_id", None)
    requested_snapshot = getattr(args, "snapshot_id", None)
    bound = origin.get("binding_status") == "bound"
    if bound and (not actual_case or not actual_continuation):
        return "continuation_context_required"
    if expected_case != actual_case and (expected_case is not None or actual_case is not None):
        return "continuation_case_mismatch"
    if expected_continuation and actual_continuation is not None and actual_continuation != expected_continuation:
        return "continuation_id_mismatch"
    if bound and expected_continuation and actual_continuation != expected_continuation:
        return "continuation_id_mismatch"
    if (
        bound and origin.get("snapshot_id") and requested_snapshot is not None
        and requested_snapshot != origin.get("snapshot_id")
    ):
        return "continuation_snapshot_mismatch"
    if bound and origin.get("snapshot_id") and requested_snapshot is None:
        return "continuation_context_required"
    return None


def continuation_rejection(
    header: dict[str, Any], reason: str, *, command: str,
) -> dict[str, Any]:
    return {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
        "complete": False,
        "reason": reason,
        "origin": header.get("origin"),
        "entries" if command == "bundle-page" else "events": [],
    }


def hash_hex(value: bytes | str) -> str:
    return value.hex() if isinstance(value, bytes) else value


def projection_reference_hash(
    event_rowid: int, block_digest: bytes | str, offset: int, size: int,
    projection_hash: bytes | str,
) -> bytes:
    return digest_bytes(canonical([
        event_rowid, hash_hex(block_digest), offset, size, hash_hex(projection_hash),
    ]))


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def stored_json(value: Any) -> bytes:
    """Encode JSON without changing provider field order; identity still uses canonical()."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def normalized_metadata_key(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


def metadata_shape(
    value: Any, depth: int = 0,
    extra_allowed_keys: frozenset[str] = frozenset(),
) -> Any | None:
    """Return a value-free structural shape only for conservatively safe metadata."""
    if depth > AUTO_METADATA_MAX_DEPTH:
        return None
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        if len(value) > AUTO_METADATA_MAX_STRING:
            return None
        if normalized_metadata_key(value) in AUTO_METADATA_DENIED_VALUES:
            return None
        if redact(value) != value:
            return None
        return "string"
    if isinstance(value, list):
        if len(value) > AUTO_METADATA_MAX_ITEMS:
            return None
        children = []
        for child in value:
            shaped = metadata_shape(child, depth + 1, extra_allowed_keys)
            if shaped is None:
                return None
            encoded = canonical(shaped).decode("utf-8")
            if encoded not in children:
                children.append(encoded)
        return ["array", sorted(children)]
    if isinstance(value, dict):
        if len(value) > AUTO_METADATA_MAX_ITEMS:
            return None
        fields = []
        for key in sorted(value):
            if not isinstance(key, str) or not key:
                return None
            normalized = normalized_metadata_key(key)
            if normalized in AUTO_METADATA_DENIED_KEYS:
                return None
            declared_metadata_key = (
                normalized in AUTO_METADATA_ALLOWED_KEYS
                or normalized in extra_allowed_keys
                or normalized.endswith(AUTO_METADATA_ALLOWED_SUFFIXES)
            )
            if not declared_metadata_key:
                if depth != 0 or not normalized or isinstance(value[key], (dict, list)):
                    return None
                if (
                    isinstance(value[key], str)
                    and (
                        len(value[key]) > AUTO_METADATA_UNKNOWN_SCALAR_MAX_STRING
                        or AUTO_METADATA_IDENTIFIER_RE.fullmatch(value[key]) is None
                    )
                ):
                    return None
            shaped = metadata_shape(value[key], depth + 1, extra_allowed_keys)
            if shaped is None:
                return None
            fields.append([key, shaped])
        return ["object", fields]
    return None


def auto_metadata_candidate(provider: str, row: dict[str, Any]) -> tuple[str, str] | None:
    """Classify a bounded, contentless event into a provider/category shape identity."""
    if len(canonical(row)) > AUTO_METADATA_MAX_BYTES:
        return None
    row_type = row.get("type")
    if not isinstance(row_type, str) or not row_type or len(row_type) > 128:
        return None
    if provider == "claude":
        category = str(row.get("subtype") or row_type)
    elif provider == "codex":
        payload = row.get("payload")
        payload_type = payload.get("type") if isinstance(payload, dict) else None
        category = str(payload_type or row_type)
    else:
        return None
    extra_allowed_keys = frozenset()
    if provider == "codex" and isinstance(row.get("payload"), dict):
        payload = row["payload"]
        if row_type == "event_msg" and payload.get("type") == "item_completed":
            extra_allowed_keys = frozenset({"item"})
    if (
        provider == "codex" and category == "token_usage_record"
        and codex_token_usage_record_supported(row)
    ):
        return category, digest(canonical([
            "token_usage_record", sorted(row.get("payload", {})),
        ]))
    shape = metadata_shape(row, extra_allowed_keys=extra_allowed_keys)
    if shape is None:
        return None
    return category, digest(canonical(shape))


def structure_contains_nonmetadata(value: Any) -> bool:
    """Return true when an exact unknown structure contains content or an action payload."""
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(key, str) and normalized_metadata_key(key) in AUTO_METADATA_DENIED_KEYS:
                return True
            if structure_contains_nonmetadata(child):
                return True
    elif isinstance(value, list):
        return any(structure_contains_nonmetadata(child) for child in value)
    return False


def load_auto_metadata_extensions(
    db: sqlite3.Connection, provider: str,
) -> frozenset[tuple[str, str]]:
    return frozenset(
        (row["category"], row["structural_signature"])
        for row in db.execute(
            "SELECT category,structural_signature FROM adapter_extensions WHERE provider=?",
            (provider,),
        )
    )


def redact(text: str) -> str:
    lowered = text.translate(ASCII_LOWER_TABLE)
    for name, pattern in REDACTION_RULES:
        if name == "assignment":
            parts: list[str] = []
            position = 0
            for match in pattern.finditer(lowered):
                parts.extend((text[position:match.start(3)], "[REDACTED]"))
                position = match.end(3)
            if parts:
                parts.append(text[position:])
                text = "".join(parts)
                lowered = text.translate(ASCII_LOWER_TABLE)
        elif name == "bearer":
            parts = []
            position = 0
            for match in pattern.finditer(lowered):
                parts.extend((text[position:match.start(3)], "[REDACTED]"))
                position = match.end(3)
            if parts:
                parts.append(text[position:])
                text = "".join(parts)
                lowered = text.translate(ASCII_LOWER_TABLE)
        elif name == "url-credentials":
            parts = []
            position = 0
            for match in pattern.finditer(lowered):
                parts.extend((text[position:match.start(2)], "[REDACTED]@"))
                position = match.end()
            if parts:
                parts.append(text[position:])
                text = "".join(parts)
                lowered = text.translate(ASCII_LOWER_TABLE)
        else:
            text = pattern.sub(f"[{name.upper()}_REDACTED]", text)
    return BASE64_RE.sub("[BASE64_REDACTED]", text)


def search_projection(text: str) -> str:
    """Return the exact redacted text admitted to the default search index."""
    return redact(text)


def redacted_search_text(
    value: Any, *, include_keys: bool = False, apply_text_rules: bool = True,
    include_excluded_marker: bool = False,
) -> str:
    out: list[str] = []
    keys: set[str] = set()

    def walk(item: Any, key: str = "") -> None:
        leaf = key.rsplit(".", 1)[-1]
        if leaf in SEARCH_EXCLUDED_LEAVES:
            if include_excluded_marker:
                out.append("[REDACTED]")
            return
        if leaf and SECRET_FIELD_RE.search(leaf):
            out.append("[REDACTED]")
            return
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            if include_keys:
                keys.add(str(item))
        elif isinstance(item, bool):
            if include_keys:
                keys.add("true" if item else "false")
        elif item is None:
            if include_keys:
                keys.add("null")
        elif isinstance(item, list):
            for child in item:
                walk(child, key)
        elif isinstance(item, dict):
            for child_key, child in item.items():
                if include_keys:
                    keys.add(str(child_key))
                walk(child, child_key)

    walk(value)
    projected = "\n".join([*(part for part in out if part), *sorted(keys)])
    return redact(projected) if apply_text_rules else projected


PROJECTION_ORIGINS = ("native", "copied", "unclassified")
PROJECTION_LITERAL_COLUMNS = tuple(origin + "_literal" for origin in PROJECTION_ORIGINS)
REGION_BOUNDARY_TOKEN = "sessiondbregionboundary"
REGION_BOUNDARY = f" {REGION_BOUNDARY_TOKEN} "
REGION_SEPARATOR = "\0"
# Claude rewrites these session-state records many times per session, so only
# the best-ranked copy per session belongs among the leading ranked results.
REPEATED_SESSION_STATE_CATEGORIES = frozenset({
    "agent-name", "ai-title", "custom-title", "last-prompt", "queue-operation",
    "mode", "bridge-session", "worktree-state", "relocated",
    "file-history-snapshot", "file-history-delta",
})


def _looks_like_transcript_record(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    keys = set(value)
    return bool(
        {"timestamp", "type", "payload"} <= keys
        or ({"type", "sessionId"} <= keys and ("message" in keys or "content" in keys))
        or ({"type", "uuid"} <= keys and ("message" in keys or "content" in keys))
    )


def _split_projection_string(text: str) -> list[tuple[str, str]]:
    """Separate copied JSONL transcript records without suppressing surrounding evidence."""
    parts = text.splitlines(keepends=True)
    if not parts:
        return [("native", text)]
    classified: list[tuple[str, str]] = []
    for part in parts:
        stripped = part.strip()
        origin = "native"
        transcript_markers = (
            all(marker in stripped for marker in ('"timestamp"', '"type"', '"payload"'))
            or ('"type"' in stripped and '"sessionId"' in stripped
                and ('"message"' in stripped or '"content"' in stripped))
            or ('"type"' in stripped and '"uuid"' in stripped
                and ('"message"' in stripped or '"content"' in stripped))
        )
        if stripped.startswith("{") and stripped.endswith("}") and transcript_markers:
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                origin = "unclassified"
            else:
                if _looks_like_transcript_record(parsed):
                    origin = "copied"
        if classified and classified[-1][0] == origin:
            classified[-1] = (origin, classified[-1][1] + part)
        else:
            classified.append((origin, part))
    return classified


def normalized_search_regions(value: Any) -> dict[str, list[str]]:
    """Return redacted, origin-labelled regions used by both indexing and verification."""
    regions: dict[str, list[str]] = {origin: [] for origin in PROJECTION_ORIGINS}

    def walk(item: Any, key: str = "") -> None:
        leaf = key.rsplit(".", 1)[-1]
        if leaf in SEARCH_EXCLUDED_LEAVES:
            return
        if leaf and SECRET_FIELD_RE.search(leaf):
            regions["native"].append("[REDACTED]")
            return
        if isinstance(item, str):
            for origin, part in _split_projection_string(redact(item)):
                projected = " ".join(part.split())
                if projected:
                    regions[origin].append(projected)
        elif isinstance(item, list):
            for child in item:
                walk(child, key)
        elif isinstance(item, dict):
            for child_key, child in item.items():
                walk(child, str(child_key))

    walk(value)
    return regions


def encoded_search_regions(regions: dict[str, list[str]]) -> bytes:
    return stored_json({origin: regions.get(origin, []) for origin in PROJECTION_ORIGINS})


def decoded_search_regions(data: bytes) -> dict[str, list[str]]:
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("invalid search projection regions")
    result: dict[str, list[str]] = {}
    for origin in PROJECTION_ORIGINS:
        items = value.get(origin, [])
        if not isinstance(items, list) or not all(isinstance(item, str) for item in items):
            raise ValueError("invalid search projection region list")
        result[origin] = items
    return result


def normalized_search_projection(value: Any) -> tuple[str, str]:
    """Compatibility helper returning the flattened redacted display projection."""
    regions = normalized_search_regions(value)
    projection = " ".join(
        text for origin in PROJECTION_ORIGINS for text in regions[origin]
    )
    return projection, projection.casefold()


def lexical_projection_tokens(text: str) -> str:
    terms = lexical_terms(text)
    tokens = {lexical_word_token(term) for term in terms}
    tokens.update(
        lexical_pair_token(left, right)
        for left, right in zip(terms, terms[1:])
    )
    return " ".join(sorted(tokens))


# FTS5's unicode61 tokenizer treats private-use characters as part of words, but query terms
# split on them, so "design.md\ue201" would never match the word "md". Index them as spaces.
PRIVATE_USE_RE = re.compile("[\ue000-\uf8ff\U000f0000-\U000ffffd\U00100000-\U0010fffd]")


ACCENT_KEEPING_TOKENIZER = "unicode61 remove_diacritics 0"
ACCENT_FOLDING_TOKENIZER = "unicode61 remove_diacritics 2"
# A word as unicode61 sees it: letters and digits plus any combining marks left after NFC.
INDEX_WORD_RE = re.compile(
    r"(?:[^\W_]|[̀-ͯ᪰-᫿᷀-᷿⃐-⃿︠-︯])+", re.UNICODE,
)


def _is_latin_word(term: str) -> bool:
    return all(
        char.isdigit() or unicodedata.combining(char) or "LATIN" in unicodedata.name(char, "")
        for char in term
    )


def fts_index_keeps_accents(db: sqlite3.Connection) -> bool:
    """True when the search index stores words with their accents (remove_diacritics 0)."""
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE name='positional_projection_fts'"
    ).fetchone()
    return bool(row) and "remove_diacritics0" in re.sub(r"\s+", "", row[0]).lower()


def fts_projection_text(text: str, keep_accents: bool = False) -> str:
    """Case-fold for the index; fold Latin accents only for the older accent-folding index."""
    normalized = PRIVATE_USE_RE.sub(" ", unicodedata.normalize("NFC", text).casefold())
    if keep_accents:
        return normalized
    return re.sub(
        r"[^\W_]+",
        lambda match: _accent_fold(match.group(0)) if all(
            char.isdigit() or "LATIN" in unicodedata.name(char, "")
            for char in match.group(0)
        ) else match.group(0),
        normalized,
        flags=re.UNICODE,
    )


def lexical_word_token(term: str) -> str:
    return "w" + digest(term.encode())[:32]


def lexical_pair_token(left: str, right: str) -> str:
    return "p" + digest((left + "\0" + right).encode())[:32]


class ProjectionBlockWriter:
    def __init__(self, db: sqlite3.Connection, handle: Any) -> None:
        self.db = db
        self.handle = handle
        self.buffer = bytearray()
        self.pending: list[tuple[int, int, int, bytes]] = []

    def add(self, event_rowid: int, projection: bytes) -> None:
        if self.buffer and len(self.buffer) + len(projection) > PROJECTION_BLOCK_TARGET_BYTES:
            self.flush()
        offset = len(self.buffer)
        self.buffer.extend(projection)
        self.pending.append((event_rowid, offset, len(projection), digest_bytes(projection)))
        if len(self.buffer) >= PROJECTION_BLOCK_TARGET_BYTES:
            self.flush()

    def flush(self) -> None:
        if not self.pending:
            return
        raw = bytes(self.buffer)
        block_digest = digest_bytes(raw)
        compressed = zstd.compress(raw, level=3)
        stored = self.db.execute(
            "SELECT block_key,pack_offset,compressed_bytes,raw_bytes,compressed_hash "
            "FROM search_projection_blocks WHERE block_digest=?",
            (block_digest,),
        ).fetchone()
        if stored:
            if stored["raw_bytes"] != len(raw):
                raise ValueError(f"projection block identity mismatch: {block_digest.hex()}")
            block_key = stored["block_key"]
        else:
            pack_offset = self.handle.tell()
            self.handle.write(compressed)
            cursor = self.db.execute(
                "INSERT INTO search_projection_blocks("
                "block_digest,pack_offset,compressed_bytes,raw_bytes,compressed_hash"
                ") VALUES(?,?,?,?,?)",
                (block_digest, pack_offset, len(compressed), len(raw), digest_bytes(compressed)),
            )
            block_key = cursor.lastrowid
        self.db.executemany(
            "INSERT OR REPLACE INTO search_projection_refs VALUES(?,?,?,?,?,?)",
            ((event_rowid, block_key, offset, size, value_hash,
              projection_reference_hash(event_rowid, block_digest, offset, size, value_hash))
             for event_rowid, offset, size, value_hash in self.pending),
        )
        self.buffer.clear()
        self.pending.clear()


def accented_term_variants(indexed_text: str) -> set[tuple[str, str]]:
    """(unaccented form, spelling) for each accented Latin word in accent-keeping index text."""
    variants = set()
    for term in set(INDEX_WORD_RE.findall(indexed_text)):
        if term.isascii() or not _is_latin_word(term):
            continue
        folded = _accent_fold(term)
        if folded != term:
            variants.add((folded, term))
    return variants


def insert_projection_index_row(
    db: sqlite3.Connection, table: str, rowid: int, columns: list[str], keep_accents: bool,
) -> None:
    indexed = [fts_projection_text(value, keep_accents) for value in columns]
    db.execute(
        f"INSERT INTO {table}("
        "rowid,native,copied,unclassified,native_literal,copied_literal,unclassified_literal"
        ") VALUES(?,?,?,?,?,?,?)",
        (rowid, *indexed,
         *(literal_tokens(value, accent_tokens=not keep_accents) for value in columns)),
    )
    if keep_accents:
        variants = set()
        for value in indexed:
            variants.update(accented_term_variants(value))
        if variants:
            db.executemany("INSERT OR IGNORE INTO search_term_variants VALUES(?,?)", variants)


def put_projection_regions(
    db: sqlite3.Connection, event_rowid: int, regions: dict[str, list[str]],
    writer: ProjectionBlockWriter,
) -> None:
    columns = [REGION_BOUNDARY.join(regions[origin]) for origin in PROJECTION_ORIGINS]
    insert_projection_index_row(
        db, "positional_projection_fts", event_rowid, columns, fts_index_keeps_accents(db),
    )
    writer.add(event_rowid, encoded_search_regions(regions))


def put_search_projection(
    db: sqlite3.Connection, event_rowid: int, value: Any, writer: ProjectionBlockWriter,
) -> None:
    put_projection_regions(db, event_rowid, normalized_search_regions(value), writer)


def compact_event_metadata(value: Any) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()

    def walk(item: Any, key: str = "") -> None:
        if len(values) >= 16:
            return
        leaf = key.rsplit(".", 1)[-1]
        if leaf in CONTENT_VALUE_KEYS:
            if isinstance(item, dict):
                for child_key, child in item.items():
                    if child_key != "content":
                        walk(child, child_key)
            return
        if isinstance(item, (str, int, float, bool)) and not isinstance(item, bool):
            text = str(item)
            if text and len(text) <= 128 and text not in seen:
                seen.add(text)
                values.append(text)
        elif isinstance(item, list):
            for child in item:
                walk(child, key)
        elif isinstance(item, dict):
            for child_key, child in item.items():
                walk(child, child_key)

    walk(redact_value(value))
    return values


def flatten_text(value: Any) -> str:
    out: list[str] = []

    def walk(item: Any, key: str = "") -> None:
        if isinstance(item, str):
            if key.rsplit(".", 1)[-1] not in SEARCH_EXCLUDED_LEAVES:
                out.append(item)
        elif isinstance(item, list):
            for child in item:
                walk(child, key)
        elif isinstance(item, dict):
            for child_key, child in item.items():
                walk(child, child_key)

    walk(value)
    return "\n".join(part for part in out if part)


def flatten_search_text(value: Any) -> str:
    out: list[str] = []

    def walk(item: Any, key: str = "") -> None:
        if isinstance(item, str):
            leaf = key.rsplit(".", 1)[-1]
            if leaf not in SEARCH_EXCLUDED_LEAVES:
                out.append(item)
        elif isinstance(item, list):
            for child in item:
                walk(child, key)
        elif isinstance(item, dict):
            for child_key, child in item.items():
                walk(child, child_key)

    walk(value)
    return "\n".join(part for part in out if part)


def flatten_raw_text(value: Any) -> str:
    out: list[str] = []

    def walk(item: Any) -> None:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            out.append(str(item))
        elif isinstance(item, bool):
            out.append("true" if item else "false")
        elif item is None:
            out.append("null")
        elif isinstance(item, list):
            for child in item:
                walk(child)
        elif isinstance(item, dict):
            for key, child in item.items():
                out.append(str(key))
                walk(child)

    walk(value)
    return "\n".join(part for part in out if part)


def normalize_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except ValueError:
        return None


def literal_tokens(text: str, *, accent_tokens: bool = True) -> str:
    """Return exact-search anchors for CJK, emoji, and symbol-only queries.

    The accent word and pair anchors serve only the older accent-folding index; an
    accent-keeping index answers accented words and phrases from its word positions."""
    tokens: set[str] = set()
    for match in LITERAL_RUN_RE.finditer(text.casefold()):
        previous = None
        for char in match.group(0):
            codepoint = ord(char)
            tokens.add(f"u{codepoint:x}")
            if previous is not None:
                tokens.add(f"b{ord(previous):x}z{codepoint:x}")
            previous = char
    if accent_tokens:
        tokens.update(accent_term_tokens(text))
        tokens.update(accent_phrase_tokens(text))
    tokens.update(structured_tokens(text))
    return " ".join(sorted(tokens))


def structured_tokens(text: str) -> list[str]:
    return [
        _codepoint_anchor("k", _accent_fold(match.group(0)))
        for match in STRUCTURED_TOKEN_RE.finditer(unicodedata.normalize("NFC", text).casefold())
    ]


def _codepoint_anchor(prefix: str, value: str) -> str:
    return prefix + "z".join(f"{ord(char):x}" for char in value)


def structured_query_tokens(text: str) -> list[str]:
    return structured_tokens(text)


def accent_term_tokens(text: str) -> list[str]:
    tokens = []
    for match in re.finditer(r"[^\W_]+", unicodedata.normalize("NFC", text).casefold(), re.UNICODE):
        term = match.group(0)
        if _has_diacritic(term):
            tokens.append("a" + "z".join(f"{ord(char):x}" for char in term))
    return tokens


def accent_phrase_tokens(text: str) -> list[str]:
    terms = [
        match.group(0) for match in re.finditer(
            r"[^\W_]+", unicodedata.normalize("NFC", text).casefold(), re.UNICODE,
        )
    ]
    return [
        "q" + "y".join("z".join(f"{ord(char):x}" for char in term) for term in pair)
        for pair in zip(terms, terms[1:]) if all(_has_diacritic(term) for term in pair)
    ]


def project_from_path(path: Path) -> str | None:
    parts = path.parts
    if ".claude" in parts and "projects" in parts:
        index = parts.index("projects")
        return parts[index + 1] if index + 1 < len(parts) else None
    return None


@dataclass
class Parsed:
    session_id: str
    project: str | None
    parent_id: str | None
    root_id: str | None
    timestamp: str | None
    category: str
    role: str | None
    tool: str | None
    tool_event: bool
    content: Any | None
    content_paths: tuple[tuple[str, ...], ...]
    structure_extras: tuple[tuple[tuple[str, ...], Any], ...]
    supported: bool
    issue: str | None


def tool_from(value: Any) -> str | None:
    if isinstance(value, dict):
        kind = value.get("type")
        if isinstance(kind, str) and kind in {"tool_use", "function_call", "custom_tool_call", "mcp_tool_call"}:
            return str(value.get("name") or value.get("tool") or kind)
        for child in value.values():
            found = tool_from(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = tool_from(child)
            if found:
                return found
    return None


def claude_tool(value: Any, calls: dict[str, str]) -> tuple[str | None, bool]:
    tool = tool_from(value)
    is_tool = False
    blocks = value if isinstance(value, list) else [value]
    for block in blocks:
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "tool_use":
            is_tool = True
            name = str(block.get("name") or tool or "tool_use")
            if block.get("id"):
                calls[str(block["id"])] = name
            tool = name
        elif kind == "tool_result":
            is_tool = True
            call_id = block.get("tool_use_id")
            if call_id and str(call_id) in calls:
                tool = calls[str(call_id)]
    return tool, is_tool


def parse_claude(
    row: dict[str, Any], path: Path, meta: dict[str, Any],
    auto_extensions: frozenset[tuple[str, str]] = frozenset(),
) -> Parsed:
    row_type = str(row.get("type") or "unknown")
    message = row.get("message")
    session_id = str(row.get("sessionId") or path.stem)
    role = row_type if row_type in {"user", "assistant", "system"} else None
    content: Any | None = None
    content_paths: tuple[tuple[str, ...], ...] = ()
    if isinstance(message, dict):
        role = str(message.get("role") or role) if message.get("role") or role else None
        if message.get("content") is not None:
            content = message["content"]
            content_paths = (("message", "content"),)
    elif message is not None:
        content = message
        content_paths = (("message",),)
    if content is None:
        for key in ("content", "summary", "lastPrompt", "aiTitle"):
            if row.get(key) is not None:
                content = row[key]
                content_paths = ((key,),)
                break
    artifact_record = row_type in CLAUDE_ARTIFACT_TYPES
    if content is None and artifact_record and isinstance(row.get("artifacts"), dict):
        content = row["artifacts"]
        content_paths = (("artifacts",),)
    extras = []
    for key in ("toolUseResult", "attachment", "snapshot", "compactMetadata"):
        if row.get(key) is not None and (key,) not in content_paths and len(canonical(row[key])) >= MIN_SHARED_TEXT_BYTES:
            extras.append(((key,), row[key]))
    child = row.get("agentId")
    parent = session_id if child else None
    if child:
        session_id = str(child)
    calls = meta.setdefault("__sessiondb_claude_tool_calls", {})
    tool, tool_event = claude_tool(content, calls)
    if row_type in {"tool_use", "tool_result"}:
        tool_event = True
    category = str(row.get("subtype") or row_type)
    atis_latch = (
        row_type == "atis-latch"
        and set(row) == {"type", "sessionId", "atis"}
        and isinstance(row.get("sessionId"), str)
        and isinstance(row.get("atis"), str)
    )
    supported = (
        row_type in CLAUDE_TYPES or atis_latch
        or (artifact_record and isinstance(row.get("artifacts"), dict))
    )
    candidate = auto_metadata_candidate("claude", row) if not supported else None
    if (
        candidate in auto_extensions and content is None and not content_paths and not extras
        and role is None and tool is None and not tool_event
    ):
        supported = True
    return Parsed(
        session_id, project_from_path(path), parent, parent or session_id,
        str(row.get("timestamp")) if row.get("timestamp") else None,
        category, role, tool, tool_event, content,
        content_paths, tuple(extras), supported,
        None if supported else f"unknown Claude row type: {row_type}",
    )


def codex_content(
    payload: dict[str, Any], payload_type: str | None,
) -> tuple[Any | None, tuple[tuple[str, ...], ...], tuple[tuple[tuple[str, ...], Any], ...]]:
    if payload_type == "transcript_segment" and payload.get("text") is not None:
        primary_key = "text"
    else:
        primary_key = next((
            key for key in ("content", "message", "summary", "arguments", "output", "input", "last_agent_message")
            if payload.get(key) is not None
        ), None)
    if primary_key is None and payload_type == "tool_search_output" and payload.get("tools") is not None:
        primary_key = "tools"
    if primary_key is not None:
        content = payload[primary_key]
        paths = (("payload", primary_key),)
        extras = tuple(
            (("payload", key), value) for key, value in payload.items()
            if key != primary_key and len(canonical(value)) >= MIN_SHARED_TEXT_BYTES
        )
        return content, paths, extras
    if payload_type in {"mcp_tool_call_end", "image_generation_end"}:
        return payload, (("payload",),), ()
    return None, (), ()


def codex_token_usage_record_supported(row: dict[str, Any]) -> bool:
    payload = row.get("payload")
    if not isinstance(payload, dict) or set(payload) != {
        *CODEX_TOKEN_USAGE_ID_KEYS, "thread_token_usage", "turn_token_usage", "usage",
    }:
        return False
    if any(not isinstance(payload[key], str) for key in CODEX_TOKEN_USAGE_ID_KEYS):
        return False
    for key in ("thread_token_usage", "turn_token_usage", "usage"):
        counters = payload.get(key)
        if not isinstance(counters, dict) or set(counters) != CODEX_TOKEN_USAGE_COUNTER_KEYS:
            return False
        if any(isinstance(value, bool) or not isinstance(value, int) for value in counters.values()):
            return False
    return True


def codex_compaction_supported(row: dict[str, Any]) -> bool:
    """Accept the bounded opaque Codex compaction envelope without indexing its ciphertext."""
    payload = row.get("payload")
    metadata = payload.get("internal_chat_message_metadata_passthrough") if isinstance(payload, dict) else None
    ordinal = row.get("ordinal")
    return bool(
        # Newer Codex writers add a row-level "ordinal"; accept it, and nothing else.
        {"timestamp", "type", "payload"} <= set(row) <= {"timestamp", "type", "payload", "ordinal"}
        and ("ordinal" not in row or (isinstance(ordinal, int) and not isinstance(ordinal, bool)))
        and row.get("type") == "response_item"
        and isinstance(row.get("timestamp"), str)
        and isinstance(payload, dict)
        and set(payload) == {
            "type", "id", "encrypted_content", "internal_chat_message_metadata_passthrough",
        }
        and payload.get("type") == "compaction"
        and isinstance(payload.get("id"), str)
        and bool(payload["id"])
        and isinstance(payload.get("encrypted_content"), str)
        and bool(payload["encrypted_content"])
        and isinstance(metadata, dict)
        and set(metadata) == {"turn_id"}
        and isinstance(metadata.get("turn_id"), str)
        and bool(metadata["turn_id"])
    )


def codex_completed_item(
    payload: dict[str, Any],
) -> tuple[
    bool, str | None, str | None, bool, Any | None,
    tuple[tuple[str, ...], ...],
]:
    """Map supported Codex Desktop completion items without indexing duplicate wrappers."""
    item = payload.get("item")
    if not isinstance(item, dict) or not isinstance(item.get("type"), str):
        return False, None, None, False, None, ()
    item_type = item["type"]
    if item_type in CODEX_COMPLETED_DUPLICATE_TYPES:
        return True, None, None, False, None, ()
    if item_type == "UserMessage":
        content = item.get("content")
        paths = (("payload", "item", "content"),) if content is not None else ()
        return True, "user", None, False, content, paths
    if item_type == "ContextCompaction":
        return True, None, None, False, None, ()
    if item_type in CODEX_COMPLETED_TOOL_TYPES:
        return True, None, CODEX_COMPLETED_TOOL_TYPES[item_type], True, item, (("payload", "item"),)
    if item_type in CODEX_COMPLETED_UNIQUE_TOOL_TYPES:
        tool = item.get("tool") or item.get("name") or item.get("kind") or item_type
        return True, None, str(tool), True, item, (("payload", "item"),)
    return False, None, None, False, None, ()


def parse_codex(
    row: dict[str, Any], path: Path, meta: dict[str, Any],
    auto_extensions: frozenset[tuple[str, str]] = frozenset(),
) -> Parsed:
    row_type = str(row.get("type") or "unknown")
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    if row_type == "session_meta":
        meta.update(payload)
    session_id = str(meta.get("id") or meta.get("session_id") or path.stem)
    parent = meta.get("parent_thread_id") or meta.get("parent_session_id")
    root = meta.get("root_thread_id") or meta.get("root_session_id") or parent or session_id
    project = str(meta.get("cwd")) if meta.get("cwd") else None
    payload_type = str(payload.get("type")) if payload.get("type") else None
    category = payload_type or row_type
    role = str(payload.get("role")) if payload.get("role") else None
    if payload_type == "user_message":
        role = "user"
    elif payload_type == "agent_message":
        role = "assistant"
    content, content_paths, structure_extras = codex_content(payload, payload_type)
    tool = tool_from(payload)
    tool_event = bool(payload_type in {
        "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output",
        "mcp_tool_call", "mcp_tool_call_output", "mcp_tool_call_end", "tool_search_call",
        "tool_search_output",
    })
    completed_supported = True
    if payload_type == "item_completed":
        (
            completed_supported, role, tool, tool_event, content, content_paths,
        ) = codex_completed_item(payload)
        structure_extras = ()
    call_id = payload.get("call_id")
    tool_calls = meta.setdefault("__sessiondb_tool_calls", {})
    if not tool and payload_type in {"mcp_tool_call", "mcp_tool_call_end"}:
        invocation = payload.get("invocation") if isinstance(payload.get("invocation"), dict) else payload
        tool = invocation.get("tool") or invocation.get("name")
    if tool and call_id:
        tool_calls[str(call_id)] = str(tool)
    elif call_id and str(call_id) in tool_calls:
        tool = tool_calls[str(call_id)]
    if call_id and payload_type in {
        "function_call_output", "custom_tool_call_output",
        "mcp_tool_call_output", "mcp_tool_call_end",
    }:
        tool_calls.pop(str(call_id), None)
    supported = (
        row_type in CODEX_TYPES
        and (
            (row_type == "realtime_item" and payload_type in CODEX_REALTIME_PAYLOAD_TYPES)
            or (row_type != "realtime_item" and (not payload_type or payload_type in CODEX_PAYLOAD_TYPES))
        )
        and completed_supported
    )
    if row_type == "token_usage_record":
        supported = codex_token_usage_record_supported(row)
    elif row_type == "response_item" and payload_type == "compaction":
        supported = codex_compaction_supported(row)
    candidate = auto_metadata_candidate("codex", row) if not supported else None
    if (
        candidate in auto_extensions and content is None and not content_paths
        and not structure_extras and role is None and tool is None and not tool_event
    ):
        supported = True
    issue = None
    if not supported:
        issue = f"unknown Codex shape: {row_type}/{payload_type or '-'}"
    return Parsed(
        session_id, project, str(parent) if parent else None, str(root),
        str(row.get("timestamp")) if row.get("timestamp") else None,
        category, role, str(tool) if tool else None, tool_event, content, content_paths, structure_extras,
        supported, issue,
    )


class SessionConnection(sqlite3.Connection):
    pass


def _path_identity(path: Path) -> tuple[int, int] | None:
    try:
        value = path.stat()
    except FileNotFoundError:
        return None
    return value.st_dev, value.st_ino


def _migrate_projection_storage_v9_to_v10_locked(db: sqlite3.Connection) -> None:
    block_columns = {
        row[1] for row in db.execute("PRAGMA table_info(search_projection_blocks)")
    }
    ref_columns = {
        row[1] for row in db.execute("PRAGMA table_info(search_projection_refs)")
    }
    if not block_columns and not ref_columns:
        db.execute(
            "UPDATE metadata SET value=? WHERE key='schema_version'", (SCHEMA_VERSION,),
        )
        db.commit()
        return
    if "block_key" in block_columns:
        db.execute(
            "UPDATE metadata SET value=? WHERE key='schema_version'", (SCHEMA_VERSION,),
        )
        db.commit()
        return
    if "block_id" not in block_columns or "block_id" not in ref_columns:
        raise ValueError("schema v9 projection storage is incomplete; rebuild required")
    def unhex_digest(value: str | None) -> bytes | None:
        if value is None:
            return None
        result = bytes.fromhex(value)
        if len(result) != 32:
            raise ValueError("projection digest must contain 32 bytes")
        return result

    compressed_hashes: dict[str, bytes] = {}
    needs_compressed_hashes = (
        "compressed_hash" not in block_columns
        or db.execute(
            "SELECT EXISTS(SELECT 1 FROM search_projection_blocks "
            "WHERE compressed_hash IS NULL)"
        ).fetchone()[0]
    )
    if needs_compressed_hashes:
        projection_pack = projection_pack_path(db)
        with projection_pack.open("rb") as handle:
            for row in db.execute(
                "SELECT block_id,pack_offset,compressed_bytes "
                "FROM search_projection_blocks ORDER BY pack_offset,block_id"
            ):
                handle.seek(row["pack_offset"])
                compressed = handle.read(row["compressed_bytes"])
                if len(compressed) != row["compressed_bytes"]:
                    raise ValueError(f"projection block extent missing: {row['block_id']}")
                compressed_hashes[row["block_id"]] = digest_bytes(compressed)

    def compressed_hash(block_id: str) -> bytes:
        try:
            return compressed_hashes[block_id]
        except KeyError as error:
            raise ValueError(f"projection block hash unavailable: {block_id}") from error

    def reference_hash(
        event_rowid: int, block_id: str, offset: int, size: int, value_hash: str,
    ) -> bytes:
        return projection_reference_hash(
            event_rowid, unhex_digest(block_id), offset, size, unhex_digest(value_hash),
        )

    db.create_function("sessiondb_unhex", 1, unhex_digest, deterministic=True)
    db.create_function("sessiondb_compressed_hash", 1, compressed_hash, deterministic=True)
    db.create_function("sessiondb_reference_hash", 5, reference_hash, deterministic=True)
    compressed_hash_sql = (
        "COALESCE(sessiondb_unhex(b.compressed_hash),"
        "sessiondb_compressed_hash(b.block_id))"
        if "compressed_hash" in block_columns else
        "sessiondb_compressed_hash(b.block_id)"
    )
    reference_hash_sql = (
        "COALESCE(sessiondb_unhex(r.reference_hash),sessiondb_reference_hash("
        "r.event_rowid,r.block_id,r.projection_offset,r.projection_bytes,r.projection_hash))"
        if "reference_hash" in ref_columns else
        "sessiondb_reference_hash(r.event_rowid,r.block_id,r.projection_offset,"
        "r.projection_bytes,r.projection_hash)"
    )
    db.execute("PRAGMA foreign_keys=OFF")
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DROP TRIGGER IF EXISTS events_projection_bd")
        statements = (
            "CREATE TABLE search_projection_block_map ("
            "block_id TEXT PRIMARY KEY,block_key INTEGER NOT NULL UNIQUE) WITHOUT ROWID",
            "INSERT INTO search_projection_block_map(block_id,block_key) "
            "SELECT block_id,row_number() OVER (ORDER BY pack_offset,block_id) "
            "FROM search_projection_blocks",
            "CREATE TABLE search_projection_blocks_v10 ("
            "block_key INTEGER PRIMARY KEY,block_digest BLOB NOT NULL UNIQUE "
            "CHECK(length(block_digest)=32),"
            "pack_offset INTEGER NOT NULL,compressed_bytes INTEGER NOT NULL,"
            "raw_bytes INTEGER NOT NULL,compressed_hash BLOB NOT NULL "
            "CHECK(length(compressed_hash)=32))",
            "INSERT INTO search_projection_blocks_v10 "
            "SELECT m.block_key,sessiondb_unhex(b.block_id),b.pack_offset,"
            f"b.compressed_bytes,b.raw_bytes,{compressed_hash_sql} "
            "FROM search_projection_blocks b JOIN search_projection_block_map m USING(block_id)",
            "CREATE TABLE search_projection_refs_v10 ("
            "event_rowid INTEGER PRIMARY KEY,block_key INTEGER NOT NULL "
            "REFERENCES search_projection_blocks(block_key),projection_offset INTEGER NOT NULL,"
            "projection_bytes INTEGER NOT NULL,projection_hash BLOB NOT NULL "
            "CHECK(length(projection_hash)=32),reference_hash BLOB NOT NULL "
            "CHECK(length(reference_hash)=32)) WITHOUT ROWID",
            "INSERT INTO search_projection_refs_v10 "
            "SELECT r.event_rowid,m.block_key,r.projection_offset,r.projection_bytes,"
            f"sessiondb_unhex(r.projection_hash),{reference_hash_sql} "
            "FROM search_projection_refs r JOIN search_projection_block_map m USING(block_id)",
        )
        for statement in statements:
            db.execute(statement)
        old_count = db.execute("SELECT COUNT(*) FROM search_projection_refs").fetchone()[0]
        new_count = db.execute("SELECT COUNT(*) FROM search_projection_refs_v10").fetchone()[0]
        if old_count != new_count:
            raise ValueError(f"projection reference migration lost rows: {old_count} != {new_count}")
        mismatches = db.execute(
            "SELECT COUNT(*) FROM search_projection_refs r "
            "JOIN search_projection_block_map m USING(block_id) "
            "JOIN search_projection_refs_v10 n USING(event_rowid) "
            "JOIN search_projection_blocks_v10 b ON b.block_key=n.block_key "
            "WHERE n.block_key<>m.block_key OR b.block_digest<>sessiondb_unhex(r.block_id) "
            "OR n.projection_offset<>r.projection_offset "
            "OR n.projection_bytes<>r.projection_bytes "
            "OR n.projection_hash<>sessiondb_unhex(r.projection_hash) "
            f"OR n.reference_hash<>{reference_hash_sql}"
        ).fetchone()[0]
        if mismatches:
            raise ValueError(f"projection reference migration changed {mismatches} logical rows")
        for statement in (
            "DROP TABLE search_projection_refs",
            "DROP TABLE search_projection_blocks",
            "ALTER TABLE search_projection_blocks_v10 RENAME TO search_projection_blocks",
            "ALTER TABLE search_projection_refs_v10 RENAME TO search_projection_refs",
            "DROP TABLE search_projection_block_map",
        ):
            db.execute(statement)
        db.execute("""
            CREATE TRIGGER events_projection_bd BEFORE DELETE ON events BEGIN
              DELETE FROM positional_projection_fts WHERE rowid=old.rowid;
              DELETE FROM search_projection_refs WHERE event_rowid=old.rowid;
            END
        """)
        db.execute("UPDATE metadata SET value=? WHERE key='schema_version'", (SCHEMA_VERSION,))
        foreign_key_failures = db.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_failures:
            raise ValueError("projection storage migration created invalid references")
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.execute("PRAGMA foreign_keys=ON")


def migrate_projection_storage_v9_to_v10(db: sqlite3.Connection) -> None:
    """Compact projection references without rebuilding FTS rows or the retrieval pack."""
    lease = acquire_projection_pack_lease(db, exclusive=True)
    try:
        _migrate_projection_storage_v9_to_v10_locked(db)
    finally:
        release_projection_pack_lease(lease)


def connect(path: Path, *, read_only: bool = False, timeout: float = 60.0) -> sqlite3.Connection:
    resolved = path.resolve()
    for attempt in range(3):
        before = _path_identity(resolved)
        if read_only:
            db = sqlite3.connect(
                f"file:{resolved}?mode=ro", timeout=timeout, uri=True, factory=SessionConnection,
            )
        else:
            db = sqlite3.connect(resolved, timeout=timeout, factory=SessionConnection)
        after = _path_identity(resolved)
        if before is None or before == after:
            db._sessiondb_path = resolved
            db._sessiondb_path_identity = after
            break
        db.close()
        if attempt == 2:
            raise RuntimeError(f"database path changed while opening: {resolved}")
    db.row_factory = sqlite3.Row
    db.execute(f"PRAGMA busy_timeout={max(0, int(timeout * 1000))}")
    tables = {
        row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if tables and "metadata" not in tables:
        db.close()
        raise ValueError("pre-v9 SessionDB schema detected; rebuild from retained transcripts")
    version = None
    if "metadata" in tables:
        version = db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
    if tables and not version:
        db.close()
        raise ValueError("unversioned database detected; refusing to modify it")
    if version and version[0] == "9":
        if read_only:
            db.close()
            raise ValueError("SessionDB schema v9 requires a one-time writer migration to v10")
        migrate_projection_storage_v9_to_v10(db)
        version = (SCHEMA_VERSION,)
    if version and version[0] != SCHEMA_VERSION:
        db.close()
        raise ValueError(f"unsupported SessionDB schema version: {version[0]}")
    if not read_only:
        db.executescript(SCHEMA)
        projection_columns = {
            row[1] for row in db.execute("PRAGMA table_info(positional_projection_fts)")
        }
        if not set(PROJECTION_ORIGINS + PROJECTION_LITERAL_COLUMNS) <= projection_columns:
            db.execute("DROP TABLE positional_projection_fts")
            db.execute(
                "CREATE VIRTUAL TABLE positional_projection_fts USING fts5("
                "native,copied,unclassified,native_literal,copied_literal,unclassified_literal,"
                "content='',contentless_delete=1,detail=column,"
                f"tokenize='{ACCENT_KEEPING_TOKENIZER}')"
            )
            db.execute("DELETE FROM search_projection_refs")
            db.execute("DELETE FROM search_projection_blocks")
            db.execute(
                "DELETE FROM metadata WHERE key IN "
                "('projection_version','projection_snapshot_id','projection_event_count')"
            )
        trigger = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_projection_bd'"
        ).fetchone()
        if trigger and "positional_projection_fts" not in trigger[0]:
            db.execute("DROP TRIGGER events_projection_bd")
            db.executescript("""
                CREATE TRIGGER events_projection_bd BEFORE DELETE ON events BEGIN
                  DELETE FROM positional_projection_fts WHERE rowid=old.rowid;
                  DELETE FROM search_projection_refs WHERE event_rowid=old.rowid;
                END;
            """)
        # Every search, including `query --text`, reads the positional index; these legacy
        # word indexes were written on each ingest but no longer read.
        db.execute("DROP TABLE IF EXISTS search_fts")
        db.execute("DROP TABLE IF EXISTS literal_fts")
        db.execute("INSERT OR IGNORE INTO metadata VALUES('schema_version',?)", (SCHEMA_VERSION,))
        db.commit()
    return db


def connection_path_is_current(db: sqlite3.Connection) -> bool:
    # Check the pathname before issuing SQL: SQLite can touch the old inode's
    # sidecars when a backup has replaced the database under a stale reader.
    database = getattr(db, "_sessiondb_path", None)
    return database is not None and db._sessiondb_path_identity == _path_identity(database)


def pack_path(db: sqlite3.Connection) -> Path:
    database = db.execute("PRAGMA database_list").fetchone()["file"]
    return Path(database + ".objects")


def projection_pack_path(db: sqlite3.Connection) -> Path:
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])
    row = db.execute(
        "SELECT value FROM metadata WHERE key='projection_pack_generation'"
    ).fetchone()
    name = row[0] if row else database.name + ".retrieval.active"
    if Path(name).name != name or not name.startswith(database.name + ".retrieval."):
        raise ValueError(f"invalid projection pack generation for {database.name}: {name}")
    return database.parent / name


def projection_integrity_receipt_path(database: Path) -> Path:
    """Return the watcher-owned receipt path for one database."""
    database = database.resolve()
    return database.parent / "freshness" / f"{database.name}.{PROJECTION_INTEGRITY_RECEIPT_NAME}"


def projection_pack_identity(path: Path) -> dict[str, Any]:
    stat_result = path.stat()
    return {
        "generation": path.name,
        "size": stat_result.st_size,
        "mtime_ns": stat_result.st_mtime_ns,
        "ctime_ns": stat_result.st_ctime_ns,
        "device": stat_result.st_dev,
        "inode": stat_result.st_ino,
    }


def sqlite_generation_identity(db: sqlite3.Connection) -> dict[str, Any]:
    """Bind an optimization receipt to the current database and WAL files."""
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])

    def identity(path: Path) -> dict[str, int] | None:
        try:
            stat_result = path.stat()
        except FileNotFoundError:
            return None
        return {
            "size": stat_result.st_size,
            "mtime_ns": stat_result.st_mtime_ns,
            "ctime_ns": stat_result.st_ctime_ns,
            "device": stat_result.st_dev,
            "inode": stat_result.st_ino,
        }

    database_identity = identity(database)
    if database_identity is None:
        raise FileNotFoundError(database)
    return {
        "database": database_identity,
        "wal": identity(Path(str(database) + "-wal")),
    }


def owned_projection_pack(database: Path, path: Path) -> bool:
    """Return whether a derivative pack name is scoped to this database file."""
    database = database.resolve()
    return (
        path.parent.resolve() == database.parent
        and path.name.startswith(database.name + ".retrieval.")
    )


def projection_pack_lock_path_for_database(database: Path) -> Path:
    database = database.resolve()
    return Path(str(database) + ".retrieval.lock")


def projection_pack_lock_path(db: sqlite3.Connection) -> Path:
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])
    return projection_pack_lock_path_for_database(database)


def acquire_projection_pack_lease_path(
    database: Path, *, exclusive: bool, timeout: float = 60.0,
) -> Any:
    path = projection_pack_lock_path_for_database(database)
    flags = (os.O_RDWR | os.O_CREAT) if exclusive else os.O_RDONLY
    descriptor = os.open(path, flags, 0o600)
    handle = os.fdopen(descriptor, "r+b" if exclusive else "rb", buffering=0)
    deadline = time.monotonic() + timeout
    try:
        operation = (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB
        while True:
            try:
                fcntl.flock(handle, operation)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"projection pack lease timed out after {timeout:g}s")
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
    except Exception:
        handle.close()
        raise
    return handle


def acquire_projection_pack_lease(
    db: sqlite3.Connection, *, exclusive: bool, timeout: float = 60.0,
) -> Any:
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])
    return acquire_projection_pack_lease_path(database, exclusive=exclusive, timeout=timeout)


def release_projection_pack_lease(handle: Any) -> None:
    try:
        fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def ensure_projection_pack(db: sqlite3.Connection) -> Path:
    path = projection_pack_path(db)
    db.execute(
        "INSERT OR IGNORE INTO metadata(key,value) VALUES('projection_pack_generation',?)",
        (path.name,),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
    lock_path = projection_pack_lock_path(db)
    descriptor = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o600)
    os.close(descriptor)
    return path


def put_packed_object(
    db: sqlite3.Connection, data: bytes, kind: str, *, object_hash: str | None = None,
    raw_bytes: int | None = None, pack_handle: Any | None = None,
) -> str:
    object_hash = object_hash or digest(data)
    if db.execute("SELECT 1 FROM objects WHERE object_hash=?", (object_hash,)).fetchone():
        return object_hash
    compressed = zstd.compress(data, level=ZSTD_LEVEL)
    path = pack_path(db)
    path.parent.mkdir(parents=True, exist_ok=True)
    offset = pack_handle.tell() if pack_handle is not None else (path.stat().st_size if path.exists() else 0)
    if pack_handle is not None:
        pack_handle.write(compressed)
    else:
        with path.open("ab") as handle:
            handle.write(compressed)
            handle.flush()
    db.execute(
        "INSERT INTO objects VALUES(?,?,?,?,?,?,?,?)",
        (object_hash, kind, raw_bytes if raw_bytes is not None else len(data), len(data), digest(data),
         offset, len(compressed), COMPRESSION),
    )
    return object_hash


def structured_text_spans(text: str) -> list[tuple[int, int, str, str]]:
    candidates = []
    for priority, purpose, pattern in STRUCTURED_TEXT_PATTERNS:
        group = "body" if "body" in pattern.groupindex else 1
        matches = (
            (pattern.match(text, opener.start()) for opener in TAG_OPEN_RE.finditer(text))
            if group == "body" else pattern.finditer(text)
        )
        for match in matches:
            if match is None:
                continue
            block = match.group(group)
            if priority == 0 or len(canonical(block)) >= MIN_SHARED_TEXT_BYTES:
                candidates.append((priority, match.start(group), match.end(group), block, purpose))
    selected: list[tuple[int, int, str, str]] = []
    for _, start, end, block, purpose in sorted(candidates, key=lambda item: (item[0], item[2] - item[1], item[1])):
        if not any(start < chosen_end and end > chosen_start for chosen_start, chosen_end, _, _ in selected):
            selected.append((start, end, block, purpose))
    return sorted(selected)


def factor_shared_content(
    db: sqlite3.Connection, value: Any, pack_handle: Any | None = None,
) -> tuple[Any, list[tuple[str, str]]]:
    references: list[tuple[str, str]] = []

    def reference(text: str, purpose: str) -> dict[str, str]:
        kind = "known-instruction" if purpose == "global-claude-md" else "shared-text"
        child_hash = put_packed_object(db, canonical(text), kind, pack_handle=pack_handle)
        references.append((child_hash, purpose))
        return {REF_KEY: child_hash}

    def factor(item: Any, root: bool = False) -> Any:
        if isinstance(item, str):
            spans = structured_text_spans(item)
            if spans:
                parts: list[Any] = []
                position = 0
                for start, end, block, purpose in spans:
                    if start > position:
                        parts.append(item[position:start])
                    parts.append(reference(block, purpose))
                    position = end
                if position < len(item):
                    parts.append(item[position:])
                return {CONCAT_KEY: parts}
            if not root and len(canonical(item)) >= MIN_SHARED_TEXT_BYTES:
                return reference(item, "structured-value")
            return item
        if isinstance(item, list):
            return [factor(child) for child in item]
        if isinstance(item, dict):
            return {key: factor(child) for key, child in item.items()}
        return item

    return factor(value, root=True), references


def put_content_object(db: sqlite3.Connection, item: Any, pack_handle: Any | None = None) -> str:
    original = stored_json(item)
    manifest, references = factor_shared_content(db, item, pack_handle)
    if not references:
        return put_packed_object(
            db, stored_json(item), "content", object_hash=digest(original),
            raw_bytes=len(original), pack_handle=pack_handle,
        )
    object_hash = digest(original)
    if db.execute("SELECT 1 FROM objects WHERE object_hash=?", (object_hash,)).fetchone():
        return object_hash
    put_packed_object(
        db, stored_json(manifest), "manifest", object_hash=object_hash,
        raw_bytes=len(original), pack_handle=pack_handle,
    )
    for ordinal, (child_hash, purpose) in enumerate(references):
        db.execute(
            "INSERT INTO object_references VALUES(?,?,?,?)",
            (object_hash, ordinal, child_hash, purpose),
        )
    return object_hash


def put_content_references(
    db: sqlite3.Connection, content: Any, source_id: str, line_number: int,
    pack_handle: Any | None = None,
) -> tuple[str, str, list[str]]:
    shape = "list" if isinstance(content, list) else "scalar"
    items = content if isinstance(content, list) else [content]
    refs = []
    for item in items:
        refs.append(put_content_object(db, item, pack_handle))
    return digest(stored_json(content)), shape, refs


def parsed_structure(
    row: dict[str, Any], content_paths: tuple[tuple[str, ...], ...],
    extra_hashes: dict[tuple[str, ...], str],
) -> bytes:
    structure = json.loads(json.dumps(row, ensure_ascii=False))
    multiple = len(content_paths) > 1
    for path in content_paths:
        parent: Any = structure
        for part in path[:-1]:
            parent = parent[part]
        parent[path[-1]] = {EVENT_CONTENT_KEY: ".".join(path) if multiple else None}
    for path, object_hash in extra_hashes.items():
        parent = structure
        for part in path[:-1]:
            parent = parent[part]
        parent[path[-1]] = {EVENT_OBJECT_KEY: object_hash}
    return stored_json(structure)


def expand_parsed_structure(
    structure_bytes: bytes, content: Any | None, object_reader: Any,
) -> dict[str, Any]:
    structure = json.loads(structure_bytes)

    def expand(value: Any) -> Any:
        if isinstance(value, dict) and set(value) == {EVENT_CONTENT_KEY}:
            key = value[EVENT_CONTENT_KEY]
            return content[key] if key is not None else content
        if isinstance(value, dict) and set(value) == {EVENT_OBJECT_KEY}:
            return object_reader(value[EVENT_OBJECT_KEY])
        if isinstance(value, list):
            return [expand(child) for child in value]
        if isinstance(value, dict):
            return {key: expand(child) for key, child in value.items()}
        return value

    result = expand(structure)
    if not isinstance(result, dict):
        raise ValueError("parsed event structure is not an object")
    return result


class SourceBoundaryChanged(RuntimeError):
    """The source can no longer satisfy a previously captured sync watermark."""


def stable_source_snapshot(
    path: Path, attempts: int = 3, watermark: Any | None = None,
) -> tuple[Any, bytes]:
    if watermark is not None:
        expected_size = int(watermark.st_size)
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if (
                (before.st_dev, before.st_ino) != (watermark.st_dev, watermark.st_ino)
                or before.st_size < expected_size
            ):
                raise SourceBoundaryChanged(f"source changed across sync boundary: {path}")
            data = handle.read(expected_size)
            after = os.fstat(handle.fileno())
        same_identity = (
            (after.st_dev, after.st_ino) == (watermark.st_dev, watermark.st_ino)
        )
        boundary_unchanged = not (
            before.st_size == expected_size
            and before.st_mtime_ns != watermark.st_mtime_ns
        ) and not (
            after.st_size == expected_size
            and after.st_mtime_ns != watermark.st_mtime_ns
        )
        if (
            same_identity and boundary_unchanged
            and after.st_size >= expected_size and len(data) == expected_size
        ):
            return watermark, data
        raise SourceBoundaryChanged(f"source changed across sync boundary: {path}")
    for _ in range(attempts):
        before = path.stat()
        data = path.read_bytes()
        after = path.stat()
        if (
            before.st_size == after.st_size == len(data)
            and before.st_mtime_ns == after.st_mtime_ns
        ):
            return after, data
    raise RuntimeError(f"source changed during snapshot: {path}")


def jsonl_lines(data: bytes) -> list[bytes]:
    """Split JSONL only on LF; bare CR bytes belong to the same record."""
    if not data:
        return []
    parts = data.split(b"\n")
    lines = [part + b"\n" for part in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    return lines


def adapter_state_key(source_id: str) -> str:
    return f"adapter_state:{source_id}"


def load_adapter_state(db: sqlite3.Connection, source_id: str) -> dict[str, Any] | None:
    row = db.execute("SELECT value FROM metadata WHERE key=?", (adapter_state_key(source_id),)).fetchone()
    return json.loads(row["value"]) if row else None


def save_adapter_state(db: sqlite3.Connection, source_id: str, value: dict[str, Any]) -> None:
    db.execute(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES(?,?)",
        (adapter_state_key(source_id), json.dumps(value, ensure_ascii=False, separators=(",", ":"))),
    )


def persistent_adapter_state(provider: str, value: dict[str, Any]) -> dict[str, Any]:
    """Retain only parser state required to interpret future appended rows."""
    if provider == "codex":
        keys = {
            "id", "session_id", "parent_thread_id", "parent_session_id",
            "root_thread_id", "root_session_id", "cwd", "__sessiondb_tool_calls",
        }
    else:
        keys = {"__sessiondb_claude_tool_calls"}
    return {key: value[key] for key in keys if key in value and value[key] not in ({}, None)}


def store_source_lines(
    db: sqlite3.Connection, provider: str, path: Path, source_id: str, source_bytes: bytes,
    start_line: int, structure_buffer: bytearray, meta: dict[str, Any], sessions: set[str],
    pack_handle: Any, projection_writer: ProjectionBlockWriter,
    auto_extensions: frozenset[tuple[str, str]] = frozenset(),
) -> tuple[int, int]:
    count = issues = 0
    for line_number, raw_line in enumerate(io.BytesIO(source_bytes), start_line):
        if not raw_line.strip():
            continue
        try:
            row = json.loads(raw_line)
            if not isinstance(row, dict):
                raise ValueError("record is not an object")
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            raw_hash = digest(raw_line)
            event_id = digest(f"{source_id}:{line_number}:{raw_hash}".encode())
            structure_bytes = b'{"unreadable":true}'
            retained = {
                "encoding": "base64",
                "unreadable_bytes_base64": base64.b64encode(raw_line).decode("ascii"),
            }
            content_hash = put_content_object(db, retained, pack_handle)
            db.execute(
                "INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (event_id, source_id, provider, path.stem, project_from_path(path), line_number,
                 None, None, None, "file_mtime", "unreadable", None, None, 0, None,
                 raw_hash, content_hash, "dict", 0, len(structure_buffer), len(structure_bytes),
                 str(exc), line_number),
            )
            structure_buffer.extend(structure_bytes)
            event_rowid = db.execute("SELECT rowid FROM events WHERE event_id=?", (event_id,)).fetchone()[0]
            db.execute("INSERT INTO event_content VALUES(?,?,?)", (event_id, 0, content_hash))
            put_search_projection(db, event_rowid, "[REDACTED]", projection_writer)
            issues += 1
            count += 1
            continue
        raw = stored_json(row)
        raw_hash = digest(raw)
        parsed = (
            parse_claude(row, path, meta, auto_extensions)
            if provider == "claude" else parse_codex(row, path, meta, auto_extensions)
        )
        content_hash = None
        content_shape = None
        content_parts: list[str] = []
        if parsed.content is not None:
            content_hash, content_shape, content_parts = put_content_references(
                db, parsed.content, source_id, line_number, pack_handle,
            )
        event_id = digest(f"{source_id}:{line_number}:{raw_hash}".encode())
        extra_hashes = {
            extra_path: put_content_object(db, extra_value, pack_handle)
            for extra_path, extra_value in parsed.structure_extras
        }
        structure_bytes = parsed_structure(row, parsed.content_paths, extra_hashes)
        timestamp_utc = normalize_timestamp(parsed.timestamp)
        db.execute(
            "INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, source_id, provider, parsed.session_id, parsed.project, line_number,
             parsed.timestamp, timestamp_utc, timestamp_utc, "exact" if timestamp_utc else "pending",
             parsed.category, parsed.role, parsed.tool, int(parsed.tool_event), parsed.parent_id,
             raw_hash, content_hash, content_shape, int(parsed.supported),
             len(structure_buffer), len(structure_bytes), parsed.issue, line_number),
        )
        structure_buffer.extend(structure_bytes)
        for ordinal, object_hash in enumerate(content_parts):
            db.execute("INSERT INTO event_content VALUES(?,?,?)", (event_id, ordinal, object_hash))
        event_rowid = db.execute("SELECT rowid FROM events WHERE event_id=?", (event_id,)).fetchone()[0]
        put_search_projection(db, event_rowid, row, projection_writer)
        if parsed.session_id not in sessions:
            db.execute(
                "INSERT OR REPLACE INTO sessions VALUES(?,?,?,?,?,?)",
                (parsed.session_id, provider, parsed.project, parsed.parent_id, parsed.root_id, source_id),
            )
            sessions.add(parsed.session_id)
        count += 1
        if not parsed.supported:
            issues += 1
    return count, issues


def update_source_ordering(db: sqlite3.Connection, source_id: str, stat: Any) -> None:
    exact_rows = db.execute(
        "SELECT event_id,line_number,timestamp_utc FROM events WHERE source_id=? ORDER BY line_number",
        (source_id,),
    ).fetchall()
    exact_positions = [(row["line_number"], row["timestamp_utc"]) for row in exact_rows if row["timestamp_utc"]]
    file_time = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    first_exact = exact_positions[0][1] if exact_positions else None
    last_exact = None
    for event in exact_rows:
        if event["timestamp_utc"]:
            last_exact = event["timestamp_utc"]
            continue
        if last_exact:
            order_timestamp, order_kind = last_exact, "forward_fill"
        elif first_exact:
            order_timestamp, order_kind = first_exact, "back_fill"
        else:
            order_timestamp, order_kind = file_time, "file_mtime"
        db.execute(
            "UPDATE events SET order_timestamp=?,order_kind=? WHERE event_id=?",
            (order_timestamp, order_kind, event["event_id"]),
        )


def apply_metadata_migration(
    db: sqlite3.Connection, provider: str, from_version: str, to_version: str,
    source_ids: Iterable[str], migration: AdapterMigration,
) -> dict[str, Any]:
    """Promote declared contentless metadata from stored exact structures only."""
    if migration.mode != "metadata-only" or not migration.categories:
        raise ValueError("metadata migration requires declared categories")
    ids = tuple(dict.fromkeys(source_ids))
    if not ids:
        return {"sources": 0, "events": 0}
    parser = parse_claude if provider == "claude" else parse_codex
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("DROP TABLE IF EXISTS temp.sessiondb_migration_sources")
        db.execute(
            "CREATE TEMP TABLE sessiondb_migration_sources(source_id TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        db.executemany(
            "INSERT INTO sessiondb_migration_sources VALUES(?)", ((source_id,) for source_id in ids),
        )
        sources = db.execute(
            "SELECT s.source_id,s.path,s.adapter_version FROM sources s "
            "JOIN sessiondb_migration_sources m USING(source_id)"
        ).fetchall()
        if len(sources) != len(ids) or any(row["adapter_version"] != from_version for row in sources):
            raise ValueError("metadata migration source version mismatch")
        events = db.execute(
            "SELECT e.*,s.path FROM events e JOIN sources s USING(source_id) "
            "JOIN sessiondb_migration_sources m USING(source_id) "
            f"WHERE e.supported=0 OR e.category IN ({','.join('?' for _ in migration.categories)}) "
            "ORDER BY e.source_id,e.line_number",
            tuple(sorted(migration.categories)),
        ).fetchall()
        promoted = 0
        for event in events:
            structure = read_event_structure(db, event)
            if not isinstance(structure, dict):
                raise ValueError(f"metadata event is not structured: {event['event_id']}")
            parsed = parser(structure, Path(event["path"]), {})
            if event["category"] not in migration.categories:
                if parsed.supported:
                    raise ValueError(
                        f"metadata migration omitted supported category: {event['category']}"
                    )
                continue
            if (
                not parsed.supported or parsed.category != event["category"]
                or parsed.content is not None or parsed.content_paths or parsed.structure_extras
                or parsed.role is not None or parsed.tool is not None or parsed.tool_event
                or event["content_hash"] is not None
            ):
                raise ValueError(f"metadata migration changed event semantics: {event['event_id']}")
            if not event["supported"]:
                db.execute(
                    "UPDATE events SET supported=1,issue=NULL WHERE event_id=?", (event["event_id"],),
                )
                promoted += 1
        db.execute(
            "UPDATE sources SET adapter_version=?,issue_count=("
            "SELECT COUNT(*) FROM events e WHERE e.source_id=sources.source_id AND e.supported=0) "
            "WHERE source_id IN (SELECT source_id FROM sessiondb_migration_sources)",
            (to_version,),
        )
        invalid = db.execute(
            "SELECT COUNT(*) FROM sources s JOIN sessiondb_migration_sources m USING(source_id) "
            "WHERE s.adapter_version<>? OR s.issue_count<>("
            "SELECT COUNT(*) FROM events e WHERE e.source_id=s.source_id AND e.supported=0)",
            (to_version,),
        ).fetchone()[0]
        remaining = db.execute(
            "SELECT COUNT(*) FROM events e JOIN sessiondb_migration_sources m USING(source_id) "
            f"WHERE e.supported=0 AND e.category IN ({','.join('?' for _ in migration.categories)})",
            tuple(sorted(migration.categories)),
        ).fetchone()[0]
        if invalid or remaining:
            raise ValueError("metadata migration validation failed")
        db.execute("DROP TABLE temp.sessiondb_migration_sources")
        db.commit()
    except Exception:
        db.rollback()
        raise
    clear_caches()
    return {"sources": len(ids), "events": promoted}


def auto_metadata_admission_signature(
    provider: str, category: str, structure: Any, path: Path,
) -> str | None:
    """Return the safe passive signature that unattended admission may persist."""
    if not isinstance(structure, dict):
        return None
    candidate = auto_metadata_candidate(provider, structure)
    if candidate is None or candidate[0] != category:
        return None
    parser = parse_claude if provider == "claude" else parse_codex
    parsed = parser(structure, path, {})
    if (
        parsed.content is not None or parsed.content_paths or parsed.structure_extras
        or parsed.role is not None or parsed.tool is not None or parsed.tool_event
    ):
        return None
    return candidate[1]


def apply_auto_metadata_admissions(db: sqlite3.Connection) -> dict[str, Any]:
    """Admit only uniformly safe unsupported metadata groups from exact stored structures."""
    admitted: list[dict[str, Any]] = []
    rejected_groups = 0
    db.execute("BEGIN IMMEDIATE")
    try:
        rows = db.execute(
            "SELECT e.*,s.path FROM events e JOIN sources s USING(source_id) "
            "WHERE e.supported=0 AND e.category<>'unreadable' "
            "ORDER BY e.provider,e.category,e.source_id,e.line_number"
        ).fetchall()
        grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            grouped.setdefault((row["provider"], row["category"]), []).append(row)
        approved: list[tuple[str, str, str, str]] = []
        approved_event_ids: list[str] = []
        for (provider, category), events in grouped.items():
            candidates: list[tuple[sqlite3.Row, str]] = []
            for event in events:
                structure = read_event_structure(db, event)
                signature = auto_metadata_admission_signature(
                    provider, category, structure, Path(event["path"]),
                )
                if signature is None:
                    candidates = []
                    break
                candidates.append((event, signature))
            if not candidates:
                rejected_groups += 1
                continue
            signatures: dict[str, str] = {}
            for event, signature in candidates:
                signatures.setdefault(signature, event["event_id"])
                approved_event_ids.append(event["event_id"])
            approved.extend(
                (provider, category, signature, sample_event_id)
                for signature, sample_event_id in signatures.items()
            )
            admitted.append({
                "provider": provider, "category": category,
                "structural_signatures": len(signatures), "events": len(candidates),
            })
        if approved_event_ids:
            now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            db.execute("DROP TABLE IF EXISTS temp.sessiondb_auto_admissions")
            db.execute(
                "CREATE TEMP TABLE sessiondb_auto_admissions(event_id TEXT PRIMARY KEY) WITHOUT ROWID"
            )
            db.executemany(
                "INSERT INTO sessiondb_auto_admissions VALUES(?)",
                ((event_id,) for event_id in approved_event_ids),
            )
            db.executemany(
                "INSERT OR IGNORE INTO adapter_extensions VALUES(?,?,?,?,?)",
                ((provider, category, signature, now, sample) for provider, category, signature, sample in approved),
            )
            db.execute(
                "UPDATE events SET supported=1,issue=NULL "
                "WHERE event_id IN (SELECT event_id FROM sessiondb_auto_admissions)"
            )
            db.execute(
                "UPDATE sources SET issue_count=(SELECT COUNT(*) FROM events e "
                "WHERE e.source_id=sources.source_id AND e.supported=0) "
                "WHERE source_id IN (SELECT e.source_id FROM events e "
                "JOIN sessiondb_auto_admissions a USING(event_id))"
            )
            invalid = db.execute(
                "SELECT COUNT(*) FROM events e JOIN sessiondb_auto_admissions a USING(event_id) "
                "WHERE e.supported<>1 OR e.issue IS NOT NULL"
            ).fetchone()[0]
            if invalid:
                raise ValueError("automatic metadata admission validation failed")
            db.execute("DROP TABLE temp.sessiondb_auto_admissions")
        db.commit()
    except Exception:
        db.rollback()
        raise
    if admitted:
        clear_caches()
    return {
        "admitted": admitted,
        "admitted_groups": len(admitted),
        "admitted_events": sum(item["events"] for item in admitted),
        "rejected_groups": rejected_groups,
    }


def ingest(
    db: sqlite3.Connection, provider: str, path: Path,
    source_watermark: Any | None = None,
) -> dict[str, Any]:
    projection_was_current = (
        db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        or projection_status(db)["complete"]
    )
    resolved = path.resolve()
    initial_stat = source_watermark or resolved.stat()
    existing = db.execute(
        "SELECT * FROM sources WHERE provider=? AND path=?", (provider, str(resolved)),
    ).fetchone()
    if existing and existing["adapter_version"] == ADAPTER_VERSION[provider] and existing["status"] == "complete" and (
        existing["size_bytes"], existing["mtime_ns"]
    ) == (initial_stat.st_size, initial_stat.st_mtime_ns):
        source_id = existing["source_id"]
        events = db.execute("SELECT COUNT(*) FROM events WHERE source_id=?", (source_id,)).fetchone()[0]
        return {
            "path": str(path), "status": "unchanged", "events": 0,
            "nonblank_lines": events, "issues": existing["issue_count"],
        }
    stat, source_bytes = stable_source_snapshot(resolved, watermark=source_watermark)
    source_hash = digest(source_bytes)
    source_id = existing["source_id"] if existing else digest((provider + "\0" + str(resolved)).encode())
    if existing and existing["source_hash"] == source_hash and existing["adapter_version"] == ADAPTER_VERSION[provider]:
        db.execute(
            "UPDATE sources SET size_bytes=?,mtime_ns=? WHERE source_id=?",
            (stat.st_size, stat.st_mtime_ns, source_id),
        )
        update_source_ordering(db, source_id, stat)
        if projection_was_current:
            refresh_projection_metadata(db)
        db.commit()
        events = db.execute("SELECT COUNT(*) FROM events WHERE source_id=?", (source_id,)).fetchone()[0]
        return {"path": str(path), "status": "unchanged", "events": 0,
                "nonblank_lines": events, "issues": existing["issue_count"]}

    append_state = load_adapter_state(db, source_id) if existing else None
    append_only = bool(
        existing and append_state is not None
        and existing["adapter_version"] == ADAPTER_VERSION[provider]
        and existing["status"] == "complete"
        and stat.st_size > existing["size_bytes"]
        and source_bytes[:existing["size_bytes"]].endswith(b"\n")
        and digest(source_bytes[:existing["size_bytes"]]) == existing["source_hash"]
    )

    db.execute("BEGIN IMMEDIATE")
    if append_only:
        structure_buffer = bytearray(zstd.decompress(
            existing["structure_zstd"], max_output_size=existing["structure_raw_bytes"],
        ))
        meta = append_state
        sessions = {
            row[0] for row in db.execute("SELECT session_id FROM sessions WHERE source_id=?", (source_id,))
        }
        start_line = source_bytes[:existing["size_bytes"]].count(b"\n") + 1
        ingest_bytes = source_bytes[existing["size_bytes"]:]
        prior_issues = existing["issue_count"]
    else:
        db.execute("DELETE FROM events WHERE source_id=?", (source_id,))
        db.execute("DELETE FROM sessions WHERE source_id=?", (source_id,))
        db.execute("DELETE FROM sources WHERE source_id=?", (source_id,))
        db.execute(
            "INSERT INTO sources VALUES(?,?,?,?,?,?,?,?,0,0,?)",
            (source_id, provider, str(resolved), stat.st_size, stat.st_mtime_ns, source_hash,
             ADAPTER_VERSION[provider], "ingesting", zstd.compress(b"", level=ZSTD_LEVEL)),
        )
        structure_buffer = bytearray()
        meta = {}
        sessions = set()
        start_line = 1
        ingest_bytes = source_bytes
        prior_issues = 0
    object_pack = pack_path(db)
    object_pack.parent.mkdir(parents=True, exist_ok=True)
    search_pack = ensure_projection_pack(db)
    auto_extensions = load_auto_metadata_extensions(db, provider)
    with object_pack.open("ab") as pack_handle, search_pack.open("ab") as projection_handle:
        projection_writer = ProjectionBlockWriter(db, projection_handle)
        count, issues = store_source_lines(
            db, provider, resolved, source_id, ingest_bytes, start_line,
            structure_buffer, meta, sessions, pack_handle, projection_writer, auto_extensions,
        )
        projection_writer.flush()
        pack_handle.flush()
        os.fsync(pack_handle.fileno())
        projection_handle.flush()
        os.fsync(projection_handle.fileno())
    update_source_ordering(db, source_id, stat)
    save_adapter_state(db, source_id, persistent_adapter_state(provider, meta))
    db.execute(
        "UPDATE sources SET size_bytes=?,mtime_ns=?,source_hash=?,adapter_version=?,status='complete',"
        "issue_count=?,structure_raw_bytes=?,structure_zstd=? "
        "WHERE source_id=?",
        (stat.st_size, stat.st_mtime_ns, source_hash, ADAPTER_VERSION[provider], prior_issues + issues,
         len(structure_buffer), zstd.compress(bytes(structure_buffer), level=ZSTD_LEVEL), source_id),
    )
    if projection_was_current:
        refresh_projection_metadata(db)
    db.commit()
    clear_caches()
    total_events = db.execute("SELECT COUNT(*) FROM events WHERE source_id=?", (source_id,)).fetchone()[0]
    return {
        "path": str(path), "status": "appended" if append_only else "imported", "events": count,
        "nonblank_lines": total_events, "issues": prior_issues + issues,
    }


def ingest_many(db: sqlite3.Connection, provider: str, paths: Iterable[Path]) -> list[dict[str, Any]]:
    results = []
    for path in paths:
        try:
            results.append(ingest(db, provider, path))
        except Exception as exc:
            db.rollback()
            results.append({"path": str(path), "status": "failed", "events": 0, "issues": 1, "error": str(exc)})
    return results


def projection_snapshot_id(db: sqlite3.Connection) -> str:
    state = [PROJECTION_VERSION]
    state.extend(
        "\0".join((
            row["provider"], row["source_id"], row["source_hash"], str(row["size_bytes"]),
            str(row["mtime_ns"]), row["adapter_version"], row["status"], str(row["issue_count"]),
        ))
        for row in db.execute(
            "SELECT provider,source_id,source_hash,size_bytes,mtime_ns,adapter_version,status,issue_count "
            "FROM sources ORDER BY provider,source_id"
        )
    )
    state.extend(
        "\0".join((row["provider"], row["category"], row["structural_signature"]))
        for row in db.execute(
            "SELECT provider,category,structural_signature FROM adapter_extensions "
            "ORDER BY provider,category,structural_signature"
        )
    )
    return digest("\n".join(state).encode())


def refresh_projection_metadata(db: sqlite3.Connection) -> str:
    snapshot_id = projection_snapshot_id(db)
    event_count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    projection_count = db.execute("SELECT COUNT(*) FROM positional_projection_fts").fetchone()[0]
    passage_count = db.execute("SELECT COUNT(*) FROM search_projection_refs").fetchone()[0]
    if event_count != projection_count or event_count != passage_count:
        raise ValueError(
            f"projection count mismatch: {projection_count} indexes and "
            f"{passage_count} passages for {event_count} events"
        )
    for key, value in (
        ("projection_version", PROJECTION_VERSION),
        ("projection_snapshot_id", snapshot_id),
        ("projection_event_count", str(event_count)),
    ):
        db.execute("INSERT OR REPLACE INTO metadata(key,value) VALUES(?,?)", (key, value))
    return snapshot_id


def projection_status(
    db: sqlite3.Connection, *, pack_available: bool | None = None, deep: bool = True,
) -> dict[str, Any]:
    expected = projection_snapshot_id(db)
    stored = {
        row["key"]: row["value"] for row in db.execute(
            "SELECT key,value FROM metadata WHERE key IN "
            "('projection_version','projection_snapshot_id','projection_event_count',"
            "'projection_pack_generation')"
        )
    }
    if deep:
        event_count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        projection_count = db.execute("SELECT COUNT(*) FROM positional_projection_fts").fetchone()[0]
        passage_count = db.execute("SELECT COUNT(*) FROM search_projection_refs").fetchone()[0]
    else:
        event_count = int(stored.get("projection_event_count", "-1"))
        projection_count = event_count
        passage_count = event_count
    invalid_refs = 0
    if deep:
        invalid_refs = db.execute(
            "SELECT COUNT(*) FROM search_projection_refs r "
            "LEFT JOIN search_projection_blocks b USING(block_key) "
            "WHERE b.block_key IS NULL OR r.projection_offset<0 OR r.projection_bytes<0 "
            "OR r.projection_offset+r.projection_bytes>b.raw_bytes "
            "OR b.compressed_hash IS NULL OR r.reference_hash IS NULL"
        ).fetchone()[0]
    complete = (
        stored.get("projection_version") == PROJECTION_VERSION
        and stored.get("projection_snapshot_id") == expected
        and stored.get("projection_event_count") == str(event_count)
        and projection_count == event_count
        and passage_count == event_count
        and invalid_refs == 0
        and (projection_pack_path(db).is_file() if pack_available is None else pack_available)
    )
    return {
        "complete": complete,
        "snapshot_id": expected,
        "projection_version": PROJECTION_VERSION,
        "events": event_count,
        "projections": projection_count, "passages": passage_count,
        "invalid_references": invalid_refs,
        "reason": "projection_current" if complete else "projection_missing_or_stale",
    }


def projection_integrity_receipt_digest(receipt: dict[str, Any]) -> str:
    value = {key: item for key, item in receipt.items() if key != "receipt_sha256"}
    return digest(canonical(value))


def build_projection_integrity_receipt(
    db: sqlite3.Connection, *, created_at: str | None = None,
) -> dict[str, Any]:
    """Deep-check one committed generation and describe it without corpus content."""
    sqlite_before = sqlite_generation_identity(db)
    status = projection_status(db, pack_available=True, deep=True)
    if not status["complete"]:
        raise ValueError(f"projection integrity receipt requires a complete index: {status['reason']}")
    stored = {
        row["key"]: row["value"] for row in db.execute(
            "SELECT key,value FROM metadata WHERE key IN "
            "('schema_version','projection_version','projection_snapshot_id',"
            "'projection_event_count','projection_pack_generation')"
        )
    }
    pack = projection_pack_path(db)
    sqlite_after = sqlite_generation_identity(db)
    if sqlite_before != sqlite_after:
        raise ValueError("database generation changed during receipt verification")
    receipt = {
        "schema": PROJECTION_INTEGRITY_RECEIPT_SCHEMA,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        "schema_version": stored.get("schema_version"),
        "projection_version": stored.get("projection_version"),
        "snapshot_id": stored.get("projection_snapshot_id"),
        "event_count": status["events"],
        "projection_count": status["projections"],
        "passage_count": status["passages"],
        "invalid_references": status["invalid_references"],
        "sqlite": sqlite_after,
        "pack": projection_pack_identity(pack),
    }
    receipt["receipt_sha256"] = projection_integrity_receipt_digest(receipt)
    return receipt


def _projection_status_from_receipt(
    db: sqlite3.Connection, path: Path | None = None,
) -> dict[str, Any] | None:
    """Return status plus private bindings when every cheap receipt check matches."""
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])
    receipt_path = path or projection_integrity_receipt_path(database)
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(receipt, dict):
        return None
    expected_digest = receipt.get("receipt_sha256")
    if (
        receipt.get("schema") != PROJECTION_INTEGRITY_RECEIPT_SCHEMA
        or not isinstance(expected_digest, str)
        or expected_digest != projection_integrity_receipt_digest(receipt)
    ):
        return None
    try:
        sqlite_before = sqlite_generation_identity(db)
        stored = {
            row["key"]: row["value"] for row in db.execute(
                "SELECT key,value FROM metadata WHERE key IN "
                "('schema_version','projection_version','projection_snapshot_id',"
                "'projection_event_count','projection_pack_generation')"
            )
        }
        event_count = int(stored.get("projection_event_count", "-1"))
        pack = projection_pack_path(db)
        pack_identity = projection_pack_identity(pack)
        sqlite_after = sqlite_generation_identity(db)
    except (OSError, TypeError, ValueError):
        return None
    valid = (
        sqlite_before == sqlite_after == receipt.get("sqlite")
        and stored.get("schema_version") == receipt.get("schema_version") == SCHEMA_VERSION
        and stored.get("projection_version") == receipt.get("projection_version") == PROJECTION_VERSION
        and stored.get("projection_snapshot_id") == receipt.get("snapshot_id")
        and event_count == receipt.get("event_count")
        and receipt.get("projection_count") == event_count
        and receipt.get("passage_count") == event_count
        and receipt.get("invalid_references") == 0
        and stored.get("projection_pack_generation") == pack_identity["generation"]
        and receipt.get("pack") == pack_identity
    )
    if not valid:
        return None
    return {
        "complete": True,
        "snapshot_id": receipt["snapshot_id"],
        "projection_version": PROJECTION_VERSION,
        "events": event_count,
        "projections": event_count,
        "passages": event_count,
        "invalid_references": 0,
        "reason": "projection_current_receipt",
        "receipt_created_at": receipt.get("created_at"),
        "_receipt_sha256": expected_digest,
        "_sqlite_generation": receipt["sqlite"],
    }


def projection_status_from_receipt(
    db: sqlite3.Connection, path: Path | None = None,
) -> dict[str, Any] | None:
    """Return receipt-backed status without its private transaction bindings."""
    status = _projection_status_from_receipt(db, path)
    if status is None:
        return None
    return {key: value for key, value in status.items() if not key.startswith("_")}


def repair_projection_metadata(db: sqlite3.Connection) -> bool:
    """Writer-only: re-record metadata for a verified-consistent projection whose metadata drifted.

    Readers still verify counts and references on every search; current metadata only spares
    them the query-scoped fallback scan, so this requires row-level agreement, not equal counts.
    """
    if projection_status(db, deep=False)["complete"]:
        return False  # metadata already matches the current source snapshot
    db.execute("BEGIN IMMEDIATE")
    try:
        status = projection_status(db, deep=True)
        stored_version = db.execute(
            "SELECT value FROM metadata WHERE key='projection_version'"
        ).fetchone()
        consistent = (
            not status["complete"]
            and stored_version is not None and stored_version[0] == PROJECTION_VERSION
            and status["invalid_references"] == 0
            and status["events"] == status["projections"] == status["passages"]
            and projection_pack_path(db).is_file()
            and db.execute(
                "SELECT NOT EXISTS (SELECT 1 FROM events e WHERE NOT EXISTS "
                "(SELECT 1 FROM search_projection_refs r WHERE r.event_rowid=e.rowid))"
            ).fetchone()[0]
            and db.execute(
                "SELECT NOT EXISTS (SELECT 1 FROM search_projection_refs r WHERE NOT EXISTS "
                "(SELECT 1 FROM events e WHERE e.rowid=r.event_rowid))"
            ).fetchone()[0]
            and db.execute(
                "SELECT NOT EXISTS (SELECT 1 FROM events e WHERE NOT EXISTS "
                "(SELECT 1 FROM positional_projection_fts f WHERE f.rowid=e.rowid))"
            ).fetchone()[0]
        )
        if not consistent:
            db.rollback()
            return False
        refresh_projection_metadata(db)
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return True


def rebuild_receipt_digest(receipt: dict[str, Any]) -> str:
    payload = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    return digest(canonical(payload))


def rebuild_receipt_ledger_dir() -> Path:
    return Path.home() / ".local" / "share" / "sessiondb-public" / "data" / "rebuild-receipts"


def rebuild_receipt_consumption_path(receipt_sha256: str) -> Path:
    return rebuild_receipt_ledger_dir() / (receipt_sha256 + ".consumed.json")


def validate_frozen_rebuild_inputs(receipt: dict[str, Any]) -> None:
    repository = receipt.get("repository")
    plan = receipt.get("plan")
    if not isinstance(repository, dict) or not isinstance(plan, dict):
        raise ValueError("rebuild readiness frozen inputs are missing")
    try:
        root = Path(repository["root"]).resolve(strict=True)
        plan_path = Path(plan["path"]).resolve(strict=True)
    except (KeyError, OSError, RuntimeError) as exc:
        raise ValueError("rebuild readiness frozen inputs are unavailable") from exc
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True,
            capture_output=True, text=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"], cwd=root, check=True,
            capture_output=True, text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ValueError("cannot verify the frozen rebuild revision") from exc
    if revision != receipt.get("revision") or dirty:
        raise ValueError("rebuild readiness revision is no longer frozen")
    try:
        expected_plan = (root / plan.get("relative_path", "")).resolve(strict=True)
    except (OSError, RuntimeError, TypeError) as exc:
        raise ValueError("rebuild readiness plan path is invalid") from exc
    if plan_path != expected_plan:
        raise ValueError("rebuild readiness plan path is outside the frozen repository")
    if digest(plan_path.read_bytes()) != plan.get("sha256"):
        raise ValueError("rebuild readiness plan has changed")


def validate_rebuild_readiness_receipt(
    db: sqlite3.Connection, path: Path, event_count: int,
) -> dict[str, Any]:
    path = path.expanduser()
    file_stat = path.lstat()
    if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode) or file_stat.st_size > 1024 * 1024:
        raise ValueError("invalid rebuild readiness receipt file")
    path = path.resolve(strict=True)
    receipt = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(receipt, dict) or receipt.get("schema") != REBUILD_READINESS_SCHEMA:
        raise ValueError("unsupported rebuild readiness receipt")
    claimed_digest = receipt.get("receipt_sha256")
    if not isinstance(claimed_digest, str) or claimed_digest != rebuild_receipt_digest(receipt):
        raise ValueError("rebuild readiness receipt integrity failed")
    if rebuild_receipt_consumption_path(claimed_digest).exists():
        raise ValueError("rebuild readiness receipt was already consumed")
    try:
        created_at = datetime.fromisoformat(receipt["created_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("invalid rebuild readiness timestamp") from exc
    if created_at.tzinfo is None:
        raise ValueError("rebuild readiness timestamp must include a timezone")
    age = (datetime.now(timezone.utc) - created_at.astimezone(timezone.utc)).total_seconds()
    if age < -300 or age > REBUILD_READINESS_MAX_AGE_SECONDS:
        raise ValueError("rebuild readiness receipt is stale")
    if receipt.get("module_sha256") != digest(Path(__file__).read_bytes()):
        raise ValueError("rebuild readiness receipt does not match SessionDB code")
    validate_frozen_rebuild_inputs(receipt)
    corpus = receipt.get("corpus")
    if not isinstance(corpus, dict):
        raise ValueError("rebuild readiness corpus binding is missing")
    if corpus.get("events") != event_count or corpus.get("snapshot_id") != projection_snapshot_id(db):
        raise ValueError("rebuild readiness receipt does not match the corpus")
    if corpus.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("rebuild readiness receipt does not match the schema")
    if receipt.get("projection_version") != PROJECTION_VERSION:
        raise ValueError("rebuild readiness receipt does not match the projection design")
    if receipt.get("unit_tests", {}).get("pass") is not True:
        raise ValueError("rebuild readiness unit tests did not pass")
    if receipt.get("representative_samples", {}).get("pass") is not True:
        raise ValueError("rebuild readiness representative samples did not pass")
    if receipt.get("tokenizer", {}).get("pass") is not True:
        raise ValueError("rebuild readiness tokenizer validation did not pass")
    review = receipt.get("material_review")
    if not isinstance(review, dict) or (
        review.get("outcome") != "approved"
        or review.get("material_changes_reviewed") is not True
        or review.get("reviewed_revision") != receipt.get("revision")
        or review.get("plan_sha256") != receipt.get("plan", {}).get("sha256")
    ):
        raise ValueError("rebuild readiness material review is missing or mismatched")
    required_free = receipt.get("capacity", {}).get("required_free_bytes")
    if receipt.get("capacity", {}).get("pass") is not True or not isinstance(required_free, int) or required_free < 0:
        raise ValueError("rebuild readiness capacity check is invalid")
    if shutil.disk_usage(Path(db.execute("PRAGMA database_list").fetchone()["file"]).parent).free < required_free:
        raise ValueError("free disk has fallen below the reviewed rebuild requirement")
    return receipt


def preflight_full_rebuild_readiness(
    db: sqlite3.Connection, path: Path | None,
) -> dict[str, Any] | None:
    event_count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    if event_count < FULL_REBUILD_RECEIPT_MIN_EVENTS:
        return None
    if path is None:
        raise ValueError("full-corpus rebuild requires a current one-use readiness receipt")
    return validate_rebuild_readiness_receipt(db, path, event_count)


def consume_rebuild_readiness_receipt(
    db: sqlite3.Connection, path: Path, event_count: int,
) -> Path:
    receipt = validate_rebuild_readiness_receipt(db, path, event_count)
    resolved = path.expanduser().absolute()
    ledger = rebuild_receipt_ledger_dir()
    ledger.mkdir(parents=True, exist_ok=True, mode=0o700)
    consumed = rebuild_receipt_consumption_path(receipt["receipt_sha256"])
    try:
        descriptor = os.open(consumed, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise ValueError("rebuild readiness receipt was already consumed") from exc
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(canonical({
            "receipt_sha256": receipt["receipt_sha256"],
            "revision": receipt["revision"],
            "corpus": receipt["corpus"],
            "consumed_at": datetime.now(timezone.utc).isoformat(),
        }))
        handle.flush()
        os.fsync(handle.fileno())
    fsync_directory(ledger)
    try:
        resolved.unlink()
        fsync_directory(resolved.parent)
    except FileNotFoundError:
        pass
    return consumed


def build_search_projections(
    db: sqlite3.Connection, progress: Callable[[dict[str, Any]], None] | None = None,
    readiness_receipt: Path | None = None,
) -> dict[str, Any]:
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])
    lease = acquire_projection_pack_lease(db, exclusive=True)
    descriptor = None
    new_pack = None
    consumed_receipt = None
    try:
        db.execute("BEGIN IMMEDIATE")
        event_count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        if event_count >= FULL_REBUILD_RECEIPT_MIN_EVENTS:
            if readiness_receipt is None:
                raise ValueError(
                    "full-corpus rebuild requires a current one-use readiness receipt"
                )
            consumed_receipt = consume_rebuild_readiness_receipt(
                db, readiness_receipt, event_count,
            )
        old_pack = projection_pack_path(db)
        generation = database.name + ".retrieval." + os.urandom(12).hex()
        new_pack = database.parent / generation
        descriptor = os.open(new_pack, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        detail = "full" if fts_index_is_positional(db) else "column"
        db.execute("DROP TABLE IF EXISTS positional_projection_fts")
        create_search_fts(db, "positional_projection_fts", detail)
        db.execute("DELETE FROM search_term_variants")
        db.execute("DELETE FROM search_projection_refs")
        db.execute("DELETE FROM search_projection_blocks")
        db.execute(
            "INSERT OR REPLACE INTO metadata(key,value) VALUES('projection_pack_generation',?)",
            (generation,),
        )
        with os.fdopen(descriptor, "ab") as projection_handle:
            descriptor = None
            writer = ProjectionBlockWriter(db, projection_handle)
            with forbid_source_transcript_reads() as source_reads:
                for processed, row in enumerate(
                    db.execute("SELECT rowid AS event_rowid,* FROM events ORDER BY rowid"), 1,
                ):
                    value: Any = "[REDACTED]"
                    if row["category"] != "unreadable":
                        value = read_event_structure(db, row)
                    put_search_projection(db, row["event_rowid"], value, writer)
                    if progress and (processed == 1 or processed % 10_000 == 0 or processed == event_count):
                        progress({"phase": "projection_rebuild", "processed": processed, "total": event_count})
            writer.flush()
            projection_handle.flush()
            os.fsync(projection_handle.fileno())
        fsync_directory(database.parent)
        db.execute("DROP TABLE IF EXISTS lexical_projection_fts")
        db.execute("DROP TABLE IF EXISTS supplemental_projection_literal_fts")
        snapshot_id = refresh_projection_metadata(db)
        db.commit()
    except Exception:
        db.rollback()
        if descriptor is not None:
            os.close(descriptor)
        try:
            if new_pack is not None:
                new_pack.unlink()
        except FileNotFoundError:
            pass
        release_projection_pack_lease(lease)
        raise
    try:
        if old_pack != new_pack and old_pack.exists() and owned_projection_pack(database, old_pack):
            old_pack.unlink()
    finally:
        release_projection_pack_lease(lease)
    clear_caches()
    return {
        "complete": True, "events": event_count, "snapshot_id": snapshot_id,
        "projection_version": PROJECTION_VERSION, "source_reads": source_reads["attempts"],
        "readiness_receipt": str(consumed_receipt) if consumed_receipt else None,
    }


def upgrade_phrase_index(
    db: sqlite3.Connection, readiness_receipt: Path | None = None,
) -> dict[str, Any]:
    """Compatibility entrypoint: rebuild the current positional projection."""
    return build_search_projections(db, readiness_receipt=readiness_receipt)


_SOURCE_CACHE: OrderedDict[tuple[str, str], bytes] = OrderedDict()
_STORED_OBJECT_CACHE: OrderedDict[tuple[str, str], tuple[dict[str, Any], Any]] = OrderedDict()
_CONTENT_OBJECT_CACHE: OrderedDict[Any, Any] = OrderedDict()
_OBJECT_CHILDREN_CACHE: OrderedDict[tuple[str, str], list[str]] = OrderedDict()
_REDACTED_OBJECT_CACHE: OrderedDict[tuple[str, str], Any] = OrderedDict()
_STRUCTURE_BUNDLE_CACHE: OrderedDict[tuple[str, str, str], bytes] = OrderedDict()
_STRUCTURE_META_CACHE: OrderedDict[tuple[str, str], tuple[str, int]] = OrderedDict()
_EVENT_SEARCH_TEXT_CACHE: OrderedDict[tuple[str, str, str, bool], str] = OrderedDict()
_EVENT_PROJECTED_TEXT_CACHE: OrderedDict[tuple[str, str, bool], str] = OrderedDict()
_RESPONSE_FACTOR_CACHE: OrderedDict[str, dict[str, Any]] = OrderedDict()
_QUERY_CLEAN_CACHE: OrderedDict[Any, dict[str, Any]] = OrderedDict()
EVENT_SEARCH_TEXT_CACHE_LIMIT = 32768
_SOURCE_READ_GUARD: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "sessiondb_source_read_guard", default=None,
)
_SOURCE_READ_REASON: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "sessiondb_source_read_reason", default=None,
)


def cache_put(cache: OrderedDict[Any, Any], key: Any, value: Any, limit: int = CACHE_LIMIT) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > limit:
        cache.popitem(last=False)


def clear_caches() -> None:
    for cache in (
        _SOURCE_CACHE, _STORED_OBJECT_CACHE, _CONTENT_OBJECT_CACHE, _OBJECT_CHILDREN_CACHE,
        _REDACTED_OBJECT_CACHE, _STRUCTURE_BUNDLE_CACHE, _STRUCTURE_META_CACHE, _EVENT_SEARCH_TEXT_CACHE,
        _EVENT_PROJECTED_TEXT_CACHE, _RESPONSE_FACTOR_CACHE, _QUERY_CLEAN_CACHE,
    ):
        cache.clear()


@contextmanager
def forbid_source_transcript_reads() -> Iterator[dict[str, Any]]:
    """Fail a serving test on every source-read attempt, including cache hits."""
    telemetry: dict[str, Any] = {"attempts": 0, "sources": []}
    token = _SOURCE_READ_GUARD.set(telemetry)
    try:
        yield telemetry
    finally:
        _SOURCE_READ_GUARD.reset(token)


@contextmanager
def source_transcript_read_reason(reason: str) -> Iterator[None]:
    token = _SOURCE_READ_REASON.set(reason)
    try:
        yield
    finally:
        _SOURCE_READ_REASON.reset(token)


def source_read_audit_path(db: sqlite3.Connection) -> Path:
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])
    return Path(str(database) + ".source-reads.jsonl")


def record_source_read(db: sqlite3.Connection, source_id: str, source_path: str) -> None:
    audit_path = source_read_audit_path(db)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "source_id": source_id,
        "source_path": source_path,
        "reason": _SOURCE_READ_REASON.get() or "unclassified",
    }
    with audit_path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
        fcntl.flock(handle, fcntl.LOCK_UN)
    os.chmod(audit_path, 0o600)


def read_source(db: sqlite3.Connection, source_id: str) -> bytes:
    row = db.execute(
        "SELECT path,source_hash FROM sources WHERE source_id=?",
        (source_id,),
    ).fetchone()
    if not row:
        raise KeyError(source_id)
    guard = _SOURCE_READ_GUARD.get()
    if guard is not None:
        guard["attempts"] += 1
        guard["sources"].append(row["path"])
        raise RuntimeError(f"serving path attempted source transcript read: {row['path']}")
    record_source_read(db, source_id, row["path"])
    key = (row["path"], row["source_hash"])
    if key not in _SOURCE_CACHE:
        data = Path(row["path"]).read_bytes()
        if digest(data) != row["source_hash"]:
            raise ValueError(f"source hash verification failed: {source_id}")
        cache_put(_SOURCE_CACHE, key, data, 64)
    return _SOURCE_CACHE[key]


def source_row(db: sqlite3.Connection, source_id: str, line_number: int) -> tuple[bytes, Any]:
    lines = jsonl_lines(read_source(db, source_id))
    if line_number < 1 or line_number > len(lines):
        raise ValueError(f"source line missing: {source_id}:{line_number}")
    raw = lines[line_number - 1]
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        value = raw.decode("utf-8", errors="replace")
    return raw, value


def content_from_row(provider: str, value: Any) -> Any | None:
    if not isinstance(value, dict):
        return None
    if provider == "codex":
        payload = value.get("payload") if isinstance(value.get("payload"), dict) else {}
        payload_type = str(payload.get("type")) if payload.get("type") else None
        return codex_content(payload, payload_type)[0]
    message = value.get("message")
    if isinstance(message, dict) and message.get("content") is not None:
        return message["content"]
    if message is not None and not isinstance(message, dict):
        return message
    for key in ("content", "summary", "lastPrompt", "aiTitle"):
        if value.get(key) is not None:
            return value[key]
    return None


def read_stored_object(db: sqlite3.Connection, object_hash: str) -> tuple[sqlite3.Row, Any]:
    object_pack = pack_path(db)
    pack_stat = object_pack.stat()
    cache_key = (str(object_pack.resolve()), object_hash, pack_stat.st_mtime_ns, pack_stat.st_size)
    cached = _STORED_OBJECT_CACHE.get(cache_key)
    if cached is not None:
        return cached
    row = db.execute("SELECT * FROM objects WHERE object_hash=?", (object_hash,)).fetchone()
    if not row:
        raise KeyError(object_hash)
    if row["compression"] != COMPRESSION:
        raise ValueError(f"unsupported object compression: {row['compression']}")
    with object_pack.open("rb") as handle:
        handle.seek(row["pack_offset"])
        compressed = handle.read(row["compressed_bytes"])
    if len(compressed) != row["compressed_bytes"]:
        raise ValueError(f"stored object extent missing: {object_hash}")
    if row["stored_bytes"] > MAX_OBJECT_BYTES:
        raise ValueError(f"stored object exceeds safety limit: {object_hash}")
    data = zstd.decompress(compressed, max_output_size=row["stored_bytes"])
    if len(data) != row["stored_bytes"] or digest(data) != row["stored_hash"]:
        raise ValueError(f"stored object length verification failed: {object_hash}")
    result = (dict(row), json.loads(data))
    cache_put(_STORED_OBJECT_CACHE, cache_key, result)
    return result


def read_content_object(
    db: sqlite3.Connection, object_hash: str, *, _visited: set[str] | None = None, _depth: int = 0,
) -> Any:
    object_pack = pack_path(db)
    pack_stat = object_pack.stat()
    cache_key = (str(object_pack.resolve()), object_hash, pack_stat.st_mtime_ns, pack_stat.st_size)
    if _depth == 0 and cache_key in _CONTENT_OBJECT_CACHE:
        return _CONTENT_OBJECT_CACHE[cache_key]
    if _depth > MAX_OBJECT_DEPTH:
        raise ValueError(f"object reference depth exceeded: {object_hash}")
    visited = set() if _visited is None else _visited
    if object_hash in visited:
        raise ValueError(f"object reference cycle detected: {object_hash}")
    visited.add(object_hash)
    row, value = read_stored_object(db, object_hash)
    if row["kind"] == "manifest":
        def expand(item: Any) -> Any:
            if isinstance(item, dict) and set(item) == {REF_KEY}:
                child = read_content_object(
                    db, item[REF_KEY], _visited=visited, _depth=_depth + 1,
                )
                if not isinstance(child, str):
                    raise ValueError(f"instruction reference is not text: {item[REF_KEY]}")
                return child
            if isinstance(item, dict) and set(item) == {CONCAT_KEY}:
                parts = [expand(child) for child in item[CONCAT_KEY]]
                if not all(isinstance(part, str) for part in parts):
                    raise ValueError(f"invalid concatenation manifest: {object_hash}")
                return "".join(parts)
            if isinstance(item, list):
                return [expand(child) for child in item]
            if isinstance(item, dict):
                return {key: expand(child) for key, child in item.items()}
            return item
        value = expand(value)
    visited.remove(object_hash)
    if digest(stored_json(value)) != object_hash:
        raise ValueError(f"content hash verification failed: {object_hash}")
    if _depth == 0:
        cache_put(_CONTENT_OBJECT_CACHE, cache_key, value)
    return value


def redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, list):
        return [redact_value(child) for child in value]
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if (
                SECRET_FIELD_RE.search(str(key)) or str(key).rsplit(".", 1)[-1] in SEARCH_EXCLUDED_LEAVES
            )
            else redact_value(child)
            for key, child in value.items()
        }
    return value


def redacted_content_object(db: sqlite3.Connection, object_hash: str) -> Any:
    object_pack = pack_path(db)
    stat = object_pack.stat()
    key = (str(object_pack.resolve()), object_hash, stat.st_mtime_ns, stat.st_size)
    cached = _REDACTED_OBJECT_CACHE.get(key)
    if cached is None:
        cached = redact_value(read_content_object(db, object_hash))
        cache_put(_REDACTED_OBJECT_CACHE, key, cached)
    return cached


def factor_redacted_response(value: Any) -> tuple[str, dict[str, Any]]:
    if (
        isinstance(value, dict) and value.get("type") in {"text", "input_text", "output_text"}
        and set(value) == {"type", "text"} and isinstance(value.get("text"), str)
    ):
        value = value["text"]
    root_hash = digest(stored_json(value))
    cached = _RESPONSE_FACTOR_CACHE.get(root_hash)
    if cached is not None:
        return root_hash, cached
    objects: dict[str, Any] = {}

    def add(item: Any) -> str:
        item_hash = digest(stored_json(item))
        if item_hash in objects:
            return item_hash
        references = False

        def factor(child: Any, root: bool = False) -> Any:
            nonlocal references
            if isinstance(child, str):
                spans = [span for span in structured_text_spans(child) if not (span[0] == 0 and span[1] == len(child))]
                if spans:
                    parts: list[Any] = []
                    position = 0
                    for start, end, block, _purpose in spans:
                        if start > position:
                            parts.append(child[position:start])
                        child_hash = add(block)
                        parts.append({REF_KEY: child_hash})
                        references = True
                        position = end
                    if position < len(child):
                        parts.append(child[position:])
                    return {CONCAT_KEY: parts}
                if not root and len(canonical(child)) >= MIN_SHARED_TEXT_BYTES:
                    references = True
                    return {REF_KEY: add(child)}
                return child
            if isinstance(child, list):
                return [factor(grandchild) for grandchild in child]
            if isinstance(child, dict):
                return {key: factor(grandchild) for key, grandchild in child.items()}
            return child

        manifest = factor(item, root=True)
        objects[item_hash] = manifest if references else item
        return item_hash

    add(value)
    cache_put(_RESPONSE_FACTOR_CACHE, root_hash, objects, CACHE_LIMIT)
    return root_hash, objects


def read_event_content(db: sqlite3.Connection, event: sqlite3.Row) -> Any | None:
    if not event["content_hash"]:
        return None
    refs = db.execute(
        "SELECT object_hash FROM event_content WHERE event_id=? ORDER BY ordinal", (event["event_id"],),
    ).fetchall()
    values = [read_content_object(db, ref[0]) for ref in refs]
    value = values if event["content_shape"] == "list" else (values[0] if values else None)
    if digest(stored_json(value)) != event["content_hash"]:
        raise ValueError(f"event content hash verification failed: {event['event_id']}")
    return value


def exact_object_closure(db: sqlite3.Connection, roots: Iterable[str]) -> set[str]:
    visited: set[str] = set()
    frontier = list(roots)
    while frontier:
        object_hash = frontier.pop()
        if object_hash in visited:
            continue
        visited.add(object_hash)
        frontier.extend(
            row[0] for row in db.execute(
                "SELECT child_hash FROM object_references WHERE parent_hash=? ORDER BY ordinal",
                (object_hash,),
            )
        )
    return visited


def read_event_structure(db: sqlite3.Connection, event: sqlite3.Row) -> dict[str, Any]:
    if event["category"] == "unreadable":
        raise ValueError(f"unreadable source event has no parsed structure: {event['event_id']}")
    database = db.execute("PRAGMA database_list").fetchone()["file"]
    meta_key = (database, event["source_id"])
    metadata = _STRUCTURE_META_CACHE.get(meta_key)
    if metadata is None:
        source = db.execute(
            "SELECT source_hash,structure_raw_bytes FROM sources WHERE source_id=?",
            (event["source_id"],),
        ).fetchone()
        if not source:
            raise KeyError(event["source_id"])
        metadata = (source["source_hash"], source["structure_raw_bytes"])
        cache_put(_STRUCTURE_META_CACHE, meta_key, metadata, 1024)
    source_hash, structure_raw_bytes = metadata
    cache_key = (database, event["source_id"], source_hash)
    bundle = _STRUCTURE_BUNDLE_CACHE.get(cache_key)
    if bundle is None:
        source = db.execute(
            "SELECT structure_zstd FROM sources WHERE source_id=? AND source_hash=?",
            (event["source_id"], source_hash),
        ).fetchone()
        if not source:
            raise KeyError(event["source_id"])
        bundle = zstd.decompress(source["structure_zstd"], max_output_size=structure_raw_bytes)
        if len(bundle) != structure_raw_bytes:
            raise ValueError(f"structure bundle length verification failed: {event['source_id']}")
        cache_put(_STRUCTURE_BUNDLE_CACHE, cache_key, bundle, 128)
    start = event["structure_offset"]
    structure = bundle[start:start + event["structure_bytes"]]
    if len(structure) != event["structure_bytes"]:
        raise ValueError(f"parsed event structure missing: {event['event_id']}")
    value = expand_parsed_structure(
        structure, read_event_content(db, event), lambda object_hash: read_content_object(db, object_hash),
    )
    if digest(stored_json(value)) != event["raw_hash"]:
        raise ValueError(f"parsed event hash verification failed: {event['event_id']}")
    return value


def read_raw_event(db: sqlite3.Connection, event: sqlite3.Row) -> Any:
    if event["category"] != "unreadable":
        return read_event_structure(db, event)
    retained = read_event_content(db, event)
    if isinstance(retained, dict) and retained.get("encoding") == "base64":
        raw = base64.b64decode(retained["unreadable_bytes_base64"], validate=True)
        if digest(raw) != event["raw_hash"]:
            raise ValueError(f"retained unreadable event hash verification failed: {event['event_id']}")
        return raw.decode("utf-8", errors="replace")
    raw, value = source_row(db, event["source_id"], event["line_number"])
    actual = digest(stored_json(value)) if isinstance(value, dict) else digest(raw)
    if actual != event["raw_hash"]:
        raise ValueError(f"raw event hash verification failed: {event['event_id']}")
    return value


def reconstruct(db: sqlite3.Connection, provider: str, session_id: str, raw: bool = False) -> list[dict[str, Any]]:
    rows = db.execute(
        "SELECT e.*,s.path AS source_path FROM events e JOIN sources s USING(source_id) "
        "WHERE e.provider=? AND e.session_id=? "
        "ORDER BY e.order_timestamp,e.source_id,e.line_number,e.event_id",
        (provider, session_id),
    ).fetchall()
    result = []
    for row in rows:
        object_hash = row["raw_hash"] if raw else row["content_hash"]
        result.append({
            "sequence": row["sequence"], "timestamp": row["timestamp"],
            "timestamp_utc": row["timestamp_utc"], "order_timestamp": row["order_timestamp"],
            "order_kind": row["order_kind"], "category": row["category"],
            "role": row["role"], "tool": row["tool"], "object_hash": object_hash,
            "content": read_raw_event(db, row) if raw else (
                read_event_content(db, row) if row["content_hash"] else None
            ),
            "source": {"path": row["source_path"], "line": row["line_number"], "raw_hash": row["raw_hash"]},
        })
    return result


def query_text_candidate_expression(db: sqlite3.Connection, text: str, raw: bool) -> str | None:
    """Index candidates for `query --text`: every event whose text can contain the substring.

    Each whole word of the substring must be a word of the event (CJK, emoji and symbol runs use
    their character anchors instead); the substring check runs afterwards. A raw search has no
    index candidates and checks every event in scope: raw text also holds keys, flags, and
    redacted or binary values the index leaves out."""
    if raw:
        return None
    words = re.findall(r"[^\W_]+", text, re.UNICODE)
    lexical = "{" + " ".join(PROJECTION_ORIGINS) + "}"
    literal = "{" + " ".join(PROJECTION_LITERAL_COLUMNS) + "}"
    with index_spellings(db) as spellings:
        if LITERAL_SEARCH_RE.search(text) or not words:
            tokens = literal_tokens(text, accent_tokens=spellings is None).split()
            expression = (
                f"{literal} : (" + " AND ".join(fts_phrase([token]) for token in tokens) + ")"
                if tokens else None
            )
        else:
            terms = list(dict.fromkeys(
                term for word in words for term in fts_candidate_parts(word)[0]
            ))
            expression = (
                f"{lexical} : (" + " AND ".join(fts_phrase([term]) for term in terms) + ")"
                if terms else None
            )
            near = query_text_near_group(text, words) if fts_index_is_positional(db) else None
            if expression and near:
                expression = f"({expression}) AND ({lexical} : {near})"
    return expression


def query_text_near_group(text: str, words: list[str]) -> str | None:
    """Require the substring's words close together, as they must be where it occurs.

    The first word may end a longer word (`not` inside `cannot`), so it joins only when the
    substring starts at a word boundary; the last may begin one, so it is a prefix unless the
    substring ends at a boundary. Each gap may hold the one boundary token placed between two
    stored text values, which the substring check can span."""
    needle = " ".join(text.split())
    parts = [fts_candidate_parts(word, preserve_occurrences=True)[0] for word in words]
    if len(words) < 2 or any(len(part) != 1 for part in parts):
        return None
    group = [fts_phrase(part) for part in parts]
    if re.match(r"[^\W_]", needle[-1], re.UNICODE) is not None:
        group[-1] += "*"
    if re.match(r"[^\W_]", needle[0], re.UNICODE) is not None:
        group = group[1:]
    if len(group) < 2:
        return None
    return f"NEAR({' '.join(group)}, {len(group) - 1})"


# A session filter alone would scan every event. Its sources come from the small sessions table;
# unreadable lines get no sessions row (their session is the file stem), so issue-bearing sources
# stay in scope and the prefilter remains a superset. The event-level clause still decides.
SESSION_SOURCE_PREFILTER = (
    "e.source_id IN (SELECT source_id FROM sessions WHERE session_id LIKE ? ESCAPE '\\' "
    "UNION SELECT source_id FROM sources WHERE issue_count>0)"
)


def session_source_prefilter(session: str | None) -> tuple[list[str], list[Any]]:
    if not session:
        return [], []
    escaped = session.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return [SESSION_SOURCE_PREFILTER], [f"%{escaped}%"]


def matching_rows(db: sqlite3.Connection, args: argparse.Namespace) -> tuple[Iterable[sqlite3.Row], bool]:
    clauses, values = session_source_prefilter(args.session)
    clauses.insert(0, "1=1")
    for column, value in (
        ("e.provider", args.provider), ("e.session_id", args.session),
        ("e.role", args.role), ("e.tool", args.tool), ("e.parent_session_id", args.lineage),
    ):
        if value:
            clauses.append(f"{column} LIKE ? ESCAPE '\\'")
            escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            values.append(f"%{escaped}%")
    if args.project:
        clauses.append("e.project LIKE ? ESCAPE '\\'")
        escaped = args.project.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        values.append(f"%{escaped}%")
    if args.since:
        clauses.append("e.order_timestamp >= ?")
        values.append(normalize_timestamp(args.since) or args.since)
    if args.until:
        clauses.append("e.order_timestamp <= ?")
        values.append(normalize_timestamp(args.until) or args.until)
    if args.text:
        expression = query_text_candidate_expression(db, args.text, args.raw)
        if expression:
            clauses.append(
                "e.rowid IN (SELECT rowid FROM positional_projection_fts "
                "WHERE positional_projection_fts MATCH ?)"
            )
            values.append(expression)
    sql = (
        "SELECT e.rowid AS event_rowid,e.*,src.path AS source_path "
        "FROM events e JOIN sources src USING(source_id) "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY e.order_timestamp,e.provider,e.session_id,e.source_id,e.line_number,e.event_id"
    )
    if args.text:
        return db.execute(sql, tuple(values)), False
    needed = max(0, getattr(args, "offset", 0)) + max(0, args.limit) + 1
    rows = db.execute(sql + " LIMIT ?", (*values, needed)).fetchall()
    return rows, len(rows) == needed


def without_duplicate_completed_item(structure: Any) -> Any:
    """Drop a Codex completed item that repeats a paired message, so `query --text` finds it once."""
    payload = structure.get("payload") if isinstance(structure, dict) else None
    item = payload.get("item") if isinstance(payload, dict) and payload.get("type") == "item_completed" else None
    if not isinstance(item, dict) or item.get("type") not in CODEX_COMPLETED_DUPLICATE_TYPES:
        return structure
    return {**structure, "payload": {key: value for key, value in payload.items() if key != "item"}}


def event_search_text(db: sqlite3.Connection, row: sqlite3.Row, raw: bool) -> str:
    database = db.execute("PRAGMA database_list").fetchone()["file"]
    cache_key = (database, row["event_id"], raw)
    cached = _EVENT_PROJECTED_TEXT_CACHE.get(cache_key)
    if cached is not None:
        return cached
    structure = (
        read_raw_event(db, row) if raw and row["category"] == "unreadable"
        else read_event_structure(db, row)
    )
    if not raw:
        structure = without_duplicate_completed_item(structure)
    text = flatten_raw_text(structure) if raw else redacted_search_text(structure)
    cache_put(_EVENT_PROJECTED_TEXT_CACHE, cache_key, text, EVENT_SEARCH_TEXT_CACHE_LIMIT)
    return text


def normalized_event_search_text(db: sqlite3.Connection, row: sqlite3.Row, raw: bool) -> str:
    database = db.execute("PRAGMA database_list").fetchone()["file"]
    cache_key = (database, row["event_id"], row["content_hash"], raw)
    cached = _EVENT_SEARCH_TEXT_CACHE.get(cache_key)
    if cached is not None:
        return cached
    text = event_search_text(db, row, raw)
    normalized = " ".join(text.split()).casefold()
    cache_put(_EVENT_SEARCH_TEXT_CACHE, cache_key, normalized, EVENT_SEARCH_TEXT_CACHE_LIMIT)
    return normalized


def projected_text_may_differ(row: sqlite3.Row) -> bool:
    """A Codex completed item that repeats a paired message is in the stored search text, but
    `query --text` leaves it out; only the rebuilt record can say whether the rest matches."""
    return row["category"] == "item_completed" and row["content_hash"] is None


@contextmanager
def stored_query_text(db: sqlite3.Connection, raw: bool) -> Iterator[Callable[[sqlite3.Row], str | None]]:
    """Read `query --text` candidates from the stored redacted search text, not rebuilt records.

    It yields None, and the caller rebuilds the record, for raw search, an event without stored
    text, or one with an embedded transcript copy, whose stored regions are regrouped by origin."""
    lease = handle = None
    if not raw:
        try:
            lease = acquire_projection_pack_lease(db, exclusive=False, timeout=READER_LOCK_WAIT_SECONDS)
            handle = projection_pack_path(db).open("rb")
        except (OSError, TimeoutError, ValueError):
            pass
    blocks: OrderedDict[int, bytes] = OrderedDict()

    def text(row: sqlite3.Row) -> str | None:
        if handle is None:
            return None
        try:
            regions, _compressed = read_projection_for_event(db, handle, row["event_rowid"], blocks)
        except ValueError:
            return None
        while len(blocks) > STORED_QUERY_TEXT_BLOCKS:
            blocks.popitem(last=False)
        if regions["copied"] or regions["unclassified"]:
            return None
        return " ".join(regions["native"]).casefold()

    try:
        yield text
    finally:
        if handle is not None:
            handle.close()
        if lease is not None:
            release_projection_pack_lease(lease)


STORED_QUERY_TEXT_BLOCKS = 64


def result_window(db: sqlite3.Connection, args: argparse.Namespace) -> tuple[list[sqlite3.Row], dict[str, Any]]:
    candidates, candidate_scan_capped = matching_rows(db, args)
    offset = max(0, getattr(args, "offset", 0))
    limit = max(0, args.limit)
    needed = offset + limit + 1
    verification_scan_truncated = False
    if args.text:
        needle = " ".join(args.text.split()).casefold()
        filtered = []
        with stored_query_text(db, args.raw) as stored_text:
            for row in candidates:
                if not args.raw and row["category"] == "unreadable":
                    continue
                projected = stored_text(row)
                if needle in (
                    normalized_event_search_text(db, row, args.raw)
                    if projected is None or (needle in projected and projected_text_may_differ(row))
                    else projected
                ):
                    filtered.append(row)
                    if len(filtered) >= needed:
                        verification_scan_truncated = True
                        break
    else:
        filtered = candidates
    hits = filtered[offset:offset + limit]
    before = max(0, getattr(args, "before", 0))
    after = max(0, getattr(args, "after", 0))
    before_clipped = after_clipped = False
    if before or after:
        expanded: dict[str, sqlite3.Row] = {}
        for hit in hits:
            prior = db.execute(
                "SELECT e.*,src.path AS source_path FROM events e JOIN sources src USING(source_id) "
                "WHERE e.source_id=? AND e.line_number<? ORDER BY e.line_number DESC LIMIT ?",
                (hit["source_id"], hit["line_number"], before),
            ).fetchall() if before else []
            following = db.execute(
                "SELECT e.*,src.path AS source_path FROM events e JOIN sources src USING(source_id) "
                "WHERE e.source_id=? AND e.line_number>? ORDER BY e.line_number LIMIT ?",
                (hit["source_id"], hit["line_number"], after),
            ).fetchall() if after else []
            before_clipped |= len(prior) < before
            after_clipped |= len(following) < after
            for row in (*reversed(prior), hit, *following):
                expanded[row["event_id"]] = row
        rows = sorted(
            expanded.values(),
            key=lambda row: (
                row["order_timestamp"], row["provider"], row["session_id"],
                row["source_id"], row["line_number"], row["event_id"],
            ),
        )
    else:
        rows = hits
    return rows, {
        "offset": offset, "limit": limit, "total_hits": len(filtered),
        "total_hits_is_lower_bound": candidate_scan_capped or verification_scan_truncated,
        "candidate_scan_capped": candidate_scan_capped,
        "verification_scan_truncated": verification_scan_truncated,
        "returned_hits": len(hits), "has_more": (
            candidate_scan_capped or verification_scan_truncated or offset + len(hits) < len(filtered)
        ),
        "context": {
            "before": before, "after": after, "before_clipped": before_clipped,
            "after_clipped": after_clipped, "returned_events": len(rows),
        },
    }


def result_rows(db: sqlite3.Connection, args: argparse.Namespace) -> list[sqlite3.Row]:
    return result_window(db, args)[0]


def matching_snippet(db: sqlite3.Connection, row: sqlite3.Row, text: str, raw: bool) -> str | None:
    if not text:
        return None
    projected = " ".join(event_search_text(db, row, raw).split())
    needle = " ".join(text.split())
    index = projected.casefold().find(needle.casefold())
    if index < 0:
        return None
    start = max(0, index - 60)
    end = min(len(projected), index + len(needle) + 100)
    return projected[start:end]


def query(db: sqlite3.Connection, args: argparse.Namespace) -> list[dict[str, Any]]:
    return query_window(db, args)[0]


def query_window(
    db: sqlite3.Connection, args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Expanded query events plus the page metadata the bare list cannot carry."""
    rows, page = result_window(db, args)
    page["next_offset"] = page["offset"] + page["returned_hits"] if page["has_more"] else None
    result = []
    for row in rows:
        content = read_event_content(db, row) if row["content_hash"] else None
        text = flatten_text(content if args.raw else redact_value(content)) if content is not None else ""
        item = {
             "provider": row["provider"], "session_id": row["session_id"], "timestamp": row["timestamp"],
             "order_timestamp": row["order_timestamp"], "order_kind": row["order_kind"],
             "category": row["category"], "role": row["role"], "tool": row["tool"], "text": text,
             "supported": bool(row["supported"]), "issue": row["issue"],
             "source": {"path": row["source_path"], "line": row["line_number"], "hash": row["raw_hash"]}}
        if getattr(args, "include_structure", False):
            structure = (
                read_raw_event(db, row) if row["category"] == "unreadable"
                else read_event_structure(db, row)
            )
            item["structure"] = structure if args.raw else redact_value(structure)
        if args.text and args.text.casefold() not in text.casefold():
            item["match"] = matching_snippet(db, row, args.text, args.raw)
        result.append(item)
    return result, page


def clean_query_truncation_notice(page: dict[str, Any] | None) -> str | None:
    """One stderr line when the clean event cap dropped events from this page."""
    if not page or not page.get("response_truncated"):
        return None
    if page.get("next_offset") is not None:
        step = f"rerun with --offset {page['next_offset']} for the rest"
    else:
        step = "narrow --since/--until or reduce --before/--after to see the rest"
    return (
        f"sessiondb: warning: clean query output capped at {MAX_CLEAN_EVENTS} events; {step}. "
        "The result bundle holds only the returned events."
    )


def expanded_query_truncation_notice(page: dict[str, Any]) -> str | None:
    """One plain-text stderr line for a bare-list page that withholds matching hits."""
    if not page["has_more"]:
        return None
    total = (
        f"at least {page['total_hits']}" if page["total_hits_is_lower_bound"]
        else str(page["total_hits"])
    )
    return (
        f"sessiondb: warning: query output truncated: returned {page['returned_hits']} of {total} "
        f"matching hits from offset {page['offset']}; rerun with --offset {page['next_offset']} "
        "for the rest, or add --page-envelope for machine-readable paging"
    )


def _query_filter_matches(value: str | None, requested: str | None) -> bool:
    return not requested or (value is not None and requested.casefold() in value.casefold())


def _drift_path(item: Any) -> str | None:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        for key in ("path", "source", "source_path"):
            if isinstance(item.get(key), str):
                return item[key]
    return None


def discover_transcript_sources(
    roots: dict[str, Iterable[str | Path]],
) -> list[tuple[str, Path, str]]:
    result = []
    for provider, provider_roots in roots.items():
        for root in provider_roots:
            root = Path(root)
            if not root.exists():
                continue
            for path in root.rglob("*.jsonl"):
                try:
                    is_source = path.is_file() and bool(path.stat().st_size)
                except OSError:
                    # Active and archived Codex sessions can move between the
                    # directory walk and the stat. A later discovery round owns
                    # the new location; the vanished path is not a hard failure.
                    continue
                if not is_source:
                    continue
                if provider == "claude":
                    source_class = "subagent" if "subagents" in path.parts else "root"
                else:
                    source_class = "archived" if "archived_sessions" in path.parts else "active"
                result.append((provider, path.resolve(), source_class))
    return sorted(result, key=lambda item: (item[0], str(item[1])))


def coverage_item_affects_query(item: Any, args: argparse.Namespace) -> bool:
    """Scope structured watcher issues when they expose query dimensions."""
    if not isinstance(item, dict):
        return True
    for key, requested in (
        ("provider", getattr(args, "provider", None)),
        ("project", getattr(args, "project", None)),
        ("session_id", getattr(args, "session", None)),
    ):
        if requested and item.get(key) is not None and not _query_filter_matches(str(item[key]), requested):
            return False
    return True


def appended_tail_affects_query(
    db: sqlite3.Connection, source_id: str, args: argparse.Namespace,
) -> bool:
    """Scope an appended tail by its source's indexed dimensions; tail events are newest."""
    for column, requested in (
        ("project", getattr(args, "project", None)),
        ("session_id", getattr(args, "session", None)),
        ("parent_session_id", getattr(args, "lineage", None)),
    ):
        if requested and not any(
            _query_filter_matches(value[0], requested)
            for value in db.execute(f"SELECT DISTINCT {column} FROM events WHERE source_id=?", (source_id,))
        ):
            return False
    until = getattr(args, "until", None)
    if until:
        normalized_until = normalize_timestamp(until) or until
        first_event = db.execute(
            "SELECT MIN(order_timestamp) FROM events WHERE source_id=?", (source_id,),
        ).fetchone()[0]
        if first_event and first_event > normalized_until:
            return False
    return True


def _indexed_prefix_matches(path: Path, size: int, expected_hash: str) -> bool:
    """True when the file still starts with exactly the indexed bytes."""
    hasher = hashlib.sha256()
    remaining = size
    try:
        with path.open("rb") as handle:
            while remaining:
                chunk = handle.read(min(1 << 20, remaining))
                if not chunk:
                    return False
                hasher.update(chunk)
                remaining -= len(chunk)
    except OSError:
        return False
    return hasher.hexdigest() == expected_hash


def _cap_coverage(coverage: Any, keep: int) -> Any:
    """Shorten a coverage source list for inline output, keeping its count."""
    if not isinstance(coverage, dict) or len(coverage.get("affected_sources") or []) <= keep:
        return coverage
    sources = coverage["affected_sources"]
    capped = {**coverage, "affected_sources": sources[:keep],
              "affected_sources_total": len(sources)}
    if isinstance(capped.get("fallback"), dict):
        capped["fallback"] = {**capped["fallback"], "sources": sources[:keep]}
    capped["issues"] = (coverage.get("issues") or [])[:keep]
    return capped


def live_source_drift(
    db: sqlite3.Connection, args: argparse.Namespace, roots: dict[str, Iterable[str]],
) -> list[dict[str, Any]]:
    """Compare discovered transcripts with this read snapshot's indexed watermarks."""
    rows = {
        row["path"]: row for row in db.execute(
            "SELECT source_id,provider,path,size_bytes,mtime_ns,status,source_hash FROM sources"
        )
    }
    now = time.time()
    affected = []
    for provider, path, _source_class in discover_transcript_sources(roots):
        if not _query_filter_matches(provider, getattr(args, "provider", None)):
            continue
        try:
            current = path.stat()
        except FileNotFoundError:
            # Moved between discovery and stat; its new location is a new discovery.
            continue
        except OSError as exc:
            affected.append({
                "path": str(path), "provider": provider, "indexed_bytes": 0,
                "reason": "source_unreadable", "blocking": True, "error": str(exc),
            })
            continue
        row = rows.get(str(path))
        if row is not None and row["status"] == "complete" and (
            current.st_size, current.st_mtime_ns,
        ) == (row["size_bytes"], row["mtime_ns"]):
            continue
        if row is None:
            item = {"path": str(path), "provider": provider, "indexed_bytes": 0,
                    "reason": "unindexed_source"}
        elif (
            row["status"] == "complete" and current.st_size > row["size_bytes"]
            and _indexed_prefix_matches(path, row["size_bytes"], row["source_hash"])
        ):
            if not appended_tail_affects_query(db, row["source_id"], args):
                continue
            item = {"path": str(path), "provider": provider, "indexed_bytes": row["size_bytes"],
                    "reason": "source_tail"}
        else:
            # A shrink, rewrite (including a larger replacement), or unfinished import
            # invalidates the indexed offset and scope, so the whole file is relevant.
            item = {"path": str(path), "provider": provider, "indexed_bytes": 0,
                    "reason": "source_rewrite"}
        quiet_seconds = max(0.0, now - current.st_mtime)
        if quiet_seconds > ACTIVE_DRIFT_GRACE_SECONDS:
            item.update({
                "reason": "persistent_" + item["reason"], "blocking": True,
                "quiet_seconds": round(quiet_seconds, 3),
            })
        else:
            item["reason"] = "active_" + item["reason"]
        affected.append(item)
    return affected


def drift_affects_query(
    db: sqlite3.Connection, item: Any, args: argparse.Namespace,
) -> tuple[bool, dict[str, Any]]:
    """Conservatively decide whether an unindexed source tail can affect a query."""
    path = _drift_path(item)
    fallback = {"path": path, "indexed_bytes": 0}
    if not path:
        fallback.update({"reason": "unclassified_drift", "blocking": True})
        return True, fallback
    resolved = str(Path(path).expanduser().resolve())
    row = db.execute(
        "SELECT source_id,provider,path,size_bytes FROM sources WHERE path IN (?,?) LIMIT 1",
        (path, resolved),
    ).fetchone()
    if row is None:
        fallback["path"] = resolved
        try:
            quiet_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - Path(resolved).stat().st_mtime)
        except OSError:
            quiet_seconds = ACTIVE_DRIFT_GRACE_SECONDS + 1
        fallback["reason"] = (
            "active_unindexed_source" if quiet_seconds <= ACTIVE_DRIFT_GRACE_SECONDS
            else "persistent_unindexed_source"
        )
        fallback["blocking"] = quiet_seconds > ACTIVE_DRIFT_GRACE_SECONDS
        return True, fallback
    fallback.update({
        "path": row["path"], "provider": row["provider"],
        "indexed_bytes": row["size_bytes"], "reason": "active_source_tail",
    })
    if not _query_filter_matches(row["provider"], getattr(args, "provider", None)):
        return False, fallback
    if not appended_tail_affects_query(db, row["source_id"], args):
        return False, fallback
    try:
        current = Path(row["path"]).stat()
    except OSError:
        fallback.update({"reason": "drift_source_unreadable", "blocking": True})
        return True, fallback
    if current.st_size <= row["size_bytes"]:
        return False, fallback
    quiet_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - current.st_mtime)
    if quiet_seconds > ACTIVE_DRIFT_GRACE_SECONDS:
        fallback.update({
            "reason": "persistent_source_drift", "blocking": True,
            "quiet_seconds": round(quiet_seconds, 3),
        })
    return True, fallback


def query_coverage(db: sqlite3.Connection, args: argparse.Namespace) -> dict[str, Any] | str:
    """Return query-scoped watcher state and any bounded raw-tail fallback."""
    database = Path(db.execute("PRAGMA database_list").fetchone()["file"])
    status_path = database.parent / "freshness" / "watcher_status.json"
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return "missing"
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "unreadable",
            "issues": [f"watcher status could not be read: {exc}"],
        }

    issues = [
        item for item in (status.get("issues") or [])
        if coverage_item_affects_query(item, args)
    ]
    issues.extend(
        item for item in (status.get("hard_failures") or [])
        if coverage_item_affects_query(item, args)
    )
    if status.get("error"):
        issues.append(status["error"])
    roots = status.get("discovery_roots")
    live_checked = isinstance(roots, dict) and bool(roots)
    affected_sources = []
    if live_checked:
        # Source watermarks, not status age, decide freshness: the watcher writes status
        # only after filesystem events, so an idle healthy status can be arbitrarily old.
        drifted = []
        try:
            affected_sources = live_source_drift(db, args, roots)
        except (OSError, sqlite3.Error) as exc:
            issues.append(f"live source check failed: {exc}")
        if status.get("coverage_complete_except_drift") is not True:
            issues.append(f"watcher sync incomplete: {status.get('reason') or 'unknown'}")
    else:
        drifted = status.get("sources_drifted_after_sync") or []
        for item in drifted:
            affected, fallback = drift_affects_query(db, item, args)
            if affected:
                affected_sources.append(fallback)
    persistent_sources = [item for item in affected_sources if item.get("blocking")]
    if persistent_sources:
        issues.extend(
            f"{item['reason']}: {item.get('path') or 'unknown source'}"
            for item in persistent_sources
        )
    generated_at = status.get("generated_at")
    age_seconds = None
    if isinstance(generated_at, str):
        try:
            observed_at = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            age_seconds = max(0.0, (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds())
        except ValueError:
            issues.append("watcher generated_at is invalid")
    if issues or (not live_checked and status.get("coverage_complete") is False and not drifted):
        coverage_status = "incomplete"
        complete = False
    elif affected_sources:
        coverage_status = "catching_up"
        complete = False
    elif live_checked or status.get("coverage_complete") is True or drifted:
        coverage_status = "complete"
        complete = True
    else:
        coverage_status = "incomplete"
        complete = False
    result = {
        "complete": complete,
        "status": coverage_status,
        "generated_at": generated_at,
        "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "reason": (
            "active_sources_affect_query" if coverage_status == "catching_up"
            else "live_sources_current" if complete and live_checked
            else "active_sources_outside_query" if complete and drifted
            else status.get("reason") or ("watcher_pass" if complete else "watcher_incomplete")
        ),
        "live_checked": live_checked or None,
        "issues": issues,
        "affected_sources": affected_sources,
        "fallback": ({"mode": "raw_source_tail", "sources": affected_sources}
                     if affected_sources else None),
    }
    return {key: value for key, value in result.items() if value not in (None, [], {})}


def coverage_is_current(coverage: Any) -> bool:
    """Complete coverage that is either live-checked or from a recent watcher status."""
    return (
        isinstance(coverage, dict) and coverage.get("complete") is True
        and (
            coverage.get("live_checked") is True
            or (
                isinstance(coverage.get("age_seconds"), (int, float))
                and coverage["age_seconds"] <= ACTIVE_DRIFT_GRACE_SECONDS
            )
        )
    )


def query_clean(db: sqlite3.Connection, args: argparse.Namespace) -> dict[str, Any]:
    if args.raw:
        raise ValueError("clean query is redacted-only; use expanded raw retrieval explicitly")
    if getattr(args, "include_structure", False):
        raise ValueError("clean query is compact; use expanded retrieval with --include-structure")
    object_pack = pack_path(db)
    pack_stat = object_pack.stat()
    database = db.execute("PRAGMA database_list").fetchone()["file"]
    data_version = db.execute("PRAGMA data_version").fetchone()[0]
    arg_names = (
        "text", "provider", "project", "session", "role", "tool", "lineage", "since", "until",
        "limit", "offset", "before", "after", "include_page", "include_hashes",
    )
    cache_key = (
        database, data_version, pack_stat.st_mtime_ns, pack_stat.st_size,
        *(getattr(args, name, None) for name in arg_names),
    )
    coverage = query_coverage(db, args)
    tails = None
    if (
        live_tails_servable(coverage) and not getattr(args, "before", 0)
        and not getattr(args, "after", 0) and not getattr(args, "offset", 0)
    ):
        tails = clean_live_tail_events(
            db, coverage["affected_sources"], args, time.monotonic() + SEARCH_DEADLINE_SECONDS,
        )
    cached_result = _QUERY_CLEAN_CACHE.get(cache_key) if tails is None else None
    if cached_result is not None:
        return {**cached_result, "coverage": coverage}
    rows, page = result_window(db, args)
    if tails is not None:
        # Serve tails only when the whole answer fits this page; otherwise report them.
        rewritten = {item["path"] for item in coverage["affected_sources"] if not item.get("indexed_bytes")}
        kept = [row for row in rows if row["source_path"] not in rewritten]
        tail_events, served = tails
        if page["has_more"] or len(kept) + len(tail_events) > min(args.limit, MAX_CLEAN_EVENTS):
            tails = None
        else:
            rows = sorted(
                [*kept, *tail_events],
                key=lambda item: (
                    item["order_timestamp"] or "", item["provider"],
                    item["session_id"] if not isinstance(item, dict) else item["parsed"].session_id,
                    item["source_id"], item["line_number"] if not isinstance(item, dict) else item["line"],
                ),
            )
            page["total_hits"] = page["returned_hits"] = len(rows)
            page["context"]["returned_events"] = len(rows)
    if len(rows) > MAX_CLEAN_EVENTS:
        rows = rows[:MAX_CLEAN_EVENTS]
        page["response_truncated"] = True
        page["has_more"] = True
        page["context"]["returned_events"] = len(rows)
    else:
        page["response_truncated"] = False
    # Hit offsets match events only without context; then the next page starts after the
    # events actually returned, which the 100-event cap may have shortened.
    context_requested = bool(page["context"]["before"] or page["context"]["after"])
    page["next_offset"] = (
        page["offset"] + len(rows) if page["has_more"] and not context_requested else None
    )
    raw_events = []
    response_objects: dict[str, Any] = {}
    exact_object_hashes: set[str] = set()

    for row in rows:
        if isinstance(row, dict):
            raw_events.append(_clean_tail_event(row, args, response_objects))
            continue
        refs = []
        redacted_items = []
        if row["content_hash"]:
            stored_refs = db.execute(
                "SELECT object_hash FROM event_content WHERE event_id=? ORDER BY ordinal",
                (row["event_id"],),
            ).fetchall()
            stored_hashes = [stored_ref["object_hash"] for stored_ref in stored_refs]
            if getattr(args, "include_hashes", False):
                exact_object_hashes.update(exact_object_closure(db, stored_hashes))
            for stored_ref in stored_refs:
                redacted_item = redacted_content_object(db, stored_ref["object_hash"])
                redacted_items.append(redacted_item)
                response_hash, bundle = factor_redacted_response(redacted_item)
                response_objects.update(bundle)
                refs.append(response_hash)
        event = {
            "p": row["provider"], "sid": row["session_id"], "role": row["role"], "refs": refs,
            "source_path": row["source_path"], "line": row["line_number"], "hash": row["raw_hash"],
        }
        if getattr(args, "include_hashes", False) and row["content_hash"]:
            event["content_hashes"] = stored_hashes
        for source, target in (
            ("tool", "tool"), ("issue", "issue"),
        ):
            if row[source] is not None:
                event[target] = row[source]
        if row["content_shape"] == "list":
            event["list"] = 1
        if not row["supported"]:
            event["ok"] = False
        if args.text and row["category"] != "unreadable":
            needle = " ".join(args.text.split()).casefold()
            content_text = "\n".join(flatten_text(item) for item in redacted_items)
            content_matches = needle in " ".join(content_text.split()).casefold()
            context_requested = bool(getattr(args, "before", 0) or getattr(args, "after", 0))
            if context_requested and (not content_matches or row["content_shape"] != "list"):
                identifiers = [
                    value for value in compact_event_metadata(read_event_structure(db, row))
                    if (UUID_RE.fullmatch(value) or PREFIXED_ID_RE.fullmatch(value))
                    and value not in {row["session_id"], row["parent_session_id"]}
                ]
                if identifiers:
                    event["meta"] = identifiers
            if not content_matches:
                if (getattr(args, "before", 0) or getattr(args, "after", 0)) and needle in normalized_event_search_text(db, row, False):
                    metadata = compact_event_metadata(read_event_structure(db, row))
                    if metadata:
                        event["meta"] = metadata
                snippet = matching_snippet(db, row, args.text, False)
                if snippet is not None:
                    event["match"] = snippet
        raw_events.append(event)
    object_ids = {object_hash: ordinal for ordinal, object_hash in enumerate(sorted(response_objects))}

    def compact_refs(value: Any) -> Any:
        if isinstance(value, dict) and set(value) == {REF_KEY} and value[REF_KEY] in object_ids:
            return {REF_KEY: object_ids[value[REF_KEY]]}
        if isinstance(value, list):
            return [compact_refs(child) for child in value]
        if isinstance(value, dict):
            return {key: compact_refs(child) for key, child in value.items()}
        return value

    response_object_hashes = sorted(response_objects)
    objects = [compact_refs(response_objects[object_hash]) for object_hash in response_object_hashes]
    source_paths = sorted({event["source_path"] for event in raw_events})
    source_ids = {path: ordinal for ordinal, path in enumerate(source_paths)}
    source_defaults: dict[str, tuple[str | None, str | None, str | None]] = {}
    sources = []
    for path in source_paths:
        source_events = [event for event in raw_events if event["source_path"] == path]
        defaults = []
        for key in ("p", "sid", "role"):
            values = {event.get(key) for event in source_events}
            defaults.append(next(iter(values)) if len(values) == 1 else None)
        source_defaults[path] = tuple(defaults)
        source = [path, *defaults]
        while source[-1] is None:
            source.pop()
        sources.append(source)
    events = []
    for event in raw_events:
        defaults = source_defaults[event["source_path"]]
        compact = {
            key: value for key, value in event.items()
            if key not in {"source_path", "refs"}
            and not (key in {"p", "sid", "role"} and value == defaults[("p", "sid", "role").index(key)])
        }
        if event["refs"]:
            compact["refs"] = [object_ids[ref] for ref in event["refs"]]
        compact["src"] = source_ids[event["source_path"]]
        events.append(compact)
    result = {"sources": sources, "events": events}
    if objects:
        result["objects"] = objects
    if getattr(args, "include_hashes", False):
        result["object_hashes"] = sorted(exact_object_hashes)
        result["response_object_hashes"] = response_object_hashes
    if (getattr(args, "include_page", False) or page["response_truncated"] or page["offset"]
            or page["has_more"] or context_requested):
        if getattr(args, "include_page", False):
            result["page"] = page
        else:
            context = {
                key: value for key, value in page["context"].items()
                if value or key == "returned_events"
            }
            compact_page = {
                "total_hits": page["total_hits"], "context": context,
            }
            if page["next_offset"] is not None:
                compact_page["next_offset"] = page["next_offset"]
            if page["has_more"] or not context_requested:
                compact_page["has_more"] = page["has_more"]
            if page["response_truncated"] or not context_requested:
                compact_page["response_truncated"] = page["response_truncated"]
            for key in (
                "offset", "total_hits_is_lower_bound", "candidate_scan_capped",
                "verification_scan_truncated",
            ):
                if page[key]:
                    compact_page[key] = page[key]
            result["page"] = compact_page
    if tails is not None:
        return {**result, "coverage": {
            key: value for key, value in coverage.items()
            if key not in ("affected_sources", "fallback", "issues")
        } | {"complete": True, "status": "complete", "reason": "live_tails_served",
             "tails_served": tails[1]}}
    if len(canonical(result)) <= 5 * 1024 * 1024:
        cache_put(_QUERY_CLEAN_CACHE, cache_key, result, 128)
    return {**result, "coverage": coverage}


def _clean_tail_event(
    event: dict[str, Any], args: argparse.Namespace, response_objects: dict[str, Any],
) -> dict[str, Any]:
    """The clean-query event an indexed copy of this tail line would produce."""
    parsed = event["parsed"]
    content = parsed.content
    refs = []
    redacted_items = []
    if content is not None:
        for item in content if isinstance(content, list) else [content]:
            redacted_item = redact_value(item)
            redacted_items.append(redacted_item)
            response_hash, bundle = factor_redacted_response(redacted_item)
            response_objects.update(bundle)
            refs.append(response_hash)
    compact = {
        "p": event["provider"], "sid": parsed.session_id, "role": parsed.role, "refs": refs,
        "source_path": str(event["path"]), "line": event["line"], "hash": event["raw_hash"],
        "live_tail": True,
    }
    if parsed.tool is not None:
        compact["tool"] = parsed.tool
    if parsed.issue is not None:
        compact["issue"] = parsed.issue
    if isinstance(content, list):
        compact["list"] = 1
    if not parsed.supported:
        compact["ok"] = False
    if args.text and event.get("search_text") is not None:
        # Like indexed events: a snippet only when the words matched outside the content.
        needle = " ".join(args.text.split())
        content_text = "\n".join(flatten_text(item) for item in redacted_items)
        if needle.casefold() not in " ".join(content_text.split()).casefold():
            index = event["search_text"].casefold().find(needle.casefold())
            if index >= 0:
                compact["match"] = event["search_text"][max(0, index - 60):index + len(needle) + 100]
    return compact


def _index_filter_clauses(args: argparse.Namespace) -> tuple[list[str], list[Any]]:
    clauses, values = session_source_prefilter(getattr(args, "session", None))
    for column, name in (
        ("e.provider", "provider"), ("e.project", "project"),
        ("e.session_id", "session"), ("e.role", "role"),
        ("e.tool", "tool"), ("e.parent_session_id", "lineage"),
    ):
        requested = getattr(args, name, None)
        if requested:
            escaped = requested.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append(f"{column} LIKE ? ESCAPE '\\'")
            values.append(f"%{escaped}%")
    if getattr(args, "since", None):
        clauses.append("e.order_timestamp >= ?")
        values.append(normalize_timestamp(args.since) or args.since)
    if getattr(args, "until", None):
        clauses.append("e.order_timestamp <= ?")
        values.append(normalize_timestamp(args.until) or args.until)
    return clauses, values


_FOLDED_CHARS: dict[str, str] = {}
_FOLDED_TEXT_CACHE: OrderedDict[str, tuple[str, list[int]]] = OrderedDict()


def _folded_text(text: str) -> tuple[str, list[int]]:
    """Per-character accent fold of text with each folded character's source index."""
    cached = _FOLDED_TEXT_CACHE.get(text)
    if cached is not None:
        _FOLDED_TEXT_CACHE.move_to_end(text)
        return cached
    parts: list[str] = []
    positions: list[int] = []
    for index, char in enumerate(text):
        part = _FOLDED_CHARS.get(char)
        if part is None:
            part = _FOLDED_CHARS.setdefault(char, _accent_fold_slow(char))
        parts.append(part)
        positions.extend([index] * len(part))
    result = ("".join(parts), positions)
    if len(text) >= ACCENT_FOLD_CACHE_MIN_CHARS:
        _FOLDED_TEXT_CACHE[text] = result
        while len(_FOLDED_TEXT_CACHE) > ACCENT_FOLD_CACHE_ENTRIES:
            _FOLDED_TEXT_CACHE.popitem(last=False)
    return result


def _folded_span(text: str, needle: str) -> tuple[int, int] | None:
    folded, positions = _folded_text(text)
    normalized_needle = _accent_fold_slow(needle)
    if not normalized_needle:
        return None
    found = folded.find(normalized_needle)
    if found < 0:
        return None
    return positions[found], positions[found + len(normalized_needle) - 1] + 1


def lexical_terms(text: str) -> list[str]:
    folded = unicodedata.normalize("NFD", text.casefold())
    accent_insensitive = "".join(char for char in folded if not unicodedata.combining(char))
    return re.findall(r"[^\W_]+", accent_insensitive, re.UNICODE)


def _lexical_boundary_at(text: str, index: int, direction: int) -> bool:
    cursor = index - 1 if direction < 0 else index
    while 0 <= cursor < len(text):
        folded = unicodedata.normalize("NFD", text[cursor].casefold())
        bases = [value for value in folded if not unicodedata.combining(value)]
        if not bases:
            cursor += direction
            continue
        return not any(re.match(r"[^\W_]", value, re.UNICODE) for value in bases)
    return True


@dataclass(frozen=True)
class SearchAtom:
    kind: str
    value: str


def unquoted_search_term(value: str) -> str:
    """Drop sentence punctuation after an unquoted word (`implementation,`); symbol runs stay."""
    stripped = value.rstrip(",;:!?.")
    return stripped if lexical_terms(stripped) else value


def search_query_tokens(text: str) -> list[Any]:
    tokens: list[Any] = []
    index = 0
    while index < len(text):
        if text[index].isspace():
            index += 1
            continue
        if text[index] in "()":
            tokens.append(text[index])
            index += 1
            continue
        if text[index] == '"':
            index += 1
            value: list[str] = []
            while index < len(text) and text[index] != '"':
                if text[index] == "\\" and index + 1 < len(text):
                    index += 1
                value.append(text[index])
                index += 1
            if index >= len(text):
                raise ValueError("unterminated quoted phrase")
            index += 1
            if not "".join(value).strip():
                raise ValueError("empty quoted phrase")
            tokens.append(SearchAtom("phrase", "".join(value)))
            continue
        end = index
        while end < len(text) and not text[end].isspace() and text[end] not in "()":
            end += 1
        value = text[index:end]
        # Operators are upper case, so a lower-case `not` or `and` in a pasted sentence stays a
        # word, and `--flag` is a word rather than NOT `-flag`.
        near = re.fullmatch(r"NEAR(?:/([0-9]{1,3}))?", value)
        if value.startswith("-") and not value.startswith("--") and len(value) > 1:
            tokens.extend(("NOT", SearchAtom("term", unquoted_search_term(value[1:]))))
        elif value in {"AND", "OR", "NOT"}:
            tokens.append(value)
        elif near:
            tokens.append(("NEAR", int(near.group(1) or 10)))
        else:
            tokens.append(SearchAtom("term", unquoted_search_term(value)))
        index = end
    return tokens


def parse_search_query(text: str) -> Any:
    tokens = search_query_tokens(text)
    if not tokens:
        raise ValueError("search query is empty")
    position = 0

    def primary() -> Any:
        nonlocal position
        if position >= len(tokens):
            raise ValueError("query ends with an operator")
        token = tokens[position]
        if token == "NOT":
            position += 1
            return ("NOT", primary())
        if token == "(":
            position += 1
            node = parse_or()
            if position >= len(tokens) or tokens[position] != ")":
                raise ValueError("unclosed query parenthesis")
            position += 1
            return node
        if isinstance(token, SearchAtom):
            position += 1
            return token
        raise ValueError(f"unexpected query operator: {token}")

    def parse_near() -> Any:
        nonlocal position
        node = primary()
        while position < len(tokens) and isinstance(tokens[position], tuple) and tokens[position][0] == "NEAR":
            distance = tokens[position][1]
            position += 1
            node = ("NEAR", distance, node, primary())
        return node

    def parse_and() -> Any:
        nonlocal position
        nodes = [parse_near()]
        while position < len(tokens) and tokens[position] not in {"OR", ")"}:
            if tokens[position] == "AND":
                position += 1
            nodes.append(parse_near())
        return nodes[0] if len(nodes) == 1 else ("AND", *nodes)

    def parse_or() -> Any:
        nonlocal position
        nodes = [parse_and()]
        while position < len(tokens) and tokens[position] == "OR":
            position += 1
            nodes.append(parse_and())
        return nodes[0] if len(nodes) == 1 else ("OR", *nodes)

    result = parse_or()
    if position != len(tokens):
        raise ValueError(f"unexpected query token: {tokens[position]}")
    return result


_COMBINING_DELETIONS: dict[int, None] | None = None


def _accent_fold_reference(value: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFD", value.casefold())
        if not unicodedata.combining(char)
    )


def _accent_fold_slow(value: str) -> str:
    """Same result as _accent_fold_reference, in one C-level pass instead of per character."""
    global _COMBINING_DELETIONS
    if value.isascii():
        return value.casefold()
    if _COMBINING_DELETIONS is None:
        _COMBINING_DELETIONS = {
            codepoint: None for codepoint in range(sys.maxunicode + 1)
            if unicodedata.combining(chr(codepoint))
        }
    return unicodedata.normalize("NFD", value.casefold()).translate(_COMBINING_DELETIONS)


_ACCENT_FOLD_CACHE: OrderedDict[str, str] = OrderedDict()
ACCENT_FOLD_CACHE_MIN_CHARS = 4096
ACCENT_FOLD_CACHE_ENTRIES = 8


def _accent_fold(value: str) -> str:
    # Search folds the same long projection once per atom and phase; reuse the result.
    if len(value) < ACCENT_FOLD_CACHE_MIN_CHARS:
        return _accent_fold_slow(value)
    cached = _ACCENT_FOLD_CACHE.get(value)
    if cached is not None:
        _ACCENT_FOLD_CACHE.move_to_end(value)
        return cached
    folded = _accent_fold_slow(value)
    _ACCENT_FOLD_CACHE[value] = folded
    while len(_ACCENT_FOLD_CACHE) > ACCENT_FOLD_CACHE_ENTRIES:
        _ACCENT_FOLD_CACHE.popitem(last=False)
    return folded


def _has_diacritic(value: str) -> bool:
    return any(unicodedata.combining(char) for char in unicodedata.normalize("NFD", value))


def _fts_recall_safe(value: str) -> bool:
    return bool(fts_candidate_terms(value))


# Beyond this many spelling combinations an unaccented phrase keeps the Python re-check.
PHRASE_SPELLING_CAP = 256


class IndexSpellings:
    """Accented spellings of unaccented query words that occur in an accent-keeping index.

    `overflow` records that some phrase had too many spelling combinations to spell out, so its
    index candidates are a superset that still needs the Python re-check."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db
        self.overflow = False
        self._terms: dict[str, list[str]] = {}
        self._pairs: dict[tuple[str, str], bool] = {}

    def spellings(self, term: str) -> list[str]:
        if term not in self._terms:
            found = [term]
            if _is_latin_word(term) and _accent_fold(term) == term:
                found.extend(row[0] for row in self.db.execute(
                    "SELECT term FROM search_term_variants WHERE folded=? ORDER BY term", (term,),
                ))
            self._terms[term] = found
        return self._terms[term]

    def _pair_occurs(self, left: str, right: str) -> bool:
        if (left, right) not in self._pairs:
            expression = "{" + " ".join(PROJECTION_ORIGINS) + "} : " + fts_phrase([left, right])
            self._pairs[(left, right)] = self.db.execute(
                "SELECT 1 FROM positional_projection_fts WHERE positional_projection_fts MATCH ? LIMIT 1",
                (expression,),
            ).fetchone() is not None
        return self._pairs[(left, right)]

    def phrase_spellings(self, words: list[str]) -> list[list[str]] | None:
        """Every spelling of the phrase whose adjacent word pairs occur; None past the cap."""
        choices = [self.spellings(word) for word in words]
        total = 1
        for choice in choices:
            total *= len(choice)
        if total <= PHRASE_SPELLING_CAP:
            return [list(combination) for combination in itertools.product(*choices)]
        chains = [[spelling] for spelling in choices[0]]
        for choice in choices[1:]:
            check_query_deadline()
            chains = [
                chain + [spelling] for chain in chains for spelling in choice
                if self._pair_occurs(chain[-1], spelling)
            ]
            if len(chains) > PHRASE_SPELLING_CAP:
                self.overflow = True
                return None
        return chains


_INDEX_SPELLINGS: contextvars.ContextVar[IndexSpellings | None] = contextvars.ContextVar(
    "sessiondb_index_spellings", default=None,
)


@contextmanager
def index_spellings(db: sqlite3.Connection) -> Iterator[IndexSpellings | None]:
    """Make query words match the index flavor: exact words plus spellings when it keeps accents."""
    spellings = IndexSpellings(db) if fts_index_keeps_accents(db) else None
    token = _INDEX_SPELLINGS.set(spellings)
    try:
        yield spellings
    finally:
        _INDEX_SPELLINGS.reset(token)


def fts_phrase(terms: list[str]) -> str:
    return '"' + " ".join(term.replace('"', '""') for term in terms) + '"'


def fts_word_choice(term: str) -> str:
    """One query word for the index: itself, or every spelling that occurs."""
    spellings = _INDEX_SPELLINGS.get()
    choices = spellings.spellings(term) if spellings is not None else [term]
    return (
        fts_phrase(choices) if len(choices) == 1
        else "(" + " OR ".join(fts_phrase([choice]) for choice in choices) + ")"
    )


def fts_candidate_parts(
    value: str, *, preserve_occurrences: bool = False,
) -> tuple[list[str], list[str]]:
    if _INDEX_SPELLINGS.get() is not None:
        # Accent-keeping index: words are indexed as written (NFC, case-folded); CJK, emoji,
        # symbols and file names keep their literal anchors, without accent anchors.
        terms = re.findall(r"[^\W_]+", unicodedata.normalize("NFC", value).casefold(), re.UNICODE)
        candidates = [term for term in terms if not LITERAL_SEARCH_RE.search(term)]
        literal_candidates = structured_query_tokens(value)
        if LITERAL_SEARCH_RE.search(value) or not candidates:
            literal_candidates.extend(literal_tokens(value, accent_tokens=False).split())
            adjacent_pairs = [token for token in literal_candidates if token.startswith("b")]
            literal_candidates = adjacent_pairs or literal_candidates
        if not preserve_occurrences:
            candidates = list(dict.fromkeys(candidates))
            literal_candidates = list(dict.fromkeys(literal_candidates))
        return candidates, literal_candidates
    terms = re.findall(r"[^\W_]+", unicodedata.normalize("NFC", value).casefold(), re.UNICODE)
    candidates = [
        _accent_fold(term) if all(
            char.isdigit() or "LATIN" in unicodedata.name(char, "") for char in term
        ) else term
        for term in terms if not LITERAL_SEARCH_RE.search(term)
    ]
    literal_candidates: list[str] = structured_query_tokens(value)
    if LITERAL_SEARCH_RE.search(value) or not candidates:
        literal_candidates.extend(literal_tokens(value).split())
        adjacent_pairs = [token for token in literal_candidates if token.startswith("b")]
        literal_candidates = adjacent_pairs or literal_candidates
    elif _has_diacritic(value):
        literal_candidates.extend(accent_term_tokens(value) + accent_phrase_tokens(value))
    if not preserve_occurrences:
        candidates = list(dict.fromkeys(candidates))
        literal_candidates = list(dict.fromkeys(literal_candidates))
    return candidates, literal_candidates


def fts_candidate_terms(value: str, *, preserve_occurrences: bool = False) -> list[str]:
    lexical, literal = fts_candidate_parts(value, preserve_occurrences=preserve_occurrences)
    return lexical + literal


_FOLDED_CHAR_LENGTHS: dict[str, int] = {}


def _folded_span_mapper(text: str) -> Callable[[int, int], tuple[int, int]]:
    """Map many folded spans to their original character positions in one pass."""
    next_offsets = []
    folded_offset = 0
    for char in text:
        length = _FOLDED_CHAR_LENGTHS.get(char)
        if length is None:
            length = _FOLDED_CHAR_LENGTHS.setdefault(char, len(_accent_fold(char)))
        folded_offset += length
        next_offsets.append(folded_offset)

    def original_span(start: int, end: int) -> tuple[int, int]:
        last = bisect.bisect_left(next_offsets, end)
        first = bisect.bisect_right(next_offsets, start)
        original_start = first if first <= min(last, len(text) - 1) and (
            (next_offsets[first - 1] if first else 0) <= start
        ) else 0
        return original_start, (last + 1 if last < len(text) else len(text))

    return original_span


_LATIN_ACCENT_FOLD_MAP: dict[str, str] | None = None
_LATIN_ACCENT_RE: re.Pattern[str] | None = None
_NON_VIETNAMESE_LATIN_ACCENT_RE: re.Pattern[str] | None = None
VIETNAMESE_BASE_VARIANTS = {
    "a": "aáàảãạăắằẳẵặâấầẩẫậ",
    "e": "eéèẻẽẹêếềểễệ",
    "i": "iíìỉĩị",
    "o": "oóòỏõọôốồổỗộơớờởỡợ",
    "u": "uúùủũụưứừửữự",
    "y": "yýỳỷỹỵ",
}


def _fold_latin_accents_fast(value: str) -> str:
    global _LATIN_ACCENT_FOLD_MAP, _LATIN_ACCENT_RE
    if _LATIN_ACCENT_FOLD_MAP is None:
        mapping = {}
        for start, end in ((0x00C0, 0x024F), (0x1E00, 0x1EFF)):
            for codepoint in range(start, end + 1):
                char = chr(codepoint).casefold()
                if len(char) == 1:
                    folded = _accent_fold(char)
                    if folded != char:
                        mapping[char] = folded
        _LATIN_ACCENT_FOLD_MAP = mapping
        _LATIN_ACCENT_RE = re.compile("[" + "".join(
            re.escape(char) for char in mapping
        ) + "]")
    folded = value.casefold()
    if _LATIN_ACCENT_RE is not None:
        folded = _LATIN_ACCENT_RE.sub(
            lambda match: _LATIN_ACCENT_FOLD_MAP[match.group(0)], folded,
        )
    return re.sub(r"[\u0300-\u036f]", "", folded)


def _has_non_vietnamese_latin_accent(value: str) -> bool:
    global _NON_VIETNAMESE_LATIN_ACCENT_RE
    if _NON_VIETNAMESE_LATIN_ACCENT_RE is None:
        vietnamese = set("".join(VIETNAMESE_BASE_VARIANTS.values()))
        variants = []
        for start, end in ((0x00C0, 0x024F), (0x1E00, 0x1EFF)):
            for codepoint in range(start, end + 1):
                char = chr(codepoint).casefold()
                if (
                    len(char) == 1 and char not in vietnamese
                    and _accent_fold(char) != char
                ):
                    variants.append(char)
        _NON_VIETNAMESE_LATIN_ACCENT_RE = re.compile(
            "[" + "".join(re.escape(char) for char in variants) + "]"
            r"|[\u0300-\u036f]"
        )
    normalized = unicodedata.normalize("NFC", value).casefold()
    return _NON_VIETNAMESE_LATIN_ACCENT_RE.search(normalized) is not None


def vietnamese_accent_candidate_tokens(terms: list[str], cap: int = 1024) -> list[str] | None:
    tokens: list[str] = []
    for term in terms:
        choices = [VIETNAMESE_BASE_VARIANTS.get(char, char) for char in term]
        combinations = 1
        for variants in choices:
            combinations *= len(variants)
        if combinations > cap or len(tokens) + combinations > cap:
            return None
        for variant in map("".join, itertools.product(*choices)):
            if variant != term:
                tokens.extend(accent_term_tokens(variant))
    return list(dict.fromkeys(tokens))


def accented_phrase_candidate_rowids(
    db: sqlite3.Connection, terms: list[str], candidates: set[int],
) -> set[int] | None:
    if not terms or not all(term.isascii() and term.isalpha() for term in terms):
        return None
    tokens = vietnamese_accent_candidate_tokens(terms)
    if tokens is None:
        return None
    if not tokens:
        return set()
    disjunction = " OR ".join('"' + token + '"' for token in tokens)
    matches: set[int] = set()
    for origin in PROJECTION_ORIGINS:
        expression = f"{origin}_literal : ({disjunction})"
        matches.update(
            row[0] for row in db.execute(
                "SELECT rowid FROM positional_projection_fts "
                "WHERE positional_projection_fts MATCH ?", (expression,),
            ) if row[0] in candidates
        )
    return matches


def _is_lexical_char(char: str) -> bool:
    return char != "_" and char.isalnum()


def _lexical_phrase_spans_fast(
    projection: str, terms: list[str], *, first_only: bool,
) -> list[tuple[int, int]]:
    first = terms[0]
    start = 0
    spans = []
    while (found := projection.find(first, start)) >= 0:
        check_query_deadline()
        if found and _is_lexical_char(projection[found - 1]):
            start = found + 1
            continue
        cursor = found + len(first)
        matched = True
        for term in terms[1:]:
            separator_start = cursor
            while cursor < len(projection) and not _is_lexical_char(projection[cursor]):
                if projection[cursor] == REGION_SEPARATOR:
                    matched = False
                    break
                cursor += 1
            if not matched or cursor == separator_start or not projection.startswith(term, cursor):
                matched = False
                break
            cursor += len(term)
        if matched and (cursor == len(projection) or not _is_lexical_char(projection[cursor])):
            spans.append((found, cursor))
            if first_only:
                return spans
        start = found + 1
    return spans


def _mixed_accent_phrase_spans_fast(
    projection: str, query_terms: list[str], *, first_only: bool,
) -> list[tuple[int, int]]:
    """Find an accent-sensitive phrase without tokenizing the complete projection."""
    folded_projection = _fold_latin_accents_fast(projection)
    folded_terms = [_accent_fold(term) for term in query_terms]
    folded_spans = _lexical_phrase_spans_fast(
        folded_projection, folded_terms, first_only=False,
    )
    spans = []
    map_span = (
        None if len(folded_projection) == len(projection) or not folded_spans
        else _folded_span_mapper(projection)
    )
    for folded_span in folded_spans:
        original_span = folded_span if map_span is None else map_span(*folded_span)
        projected_terms = [
            match.group(0) for match in re.finditer(
                r"[^\W_]+", projection[original_span[0]:original_span[1]].casefold(),
                re.UNICODE,
            )
        ]
        if len(projected_terms) != len(query_terms):
            continue
        if all(
            (candidate == query if _has_diacritic(query)
             else _accent_fold(candidate) == _accent_fold(query))
            for query, candidate in zip(query_terms, projected_terms)
        ):
            spans.append(original_span)
            if first_only:
                break
    return spans


def _phrase_spans(
    projection: str, value: str, *, allow_accent_fallback: bool = True,
    first_only: bool = False,
) -> list[tuple[int, int]]:
    check_query_deadline()
    query_terms = [
        match.group(0) for match in re.finditer(r"[^\W_]+", value.casefold(), re.UNICODE)
    ]
    if len(query_terms) < 2:
        return []
    if not any(_has_diacritic(term) for term in query_terms) and all(
        _accent_fold(term).isascii() for term in query_terms
    ):
        folded_projection = (
            _fold_latin_accents_fast(projection) if allow_accent_fallback
            else projection.casefold()
        )
        folded_terms = [_accent_fold(term) for term in query_terms]
        matches = _lexical_phrase_spans_fast(
            folded_projection, folded_terms, first_only=first_only,
        )
        if not matches:
            return []
        if len(folded_projection) == len(projection):
            return matches
        original_span = _folded_span_mapper(projection)
        return [original_span(*match) for match in matches]
    if all(_accent_fold(term).replace("đ", "d").isascii() for term in query_terms):
        return _mixed_accent_phrase_spans_fast(
            projection, query_terms, first_only=first_only,
        )
    projected_terms = [
        (match.group(0), match.start(), match.end())
        for match in re.finditer(r"[^\W_]+", projection.casefold(), re.UNICODE)
    ]
    spans = []
    for index in range(len(projected_terms) - len(query_terms) + 1):
        check_query_deadline()
        window = projected_terms[index:index + len(query_terms)]
        if all(
            (candidate == query if _has_diacritic(query)
             else _accent_fold(candidate) == _accent_fold(query))
            for query, (candidate, _start, _end) in zip(query_terms, window)
        ) and REGION_SEPARATOR not in projection[window[0][1]:window[-1][2]]:
            spans.append((window[0][1], window[-1][2]))
            if first_only:
                break
    return spans


def _atom_spans(
    projection: str, atom: SearchAtom, *, allow_accent_fallback: bool = True,
    first_only: bool = False,
) -> list[tuple[int, int]]:
    check_query_deadline()
    projection = unicodedata.normalize("NFC", projection)
    value = unicodedata.normalize("NFC", atom.value)
    if atom.kind == "phrase" and STRUCTURED_TOKEN_RE.search(value):
        query = value.casefold()
        words = list(re.finditer(r"[^\W_]+", query, re.UNICODE))
        pattern = []
        cursor = 0
        pattern.append(r"(?<![\w.-])")
        for match in words:
            pattern.append(re.escape(query[cursor:match.start()]))
            pattern.append(r"([^\W_]+)")
            cursor = match.end()
        pattern.append(re.escape(query[cursor:]))
        pattern.append(r"(?![\w.-])")
        spans = []
        for candidate in re.finditer("".join(pattern), projection.casefold(), re.UNICODE):
            check_query_deadline()
            if all(
                (found == requested if _has_diacritic(requested)
                 else _accent_fold(found) == _accent_fold(requested))
                for requested, found in zip((item.group(0) for item in words), candidate.groups())
            ):
                spans.append((candidate.start(), candidate.end()))
        return spans[:1] if first_only else spans
    if atom.kind == "phrase":
        phrase_spans = _phrase_spans(
            projection, value, allow_accent_fallback=allow_accent_fallback,
            first_only=first_only,
        )
        if phrase_spans:
            return phrase_spans
        if len(re.findall(r"[^\W_]+", value, re.UNICODE)) > 1:
            return []
    exact = _has_diacritic(atom.value)
    haystack = unicodedata.normalize("NFC", projection).casefold() if exact else _accent_fold(projection)
    needle = value.casefold() if exact else _accent_fold(value)
    if not needle:
        return []
    spans = []
    start = 0
    lexical = bool(lexical_terms(atom.value))
    while (found := haystack.find(needle, start)) >= 0:
        check_query_deadline()
        end = found + len(needle)
        left_ok = _lexical_boundary_at(haystack, found, -1)
        right_ok = _lexical_boundary_at(haystack, end, 1)
        if not lexical or (left_ok and right_ok):
            spans.append((found, end))
        start = found + 1
    return spans


def evaluate_search_query(node: Any, projection: str) -> tuple[bool, list[tuple[int, int]]]:
    check_query_deadline()
    projection = unicodedata.normalize("NFC", projection)
    if isinstance(node, SearchAtom):
        spans = _atom_spans(projection, node)
        return bool(spans), spans
    operator = node[0]
    if operator == "NOT":
        matched, _spans = evaluate_search_query(node[1], projection)
        return not matched, []
    if operator == "AND":
        results = [evaluate_search_query(child, projection) for child in node[1:]]
        return all(item[0] for item in results), [span for item in results for span in item[1]]
    if operator == "OR":
        results = [evaluate_search_query(child, projection) for child in node[1:]]
        return any(item[0] for item in results), [span for item in results if item[0] for span in item[1]]
    if operator == "NEAR":
        left_ok, left = evaluate_search_query(node[2], projection)
        right_ok, right = evaluate_search_query(node[3], projection)
        limit = node[1]
        def distance(a: tuple[int, int], b: tuple[int, int]) -> int:
            if a[1] <= b[0]:
                gap = projection[a[1]:b[0]]
            elif b[1] <= a[0]:
                gap = projection[b[1]:a[0]]
            else:
                return 0
            if REGION_SEPARATOR in gap:
                return limit + 1
            return len(re.findall(r"[^\W_]+", gap, re.UNICODE))
        pairs = []
        for a in left:
            for b in right:
                check_query_deadline()
                if distance(a, b) <= limit:
                    pairs.append((a, b))
        return left_ok and right_ok and bool(pairs), [span for pair in pairs for span in pair]
    raise ValueError(f"unsupported query node: {operator}")


def _span_cover(spans: list[tuple[int, int]], projection_length: int) -> int:
    return (
        max(span[1] for span in spans) - min(span[0] for span in spans)
        if spans else projection_length
    )


def ranking_search_spans(node: Any, projection: str) -> list[tuple[int, int]]:
    """Return the smallest representative satisfying window for relevance ranking."""
    check_query_deadline()
    projection = unicodedata.normalize("NFC", projection)
    if isinstance(node, SearchAtom):
        spans = _atom_spans(projection, node)
        return [min(spans, key=lambda span: (span[1] - span[0], span[0]))] if spans else []
    operator = node[0]
    if operator == "NOT":
        return []
    if operator == "OR":
        choices = [
            ranking_search_spans(child, projection) for child in node[1:]
            if evaluate_search_query(child, projection)[0]
        ]
        choices = [spans for spans in choices if spans]
        return min(
            choices, key=lambda spans: (_span_cover(spans, len(projection)), spans),
        ) if choices else []
    if operator == "NEAR":
        left_ok, left = evaluate_search_query(node[2], projection)
        right_ok, right = evaluate_search_query(node[3], projection)
        if not left_ok or not right_ok:
            return []
        limit = node[1]
        pairs = []
        for a in left:
            for b in right:
                check_query_deadline()
                if a[1] <= b[0]:
                    gap = projection[a[1]:b[0]]
                elif b[1] <= a[0]:
                    gap = projection[b[1]:a[0]]
                else:
                    gap = ""
                if REGION_SEPARATOR in gap:
                    continue
                if len(re.findall(r"[^\W_]+", gap, re.UNICODE)) <= limit:
                    pairs.append([a, b])
        return min(
            pairs, key=lambda spans: (_span_cover(spans, len(projection)), spans),
        ) if pairs else []
    if operator == "AND":
        positives = [child for child in node[1:] if not (
            isinstance(child, tuple) and child[0] == "NOT"
        )]
        options = []
        for child in positives:
            if isinstance(child, SearchAtom):
                child_spans = [[span] for span in _atom_spans(projection, child)]
            else:
                ranked = ranking_search_spans(child, projection)
                child_spans = [ranked] if ranked else []
            if not child_spans:
                return []
            options.append(child_spans)
        combinations = 1
        for child_spans in options:
            combinations *= len(child_spans)
        if combinations <= 10_000:
            candidates = []
            for groups in itertools.product(*options):
                check_query_deadline()
                candidates.append([span for group in groups for span in group])
            return min(
                candidates, key=lambda spans: (_span_cover(spans, len(projection)), spans),
            ) if candidates else []
        return [span for child_spans in options for span in child_spans[0]]
    return []


def positive_search_atoms(node: Any, negated: bool = False) -> list[SearchAtom]:
    if isinstance(node, SearchAtom):
        return [] if negated else [node]
    if node[0] == "NOT":
        return positive_search_atoms(node[1], not negated)
    start = 2 if node[0] == "NEAR" else 1
    return [atom for child in node[start:] for atom in positive_search_atoms(child, negated)]


def fts_atom_expression(
    atom: SearchAtom, origin: str, *, preserve_occurrences: bool = False, phrases: bool = False,
) -> str | None:
    """FTS candidates for one atom; with a positional index a phrase narrows to its word order."""
    lexical, literal = fts_candidate_parts(
        atom.value, preserve_occurrences=False,
    )
    expressions = []
    if lexical:
        ordered = fts_candidate_parts(atom.value, preserve_occurrences=True)[0]
        spellings = _INDEX_SPELLINGS.get()
        value = None
        if phrases and atom.kind == "phrase" and len(ordered) > 1:
            if spellings is None:
                value = fts_phrase(ordered)
            else:
                # An unaccented word may be written with accents: one phrase per spelling.
                options = spellings.phrase_spellings(ordered)
                if options is not None:
                    value = " OR ".join(fts_phrase(option) for option in options)
        if value is None:
            value = " AND ".join(fts_word_choice(term) for term in lexical)
        expressions.append(f"{origin} : ({value})")
    if literal:
        column = origin + "_literal"
        expressions.append(
            f"{column} : (" + " AND ".join(
                '"' + term.replace('"', '""') + '"' for term in literal
            ) + ")"
        )
    return " AND ".join(f"({value})" for value in expressions) if expressions else None


def fts_candidate_expression(node: Any, origin: str, phrases: bool = False) -> str | None:
    if isinstance(node, SearchAtom):
        return fts_atom_expression(node, origin, phrases=phrases)
    operator = node[0]
    if operator == "NOT":
        return None
    start = 2 if operator == "NEAR" else 1
    children = [fts_candidate_expression(child, origin, phrases) for child in node[start:]]
    if operator == "OR":
        if any(child is None for child in children):
            return None
        return " OR ".join(f"({child})" for child in children)
    available = [child for child in children if child is not None]
    return " AND ".join(f"({child})" for child in available) if available else None


def demote_repeated_results(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep relevance order, moving same-session repeats after every first occurrence."""
    first: list[dict[str, Any]] = []
    repeats: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for entry in entries:
        identity = (
            entry["category"] if entry["category"] in REPEATED_SESSION_STATE_CATEGORIES
            else entry.get("_projection_hash") or entry["source"]["hash"]
        )
        key = (entry["provider"], entry["session"], identity)
        (repeats if key in seen else first).append(entry)
        seen.add(key)
    return first + repeats


CLOSENESS_WINDOWS = (3, 10, 50)
CLOSENESS_UNRANKED = len(CLOSENESS_WINDOWS) + 1
# Closeness ranking spells out every combination; past this it leaves the rows unranked.
CLOSENESS_SPELLING_CAP = 64


def fts_index_is_positional(db: sqlite3.Connection) -> bool:
    """True when the search index stores word positions (FTS5 detail=full)."""
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE name='positional_projection_fts'"
    ).fetchone()
    return bool(row) and "detail=full" in re.sub(r"\s+", "", row[0]).lower()


def closeness_tiers(
    db: sqlite3.Connection, tree: Any, origins: Iterable[str],
) -> dict[tuple[str, int], int]:
    """Tier per (origin, row) for a multi-word AND query: 0 exact phrase, then words within
    3, 10, or 50 positions; rows in no window are left out (CLOSENESS_UNRANKED)."""
    def and_terms(node: Any) -> list[str] | None:
        if isinstance(node, SearchAtom):
            return fts_candidate_parts(node.value, preserve_occurrences=True)[0] if node.kind == "term" else None
        if node[0] != "AND":
            return None
        terms: list[str] = []
        for child in node[1:]:
            child_terms = and_terms(child)
            if child_terms is None:
                return None
            terms.extend(child_terms)
        return terms
    terms = and_terms(tree) if tree is not None else None
    if not terms or len(terms) < 2:
        return {}
    spellings = _INDEX_SPELLINGS.get()
    choices = [spellings.spellings(term) if spellings is not None else [term] for term in terms]
    if math.prod(len(choice) for choice in choices) > CLOSENESS_SPELLING_CAP:
        return {}
    options = [list(option) for option in itertools.product(*choices)]
    tiers: dict[tuple[str, int], int] = {}
    for origin in origins:
        queries = [f"{origin} : (" + " OR ".join(fts_phrase(option) for option in options) + ")"] + [
            f"{origin} : (" + " OR ".join(
                "NEAR(" + " ".join(fts_phrase([term]) for term in option) + f", {window})"
                for option in options
            ) + ")"
            for window in CLOSENESS_WINDOWS
        ]
        for tier, expression in enumerate(queries):
            check_query_deadline()
            for (rowid,) in db.execute(
                "SELECT rowid FROM positional_projection_fts WHERE positional_projection_fts MATCH ?",
                (expression,),
            ):
                tiers.setdefault((origin, rowid), tier)
    return tiers


SEARCH_FTS_NEXT = "positional_projection_fts_next"
SEARCH_FTS_CHANGES = "search_fts_rebuild_changes"
EVENTS_PROJECTION_TRIGGER = """
    CREATE TRIGGER events_projection_bd BEFORE DELETE ON events BEGIN
      DELETE FROM positional_projection_fts WHERE rowid=old.rowid;
      DELETE FROM search_projection_refs WHERE event_rowid=old.rowid;
    END;
"""


def create_search_fts(
    db: sqlite3.Connection, name: str, detail: str, *, keep_accents: bool = True,
) -> None:
    if detail not in ("column", "full"):
        raise ValueError("search index detail must be column or full")
    tokenizer = ACCENT_KEEPING_TOKENIZER if keep_accents else ACCENT_FOLDING_TOKENIZER
    db.execute(
        f"CREATE VIRTUAL TABLE {name} USING fts5("
        "native,copied,unclassified,native_literal,copied_literal,unclassified_literal,"
        f"content='',contentless_delete=1,detail={detail},"
        f"tokenize='{tokenizer}')"
    )


def _insert_search_fts_row(
    db: sqlite3.Connection, table: str, handle: Any, rowid: int, cache: dict[int, bytes],
    keep_accents: bool = True,
) -> bool:
    """Index one event's retained projection; False when the event no longer has one."""
    if len(cache) > 8:
        cache.clear()
    try:
        regions, _compressed = read_projection_for_event(db, handle, rowid, cache)
    except ValueError as exc:
        if "search projection missing" in str(exc):
            return False
        raise
    columns = [REGION_BOUNDARY.join(regions[origin]) for origin in PROJECTION_ORIGINS]
    insert_projection_index_row(db, table, rowid, columns, keep_accents)
    return True


def rebuild_search_fts(
    db: sqlite3.Connection, detail: str,
    progress: Callable[[dict[str, Any]], None] | None = None,
    *, keep_accents: bool = True,
) -> dict[str, Any]:
    """Rebuild only the search index in one locked transaction (for copies; see the online form).

    Projections, passages, and snapshot identity are unchanged; no transcript is read.
    """
    started = time.monotonic()
    lease = acquire_projection_pack_lease(db, exclusive=True)
    try:
        db.execute("BEGIN IMMEDIATE")
        event_count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        db.execute("DROP TABLE IF EXISTS positional_projection_fts")
        create_search_fts(db, "positional_projection_fts", detail, keep_accents=keep_accents)
        db.execute("DELETE FROM search_term_variants")
        cache: dict[int, bytes] = {}
        with projection_pack_path(db).open("rb") as handle, forbid_source_transcript_reads() as reads:
            rows = db.execute(
                "SELECT event_rowid FROM search_projection_refs ORDER BY block_key,projection_offset"
            ).fetchall()
            for processed, (rowid,) in enumerate(rows, 1):
                _insert_search_fts_row(
                    db, "positional_projection_fts", handle, rowid, cache, keep_accents,
                )
                if progress and (processed % 50_000 == 0 or processed == len(rows)):
                    progress({"phase": "search_fts_rebuild", "processed": processed, "total": len(rows)})
        db.execute("INSERT INTO positional_projection_fts(positional_projection_fts) VALUES('optimize')")
        snapshot_id = refresh_projection_metadata(db)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        release_projection_pack_lease(lease)
    clear_caches()
    return {
        "complete": True, "events": event_count, "detail": detail,
        "accents": "kept" if keep_accents else "folded", "snapshot_id": snapshot_id,
        "source_reads": reads["attempts"], "seconds": round(time.monotonic() - started, 1),
    }


def _drain_search_fts_changes(db: sqlite3.Connection, handle: Any, keep_accents: bool = True) -> int:
    """Re-index every event whose projection changed since the rebuild started (in a transaction)."""
    changed = [row[0] for row in db.execute(f"SELECT event_rowid FROM {SEARCH_FTS_CHANGES}")]
    cache: dict[int, bytes] = {}
    for rowid in changed:
        db.execute(f"DELETE FROM {SEARCH_FTS_NEXT} WHERE rowid=?", (rowid,))
        _insert_search_fts_row(db, SEARCH_FTS_NEXT, handle, rowid, cache, keep_accents)
        db.execute(f"DELETE FROM {SEARCH_FTS_CHANGES} WHERE event_rowid=?", (rowid,))
    return len(changed)


def rebuild_search_fts_online(
    db: sqlite3.Connection, detail: str, *, chunk_seconds: float = 1.0, pause_seconds: float = 0.25,
    progress: Callable[[dict[str, Any]], None] | None = None,
    between_chunks: Callable[[], None] | None = None,
    swap_lock_timeout: float = 120.0, keep_accents: bool = True,
) -> dict[str, Any]:
    """Build the new search index beside the serving one, then swap it in a short locked step.

    Searches keep using the serving index and ingestion keeps writing throughout: the build runs
    in short transactions, triggers log every projection change made meanwhile, and the swap
    re-indexes the logged events, proves the new index covers exactly the indexed events, and
    renames it into place. A failed or interrupted run leaves the serving index untouched.
    """
    started = time.monotonic()
    db.execute("BEGIN IMMEDIATE")
    try:
        for trigger in ("search_fts_rebuild_ai", "search_fts_rebuild_au", "search_fts_rebuild_ad"):
            db.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        db.execute(f"DROP TABLE IF EXISTS {SEARCH_FTS_NEXT}")
        db.execute(f"DROP TABLE IF EXISTS {SEARCH_FTS_CHANGES}")
        # Spelling rows from the serving index stay: extra ones are harmless, missing ones are not.
        create_search_fts(db, SEARCH_FTS_NEXT, detail, keep_accents=keep_accents)
        db.execute(f"CREATE TABLE {SEARCH_FTS_CHANGES}(event_rowid INTEGER PRIMARY KEY)")
        for name, timing, ref in (
            ("search_fts_rebuild_ai", "AFTER INSERT", "new"),
            ("search_fts_rebuild_au", "AFTER UPDATE", "new"),
            ("search_fts_rebuild_ad", "AFTER DELETE", "old"),
        ):
            db.execute(
                f"CREATE TRIGGER {name} {timing} ON search_projection_refs BEGIN "
                f"INSERT OR IGNORE INTO {SEARCH_FTS_CHANGES} VALUES({ref}.event_rowid); END"
            )
        planned = [row[0] for row in db.execute(
            "SELECT event_rowid FROM search_projection_refs ORDER BY block_key,projection_offset"
        )]
        db.commit()
    except Exception:
        db.rollback()
        raise
    processed = 0
    with forbid_source_transcript_reads() as reads:
        while processed < len(planned):
            lease = acquire_projection_pack_lease(db, exclusive=False)
            try:
                db.execute("BEGIN IMMEDIATE")
                chunk_started = time.monotonic()
                cache: dict[int, bytes] = {}
                with projection_pack_path(db).open("rb") as handle:
                    # At least one event per chunk, then until the chunk's time budget is spent.
                    while processed < len(planned):
                        _insert_search_fts_row(
                            db, SEARCH_FTS_NEXT, handle, planned[processed], cache, keep_accents,
                        )
                        processed += 1
                        if time.monotonic() - chunk_started >= chunk_seconds:
                            break
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                release_projection_pack_lease(lease)
            if progress and (processed == len(planned) or processed // 50_000 != (processed - 1) // 50_000):
                progress({"phase": "search_fts_online_build", "processed": processed, "total": len(planned)})
            if between_chunks:
                between_chunks()
            # Committing and immediately locking again starves writers polling on busy_timeout.
            time.sleep(pause_seconds)
        caught_up = 0
        for _pass in range(3):
            lease = acquire_projection_pack_lease(db, exclusive=False)
            try:
                db.execute("BEGIN IMMEDIATE")
                with projection_pack_path(db).open("rb") as handle:
                    drained = _drain_search_fts_changes(db, handle, keep_accents)
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                release_projection_pack_lease(lease)
            caught_up += drained
            if drained < 1000:
                break
        # Merge index segments before the swap: this holds only the write lock, so searches
        # keep running; the swap afterwards is a short catch-up and rename.
        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute(f"INSERT INTO {SEARCH_FTS_NEXT}({SEARCH_FTS_NEXT}) VALUES('optimize')")
            db.commit()
        except Exception:
            db.rollback()
            raise
        swap_started = time.monotonic()
        lease = acquire_projection_pack_lease(db, exclusive=True, timeout=swap_lock_timeout)
        try:
            db.execute("BEGIN IMMEDIATE")
            with projection_pack_path(db).open("rb") as handle:
                caught_up += _drain_search_fts_changes(db, handle, keep_accents)
            extra = db.execute(
                f"SELECT COUNT(*) FROM (SELECT rowid FROM {SEARCH_FTS_NEXT} "
                "EXCEPT SELECT event_rowid FROM search_projection_refs)"
            ).fetchone()[0]
            missing = db.execute(
                "SELECT COUNT(*) FROM (SELECT event_rowid FROM search_projection_refs "
                f"EXCEPT SELECT rowid FROM {SEARCH_FTS_NEXT})"
            ).fetchone()[0]
            if extra or missing:
                raise ValueError(f"rebuilt search index differs from indexed events: {extra} extra, {missing} missing")
            for trigger in ("search_fts_rebuild_ai", "search_fts_rebuild_au", "search_fts_rebuild_ad",
                            "events_projection_bd"):
                db.execute(f"DROP TRIGGER IF EXISTS {trigger}")
            db.execute("DROP TABLE positional_projection_fts")
            db.execute(f"ALTER TABLE {SEARCH_FTS_NEXT} RENAME TO positional_projection_fts")
            db.execute(EVENTS_PROJECTION_TRIGGER)
            db.execute(f"DROP TABLE {SEARCH_FTS_CHANGES}")
            snapshot_id = refresh_projection_metadata(db)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            release_projection_pack_lease(lease)
    clear_caches()
    return {
        "complete": True, "events": len(planned), "detail": detail,
        "accents": "kept" if keep_accents else "folded", "snapshot_id": snapshot_id,
        "changed_during_build": caught_up, "source_reads": reads["attempts"],
        "swap_seconds": round(time.monotonic() - swap_started, 2),
        "seconds": round(time.monotonic() - started, 1),
    }


# Earlier SessionDB searches repeat the query words verbatim, so they outrank what they found.
SEARCH_ECHO_EXPRESSION = (
    '{native copied unclassified} : ("sessiondb" AND "text" AND ("index" OR "query"))'
)


def demote_search_echoes(
    db: sqlite3.Connection, entries: list[dict[str, Any]], text: str,
) -> list[dict[str, Any]]:
    """Move results that are earlier SessionDB search commands or their output after the rest,
    unless the query itself is about SessionDB."""
    if "sessiondb" in text.casefold() or not entries:
        return entries
    echoes = {
        row[0] for row in db.execute(
            "SELECT rowid FROM positional_projection_fts WHERE positional_projection_fts MATCH ?",
            (SEARCH_ECHO_EXPRESSION,),
        )
    }
    if not echoes:
        return entries
    keep = [entry for entry in entries if entry.get("_event_rowid") not in echoes]
    return keep + [entry for entry in entries if entry.get("_event_rowid") in echoes]


def fts_match_origins(
    db: sqlite3.Connection, expressions: dict[str, str | None],
) -> tuple[dict[int, list[str]], dict[int, float]]:
    matches: dict[int, list[str]] = {}
    scores: dict[int, float] = {}
    for origin in PROJECTION_ORIGINS:
        expression = expressions.get(origin)
        if not expression:
            continue
        for row in db.execute(
            "SELECT rowid,bm25(positional_projection_fts) AS score "
            "FROM positional_projection_fts "
            "WHERE positional_projection_fts MATCH ?",
            (expression,),
        ):
            check_query_deadline()
            matches.setdefault(row["rowid"], []).append(origin)
            scores[row["rowid"]] = min(scores.get(row["rowid"], row["score"]), row["score"])
    return matches, scores


def fts_fully_verifies(node: Any, positional: bool = False) -> bool:
    """True when the FTS candidate set is already the exact answer.

    A positional index makes an ASCII quoted phrase exact, including hyphenated words such as
    `Rule-Writing`, so it needs no per-row Python re-check. An accent-keeping index also makes
    accented words and phrases exact, and unaccented ones through their spelled-out spellings."""
    if isinstance(node, SearchAtom) and _INDEX_SPELLINGS.get() is not None:
        if node.kind not in {"term", "phrase"} or LITERAL_SEARCH_RE.search(node.value):
            return False
        if re.fullmatch(r"[^\W_]+", node.value, re.UNICODE) is not None:
            return True
        # One punctuation-joined word (`cost-state`, `run_trials.py`) is exactly its structured
        # anchor: the anchor exists only where the whole token stands between non-word characters.
        if (
            not _has_diacritic(node.value)
            and STRUCTURED_TOKEN_RE.fullmatch(unicodedata.normalize("NFC", node.value).casefold())
        ):
            return True
        return (
            positional and node.kind == "phrase"
            and bool(fts_candidate_parts(node.value)[0])
        )
    if isinstance(node, SearchAtom):
        if (
            node.kind not in {"term", "phrase"} or _has_diacritic(node.value)
            or LITERAL_SEARCH_RE.search(node.value)
        ):
            return False
        if re.fullmatch(r"[^\W_]+", node.value, re.UNICODE) is not None:
            return True
        return (
            positional and node.kind == "phrase" and node.value.isascii()
            and bool(fts_candidate_parts(node.value)[0])
        )
    return node[0] in {"AND", "OR"} and all(
        fts_fully_verifies(child, positional) for child in node[1:]
    )


def projection_query_matches(projection: str, text: str, mode: str) -> bool:
    if mode == "ranked":
        return evaluate_search_query(parse_search_query(text), projection)[0]
    query_terms = lexical_terms(text)
    if mode == "literal" and query_terms:
        return bool(_atom_spans(projection, SearchAtom("phrase", text)))
    if mode == "all_words":
        query_words = re.findall(r"[^\W_]+", text.casefold(), re.UNICODE)
        literal_runs = [
            match.group(0).casefold() for match in LITERAL_RUN_RE.finditer(text)
            if LITERAL_SEARCH_RE.search(match.group(0))
        ]
        exact_haystack = unicodedata.normalize("NFC", projection).casefold()
        folded_haystack = None

        def contains_term(term: str) -> bool:
            nonlocal folded_haystack
            if _has_diacritic(term):
                haystack = exact_haystack
                needle = unicodedata.normalize("NFC", term).casefold()
            else:
                if folded_haystack is None:
                    folded_haystack = _accent_fold(projection)
                haystack = folded_haystack
                needle = _accent_fold(term)
            start = 0
            while (found := haystack.find(needle, start)) >= 0:
                check_query_deadline()
                end = found + len(needle)
                if (
                    _lexical_boundary_at(haystack, found, -1)
                    and _lexical_boundary_at(haystack, end, 1)
                ):
                    return True
                start = found + 1
            return False

        return (
            all(contains_term(term) for term in query_words)
            and all(run in exact_haystack for run in literal_runs)
            and bool(query_words or literal_runs)
        )
    projected_terms = lexical_terms(projection)
    if LITERAL_SEARCH_RE.search(text) or not query_terms:
        return text.casefold() in projection.casefold()
    if len(query_terms) == 1:
        return query_terms[0] in set(projected_terms)
    width = len(query_terms)
    return any(
        projected_terms[index:index + width] == query_terms
        for index in range(len(projected_terms) - width + 1)
    )


def matching_projection_regions(
    regions: dict[str, list[str]], text: str, mode: str, tree: Any | None,
    *, allow_accent_fallback: bool = True,
) -> list[tuple[str, str, list[tuple[int, int]]]]:
    matches = []
    for origin in PROJECTION_ORIGINS:
        check_query_deadline()
        projection = REGION_SEPARATOR.join(regions[origin])
        if not projection:
            continue
        if mode == "ranked":
            matched, spans = evaluate_search_query(tree, projection)
            if matched:
                spans = ranking_search_spans(tree, projection)
        elif mode == "literal":
            words = re.findall(r"[^\W_]+", text, re.UNICODE)
            kind = "phrase" if len(words) > 1 or STRUCTURED_TOKEN_RE.search(text) else "term"
            effective_accent_fallback = (
                allow_accent_fallback or _has_non_vietnamese_latin_accent(projection)
            )
            spans = _atom_spans(
                projection, SearchAtom(kind, text),
                allow_accent_fallback=effective_accent_fallback,
                first_only=True,
            )
            matched = bool(spans)
        else:
            matched = projection_query_matches(projection, text, mode)
            spans = []
        if matched:
            matches.append((origin, projection, spans))
    return matches


PASSAGE_FULL_CHARS = 1200
PASSAGE_WINDOW_CHARS = 420
READABLE_EVENT_MAX_BYTES = 4096
READABLE_RESPONSE_TEXT_MAX_BYTES = 24576


def projection_passage(projection: str, text: str, mode: str, radius: int = 100) -> str:
    # Regions are redacted before storage. A provider envelope may add many small
    # regions ahead of the actual message, so judge a short readable region on
    # its own rather than on the length of the entire indexed projection.
    regions = projection.split(REGION_SEPARATOR)
    matching_short = [part for part in regions if len(part) <= PASSAGE_FULL_CHARS
                      and projection_query_matches(part, text, mode)]
    if matching_short:
        return max(matching_short, key=len).replace(REGION_BOUNDARY, " … ")
    if len(projection) <= PASSAGE_FULL_CHARS:
        return projection.replace(REGION_SEPARATOR, " … ").replace(REGION_BOUNDARY, " … ")
    needles = [text] if mode == "literal" else lexical_terms(text)
    # Keep the beginning and a query-bearing window, within a fixed display budget.
    # The existing exact predicate has already selected this record; this only chooses
    # what to show and never changes membership or rank.
    first_span = None
    for needle in needles:
        first_span = _folded_span(projection, needle)
        if first_span:
            break
    if first_span is None and mode == "literal":
        for needle in lexical_terms(text):
            first_span = _folded_span(projection, needle)
            if first_span:
                break
    if first_span:
        start = max(0, first_span[0] - PASSAGE_WINDOW_CHARS // 3)
        end = min(len(projection), start + PASSAGE_WINDOW_CHARS)
        windows = [(0, PASSAGE_WINDOW_CHARS), (start, end)]
        merged = []
        for left, right in sorted(windows):
            if merged and left <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], right))
            else:
                merged.append((left, right))
        return " … ".join(projection[left:right] for left, right in merged).replace(
            REGION_SEPARATOR, " … ").replace(REGION_BOUNDARY, " … ")
    lowered = projection.lower()
    for needle in needles:
        lowered_needle = needle.lower()
        if len(lowered) == len(projection) and len(lowered_needle) == len(needle):
            found = lowered.find(lowered_needle)
            if found >= 0:
                return projection[
                    max(0, found - radius):min(len(projection), found + len(needle) + radius)
                ].replace(REGION_SEPARATOR, " … ").replace(REGION_BOUNDARY, " … ")
        direct = re.search(re.escape(needle), projection, re.IGNORECASE)
        if direct:
            return projection[
                max(0, direct.start() - radius):min(len(projection), direct.end() + radius)
            ].replace(REGION_SEPARATOR, " … ").replace(REGION_BOUNDARY, " … ")
    spans = sorted(span for needle in needles if (span := _folded_span(projection, needle)))
    if not spans:
        terms = lexical_terms(text)
        spans = sorted(span for term in terms if (span := _folded_span(projection, term)))
    if not spans:
        return projection[: radius * 2].replace(REGION_SEPARATOR, " … ").replace(REGION_BOUNDARY, " … ")
    focus = spans[0]
    start = max(0, focus[0] - radius)
    end = min(len(projection), focus[1] + radius)
    return projection[start:end].replace(REGION_SEPARATOR, " … ").replace(REGION_BOUNDARY, " … ")


def projection_passage_with_status(
    projection: str, text: str, mode: str,
) -> tuple[str, bool]:
    """Return one passage and whether it omits any displayed projection text."""
    passage = projection_passage(projection, text, mode)
    complete = projection.replace(REGION_SEPARATOR, " … ").replace(REGION_BOUNDARY, " … ")
    return passage, passage != complete


def passage_group_key(entry: dict[str, Any]) -> tuple[Any, ...]:
    return (
        entry.get("match_origin"), entry.get("passage"),
        bool(entry.get("passage_truncated")),
    )


def hydrate_distinct_passages(
    entries: list[dict[str, Any]], passage_limit: int | None,
    hydrate: Callable[[dict[str, Any]], tuple[str, bool]],
) -> int:
    """Hydrate one ranked prefix until its distinct-passage display target is met."""
    distinct: set[tuple[Any, ...]] = set()
    hydrated = 0
    scan_limit = None if passage_limit is None else max(passage_limit, INLINE_PASSAGE_SCAN_LIMIT)
    for entry in entries:
        if passage_limit is not None and (len(distinct) >= passage_limit or hydrated >= scan_limit):
            break
        check_query_deadline()
        passage, truncated = hydrate(entry)
        entry["passage"] = passage
        if truncated:
            entry["passage_truncated"] = True
        hydrated += 1
        distinct.add(passage_group_key(entry))
    return hydrated


def _readable_slice(value: str, start: int, max_bytes: int) -> tuple[str, int]:
    """Take complete Unicode characters while respecting a UTF-8 byte budget."""
    if max_bytes <= 0 or start >= len(value):
        return "", start
    low, high = start, min(len(value), start + max_bytes)
    while low < high:
        mid = (low + high + 1) // 2
        if len(value[start:mid].encode("utf-8")) <= max_bytes:
            low = mid
        else:
            high = mid - 1
    return value[start:low], low


def bounded_readable_text(value: str, args: argparse.Namespace, remaining: int) -> dict[str, Any]:
    """Expose byte-bounded text with explicit character offsets for continuation."""
    budget = min(READABLE_EVENT_MAX_BYTES, max(0, remaining))
    query = getattr(args, "readable_query", None)
    offset = getattr(args, "readable_offset", None)
    if offset is not None and offset < 0:
        raise ValueError("readable offset must be nonnegative")
    if offset is None and query:
        found = value.casefold().find(str(query).casefold())
        start = max(0, found - 256) if found >= 0 else 0
    else:
        start = min(offset or 0, len(value))
    # The first page also shows the ending, where a conclusion often occurs
    # long after the matched term. Its range is disclosed like every other cut.
    tail_budget = min(768, budget // 4) if offset is None and len(value.encode("utf-8")) > budget else 0
    main, end = _readable_slice(value, start, budget - tail_budget - (5 if tail_budget else 0))
    segments = [{"start": start, "end": end}]
    parts = [main]
    if tail_budget and end < len(value):
        tail = value.encode("utf-8")[-tail_budget:].decode("utf-8", "ignore")
        tail_start = len(value) - len(tail)
        tail_end = len(value)
        if tail and tail_start > end:
            parts.append(tail)
            segments.append({"start": tail_start, "end": tail_end})
    next_offset = end if end < len(value) and (len(segments) == 1 or end < segments[-1]["start"]) else None
    return {
        "text": " … ".join(parts), "readable_total_chars": len(value),
        "readable_segments": segments, "readable_truncated": start > 0 or end < len(value),
        "readable_next_offset": next_offset,
        "readable_deferred": budget == 0 and bool(value),
    }


def retrieval_result_id(event_id: str, source_id: str, line_number: int) -> str:
    return f"r1_{event_id}_{source_id}_{line_number}"


def read_projection_for_event(
    db: sqlite3.Connection, projection_handle: Any, event_rowid: int,
    block_cache: dict[int, bytes] | None = None,
) -> tuple[dict[str, list[str]], int]:
    row = db.execute(
        "SELECT r.*,b.block_digest,b.pack_offset,b.compressed_bytes,"
        "b.raw_bytes AS block_raw_bytes,b.compressed_hash "
        "FROM search_projection_refs r JOIN search_projection_blocks b USING(block_key) "
        "WHERE r.event_rowid=?", (event_rowid,),
    ).fetchone()
    if not row:
        raise ValueError(f"search projection missing for event row {event_rowid}")
    block = block_cache.get(row["block_key"]) if block_cache is not None else None
    if block is None:
        projection_handle.seek(row["pack_offset"])
        compressed = projection_handle.read(row["compressed_bytes"])
        if len(compressed) != row["compressed_bytes"] or digest_bytes(compressed) != row["compressed_hash"]:
            raise ValueError(f"compressed projection block integrity failed: {row['block_key']}")
        block = zstd.decompress(compressed, max_output_size=row["block_raw_bytes"])
        if len(block) != row["block_raw_bytes"] or digest_bytes(block) != row["block_digest"]:
            raise ValueError(f"projection block integrity failed: {row['block_key']}")
        if block_cache is not None:
            block_cache[row["block_key"]] = block
    start = row["projection_offset"]
    data = block[start:start + row["projection_bytes"]]
    if len(data) != row["projection_bytes"] or digest_bytes(data) != row["projection_hash"]:
        raise ValueError(f"search passage integrity failed for event row {event_rowid}")
    return decoded_search_regions(data), row["compressed_bytes"]


def parse_retrieval_result_id(result_id: str) -> tuple[str, str | None, int | None]:
    parts = result_id.split("_")
    if (
        len(parts) == 4 and parts[0] == "r1" and len(parts[1]) == 64 and len(parts[2]) == 64
        and all(char in "0123456789abcdef" for char in parts[1] + parts[2])
    ):
        try:
            return parts[1], parts[2], int(parts[3])
        except ValueError:
            pass
    return result_id, None, None



def query_projection_status(
    db: sqlite3.Connection, args: argparse.Namespace, status: dict[str, Any],
) -> dict[str, Any]:
    """Accept a stale corpus-wide projection when every event this query can match is projected.

    Version, reference, pack, and per-event table agreement stay corpus-wide integrity
    requirements; only events outside the query's filters may lack a projection.
    """
    stored_version = db.execute(
        "SELECT value FROM metadata WHERE key='projection_version'"
    ).fetchone()
    integrity_ok = (
        stored_version is not None and stored_version[0] == PROJECTION_VERSION
        and status["invalid_references"] == 0
        and status["projections"] == status["passages"]
        and status["projections"] <= status["events"]
    )
    if not integrity_ok:
        return status
    clauses, values = _index_filter_clauses(args)
    clauses.append(
        "NOT EXISTS (SELECT 1 FROM search_projection_refs r WHERE r.event_rowid=e.rowid)"
    )
    missing = db.execute(
        "SELECT COUNT(*) FROM events e WHERE " + " AND ".join(clauses), values,
    ).fetchone()[0]
    if missing:
        return {**status, "reason": "projection_missing_for_query", "unprojected_events": missing}
    return {
        **status, "complete": True, "reason": "projection_current_for_query",
        "unprojected_events_outside_query": status["events"] - status["projections"],
    }


def unsupported_events_in_scope(db: sqlite3.Connection, args: argparse.Namespace) -> tuple[int, int]:
    """Count unsupported events that could match a query, as (events, sources).

    Unsupported events may lack parsed dimensions, so a missing value always counts
    as in scope, and role and tool filters are not applied to them.
    """
    # Unsupported events always raise their source's issue_count, so the small sources
    # table bounds the scan and a clean corpus costs one indexed lookup.
    if db.execute("SELECT 1 FROM sources WHERE issue_count>0 LIMIT 1").fetchone() is None:
        return 0, 0
    clauses = [
        "e.source_id IN (SELECT source_id FROM sources WHERE issue_count>0)",
        "e.supported=0",
    ]
    values: list[Any] = []
    for column, name in (
        ("e.provider", "provider"), ("e.project", "project"),
        ("e.session_id", "session"), ("e.parent_session_id", "lineage"),
    ):
        requested = getattr(args, name, None)
        if requested:
            escaped = requested.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append(f"({column} IS NULL OR {column} LIKE ? ESCAPE '\\')")
            values.append(f"%{escaped}%")
    if getattr(args, "since", None):
        clauses.append("(e.order_timestamp IS NULL OR e.order_timestamp >= ?)")
        values.append(normalize_timestamp(args.since) or args.since)
    if getattr(args, "until", None):
        clauses.append("(e.order_timestamp IS NULL OR e.order_timestamp <= ?)")
        values.append(normalize_timestamp(args.until) or args.until)
    row = db.execute(
        "SELECT COUNT(*),COUNT(DISTINCT e.source_id) FROM events e WHERE " + " AND ".join(clauses),
        values,
    ).fetchone()
    return row[0], row[1]


def verify_candidate_projection_integrity(
    db: sqlite3.Connection, projection_handle: Any, clauses: list[str], values: list[Any],
) -> tuple[int, int]:
    sql = (
        "SELECT r.*,b.block_digest,b.pack_offset,b.compressed_bytes,b.compressed_hash "
        "FROM events e JOIN search_projection_refs r ON r.event_rowid=e.rowid "
        "JOIN search_projection_blocks b USING(block_key) WHERE "
        + (" AND ".join(clauses) if clauses else "1=1")
        + " ORDER BY b.pack_offset,e.rowid"
    )
    active_block = None
    verified_blocks = 0
    verified_bytes = 0
    for row in db.execute(sql, tuple(values)):
        check_query_deadline()
        expected_reference = projection_reference_hash(
            row["event_rowid"], row["block_digest"], row["projection_offset"],
            row["projection_bytes"], row["projection_hash"],
        )
        if row["reference_hash"] != expected_reference:
            raise ValueError(f"projection reference integrity failed: {row['event_rowid']}")
        if row["block_key"] != active_block:
            projection_handle.seek(row["pack_offset"])
            compressed = projection_handle.read(row["compressed_bytes"])
            if (
                len(compressed) != row["compressed_bytes"]
                or digest_bytes(compressed) != row["compressed_hash"]
            ):
                raise ValueError(f"compressed projection block integrity failed: {row['block_key']}")
            active_block = row["block_key"]
            verified_blocks += 1
            verified_bytes += len(compressed)
    return verified_blocks, verified_bytes

def _search_index(
    db: sqlite3.Connection, args: argparse.Namespace, projection_handle: Any,
    receipt_status: dict[str, Any] | None = None,
    coverage_started: float | None = None,
) -> dict[str, Any]:
    with index_spellings(db):
        return _search_index_matches(db, args, projection_handle, receipt_status, coverage_started)


def _search_index_matches(
    db: sqlite3.Connection, args: argparse.Namespace, projection_handle: Any,
    receipt_status: dict[str, Any] | None = None,
    coverage_started: float | None = None,
) -> dict[str, Any]:
    check_query_deadline(args)
    coverage_started = coverage_started or time.monotonic()
    _note_progress("coverage")
    status = receipt_status
    coverage_verification = "receipt" if status is not None else "deep"
    if status is None:
        status = projection_status(db, pack_available=True, deep=True)
    if not status["complete"]:
        status = query_projection_status(db, args, status)
    coverage_seconds = time.monotonic() - coverage_started
    _note_progress("candidate_selection", coverage_seconds=round(coverage_seconds, 6))
    if not status["complete"]:
        return {
            "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
            "complete": False, "backend": "database", "coverage": status,
            "total": None, "entries": [], "telemetry": {
                "coverage_verification": coverage_verification,
                "coverage_seconds": coverage_seconds,
            },
        }
    candidate_selection_started = time.monotonic()
    text = unicodedata.normalize("NFC", " ".join((args.text or "").split()))
    if not text:
        raise ValueError("search-index requires non-empty --text")
    mode = getattr(args, "mode", "ranked")
    if mode not in {"ranked", "literal", "all_words"}:
        raise ValueError(f"unsupported search mode: {mode}")
    order = getattr(args, "order", "relevance")
    if order not in {"relevance", "chronological"}:
        raise ValueError(f"unsupported search order: {order}")
    clauses, values = _index_filter_clauses(args)
    tree = parse_search_query(text) if mode == "ranked" else None
    atoms = (
        positive_search_atoms(tree) if tree is not None
        else [
            SearchAtom("term", match.group(0))
            for match in re.finditer(r"[^\W_]+", text.casefold(), re.UNICODE)
        ]
        if mode == "all_words"
        else [SearchAtom("term", text)]
    )
    if mode == "ranked" and not atoms:
        raise ValueError("search query requires at least one positive term or phrase")
    fts_terms = (
        fts_candidate_terms(text, preserve_occurrences=True)
        if mode == "literal"
        else list(dict.fromkeys(term for atom in atoms for term in fts_candidate_terms(atom.value)))
    )
    fts_rank: dict[int, float] = {}
    fts_origins: dict[int, list[str]] = {}
    accent_phrase_candidates: set[int] | None = None
    positional = fts_index_is_positional(db)
    if tree is not None:
        fts_queries = {
            origin: fts_candidate_expression(tree, origin, positional) for origin in PROJECTION_ORIGINS
        }
    elif mode == "literal":
        fts_queries = {
            origin: fts_atom_expression(
                SearchAtom("phrase", text), origin, preserve_occurrences=True, phrases=positional,
            )
            for origin in PROJECTION_ORIGINS
        }
    else:
        fts_queries = {}
        for origin in PROJECTION_ORIGINS:
            parts = [fts_atom_expression(atom, origin, phrases=positional) for atom in atoms]
            available = [part for part in parts if part]
            fts_queries[origin] = (
                " AND ".join(f"({part})" for part in available) if available else None
            )
    if any(fts_queries.values()):
        fts_origins, fts_rank = fts_match_origins(db, fts_queries)
        _note_progress(fts_candidates=len(fts_origins))
        check_query_deadline(args)
        phrase_terms = lexical_terms(text) if mode == "literal" else []
        if len(phrase_terms) > 1 and not _has_diacritic(text) and _INDEX_SPELLINGS.get() is None:
            accent_phrase_candidates = accented_phrase_candidate_rowids(
                db, phrase_terms, set(fts_origins),
            )
        db.execute(
            "CREATE TEMP TABLE IF NOT EXISTS sessiondb_query_hits("
            "rowid INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        db.execute("DELETE FROM sessiondb_query_hits")
        db.executemany(
            "INSERT INTO sessiondb_query_hits(rowid) VALUES(?)",
            ((rowid,) for rowid in fts_origins),
        )
        clauses.append("e.rowid IN (SELECT rowid FROM sessiondb_query_hits)")
    sql = (
        "SELECT e.rowid AS event_rowid,e.*,src.path AS source_path,"
        "r.projection_offset,r.projection_bytes,r.projection_hash,"
        "b.block_key,b.block_digest,b.pack_offset,b.compressed_bytes,"
        "b.raw_bytes AS block_raw_bytes,b.compressed_hash "
        "FROM events e JOIN sources src USING(source_id) "
        "JOIN search_projection_refs r ON r.event_rowid=e.rowid "
        "JOIN search_projection_blocks b USING(block_key) "
        f"WHERE {' AND '.join(clauses) if clauses else '1=1'} ORDER BY b.pack_offset,e.rowid"
    )
    fast_atom = (
        SearchAtom("term", text) if mode == "literal"
        else tree if mode == "ranked" and isinstance(tree, SearchAtom) and tree.kind == "term"
        else None
    )
    fast_single_term = (
        isinstance(fast_atom, SearchAtom)
        and re.fullmatch(r"[^\W_]+", fast_atom.value, re.UNICODE) is not None
        and not _has_diacritic(fast_atom.value)
        and not LITERAL_SEARCH_RE.search(fast_atom.value)
    )
    fast_literal_ascii = (
        mode == "literal" and text.isascii()
        and re.fullmatch(r"[^\W_]+", text, re.UNICODE) is not None
        and bool(fts_terms)
    )
    fast_literal_symbol = (
        mode == "literal" and len(text) == 1
        and not lexical_terms(text) and bool(literal_tokens(text))
    )
    fast_literal_pair = (
        mode == "literal" and len(text) == 2
        and LITERAL_RUN_RE.fullmatch(text) is not None
        and LITERAL_SEARCH_RE.search(text) is not None
    )
    fast_structured_literal = (
        mode == "literal"
        and STRUCTURED_TOKEN_RE.fullmatch(text) is not None
        and not _has_diacritic(text)
        and bool(structured_query_tokens(text))
    )
    literal_words = [
        match.group(0) for match in re.finditer(r"[^\W_]+", text.casefold(), re.UNICODE)
    ]
    spellings = _INDEX_SPELLINGS.get()
    fast_literal_accent_pair = (
        mode == "literal" and len(literal_words) == 2 and spellings is None
        and all(_has_diacritic(word) for word in literal_words)
        and bool(accent_phrase_tokens(text))
    )
    # An accent-keeping positional index answers plain words and phrases, accented or not.
    fast_literal_words = (
        mode == "literal" and spellings is not None
        and (positional or len(literal_words) == 1)
        and re.fullmatch(r"[^\W_]+(?:\s+[^\W_]+)*", text, re.UNICODE) is not None
        and not LITERAL_SEARCH_RE.search(text)
    )
    fast_all_words = (
        mode == "all_words"
        and re.fullmatch(r"[^\W_]+(?:\s+[^\W_]+)*", text, re.UNICODE) is not None
    )
    fast_ranked = mode == "ranked" and tree is not None and fts_fully_verifies(tree, positional)
    if (
        (STRUCTURED_TOKEN_RE.search(text) and _has_diacritic(text))
        or REGION_BOUNDARY_TOKEN in fts_terms
        # A phrase with too many spellings to spell out has only superset candidates.
        or (spellings is not None and spellings.overflow)
    ):
        fast_single_term = fast_literal_ascii = fast_literal_symbol = fast_literal_pair = False
        fast_literal_accent_pair = fast_literal_words = fast_structured_literal = False
        fast_all_words = fast_ranked = False
    if fast_single_term or fast_literal_ascii or fast_literal_symbol or fast_literal_pair or fast_literal_accent_pair or fast_literal_words or fast_structured_literal or fast_all_words or fast_ranked:
        candidate_selection_seconds = time.monotonic() - candidate_selection_started
        _note_progress(
            "candidate_integrity", candidate_selection_seconds=round(candidate_selection_seconds, 6),
        )
        candidate_integrity_started = time.monotonic()
        verified_blocks, verified_bytes = verify_candidate_projection_integrity(
            db, projection_handle, clauses, values,
        )
        candidate_integrity_seconds = time.monotonic() - candidate_integrity_started
        _note_progress(
            "candidate_selection", candidate_integrity_seconds=round(candidate_integrity_seconds, 6),
        )
        candidate_selection_started = time.monotonic()
        check_query_deadline(args)
        rows = db.execute(
            "SELECT e.rowid AS event_rowid,e.*,src.path AS source_path,"
            "r.projection_bytes,r.projection_hash "
            "FROM events e JOIN sources src USING(source_id) "
            "JOIN search_projection_refs r ON r.event_rowid=e.rowid WHERE "
            + (" AND ".join(clauses) if clauses else "1=1"), tuple(values),
        ).fetchall()
        entries = [{
            "result_id": retrieval_result_id(row["event_id"], row["source_id"], row["line_number"]),
            "provider": row["provider"], "session": row["session_id"],
            "timestamp": row["timestamp"], "order_timestamp": row["order_timestamp"],
            "order_kind": row["order_kind"], "category": row["category"],
            "role": row["role"], "tool": row["tool"], "project": row["project"],
            "supported": bool(row["supported"]),
            "source": {"path": row["source_path"], "line": row["line_number"], "hash": row["raw_hash"]},
            "passage": None, "rank": {"phrase_hits": 0, "term_span": 0,
                                        "document_bytes": row["projection_bytes"],
                                        "fts": fts_rank.get(row["event_rowid"], 0.0),
                                        "closeness_tier": CLOSENESS_UNRANKED},
            "match_origin": (fts_origins.get(row["event_rowid"]) or ["unclassified"])[0],
            "match_origins": fts_origins.get(row["event_rowid"]) or ["unclassified"],
            "_event_rowid": row["event_rowid"], "_projection_hash": row["projection_hash"],
        } for row in rows]
        if order == "chronological":
            entries.sort(key=lambda entry: (
                entry["order_timestamp"] or "", entry["provider"], entry["session"],
                entry["source"]["line"], entry["result_id"],
            ))
        else:
            if positional and mode == "ranked":
                tiers = closeness_tiers(db, tree, {entry["match_origin"] for entry in entries})
                for entry in entries:
                    entry["rank"]["closeness_tier"] = tiers.get(
                        (entry["match_origin"], entry["_event_rowid"]), CLOSENESS_UNRANKED,
                    )
            # With word positions, closeness then BM25 decide; without them BM25 is always
            # zero and shorter documents win, as before.
            entries.sort(key=lambda entry: (
                PROJECTION_ORIGINS.index(entry["match_origin"]),
                entry["rank"]["closeness_tier"], entry["rank"]["fts"],
                entry["rank"]["document_bytes"],
                entry["order_timestamp"] or "", entry["provider"],
                entry["session"], entry["source"]["line"], entry["result_id"],
            ))
            entries = demote_search_echoes(db, demote_repeated_results(entries), text)
        projection_bytes = 0
        block_bytes = 0
        preview_block_cache: dict[int, bytes] = {}
        passage_limit = getattr(args, "passage_limit", INLINE_RESULT_LIMIT)

        def hydrate_fast(entry: dict[str, Any]) -> tuple[str, bool]:
            nonlocal projection_bytes, block_bytes
            regions, compressed_bytes = read_projection_for_event(
                db, projection_handle, entry["_event_rowid"], preview_block_cache,
            )
            projection = REGION_SEPARATOR.join(regions[entry["match_origin"]])
            projection_bytes += len(encoded_search_regions(regions))
            block_bytes += compressed_bytes
            return projection_passage_with_status(projection, text, mode)

        candidate_selection_seconds += time.monotonic() - candidate_selection_started
        _note_progress(
            "hydration", candidate_selection_seconds=round(candidate_selection_seconds, 6),
            candidate_integrity_seconds=round(candidate_integrity_seconds, 6),
        )
        hydration_started = time.monotonic()
        hydrated_passages = hydrate_distinct_passages(entries, passage_limit, hydrate_fast)
        hydration_seconds = time.monotonic() - hydration_started
        for entry in entries:
            entry.pop("_event_rowid")
            entry.pop("_projection_hash")
        return {
            "protocol_version": RETRIEVAL_PROTOCOL_VERSION, "complete": True,
            "backend": "database", "snapshot_id": status["snapshot_id"],
            "projection_version": PROJECTION_VERSION, "mode": mode, "order": order,
            "query": text,
            "total": len(entries), "entries": entries,
            "telemetry": {"event_reconstructions": 0, "source_reads": 0,
                          "candidate_projections": len(entries),
                          "projection_bytes": projection_bytes,
                          "projection_pack_bytes": block_bytes,
                          "verified_by": "equivalent_positional_fts_predicate",
                          "preview_evidence_verified": hydrated_passages,
                          "coverage_verification": coverage_verification,
                          "coverage_seconds": coverage_seconds,
                          "candidate_selection_seconds": candidate_selection_seconds,
                          "candidate_integrity_seconds": candidate_integrity_seconds,
                          "hydration_seconds": hydration_seconds,
                          "integrity_verified_blocks": verified_blocks,
                          "integrity_verified_bytes": verified_bytes},
        }
    candidate_selection_seconds = time.monotonic() - candidate_selection_started
    invalid_for_query = db.execute(
        "SELECT COUNT(*) FROM events e "
        "LEFT JOIN search_projection_refs r ON r.event_rowid=e.rowid "
        "LEFT JOIN search_projection_blocks b USING(block_key) WHERE "
        + (" AND ".join(clauses) if clauses else "1=1")
        + " AND (r.event_rowid IS NULL OR b.block_key IS NULL OR r.projection_offset<0 "
          "OR r.projection_bytes<0 OR r.projection_offset+r.projection_bytes>b.raw_bytes)",
        tuple(values),
    ).fetchone()[0]
    if invalid_for_query:
        return {
            "protocol_version": RETRIEVAL_PROTOCOL_VERSION, "complete": False,
            "backend": "database", "coverage": {
                **status, "complete": False, "reason": "projection_integrity_failed_for_query",
                "invalid_references": invalid_for_query,
            }, "total": None, "entries": [],
        }
    projection_matching_started = time.monotonic()
    _note_progress(
        "projection_matching", candidate_selection_seconds=round(candidate_selection_seconds, 6),
    )
    with forbid_source_transcript_reads() as source_reads:
        rows = []
        candidate_count = 0
        projection_bytes = 0
        block_bytes = 0
        active_block = None
        block_data = b""
        for row in db.execute(sql, tuple(values)):
            check_query_deadline(args)
            candidate_count += 1
            _note_progress(rows_processed=candidate_count)
            if row["block_key"] != active_block:
                projection_handle.seek(row["pack_offset"])
                compressed = projection_handle.read(row["compressed_bytes"])
                if len(compressed) != row["compressed_bytes"]:
                    raise ValueError(f"projection block extent missing: {row['block_key']}")
                if digest_bytes(compressed) != row["compressed_hash"]:
                    raise ValueError(f"compressed projection block integrity failed: {row['block_key']}")
                block_data = zstd.decompress(
                    compressed, max_output_size=row["block_raw_bytes"],
                )
                if (
                    len(block_data) != row["block_raw_bytes"]
                    or digest_bytes(block_data) != row["block_digest"]
                ):
                    raise ValueError(f"projection block integrity failed: {row['block_key']}")
                active_block = row["block_key"]
                block_bytes += len(compressed)
            start = row["projection_offset"]
            end = start + row["projection_bytes"]
            projection_data = block_data[start:end]
            if len(projection_data) != row["projection_bytes"] or digest_bytes(projection_data) != row["projection_hash"]:
                raise ValueError(f"search passage integrity failed for event {row['event_id']}")
            regions = decoded_search_regions(projection_data)
            projection_bytes += len(projection_data)
            region_matches = matching_projection_regions(
                regions, text, mode, tree,
                allow_accent_fallback=(
                    accent_phrase_candidates is None
                    or row["event_rowid"] in accent_phrase_candidates
                ),
            )
            if region_matches:
                best = min(
                    region_matches,
                    key=lambda item: (
                        PROJECTION_ORIGINS.index(item[0]),
                        max((span[1] for span in item[2]), default=len(item[1]))
                        - min((span[0] for span in item[2]), default=0),
                    ),
                )
                origin, projection, spans = best
                phrase_hits = sum(
                    1 for atom in atoms if atom.kind == "phrase" and _atom_spans(projection, atom)
                )
                cover = (
                    max(span[1] for span in spans) - min(span[0] for span in spans)
                    if spans else len(projection)
                )
                match_origins = [
                    candidate for candidate in PROJECTION_ORIGINS
                    if any(item[0] == candidate for item in region_matches)
                ]
                rows.append((
                    row, projection, phrase_hits, cover,
                    fts_rank.get(row["event_rowid"], 0.0), origin, match_origins,
                ))
        entries = [{
            "result_id": retrieval_result_id(
                row["event_id"], row["source_id"], row["line_number"],
            ), "provider": row["provider"],
            "session": row["session_id"], "timestamp": row["timestamp"],
            "order_timestamp": row["order_timestamp"], "order_kind": row["order_kind"],
            "category": row["category"], "role": row["role"], "tool": row["tool"],
            "project": row["project"], "supported": bool(row["supported"]),
            "source": {"path": row["source_path"], "line": row["line_number"], "hash": row["raw_hash"]},
            "passage": None, "_projection": projection,
            "_projection_hash": row["projection_hash"], "_event_rowid": row["event_rowid"],
            "rank": {"phrase_hits": phrase_hits, "term_span": cover,
                     "document_bytes": row["projection_bytes"], "fts": score},
            "match_origin": origin, "match_origins": match_origins,
        } for row, projection, phrase_hits, cover, score, origin, match_origins in rows]
        if order == "chronological":
            entries.sort(key=lambda entry: (
                entry["order_timestamp"] or "", entry["provider"], entry["session"],
                digest((entry["provider"] + "\0" + entry["source"]["path"]).encode()),
                entry["source"]["line"], entry["result_id"],
            ))
        else:
            entries.sort(key=lambda entry: (
                PROJECTION_ORIGINS.index(entry["match_origin"]),
                -entry["rank"]["phrase_hits"], entry["rank"]["term_span"],
                entry["rank"]["document_bytes"], entry["rank"]["fts"],
                entry["order_timestamp"] or "", entry["provider"], entry["session"],
                digest((entry["provider"] + "\0" + entry["source"]["path"]).encode()),
                entry["source"]["line"], entry["result_id"],
            ))
            entries = demote_search_echoes(db, demote_repeated_results(entries), text)
        # Build one prefix containing up to the display target of distinct passages;
        # bundle pages hydrate the rest on demand.
        passage_limit = getattr(args, "passage_limit", INLINE_RESULT_LIMIT)

        projections: dict[int, str] = {}
        for index, entry in enumerate(entries):
            projection = entry.pop("_projection")
            entry.pop("_projection_hash")
            entry.pop("_event_rowid")
            projections[id(entry)] = projection

        projection_matching_seconds = time.monotonic() - projection_matching_started
        _note_progress("hydration", projection_matching_seconds=round(projection_matching_seconds, 6))
        hydration_started = time.monotonic()
        hydrate_distinct_passages(
            entries, passage_limit,
            lambda entry: projection_passage_with_status(projections[id(entry)], text, mode),
        )
        hydration_seconds = time.monotonic() - hydration_started
    return {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
        "complete": True, "backend": "database", "snapshot_id": status["snapshot_id"],
        "projection_version": PROJECTION_VERSION, "mode": mode, "order": order,
        "query": text,
        "total": len(entries), "entries": entries,
        "telemetry": {
            "event_reconstructions": 0, "source_reads": source_reads["attempts"],
            "candidate_projections": candidate_count, "projection_bytes": projection_bytes,
            "projection_pack_bytes": block_bytes,
            "coverage_verification": coverage_verification,
            "coverage_seconds": coverage_seconds,
            "candidate_selection_seconds": candidate_selection_seconds,
            "projection_matching_seconds": projection_matching_seconds,
            "hydration_seconds": hydration_seconds,
        },
    }


class QueryDeadlineExceeded(RuntimeError):
    pass


_QUERY_DEADLINE: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "sessiondb_query_deadline", default=None,
)


_QUERY_PROGRESS: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "sessiondb_query_progress", default=None,
)


def _note_progress(phase: str | None = None, **values: Any) -> None:
    """Keep finished phase timings so a deadline result can still say where the time went."""
    progress = _QUERY_PROGRESS.get()
    if progress is None:
        return
    progress.update(values)
    if phase is not None:
        progress["phase"] = phase
        progress["phase_started"] = time.monotonic()


def check_query_deadline(args: argparse.Namespace | None = None) -> None:
    deadline = (
        getattr(args, "_deadline_monotonic", None)
        if args is not None else _QUERY_DEADLINE.get()
    )
    if deadline is not None and time.monotonic() >= deadline:
        raise QueryDeadlineExceeded("search deadline exceeded")


def _deadline_incomplete_result(
    deadline_seconds: float, progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    telemetry: dict[str, Any] = {"deadline_seconds": deadline_seconds}
    if progress:
        for key in (
            "coverage_seconds", "candidate_selection_seconds", "candidate_integrity_seconds",
            "projection_matching_seconds", "fts_candidates", "rows_processed",
        ):
            if progress.get(key) is not None:
                telemetry[key] = progress[key]
        phase = progress.get("phase")
        if phase and progress.get("phase_started") is not None:
            telemetry["interrupted_phase"] = phase
            telemetry["phases_seconds"] = {
                phase: round(max(0.0, time.monotonic() - progress["phase_started"]), 6),
            }
    return {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
        "complete": False, "backend": "database",
        "coverage": {"complete": False, "reason": "query_deadline_exceeded"},
        "total": None, "entries": [],
        "telemetry": telemetry,
    }


def search_index(
    db: sqlite3.Connection, args: argparse.Namespace, *, keep_snapshot: bool = False,
) -> dict[str, Any]:
    deadline_seconds = float(getattr(args, "deadline_seconds", SEARCH_DEADLINE_SECONDS))
    if not math.isfinite(deadline_seconds) or deadline_seconds <= 0:
        raise ValueError("search deadline must be finite and greater than zero")
    args = argparse.Namespace(**vars(args))
    args._deadline_monotonic = time.monotonic() + deadline_seconds
    token = _QUERY_DEADLINE.set(args._deadline_monotonic)
    progress: dict[str, Any] = {}
    progress_token = _QUERY_PROGRESS.set(progress)
    db.set_progress_handler(
        lambda: 1 if time.monotonic() >= args._deadline_monotonic else 0,
        10_000,
    )
    try:
        return _search_index_with_deadline(db, args, deadline_seconds, keep_snapshot)
    except QueryDeadlineExceeded:
        return _deadline_incomplete_result(deadline_seconds, progress)
    except sqlite3.OperationalError as exc:
        if "interrupted" in str(exc).casefold() and time.monotonic() >= args._deadline_monotonic:
            return _deadline_incomplete_result(deadline_seconds, progress)
        raise
    finally:
        db.set_progress_handler(None, 0)
        _QUERY_PROGRESS.reset(progress_token)
        _QUERY_DEADLINE.reset(token)


def _search_index_with_deadline(
    db: sqlite3.Connection, args: argparse.Namespace, deadline_seconds: float,
    keep_snapshot: bool,
) -> dict[str, Any]:
    try:
        lease = acquire_projection_pack_lease(
            db, exclusive=False, timeout=min(READER_LOCK_WAIT_SECONDS, deadline_seconds),
        )
    except FileNotFoundError:
        status = {
            **projection_status(db), "complete": False,
            "reason": "projection_pack_lock_missing",
        }
        return {
            "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
            "complete": False, "backend": "database", "coverage": status,
            "total": None, "entries": [],
        }
    if not connection_path_is_current(db):
        release_projection_pack_lease(lease)
        return {
            "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
            "complete": False, "backend": "database",
            "coverage": {"complete": False, "reason": "database_inode_replaced"},
            "total": None, "entries": [],
        }
    owns_snapshot = not db.in_transaction
    coverage_started = time.monotonic()
    receipt_status = _projection_status_from_receipt(db) if owns_snapshot else None
    if owns_snapshot:
        db.execute("BEGIN")
        if receipt_status is not None:
            # Pin the read snapshot, then repeat the live-file validation. Matching
            # receipt and generation bindings prove no writer crossed this boundary.
            db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
            confirmed = _projection_status_from_receipt(db)
            if (
                confirmed is None
                or confirmed["_receipt_sha256"] != receipt_status["_receipt_sha256"]
                or confirmed["_sqlite_generation"] != receipt_status["_sqlite_generation"]
            ):
                receipt_status = None
            else:
                receipt_status = confirmed
    if receipt_status is not None:
        receipt_status = {
            key: value for key, value in receipt_status.items() if not key.startswith("_")
        }
    try:
        path = projection_pack_path(db)
        try:
            with path.open("rb") as projection_handle:
                result = _search_index(
                    db, args, projection_handle, receipt_status, coverage_started,
                )
                check_query_deadline(args)
                result.setdefault("telemetry", {})["deadline_seconds"] = deadline_seconds
                return result
        except FileNotFoundError:
            status = projection_status(db)
            return {
                "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
                "complete": False, "backend": "database", "coverage": status,
                "total": None, "entries": [],
            }
    finally:
        if owns_snapshot and not keep_snapshot:
            db.rollback()
        release_projection_pack_lease(lease)


def write_result_bundle(result: dict[str, Any], path: Path) -> dict[str, Any]:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    header = {key: value for key, value in result.items() if key != "entries"}
    header["format"] = "sessiondb-result-bundle-v1"
    header["entry_count"] = len(result["entries"])
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        payload_hash = hashlib.sha256()
        file_hash = hashlib.sha256()
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            header_line = json.dumps({"header": header}, ensure_ascii=False, separators=(",", ":")) + "\n"
            handle.write(header_line)
            payload_hash.update(header_line.encode())
            file_hash.update(header_line.encode())
            for entry in result["entries"]:
                line = json.dumps({"entry": entry}, ensure_ascii=False, separators=(",", ":")) + "\n"
                handle.write(line)
                payload_hash.update(line.encode())
                file_hash.update(line.encode())
            footer_line = json.dumps({
                "footer": {"entry_count": len(result["entries"]), "payload_sha256": payload_hash.hexdigest()},
            }, separators=(",", ":")) + "\n"
            handle.write(footer_line)
            file_hash.update(footer_line.encode())
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        data_hash = file_hash.hexdigest()
    finally:
        try:
            Path(temporary).unlink()
        except FileNotFoundError:
            pass
    return {
        **header, "bundle": {"path": str(path), "sha256": data_hash, "entries": result["total"]},
    }


def result_bundle_directories(db_path: Path) -> list[Path]:
    """The private results directory, then a per-user temp fallback for sandboxed agents."""
    database = db_path.resolve()
    fallback = (
        Path(tempfile.gettempdir()) / f"sessiondb-{os.getuid()}"
        / digest(str(database).encode())[:16] / "retrieval-results"
    )
    return [database.parent / "retrieval-results", fallback]


def _private_directory(directory: Path, *, parents_from: Path | None = None) -> None:
    """Create or accept only real, user-owned directories; tighten mode when allowed."""
    chain = [directory]
    if parents_from is not None:
        chain = [
            parent for parent in reversed(directory.parents)
            if parents_from in parent.parents
        ] + [directory]
    for level in chain:
        try:
            level.mkdir(mode=0o700)
        except FileExistsError:
            pass
        current = level.lstat()
        if not stat.S_ISDIR(current.st_mode) or current.st_uid != os.getuid():
            raise PermissionError(f"result bundle directory is not a private directory: {level}")
        if stat.S_IMODE(current.st_mode) & 0o077:
            # Sandboxed agents may be unable to chmod; only an already-private directory skips it.
            os.chmod(level, 0o700)


def automatic_result_bundle_path(
    db_path: Path, result: dict[str, Any], prefix: str = "search",
    directory: Path | None = None,
) -> Path:
    primary, fallback = result_bundle_directories(db_path)
    directory = directory or primary
    if directory == primary:
        directory.parent.mkdir(parents=True, exist_ok=True)
        _private_directory(directory)
    else:
        _private_directory(directory, parents_from=Path(tempfile.gettempdir()))
    for existing in result_bundle_directories(db_path):
        # Prune every existing private directory, so fallback bundles expire even after
        # later runs write to the primary one; cleanup must not block delivery.
        try:
            current = existing.lstat()
            if stat.S_ISDIR(current.st_mode) and current.st_uid == os.getuid():
                prune_result_bundles(db_path, directory=existing)
        except OSError:
            pass
    identity = digest(canonical({
        "snapshot_id": result.get("snapshot_id"), "mode": result.get("mode"),
        "query": result.get("query"), "nonce": os.urandom(16).hex(),
    }))[:24]
    return directory / f"{prefix}-{identity}.jsonl"


def write_automatic_bundle(
    db_path: Path, result: dict[str, Any], prefix: str, writer: Callable[[Path], Any],
) -> Any:
    """Write where the caller is allowed: the private directory, else the temp fallback."""
    failure: OSError | None = None
    for directory in result_bundle_directories(db_path):
        try:
            return writer(automatic_result_bundle_path(db_path, result, prefix, directory))
        except OSError as exc:
            failure = exc
    assert failure is not None
    raise failure


def prune_result_bundles(
    db_path: Path, *, retention_seconds: float = RESULT_BUNDLE_RETENTION_SECONDS,
    now: float | None = None, directory: Path | None = None,
) -> dict[str, int]:
    """Remove expired automatic bundles without touching active or foreign files."""
    if retention_seconds < 0:
        raise ValueError("bundle retention must not be negative")
    directory = directory or db_path.resolve().parent / "retrieval-results"
    result = {"examined": 0, "deleted": 0, "deleted_bytes": 0, "skipped_active": 0}
    if not directory.is_dir():
        return result
    cutoff = (time.time() if now is None else now) - retention_seconds
    for path in sorted([*directory.glob("search-*.jsonl"), *directory.glob("query-*.jsonl")]):
        result["examined"] += 1
        try:
            fd = os.open(
                path,
                os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0),
            )
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                file_stat = os.fstat(handle.fileno())
                if not stat.S_ISREG(file_stat.st_mode):
                    continue
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                if file_stat.st_mtime > cutoff:
                    continue
                first = handle.readline(RESULT_BUNDLE_HEADER_MAX_BYTES + 1)
                if len(first.encode("utf-8")) > RESULT_BUNDLE_HEADER_MAX_BYTES:
                    continue
                envelope = json.loads(first)
                header = envelope.get("header") if isinstance(envelope, dict) else None
                lifecycle = header.get("bundle_lifecycle") if isinstance(header, dict) else None
                if (
                    not isinstance(lifecycle, dict)
                    or header.get("format") not in {
                        "sessiondb-result-bundle-v1", "sessiondb-query-bundle-v1",
                    }
                    or lifecycle.get("automatic") is not True
                ):
                    continue
                current = path.lstat()
                if (
                    not stat.S_ISREG(current.st_mode)
                    or current.st_ino != file_stat.st_ino
                    or current.st_dev != file_stat.st_dev
                ):
                    continue
                path.unlink()
                result["deleted"] += 1
                result["deleted_bytes"] += file_stat.st_size
        except BlockingIOError:
            result["skipped_active"] += 1
        except (OSError, UnicodeError, json.JSONDecodeError, AttributeError, TypeError, ValueError):
            continue
    return result


def _encoded_size(value: Any) -> int:
    """Bytes json_print would emit for value."""
    return len(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")) + 1


def write_query_bundle(db_path: Path, result: dict[str, Any]) -> dict[str, Any]:
    """Keep an oversized clean-query answer exactly, as one private automatic bundle."""
    return write_automatic_bundle(
        db_path, {"query": id(result)}, "query", lambda path: _write_query_bundle_at(path, result),
    )


def _write_query_bundle_at(path: Path, result: dict[str, Any]) -> dict[str, Any]:
    header = {
        "format": "sessiondb-query-bundle-v1",
        "bundle_lifecycle": {
            "automatic": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "retention_seconds": RESULT_BUNDLE_RETENTION_SECONDS,
        },
    }
    payload = json.dumps({"payload": result}, ensure_ascii=False, separators=(",", ":")) + "\n"
    content = json.dumps({"header": header}, separators=(",", ":")) + "\n" + payload
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        try:
            Path(temporary).unlink()
        except FileNotFoundError:
            pass
    encoded = content.encode("utf-8")
    return {"path": str(path), "sha256": digest(encoded), "bytes": len(encoded)}


def deliver_clean_query(db_path: Path, result: dict[str, Any]) -> dict[str, Any]:
    """Return a clean query inline, or a bounded lossy preview plus its exact bundle."""
    if _encoded_size(result) <= QUERY_INLINE_MAX_BYTES:
        return result
    try:
        bundle = write_query_bundle(db_path, result)
    except OSError as exc:
        return {
            "complete": False, "reason": f"delivery_error: {exc}",
            "coverage": _cap_coverage(result.get("coverage"), 20), "page": result.get("page"),
            "events_total": len(result.get("events", [])),
        }
    objects = result.get("objects", [])
    object_previews = []
    for value in objects:
        text = flatten_text(value)
        preview = {"text": text[:QUERY_PREVIEW_OBJECT_CHARS], "chars": len(text)}
        if len(text) > QUERY_PREVIEW_OBJECT_CHARS:
            preview["truncated"] = True
        object_previews.append(preview)
    events = list(result.get("events", []))
    preview = {
        **{key: result[key] for key in ("sources", "page", "coverage") if key in result},
        "events": events, "objects": object_previews,
        "preview": {
            "lossy": True, "object_chars": QUERY_PREVIEW_OBJECT_CHARS,
            "events_shown": len(events), "events_total": len(events),
            "note": "objects are text previews; read the bundle payload for exact values",
        },
        "bundle": bundle,
    }
    while _encoded_size(preview) > QUERY_INLINE_MAX_BYTES and preview["events"]:
        preview["events"] = preview["events"][:-1]
        referenced = {ref for event in preview["events"] for ref in event.get("refs", [])}
        preview["objects"] = [
            item if index in referenced else None for index, item in enumerate(object_previews)
        ]
        preview["preview"]["events_shown"] = len(preview["events"])
    if _encoded_size(preview) > QUERY_INLINE_MAX_BYTES:
        preview["objects"] = []
        preview["sources"] = []
        preview["preview"]["events_shown"] = 0
    for keep in (20, 5, 0):
        if _encoded_size(preview) <= QUERY_INLINE_MAX_BYTES:
            break
        preview["coverage"] = _cap_coverage(result.get("coverage"), keep)
    return preview


def bundle_if_oversized(
    db_path: Path, result: dict[str, Any], requested_path: Path | None,
) -> dict[str, Any]:
    delivery_started = time.monotonic()
    result = ensure_retrieval_origin(dict(result))
    snapshot_only = not result.get("complete") and result.get("indexed_total") is not None
    if "entries" not in result or not (result.get("complete") or snapshot_only):
        return attach_command_timing(
            result, "search-index", delivery_started,
            {"delivery_preparation": _seconds_since(delivery_started)}, extend=True,
        )
    sessions: dict[tuple[Any, Any], str] = {}
    sources: dict[tuple[Any, Any], str] = {}
    session_table: dict[str, Any] = {}
    source_table: dict[str, Any] = {}
    bundled_entries = []
    visible: list[dict[str, Any]] = []
    visible_keys: set[tuple[Any, ...]] = set()
    for number, original in enumerate(result["entries"], 1):
        entry = dict(original)
        session_key = (entry.get("provider"), entry.get("session"))
        session_alias = sessions.setdefault(session_key, f"s{len(sessions) + 1}")
        source = entry.get("source") or {}
        source_key = (source.get("path"), source.get("hash"))
        source_alias = sources.setdefault(source_key, f"src{len(sources) + 1}")
        alias = f"r{number}"
        entry["alias"] = alias
        bundled_entries.append(entry)
        display_key = passage_group_key(entry)
        if (
            entry.get("passage") is not None
            and len(visible) == number - 1
            and len(visible_keys) < INLINE_RESULT_LIMIT
            and len(visible) < INLINE_PASSAGE_SCAN_LIMIT
        ):
            session_table.setdefault(session_alias, {
                "provider": entry.get("provider"), "session": entry.get("session"),
                "project": entry.get("project"),
            })
            source_table.setdefault(source_alias, {"path": source.get("path")})
            visible.append({
                "id": alias, "session": session_alias, "source": source_alias,
                "line": source.get("line"), "timestamp": entry.get("timestamp"),
                "category": entry.get("category"), "role": entry.get("role"),
                "tool": entry.get("tool"), "passage": entry.get("passage"),
                "match_origin": entry.get("match_origin"),
                **({"passage_truncated": True} if entry.get("passage_truncated") else {}),
                # Parsed from an unindexed tail: `details` resolves it once indexed.
                **({"live_tail": True} if entry.get("live_tail") else {}),
            })
            visible_keys.add(display_key)
    title_started = time.monotonic()
    full_sessions = visible_session_titles(
        db_path, {alias: dict(value) for alias, value in session_table.items()},
    )
    title_seconds = _seconds_since(title_started)
    automatic = requested_path is None
    bundled = {
        **result,
        "bundle_lifecycle": {
            "automatic": automatic,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "retention_seconds": RESULT_BUNDLE_RETENTION_SECONDS if automatic else None,
        },
        "sessions": full_sessions,
        "entries": bundled_entries,
    }
    bundle_info = None
    bundle_started = time.monotonic()
    try:
        if requested_path is not None:
            bundle_info = write_result_bundle(bundled, requested_path)["bundle"]
        else:
            bundle_info = write_automatic_bundle(
                db_path, result, "search", lambda path: write_result_bundle(bundled, path),
            )["bundle"]
    except OSError as exc:
        delivery_error = str(exc)
    else:
        delivery_error = None
    bundle_seconds = _seconds_since(bundle_started)

    def compact_sessions(aliases: set[str]) -> dict[str, Any]:
        compact = {}
        for alias, value in full_sessions.items():
            if alias not in aliases:
                continue
            title = value.get("title") or {"status": "unknown", "reason": "no_explicit_title"}
            compact_title = {"status": title.get("status")}
            if title.get("status") == "known":
                compact_title["value"] = title.get("value")
            else:
                compact_title["reason"] = title.get("reason")
            compact[alias] = {
                "provider": value.get("provider"), "session": value.get("session"),
                "project": value.get("project"), "title": compact_title,
            }
        return compact

    def occurrence(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": item["id"], "session": item["session"],
            "source": item["source"], "line": item["line"],
            **({"live_tail": True} if item.get("live_tail") else {}),
        }

    def build_inline(shown: int, coverage_keep: int) -> dict[str, Any]:
        previews: list[dict[str, Any]] = []
        groups: dict[tuple[Any, ...], dict[str, Any]] = {}
        selected_sessions: set[str] = set()
        selected_sources: set[str] = set()
        for source_item in visible[:shown]:
            item = dict(source_item)
            selected_sessions.add(item["session"])
            selected_sources.add(item["source"])
            key = passage_group_key(item)
            group = groups.get(key)
            if group is None:
                groups[key] = item
                previews.append(item)
                continue
            if "occurrences" not in group:
                group["occurrences"] = [occurrence(group)]
            group["occurrences"].append(occurrence(item))
            group["occurrence_count"] = len(group["occurrences"])
        has_more = shown < len(bundled_entries)
        delivered = {
            "protocol_version": result.get("protocol_version"),
            "complete": bool(result.get("complete")), "backend": result.get("backend"),
            "snapshot_id": result.get("snapshot_id"),
            "projection_version": result.get("projection_version"),
            "query": result.get("query"), "order": result.get("order"),
            "origin": result.get("origin"),
            "total": result.get("total"),
            "sessions": compact_sessions(selected_sessions),
            "sources": {key: value for key, value in source_table.items() if key in selected_sources},
            "entries": previews, "shown_through": shown, "has_more": has_more,
            "telemetry": result.get("telemetry"),
            "display": {
                "distinct_passages": len(groups), "stop_reason": "distinct_target",
            },
        }
        coverage = result.get("coverage")
        if isinstance(coverage, dict) and (
            coverage.get("reason") == "live_tails_served" or snapshot_only
        ):
            delivered["coverage"] = _cap_coverage(coverage, coverage_keep)
        if snapshot_only:
            delivered.update({
                "indexed_total": result.get("indexed_total"), "reason": result.get("reason"),
            })
        if bundle_info is not None:
            delivered["bundle"] = bundle_info
            details = {
                "command": "details", "snapshot_id": delivered["snapshot_id"],
                "bundle": bundle_info["path"], "db": str(db_path),
                "readable": True, "readable_query": result.get("query"),
            }
            origin = delivered.get("origin") or {}
            if origin.get("case_id") is not None:
                details["case_id"] = origin.get("case_id")
            if origin.get("binding_status") == "bound" and origin.get("continuation_id"):
                details["continuation_id"] = origin.get("continuation_id")
            delivered["follow_up"] = {"details": details}
            if has_more:
                delivered["follow_up"]["bundle_page"] = {
                    "command": "bundle-page", "bundle": bundle_info["path"],
                    "db": str(db_path), "offset": shown,
                    "snapshot_id": delivered["snapshot_id"],
                }
                if origin.get("case_id") is not None:
                    delivered["follow_up"]["bundle_page"]["case_id"] = origin.get("case_id")
                if origin.get("binding_status") == "bound" and origin.get("continuation_id"):
                    delivered["follow_up"]["bundle_page"]["continuation_id"] = origin.get("continuation_id")
        if delivery_error is not None:
            delivered.update({"complete": False, "delivery_error": delivery_error})
        return delivered

    candidates: list[tuple[int, int, dict[str, Any]]] = []
    for coverage_keep in (20, 5, 0):
        for shown in range(len(visible) + 1):
            candidate = build_inline(shown, coverage_keep)
            if _encoded_size(candidate) > QUERY_INLINE_MAX_BYTES - 2048:
                break
            candidates.append((shown, coverage_keep, candidate))
    if not candidates:
        delivered = build_inline(0, 0)
    else:
        _shown, _coverage_keep, delivered = max(candidates, key=lambda item: (item[0], item[1]))
    shown = delivered["shown_through"]
    distinct = len({passage_group_key(item) for item in visible[:shown]})
    if shown < len(visible):
        stop_reason = "byte_budget"
    elif shown == len(bundled_entries):
        stop_reason = "complete"
    elif distinct >= INLINE_RESULT_LIMIT:
        stop_reason = "distinct_target"
    elif len(visible) >= INLINE_PASSAGE_SCAN_LIMIT:
        stop_reason = "scan_limit"
    else:
        stop_reason = "unhydrated"
    delivered["display"].update({"distinct_passages": distinct, "stop_reason": stop_reason})
    elapsed = _seconds_since(delivery_started)
    delivered = attach_command_timing(
        delivered, "search-index", delivery_started,
        {
            "session_titles": title_seconds,
            "bundle_write": bundle_seconds,
            "delivery_preparation": max(0.0, elapsed - title_seconds - bundle_seconds),
        },
        extend=True,
    )
    if _encoded_size(delivered) > QUERY_INLINE_MAX_BYTES:
        raise ValueError("inline search delivery exceeds byte budget")
    return delivered


def visible_session_titles(db_path: Path, sessions: dict[str, Any]) -> dict[str, Any]:
    """Read explicit retained title records for displayed sessions only."""
    if not sessions:
        return sessions
    record_limit = 128
    byte_limit = 65536
    deadline = time.monotonic() + 0.25
    try:
        db = connect(db_path, read_only=True)
    except (OSError, sqlite3.Error):
        return {alias: {**value, "title": {"status": "unknown", "value": None,
                                            "reason": "title_store_unreadable"}}
                for alias, value in sessions.items()}
    try:
        operations = 0

        def title_progress() -> int:
            nonlocal operations
            operations += 1000
            return int(operations >= 100000 or time.monotonic() >= deadline)

        db.set_progress_handler(title_progress, 1000)
        for value in sessions.values():
            value["title"] = {"status": "unknown", "value": None,
                              "reason": "no_explicit_title"}
            provider, session = value.get("provider"), value.get("session")
            if not provider or not session:
                value["title"]["reason"] = "session_identity_missing"
                continue
            if time.monotonic() >= deadline:
                value["title"]["reason"] = "title_time_budget_exceeded"
                continue
            try:
                rows = db.execute(
                    "SELECT e.*,src.path AS source_path FROM sessions s "
                    "JOIN events e ON e.source_id=s.source_id AND e.session_id=s.session_id "
                    "JOIN sources src ON src.source_id=e.source_id "
                    "WHERE s.provider=? AND s.session_id=? AND e.category IN "
                    "('ai-title','custom-title','session_meta') "
                    "LIMIT ?", (provider, session, record_limit + 1),
                ).fetchall()
            except sqlite3.Error as exc:
                value["title"]["reason"] = (
                    "title_query_budget_exceeded" if "interrupted" in str(exc).lower()
                    else "title_store_unreadable"
                )
                continue
            inspected_bytes = 0
            if len(rows) > record_limit:
                value["title"]["reason"] = "title_record_budget_exceeded"
                continue
            rows.sort(key=lambda row: (
                row["order_timestamp"] or "", row["source_id"],
                row["line_number"], row["event_id"],
            ), reverse=True)
            try:
                for row in rows:
                    if time.monotonic() >= deadline:
                        value["title"]["reason"] = "title_time_budget_exceeded"
                        break
                    if provider == "claude" and row["category"] not in {"ai-title", "custom-title"}:
                        continue
                    if provider == "codex" and row["category"] != "session_meta":
                        continue
                    inspected_bytes += row["structure_bytes"] or 0
                    if inspected_bytes > byte_limit:
                        value["title"]["reason"] = "title_byte_budget_exceeded"
                        break
                    structure = read_event_structure(db, row)
                    if provider == "claude":
                        field = "customTitle" if row["category"] == "custom-title" else "aiTitle"
                        title = structure.get(field) or structure.get("title")
                    else:
                        payload = structure.get("payload")
                        title = payload.get("title") if isinstance(payload, dict) else None
                    if time.monotonic() >= deadline:
                        value["title"]["reason"] = "title_time_budget_exceeded"
                        break
                    if isinstance(title, str) and title.strip():
                        value["title"] = {
                            "status": "known", "value": redact(title.strip()),
                            "source": {"path": row["source_path"], "line": row["line_number"],
                                       "hash": row["raw_hash"], "event_id": row["event_id"]},
                        }
                        break
            except sqlite3.Error as exc:
                value["title"] = {"status": "unknown", "value": None,
                                  "reason": ("title_query_budget_exceeded"
                                             if "interrupted" in str(exc).lower()
                                             else "title_record_unreadable")}
            except (OSError, ValueError, KeyError, RuntimeError, zstd.ZstdError):
                value["title"] = {"status": "unknown", "value": None,
                                  "reason": "title_record_unreadable"}
    finally:
        db.set_progress_handler(None, 0)
        db.close()
    return sessions


def resolve_result_aliases(path: Path, requested: Iterable[str]) -> list[str]:
    wanted = set(requested)
    resolved: dict[str, str] = {}
    _header, entries = read_result_bundle(path)
    for entry in entries:
        alias = entry.get("alias")
        if alias in wanted:
            resolved[alias] = entry["result_id"]
    return [resolved.get(value, value) for value in requested]


def resolve_bundle_continuation(
    path: Path, args: argparse.Namespace,
) -> tuple[dict[str, Any], list[str], str | None]:
    header, iterator = read_result_bundle(path)
    entries = list(iterator)
    context_error = continuation_context_error(header, args)
    if context_error:
        return header, [], context_error
    aliases = {
        entry.get("alias"): entry.get("result_id") for entry in entries if entry.get("alias")
    }
    result_ids = {entry.get("result_id") for entry in entries}
    resolved = []
    for requested in args.result_ids:
        value = aliases.get(requested, requested)
        if (
            (header.get("origin") or {}).get("binding_status") == "bound"
            and value not in result_ids
        ):
            return header, [], "continuation_result_not_in_bundle"
        resolved.append(value)
    return header, resolved, None


def detail_request_fingerprint(args: argparse.Namespace, origin: dict[str, Any] | None) -> str:
    return digest(canonical({
        "snapshot_id": args.snapshot_id,
        "result_ids": list(dict.fromkeys(args.result_ids)),
        "before": max(0, args.before), "after": max(0, args.after),
        "readable": bool(getattr(args, "readable", False)),
        "readable_query": getattr(args, "readable_query", None),
        "readable_offset": getattr(args, "readable_offset", None),
        "case_id": (origin or {}).get("case_id"),
        "continuation_id": (origin or {}).get("continuation_id"),
    }))


def finish_detail_continuation(
    result: dict[str, Any], args: argparse.Namespace, origin: dict[str, Any] | None,
) -> dict[str, Any]:
    request_fingerprint = detail_request_fingerprint(args, origin)
    evidence = {
        key: result.get(key)
        for key in (
            "complete", "backend", "snapshot_id", "requested_snapshot_id",
            "snapshot_advanced", "results", "contexts", "events", "recovery", "reason",
        )
        if key in result
    }
    detail_fingerprint = digest(canonical(evidence))
    next_offsets = {
        event["event_id"]: event.get("readable_next_offset")
        for event in result.get("events", [])
        if event.get("readable_next_offset") is not None
    }
    previous = getattr(args, "previous_detail_fingerprint", None)
    result["origin"] = origin
    result["continuation"] = {
        "request_fingerprint": request_fingerprint,
        "detail_fingerprint": detail_fingerprint,
        "unchanged_from_previous": previous == detail_fingerprint if previous else False,
        "retry_status": (
            "unchanged" if previous == detail_fingerprint
            else "changed" if previous else "first_request"
        ),
        "next_readable_offsets": next_offsets,
    }
    return result


def read_result_bundle(path: Path) -> tuple[dict[str, Any], Iterator[dict[str, Any]]]:
    handle = path.open(encoding="utf-8")
    fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
    first = handle.readline()
    try:
        header = json.loads(first)["header"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        handle.close()
        raise ValueError(f"invalid result bundle header: {exc}") from exc
    if header.get("format") != "sessiondb-result-bundle-v1":
        handle.close()
        raise ValueError("unsupported result bundle format")

    def entries() -> Iterator[dict[str, Any]]:
        payload_hash = hashlib.sha256(first.encode())
        count = 0
        try:
            for line in handle:
                value = json.loads(line)
                if set(value) == {"footer"}:
                    footer = value["footer"]
                    if (
                        not isinstance(footer, dict)
                        or footer.get("entry_count") != count
                        or header.get("entry_count") != count
                        or footer.get("payload_sha256") != payload_hash.hexdigest()
                        or handle.readline()
                    ):
                        raise ValueError("result bundle integrity check failed")
                    return
                if set(value) != {"entry"} or not isinstance(value["entry"], dict):
                    raise ValueError("invalid result bundle entry")
                count += 1
                payload_hash.update(line.encode())
                yield value["entry"]
            raise ValueError("result bundle footer is missing")
        finally:
            handle.close()

    return header, entries()


def hydrate_bundle_passages(
    db_path: Path, header: dict[str, Any], entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    query = header.get("query")
    mode = header.get("mode", "ranked")
    if not query or mode not in {"ranked", "literal", "all_words"}:
        return entries, [{"reason": "bundle_query_contract_missing"}], False
    query = unicodedata.normalize("NFC", query)
    tree = parse_search_query(query) if mode == "ranked" else None
    db = connect(db_path, read_only=True)
    issues = []
    try:
        current_snapshot = projection_snapshot_id(db)
        with forbid_source_transcript_reads():
            for entry in entries:
                if entry.get("passage") is not None:
                    continue
                event_id, source_id, line_number = parse_retrieval_result_id(entry["result_id"])
                row = db.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,),
                ).fetchone()
                if not row or (
                    source_id is not None
                    and (row["source_id"] != source_id or row["line_number"] != line_number)
                ):
                    issues.append({"result_id": entry["result_id"], "reason": "event_missing"})
                    continue
                if row["category"] == "unreadable":
                    issues.append({"result_id": entry["result_id"], "reason": "event_unreadable"})
                    continue
                regions = normalized_search_regions(read_event_structure(db, row))
                matches = matching_projection_regions(regions, query, mode, tree)
                preferred = [item for item in matches if item[0] == entry.get("match_origin")]
                selected = (preferred or matches)
                if not selected:
                    issues.append({"result_id": entry["result_id"], "reason": "predicate_mismatch"})
                    continue
                passage, truncated = projection_passage_with_status(selected[0][1], query, mode)
                entry["passage"] = passage
                if truncated:
                    entry["passage_truncated"] = True
        return entries, issues, current_snapshot != header.get("snapshot_id")
    finally:
        db.close()


def result_bundle_page(
    path: Path, args: argparse.Namespace, db_path: Path | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    bundle_started = time.monotonic()
    header, entries = read_result_bundle(path)
    context_error = continuation_context_error(header, args)
    if context_error:
        list(entries)
        rejected = continuation_rejection(header, context_error, command="bundle-page")
        return attach_command_timing(
            rejected, "bundle-page", started,
            {"bundle_read": _seconds_since(bundle_started)},
        )
    offset = max(0, args.offset)
    limit = max(0, args.limit)
    page = []
    filtered_count = 0
    for entry in entries:
        if any(
            requested and not _query_filter_matches(entry.get(key), requested)
            for key, requested in (
                ("provider", args.provider), ("project", args.project),
                ("session", args.session), ("role", args.role), ("tool", args.tool),
            )
        ):
            continue
        if offset <= filtered_count < offset + limit:
            page.append(entry)
        filtered_count += 1
    bundle_seconds = _seconds_since(bundle_started)
    issues: list[dict[str, Any]] = []
    snapshot_advanced = False
    hydration_started = time.monotonic()
    if db_path is not None and any(entry.get("passage") is None for entry in page):
        page, issues, snapshot_advanced = hydrate_bundle_passages(db_path, header, page)
    hydration_seconds = _seconds_since(hydration_started)
    origin = header.get("origin") or {}
    for entry in page:
        entry["readable_query"] = header.get("query")
        entry["follow_up"] = {
            "command": "details", "snapshot_id": header.get("snapshot_id"),
            "result_id": entry.get("alias") or entry["result_id"], "bundle": str(path),
            "readable": True, "readable_query": header.get("query"),
            **({"db": str(db_path)} if db_path is not None else {}),
        }
        if origin.get("case_id") is not None:
            entry["follow_up"]["case_id"] = origin.get("case_id")
        if origin.get("binding_status") == "bound" and origin.get("continuation_id"):
            entry["follow_up"]["continuation_id"] = origin.get("continuation_id")
    result = {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
        "complete": bool(header.get("complete", False)) and not issues,
        "snapshot_id": header.get("snapshot_id"), "total": filtered_count,
        "offset": offset, "limit": limit, "has_more": offset + limit < filtered_count,
        "entries": page, "snapshot_advanced": snapshot_advanced, "issues": issues,
        "origin": header.get("origin"),
        "page_filters": {
            name: getattr(args, name, None)
            for name in ("provider", "project", "session", "role", "tool")
            if getattr(args, name, None) is not None
        },
        **({"coverage": header["coverage"]} if header.get("coverage") is not None else {}),
    }
    return attach_command_timing(
        result, "bundle-page", started,
        {"bundle_read": bundle_seconds, "hydration": hydration_seconds},
    )


def detail_events(db: sqlite3.Connection, args: argparse.Namespace) -> dict[str, Any]:
    status = projection_status(db)
    requested = list(dict.fromkeys(args.result_ids))
    selected: dict[str, sqlite3.Row] = {}
    for result_id in requested:
        event_id, expected_source_id, expected_line = parse_retrieval_result_id(result_id)
        row = db.execute(
            "SELECT e.*,src.path AS source_path FROM events e JOIN sources src USING(source_id) WHERE e.event_id=?",
            (event_id,),
        ).fetchone()
        if row and (
            expected_source_id is None
            or (row["source_id"] == expected_source_id and row["line_number"] == expected_line)
        ):
            selected[result_id] = row
    before = max(0, args.before)
    after = max(0, args.after)
    expanded: dict[str, sqlite3.Row] = {}
    contexts: dict[str, dict[str, Any]] = {}
    for result_id, hit in selected.items():
        prior = db.execute(
            "SELECT e.*,src.path AS source_path FROM events e JOIN sources src USING(source_id) "
            "WHERE e.source_id=? AND e.line_number<? ORDER BY e.line_number DESC LIMIT ?",
            (hit["source_id"], hit["line_number"], before),
        ).fetchall() if before else []
        following = db.execute(
            "SELECT e.*,src.path AS source_path FROM events e JOIN sources src USING(source_id) "
            "WHERE e.source_id=? AND e.line_number>? ORDER BY e.line_number LIMIT ?",
            (hit["source_id"], hit["line_number"], after),
        ).fetchall() if after else []
        ordered = [*reversed(prior), hit, *following]
        contexts[result_id] = {
            "event_ids": [row["event_id"] for row in ordered],
            "before_clipped": len(prior) < before, "after_clipped": len(following) < after,
        }
        expanded.update((row["event_id"], row) for row in ordered)
    rows = sorted(expanded.values(), key=lambda row: (
        row["order_timestamp"], row["provider"], row["session_id"],
        row["source_id"], row["line_number"], row["event_id"],
    ))
    selected_event_ids = {row["event_id"] for row in selected.values()}
    events = []
    readable_remaining = READABLE_RESPONSE_TEXT_MAX_BYTES
    with forbid_source_transcript_reads() as source_reads:
        for row in rows:
            structure = None
            if row["category"] != "unreadable":
                if not getattr(args, "readable", False):
                    structure = redact_value(read_event_structure(db, row))
            event = {
                "event_id": row["event_id"], "selected": row["event_id"] in selected_event_ids,
                "provider": row["provider"], "session": row["session_id"],
                "project": row["project"], "timestamp": row["timestamp"],
                "order_timestamp": row["order_timestamp"], "order_kind": row["order_kind"],
                "category": row["category"], "role": row["role"], "tool": row["tool"],
                "supported": bool(row["supported"]), "issue": row["issue"],
                "structure": structure,
                "source": {"path": row["source_path"], "line": row["line_number"], "hash": row["raw_hash"]},
            }
            if getattr(args, "readable", False):
                event.pop("structure")
                content = read_event_content(db, row)
                readable = bounded_readable_text(
                    flatten_text(redact_value(content)) if content is not None else "",
                    args, readable_remaining,
                )
                event.update(readable)
                readable_remaining -= len(readable["text"].encode("utf-8"))
            events.append(event)
    available = {
        result_id for result_id, row in selected.items() if row["category"] != "unreadable"
    }
    return {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION, "complete": len(available) == len(requested),
        "snapshot_id": status["snapshot_id"], "requested_snapshot_id": args.snapshot_id,
        "snapshot_advanced": args.snapshot_id != status["snapshot_id"],
        "results": [{
            "result_id": item,
            "status": "ok" if item in available else "unavailable" if item in selected else "missing",
        } for item in requested],
        "contexts": contexts, "events": events,
        "telemetry": {"source_reads": source_reads["attempts"]},
    }


def load_source_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("version") != 1 or not isinstance(value.get("sources"), list):
        raise ValueError("source manifest must use version 1 and contain a sources list")
    required = {
        "provider", "path", "logical_path", "size_bytes", "mtime_ns", "sha256",
        "adapter_version", "status", "issue_count",
    }
    identities = set()
    paths = set()
    for item in value["sources"]:
        if not isinstance(item, dict) or not required.issubset(item):
            raise ValueError("each manifest source must contain canonical identity and coverage fields")
        if item["provider"] not in ADAPTER_VERSION or item["adapter_version"] != ADAPTER_VERSION[item["provider"]]:
            raise ValueError("manifest source has an unsupported provider or adapter version")
        expected_source_id = digest((item["provider"] + "\0" + item["logical_path"]).encode())
        if item.get("source_id") not in (None, expected_source_id):
            raise ValueError("manifest source_id does not match provider and logical_path")
        if (
            not isinstance(item["size_bytes"], int) or item["size_bytes"] < 0
            or not isinstance(item["mtime_ns"], int) or item["mtime_ns"] < 0
            or not isinstance(item["issue_count"], int) or item["issue_count"] < 0
            or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
            or item["status"] != "complete"
        ):
            raise ValueError("manifest source coverage fields are invalid")
        source_id = expected_source_id
        identity = (item["provider"], source_id)
        if identity in identities or item["logical_path"] in paths:
            raise ValueError("manifest contains duplicate sources")
        identities.add(identity)
        paths.add(item["logical_path"])
    computed = manifest_snapshot_id(value)
    if value.get("snapshot_id") not in (None, computed):
        raise ValueError("source manifest snapshot_id does not match its canonical contents")
    value["snapshot_id"] = computed
    return value


def manifest_snapshot_id(manifest: dict[str, Any]) -> str:
    state = [PROJECTION_VERSION]
    canonical_sources = []
    for item in manifest["sources"]:
        logical_path = item.get("logical_path") or str(Path(item["path"]).expanduser().resolve())
        source_id = item.get("source_id") or digest((item["provider"] + "\0" + logical_path).encode())
        canonical_sources.append((item["provider"], source_id, item, logical_path))
    for provider, source_id, item, _logical_path in sorted(canonical_sources):
        state.append("\0".join((
            provider, source_id, item.get("sha256", ""), str(item.get("size_bytes", "")),
            str(item.get("mtime_ns", "")),
            item.get("adapter_version", ADAPTER_VERSION[provider]),
            item.get("status", "complete"), str(item.get("issue_count", 0)),
        )))
    extensions = manifest.get("adapter_extensions") or []
    state.extend(
        "\0".join((item["provider"], item["category"], item["structural_signature"]))
        for item in sorted(
            extensions,
            key=lambda value: (value["provider"], value["category"], value["structural_signature"]),
        )
    )
    return digest("\n".join(state).encode())


def manifest_matches_database(db: sqlite3.Connection, manifest: dict[str, Any]) -> bool:
    declared = {
        (
            item["provider"],
            item.get("source_id") or digest((
                item["provider"] + "\0" + (item.get("logical_path") or str(Path(item["path"]).expanduser().resolve()))
            ).encode()),
            item.get("sha256"), item.get("size_bytes"), item.get("mtime_ns"),
            item.get("adapter_version", ADAPTER_VERSION[item["provider"]]),
            item.get("status", "complete"), item.get("issue_count", 0),
        )
        for item in manifest["sources"]
    }
    stored = {
        (
            row["provider"], row["source_id"], row["source_hash"], row["size_bytes"], row["mtime_ns"],
            row["adapter_version"], row["status"], row["issue_count"],
        )
        for row in db.execute(
            "SELECT provider,source_id,source_hash,size_bytes,mtime_ns,adapter_version,status,issue_count FROM sources"
        )
    }
    declared_extensions = {
        (item["provider"], item["category"], item["structural_signature"])
        for item in manifest.get("adapter_extensions") or []
    }
    stored_extensions = {
        (row["provider"], row["category"], row["structural_signature"])
        for row in db.execute("SELECT provider,category,structural_signature FROM adapter_extensions")
    }
    return declared == stored and declared_extensions == stored_extensions


def _raw_filter_match(parsed: Parsed, provider: str, args: argparse.Namespace) -> tuple[bool, list[str]]:
    unknown: list[str] = []
    fields = (
        ("provider", provider, getattr(args, "provider", None)),
        ("project", parsed.project, getattr(args, "project", None)),
        ("session", parsed.session_id, getattr(args, "session", None)),
        ("role", parsed.role, getattr(args, "role", None)),
        ("tool", parsed.tool, getattr(args, "tool", None)),
        ("lineage", parsed.parent_id, getattr(args, "lineage", None)),
    )
    for name, actual, requested in fields:
        if not requested:
            continue
        if actual is None:
            if parsed.supported:
                return False, unknown
            unknown.append(name)
        elif not _query_filter_matches(str(actual), requested):
            return False, unknown
    timestamp = normalize_timestamp(parsed.timestamp)
    if getattr(args, "since", None) and timestamp is None:
        unknown.append("since")
    elif getattr(args, "since", None) and timestamp < (normalize_timestamp(args.since) or args.since):
        return False, unknown
    if getattr(args, "until", None) and timestamp is None:
        unknown.append("until")
    elif getattr(args, "until", None) and timestamp > (normalize_timestamp(args.until) or args.until):
        return False, unknown
    return not unknown, unknown


def raw_manifest_search(manifest_path: Path, args: argparse.Namespace, reason: str) -> dict[str, Any]:
    manifest = load_source_manifest(manifest_path)
    snapshot_id = manifest_snapshot_id(manifest)
    text = unicodedata.normalize("NFC", " ".join((args.text or "").split()))
    mode = getattr(args, "mode", "ranked")
    order = getattr(args, "order", "relevance")
    tree = parse_search_query(text) if mode == "ranked" and text else None
    if tree is not None and not positive_search_atoms(tree):
        raise ValueError("search query requires at least one positive term or phrase")
    needles = [text] if mode == "literal" else lexical_terms(text)
    if not text or not (needles or LITERAL_SEARCH_RE.search(text)):
        raise ValueError("raw fallback requires a non-empty searchable query")
    entries = []
    issues = []
    bytes_read = 0
    sources_read = 0
    started = time.monotonic()
    for item in manifest["sources"]:
        provider = item.get("provider")
        if provider not in ADAPTER_VERSION:
            issues.append({"path": item.get("path"), "reason": "unsupported_provider"})
            continue
        if getattr(args, "provider", None) and not _query_filter_matches(provider, args.provider):
            continue
        if item["issue_count"]:
            issues.append({
                "path": item["logical_path"], "reason": "declared_unavailable_events",
                "count": item["issue_count"],
            })
        byte_budget = getattr(args, "fallback_max_bytes", FALLBACK_MAX_SOURCE_BYTES)
        deadline = getattr(args, "fallback_deadline_seconds", FALLBACK_DEADLINE_SECONDS)
        if bytes_read + item["size_bytes"] > byte_budget or time.monotonic() - started > deadline:
            issues.append({"path": item["logical_path"], "reason": "fallback_budget_exceeded"})
            break
        path = Path(item["path"]).expanduser().resolve()
        try:
            data = path.read_bytes()
        except OSError as exc:
            issues.append({"path": str(path), "reason": f"unreadable_source: {exc}"})
            continue
        sources_read += 1
        if bytes_read + len(data) > byte_budget or time.monotonic() - started > deadline:
            issues.append({"path": item["logical_path"], "reason": "fallback_budget_exceeded"})
            break
        bytes_read += len(data)
        expected_size = item.get("size_bytes")
        expected_hash = item.get("sha256")
        if expected_size is not None and len(data) != expected_size:
            issues.append({"path": str(path), "reason": "size_mismatch"})
            continue
        if expected_hash and digest(data) != expected_hash:
            issues.append({"path": str(path), "reason": "hash_mismatch"})
            continue
        logical_path = item.get("logical_path") or str(path)
        adapter_path = Path(logical_path)
        source_id = item.get("source_id") or digest((provider + "\0" + logical_path).encode())
        meta: dict[str, Any] = {}
        auto_extensions = frozenset(
            (entry["category"], entry["structural_signature"])
            for entry in manifest.get("adapter_extensions") or []
            if entry["provider"] == provider
        )
        source_candidates = []
        for line_number, raw_line in enumerate(io.BytesIO(data), 1):
            if line_number % 1000 == 0 and time.monotonic() - started > deadline:
                issues.append({"path": logical_path, "reason": "fallback_deadline_exceeded"})
                break
            if not raw_line.strip():
                continue
            try:
                row = json.loads(raw_line)
                if not isinstance(row, dict):
                    raise ValueError("record is not an object")
                parsed = (
                    parse_claude(row, adapter_path, meta, auto_extensions)
                    if provider == "claude" else parse_codex(row, adapter_path, meta, auto_extensions)
                )
                regions = normalized_search_regions(row)
                raw_hash = digest(stored_json(row))
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                raw_hash = digest(raw_line)
                parsed = Parsed(
                    path.stem, item.get("project"), None, path.stem, None,
                    "unreadable", None, None, False, None, (), (), False, str(exc),
                )
                regions = normalized_search_regions("[REDACTED]")
                issues.append({"path": logical_path, "line": line_number, "reason": "unreadable_event"})
            region_matches = matching_projection_regions(regions, text, mode, tree)
            source_candidates.append((line_number, parsed, raw_hash, region_matches))
        exact_times = [
            normalize_timestamp(parsed.timestamp)
            for _line, parsed, _hash, _matches in source_candidates
            if normalize_timestamp(parsed.timestamp)
        ]
        file_mtime_ns = item.get("mtime_ns") or path.stat().st_mtime_ns
        file_time = datetime.fromtimestamp(
            file_mtime_ns / 1_000_000_000, timezone.utc,
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")
        last_exact = None
        first_exact = exact_times[0] if exact_times else None
        for line_number, parsed, raw_hash, region_matches in source_candidates:
            exact = normalize_timestamp(parsed.timestamp)
            if exact:
                order_timestamp, order_kind, last_exact = exact, "exact", exact
            elif last_exact:
                order_timestamp, order_kind = last_exact, "forward_fill"
            elif first_exact:
                order_timestamp, order_kind = first_exact, "back_fill"
            else:
                order_timestamp, order_kind = file_time, "file_mtime"
            if not region_matches:
                continue
            filter_parsed = Parsed(
                parsed.session_id, parsed.project, parsed.parent_id, parsed.root_id,
                order_timestamp, parsed.category, parsed.role, parsed.tool, parsed.tool_event,
                parsed.content, parsed.content_paths, parsed.structure_extras,
                parsed.supported, parsed.issue,
            )
            filters_match, unknown = _raw_filter_match(filter_parsed, provider, args)
            if unknown:
                issues.append({
                    "path": logical_path, "line": line_number,
                    "reason": "unknown_filter_fields", "fields": unknown,
                })
                continue
            if not filters_match:
                continue
            event_id = digest(f"{source_id}:{line_number}:{raw_hash}".encode())
            origin, projection, _spans = min(
                region_matches, key=lambda match: PROJECTION_ORIGINS.index(match[0]),
            )
            passage, truncated = projection_passage_with_status(projection, text, mode)
            entries.append({
                "result_id": retrieval_result_id(event_id, source_id, line_number),
                "_source_id": source_id, "provider": provider, "session": parsed.session_id,
                "timestamp": parsed.timestamp, "order_timestamp": order_timestamp,
                "order_kind": order_kind,
                "category": parsed.category, "role": parsed.role, "tool": parsed.tool,
                "project": parsed.project or item.get("project"), "supported": parsed.supported,
                "source": {"path": logical_path, "line": line_number, "hash": raw_hash},
                "passage": passage,
                **({"passage_truncated": True} if truncated else {}),
                "match_origin": origin,
                "match_origins": [
                    candidate for candidate in PROJECTION_ORIGINS
                    if any(match[0] == candidate for match in region_matches)
                ],
            })
    entries.sort(key=lambda entry: (
        *((PROJECTION_ORIGINS.index(entry["match_origin"]),) if order == "relevance" else ()),
        entry["order_timestamp"] or "", entry["provider"], entry["session"],
        entry["_source_id"], entry["source"]["line"], entry["result_id"],
    ))
    for entry in entries:
        entry.pop("_source_id")
    if time.monotonic() - started > getattr(
        args, "fallback_deadline_seconds", FALLBACK_DEADLINE_SECONDS,
    ):
        issues.append({"reason": "fallback_deadline_exceeded"})
    complete = not issues
    return {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION, "complete": complete,
        "backend": "raw_fallback", "snapshot_id": snapshot_id,
        "projection_version": PROJECTION_VERSION, "mode": mode, "order": order,
        "query": text,
        "total": len(entries) if complete else None, "entries": entries,
        "recovery": {"reason": reason, "manifest": str(manifest_path.resolve()), "issues": issues},
        "telemetry": {"source_reads": sources_read, "source_bytes": bytes_read},
    }


class LiveTailUnavailable(Exception):
    """A tail cannot be parsed exactly as ingestion would; the caller keeps the old answer."""


def iter_live_tail_events(
    db: sqlite3.Connection, item: dict[str, Any], budget: list[int], deadline_at: float,
) -> Iterator[dict[str, Any]]:
    """Yield a source's unindexed events the way ingestion would parse them.

    An appended tail resumes the saved adapter state at the indexed offset after re-verifying
    the prefix; a new or rewritten file is parsed whole; an unfinished last line is skipped.
    `budget` is a one-item list of remaining bytes shared across sources.
    """
    path = Path(item["path"])
    provider = item.get("provider")
    if item.get("blocking") or provider not in ADAPTER_VERSION:
        raise LiveTailUnavailable("not_servable")
    row = db.execute(
        "SELECT source_id,size_bytes,source_hash,status,adapter_version FROM sources WHERE path=?",
        (str(path),),
    ).fetchone()
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise LiveTailUnavailable(f"unreadable_source: {exc}") from exc
    offset = item.get("indexed_bytes") or 0
    state = load_adapter_state(db, row["source_id"]) if row is not None and offset else None
    if offset and (
        row is None or row["size_bytes"] != offset or state is None
        or row["adapter_version"] != ADAPTER_VERSION[provider]
        or not data[:offset].endswith(b"\n") or digest(data[:offset]) != row["source_hash"]
    ):
        raise LiveTailUnavailable("indexed_prefix_changed")
    end = data.rfind(b"\n") + 1
    if end - offset > budget[0] or time.monotonic() > deadline_at:
        raise LiveTailUnavailable("live_tail_budget_exceeded")
    budget[0] -= end - offset
    source_id = row["source_id"] if row is not None else digest((provider + "\0" + str(path)).encode())
    meta = dict(state or {})
    auto_extensions = load_auto_metadata_extensions(db, provider)
    last_exact = db.execute(
        "SELECT MAX(order_timestamp) FROM events WHERE source_id=?", (source_id,),
    ).fetchone()[0] if offset else None
    line_number = data[:offset].count(b"\n")
    for raw_line in io.BytesIO(data[offset:end]):
        line_number += 1
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise ValueError("record is not an object")
            parsed = (
                parse_claude(value, path, meta, auto_extensions)
                if provider == "claude" else parse_codex(value, path, meta, auto_extensions)
            )
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            raise LiveTailUnavailable(f"unreadable_event: line {line_number}: {exc}") from exc
        exact = normalize_timestamp(parsed.timestamp)
        if exact:
            last_exact = exact
        order_timestamp = exact or last_exact or datetime.fromtimestamp(
            path.stat().st_mtime, timezone.utc,
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")
        yield {
            "path": path, "provider": provider, "source_id": source_id, "line": line_number,
            "value": value, "parsed": parsed, "raw_hash": digest(stored_json(value)),
            "order_timestamp": order_timestamp,
            "order_kind": "exact" if exact else "forward_fill" if last_exact else "file_mtime",
            "offset": offset, "end": end,
        }


def _tail_filter_parsed(event: dict[str, Any]) -> Parsed:
    parsed = event["parsed"]
    return Parsed(
        parsed.session_id, parsed.project, parsed.parent_id, parsed.root_id,
        event["order_timestamp"], parsed.category, parsed.role, parsed.tool, parsed.tool_event,
        parsed.content, parsed.content_paths, parsed.structure_extras,
        parsed.supported, parsed.issue,
    )


def serve_live_tails(
    db: sqlite3.Connection, affected: list[dict[str, Any]], args: argparse.Namespace,
    deadline_at: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Search unindexed transcript bytes the way ingestion would parse them.

    Returns (entries, served, failures).
    """
    text = unicodedata.normalize("NFC", " ".join((args.text or "").split()))
    mode = getattr(args, "mode", "ranked")
    tree = parse_search_query(text) if mode == "ranked" and text else None
    atoms = positive_search_atoms(tree) if tree is not None else []
    entries: list[dict[str, Any]] = []
    served: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    budget = [LIVE_TAIL_MAX_BYTES]
    for item in affected:
        found: list[dict[str, Any]] = []
        events = 0
        bounds = (item.get("indexed_bytes") or 0, None)
        try:
            for event in iter_live_tail_events(db, item, budget, deadline_at):
                events += 1
                bounds = (event["offset"], event["end"])
                regions = normalized_search_regions(event["value"])
                region_matches = matching_projection_regions(regions, text, mode, tree)
                if not region_matches:
                    continue
                matches, unknown = _raw_filter_match(_tail_filter_parsed(event), event["provider"], args)
                if unknown:
                    raise LiveTailUnavailable(f"unknown_filter_fields: {unknown}")
                if not matches:
                    continue
                origin, projection, spans = min(
                    region_matches,
                    key=lambda match: (
                        PROJECTION_ORIGINS.index(match[0]),
                        max((span[1] for span in match[2]), default=len(match[1]))
                        - min((span[0] for span in match[2]), default=0),
                    ),
                )
                parsed = event["parsed"]
                event_id = digest(f"{event['source_id']}:{event['line']}:{event['raw_hash']}".encode())
                passage, truncated = projection_passage_with_status(projection, text, mode)
                found.append({
                    "result_id": retrieval_result_id(event_id, event["source_id"], event["line"]),
                    "provider": event["provider"], "session": parsed.session_id,
                    "timestamp": parsed.timestamp, "order_timestamp": event["order_timestamp"],
                    "order_kind": event["order_kind"], "category": parsed.category, "role": parsed.role,
                    "tool": parsed.tool, "project": parsed.project, "supported": parsed.supported,
                    "source": {"path": str(event["path"]), "line": event["line"], "hash": event["raw_hash"]},
                    "passage": passage,
                    **({"passage_truncated": True} if truncated else {}),
                    "rank": {
                        "phrase_hits": sum(
                            1 for atom in atoms if atom.kind == "phrase" and _atom_spans(projection, atom)
                        ),
                        "term_span": (
                            max(span[1] for span in spans) - min(span[0] for span in spans)
                            if spans else len(projection)
                        ),
                        "document_bytes": len(encoded_search_regions(regions)), "fts": 0.0,
                    },
                    "match_origin": origin,
                    "match_origins": [
                        candidate for candidate in PROJECTION_ORIGINS
                        if any(match[0] == candidate for match in region_matches)
                    ],
                    "live_tail": True,
                })
        except LiveTailUnavailable as exc:
            failures.append({**item, "serve_failure": str(exc)})
            continue
        entries.extend(found)
        served.append({
            "path": item["path"], "provider": item.get("provider"), "from_byte": bounds[0],
            "to_byte": bounds[1] if bounds[1] is not None else bounds[0], "events": events,
            "reason": item.get("reason"),
        })
    return entries, served, failures


def clean_live_tail_events(
    db: sqlite3.Connection, affected: list[dict[str, Any]], args: argparse.Namespace,
    deadline_at: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Tail events matching a clean query, or None when any tail cannot be served exactly."""
    needle = " ".join(args.text.split()).casefold() if args.text else None
    matched: list[dict[str, Any]] = []
    served: list[dict[str, Any]] = []
    budget = [LIVE_TAIL_MAX_BYTES]
    try:
        for item in affected:
            events = 0
            bounds = (item.get("indexed_bytes") or 0, item.get("indexed_bytes") or 0)
            for event in iter_live_tail_events(db, item, budget, deadline_at):
                events += 1
                bounds = (event["offset"], event["end"])
                matches, unknown = _raw_filter_match(_tail_filter_parsed(event), event["provider"], args)
                if unknown:
                    return None
                if not matches:
                    continue
                if needle is not None:
                    if event["parsed"].category == "unreadable":
                        continue
                    event["search_text"] = " ".join(redacted_search_text(event["value"]).split())
                    if needle not in event["search_text"].casefold():
                        continue
                matched.append(event)
            served.append({
                "path": item["path"], "provider": item.get("provider"), "from_byte": bounds[0],
                "to_byte": bounds[1], "events": events, "reason": item.get("reason"),
            })
    except LiveTailUnavailable:
        return None
    return matched, served


def merge_live_tail_entries(
    entries: list[dict[str, Any]], tail_entries: list[dict[str, Any]],
    rewritten_paths: set[str], order: str, fast_ranked: bool,
) -> list[dict[str, Any]]:
    """Insert tail results into the indexed order without reordering the indexed results.

    Indexed results keep their demotions, and only their leading ones carry passages, so a
    re-sort could surface an unhydrated result; each tail result goes before the first indexed
    result that ranks after it.
    """
    def key(entry: dict[str, Any]) -> tuple[Any, ...]:
        if order == "chronological":
            return (entry["order_timestamp"] or "", entry["provider"], entry["session"],
                    entry["source"]["path"], entry["source"]["line"], entry["result_id"])
        return (PROJECTION_ORIGINS.index(entry["match_origin"]),
                -entry["rank"]["phrase_hits"], entry["rank"]["term_span"],
                entry["rank"]["document_bytes"], entry["rank"]["fts"],
                entry["order_timestamp"] or "", entry["provider"], entry["session"],
                entry["source"]["path"], entry["source"]["line"], entry["result_id"])
    merged = [entry for entry in entries if entry["source"]["path"] not in rewritten_paths]
    for entry in tail_entries:
        if fast_ranked:
            # The fast path does not measure phrases or spans; rank tails on the same scale.
            entry["rank"] = {**entry["rank"], "phrase_hits": 0, "term_span": 0}
    for entry in sorted(tail_entries, key=key):
        position = next(
            (index for index, other in enumerate(merged)
             if not other.get("live_tail") and key(other) > key(entry)),
            len(merged),
        )
        merged.insert(position, entry)
    return merged


def live_tails_servable(coverage: Any) -> bool:
    """Only a live-checked answer whose sole gap is active, unindexed transcript bytes."""
    if not isinstance(coverage, dict) or not coverage.get("live_checked"):
        return False
    affected = coverage.get("affected_sources") or []
    return bool(affected) and not coverage.get("issues") and all(
        not item.get("blocking") and item.get("path") and str(item.get("reason", "")).startswith("active_")
        for item in affected
    )


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def _search_index_entrypoint_impl(db_path: Path, args: argparse.Namespace) -> dict[str, Any]:
    failure = None
    entry_lease = None
    snapshot_result = None
    started = time.monotonic()
    deadline_seconds = float(getattr(args, "deadline_seconds", SEARCH_DEADLINE_SECONDS))
    # With a frozen-source manifest, fall back promptly instead of waiting out a writer.
    lock_wait = max(0.001, min(
        1.0 if getattr(args, "manifest", None) else READER_LOCK_WAIT_SECONDS, deadline_seconds,
    ))
    try:
        try:
            entry_lease = acquire_projection_pack_lease_path(
                db_path, exclusive=False, timeout=lock_wait,
            )
        except FileNotFoundError:
            entry_lease = None
        db = connect(db_path, read_only=True, timeout=lock_wait)
        try:
            db.execute(f"PRAGMA busy_timeout={int(lock_wait * 1000)}")
            if math.isfinite(deadline_seconds) and deadline_seconds > 0:
                remaining = deadline_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    return _deadline_incomplete_result(deadline_seconds)
                args = argparse.Namespace(**vars(args))
                args.deadline_seconds = remaining
            manifest = load_source_manifest(args.manifest) if getattr(args, "manifest", None) else None
            result = search_index(db, args, keep_snapshot=True)
            result_coverage = result.get("coverage")
            if (
                isinstance(result_coverage, dict)
                and result_coverage.get("reason") == "query_deadline_exceeded"
            ):
                return result
            if result["complete"] and manifest is not None and not manifest_matches_database(db, manifest):
                result.update({
                    "complete": False, "coverage": {"reason": "database_manifest_mismatch"},
                })
            else:
                coverage_only = False
                if result["complete"] and manifest is None:
                    coverage = query_coverage(db, args)
                    if not coverage_is_current(coverage):
                        if isinstance(coverage, dict) and coverage.get("complete") is True:
                            coverage = {**coverage, "complete": False, "reason": "watcher_status_stale"}
                        result.update({"complete": False, "coverage": coverage})
                        coverage_only = True
                unavailable_events, unavailable_sources = unsupported_events_in_scope(db, args)
                if unavailable_events:
                    coverage_only = False
                    result.update({
                        "complete": False,
                        "coverage": {
                            "reason": "unavailable_indexed_events",
                            "events": unavailable_events,
                            "sources": unavailable_sources,
                        },
                    })
                unsupported = [entry["result_id"] for entry in result.get("entries", []) if not entry["supported"]]
                if unsupported:
                    coverage_only = False
                    result.update({
                        "complete": False,
                        "coverage": {"reason": "unsupported_matching_events", "result_ids": unsupported},
                    })
                if coverage_only and live_tails_servable(coverage):
                    # Parse the active tails in this read snapshot so the answer is complete
                    # without the caller scanning transcripts; any failure keeps the old path.
                    tail_entries, served, tail_failures = serve_live_tails(
                        db, coverage["affected_sources"], args,
                        started + deadline_seconds,
                    )
                    if not tail_failures:
                        rewritten = {
                            item["path"] for item in coverage["affected_sources"]
                            if not item.get("indexed_bytes")
                        }
                        fast = (result.get("telemetry") or {}).get("verified_by") == "equivalent_positional_fts_predicate"
                        entries = merge_live_tail_entries(
                            result.get("entries", []), tail_entries, rewritten,
                            getattr(args, "order", "relevance"), fast,
                        )
                        result.update({
                            "complete": True, "total": len(entries), "entries": entries,
                            "coverage": {
                                key: value for key, value in coverage.items()
                                if key not in ("affected_sources", "fallback", "issues")
                            } | {"complete": True, "status": "complete",
                                 "reason": "live_tails_served", "tails_served": served},
                        })
                        coverage_only = False
                if coverage_only:
                    # The verified indexed results stay usable; the caller scans only the
                    # listed unindexed tails instead of repeating the whole search raw.
                    snapshot_result = {
                        **result, "total": None, "indexed_total": result.get("total"),
                        "reason": coverage.get("reason") if isinstance(coverage, dict) else str(coverage),
                    }
        finally:
            db.close()
        if result["complete"]:
            return result
        if snapshot_result is not None and not getattr(args, "manifest", None):
            return snapshot_result
        coverage = result.get("coverage")
        failure = coverage.get("reason") if isinstance(coverage, dict) else str(coverage or "database_incomplete")
    except (OSError, sqlite3.Error, ValueError, KeyError, RuntimeError, UnicodeError, zstd.ZstdError) as exc:
        failure = f"database_error: {exc}"
    finally:
        if entry_lease is not None:
            release_projection_pack_lease(entry_lease)
    if getattr(args, "manifest", None):
        return raw_manifest_search(args.manifest, args, failure or "database_incomplete")
    return {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION, "complete": False,
        "backend": "database", "total": None, "entries": [], "reason": failure,
    }


def search_index_entrypoint(db_path: Path, args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    result = _search_index_entrypoint_impl(db_path, args)
    ensure_retrieval_origin(result, args)
    return attach_command_timing(result, "search-index", started)


def raw_manifest_details(manifest_path: Path, args: argparse.Namespace, reason: str) -> dict[str, Any]:
    manifest = load_source_manifest(manifest_path)
    snapshot_id = manifest_snapshot_id(manifest)
    if args.snapshot_id != snapshot_id:
        return {
            "protocol_version": RETRIEVAL_PROTOCOL_VERSION, "complete": False,
            "snapshot_id": snapshot_id, "reason": "stale_manifest_snapshot", "events": [],
        }
    records: dict[str, dict[str, Any]] = {}
    by_source: dict[str, list[dict[str, Any]]] = {}
    requested = list(dict.fromkeys(args.result_ids))
    target_source_ids = {
        source_id for result_id in requested
        if (source_id := parse_retrieval_result_id(result_id)[1]) is not None
    }
    issues = []
    bytes_read = 0
    sources_read = 0
    started = time.monotonic()
    for item in manifest["sources"]:
        provider = item.get("provider")
        logical_path = item["logical_path"]
        source_id = item.get("source_id") or digest((provider + "\0" + logical_path).encode())
        if source_id not in target_source_ids:
            continue
        if item["issue_count"]:
            issues.append({
                "path": logical_path, "reason": "declared_unavailable_events",
                "count": item["issue_count"],
            })
        byte_budget = getattr(args, "fallback_max_bytes", FALLBACK_MAX_SOURCE_BYTES)
        deadline = getattr(args, "fallback_deadline_seconds", FALLBACK_DEADLINE_SECONDS)
        if bytes_read + item["size_bytes"] > byte_budget or time.monotonic() - started > deadline:
            issues.append({"path": logical_path, "reason": "fallback_budget_exceeded"})
            break
        path = Path(item["path"]).expanduser().resolve()
        try:
            data = path.read_bytes()
        except OSError as exc:
            issues.append({"path": str(path), "reason": f"unreadable_source: {exc}"})
            continue
        sources_read += 1
        if bytes_read + len(data) > byte_budget or time.monotonic() - started > deadline:
            issues.append({"path": logical_path, "reason": "fallback_budget_exceeded"})
            break
        bytes_read += len(data)
        if item.get("size_bytes") is not None and len(data) != item["size_bytes"]:
            issues.append({"path": str(path), "reason": "size_mismatch"})
            continue
        if item.get("sha256") and digest(data) != item["sha256"]:
            issues.append({"path": str(path), "reason": "hash_mismatch"})
            continue
        adapter_path = Path(logical_path)
        meta: dict[str, Any] = {}
        auto_extensions = frozenset(
            (entry["category"], entry["structural_signature"])
            for entry in manifest.get("adapter_extensions") or []
            if entry["provider"] == provider
        )
        source_records = []
        for line_number, raw_line in enumerate(io.BytesIO(data), 1):
            if line_number % 1000 == 0 and time.monotonic() - started > deadline:
                issues.append({"path": logical_path, "reason": "fallback_deadline_exceeded"})
                break
            if not raw_line.strip():
                continue
            try:
                row = json.loads(raw_line)
                if not isinstance(row, dict):
                    raise ValueError("record is not an object")
                parsed = (
                    parse_claude(row, adapter_path, meta, auto_extensions)
                    if provider == "claude" else parse_codex(row, adapter_path, meta, auto_extensions)
                )
                raw_hash = digest(stored_json(row))
                structure = redact_value(row)
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                raw_hash = digest(raw_line)
                parsed = Parsed(
                    path.stem, item.get("project"), None, path.stem, None,
                    "unreadable", None, None, False, None, (), (), False, str(exc),
                )
                structure = None
                issues.append({"path": logical_path, "line": line_number, "reason": "unreadable_event"})
            event_id = digest(f"{source_id}:{line_number}:{raw_hash}".encode())
            result_id = retrieval_result_id(event_id, source_id, line_number)
            record = {
                "event_id": event_id, "_source_id": source_id, "_result_id": result_id,
                "provider": provider, "session": parsed.session_id,
                "project": parsed.project or item.get("project"), "timestamp": parsed.timestamp,
                "order_timestamp": normalize_timestamp(parsed.timestamp),
                "order_kind": "exact" if normalize_timestamp(parsed.timestamp) else "source_order",
                "category": parsed.category, "role": parsed.role, "tool": parsed.tool,
                "supported": parsed.supported, "issue": parsed.issue, "structure": structure,
                "source": {"path": logical_path, "line": line_number, "hash": raw_hash},
            }
            if getattr(args, "readable", False):
                record["text"] = flatten_text(redact_value(parsed.content)) if parsed.content is not None else ""
            records[result_id] = record
            source_records.append(record)
        exact_times = [record["order_timestamp"] for record in source_records if record["order_timestamp"]]
        first_exact = exact_times[0] if exact_times else None
        last_exact = None
        file_mtime_ns = item.get("mtime_ns") or path.stat().st_mtime_ns
        file_time = datetime.fromtimestamp(
            file_mtime_ns / 1_000_000_000, timezone.utc,
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")
        for record in source_records:
            if record["order_timestamp"]:
                last_exact = record["order_timestamp"]
            elif last_exact:
                record.update(order_timestamp=last_exact, order_kind="forward_fill")
            elif first_exact:
                record.update(order_timestamp=first_exact, order_kind="back_fill")
            else:
                record.update(order_timestamp=file_time, order_kind="file_mtime")
        by_source[source_id] = source_records
    expanded: dict[str, dict[str, Any]] = {}
    contexts = {}
    for result_id in requested:
        hit = records.get(result_id)
        if not hit:
            continue
        source_id = next(key for key, values in by_source.items() if hit in values)
        values = by_source[source_id]
        index = values.index(hit)
        prior = values[max(0, index - max(0, args.before)):index]
        following = values[index + 1:index + 1 + max(0, args.after)]
        window = [*prior, hit, *following]
        contexts[result_id] = {
            "event_ids": [item["event_id"] for item in window],
            "before_clipped": len(prior) < max(0, args.before),
            "after_clipped": len(following) < max(0, args.after),
        }
        expanded.update((item["event_id"], item) for item in window)
    events = sorted(expanded.values(), key=lambda item: (
        item["order_timestamp"] or "", item["provider"], item["session"],
        item["_source_id"],
        item["source"]["line"], item["event_id"],
    ))
    available = {
        result_id for result_id in requested
        if result_id in records and records[result_id]["structure"] is not None
    }
    readable_remaining = READABLE_RESPONSE_TEXT_MAX_BYTES
    for event in events:
        event.pop("_source_id")
        event["selected"] = event.pop("_result_id") in requested
        if getattr(args, "readable", False):
            event.pop("structure")
            readable = bounded_readable_text(event.pop("text"), args, readable_remaining)
            event.update(readable)
            readable_remaining -= len(readable["text"].encode("utf-8"))
    if time.monotonic() - started > getattr(
        args, "fallback_deadline_seconds", FALLBACK_DEADLINE_SECONDS,
    ):
        issues.append({"reason": "fallback_deadline_exceeded"})
    return {
        "protocol_version": RETRIEVAL_PROTOCOL_VERSION,
        "complete": not issues and len(available) == len(requested),
        "backend": "raw_fallback", "snapshot_id": snapshot_id,
        "results": [{
            "result_id": item,
            "status": "ok" if item in available else "unavailable" if item in records else "missing",
        } for item in requested],
        "contexts": contexts, "events": events,
        "recovery": {"reason": reason, "manifest": str(manifest_path.resolve()), "issues": issues},
        "telemetry": {"source_reads": sources_read, "source_bytes": bytes_read},
    }


def details_entrypoint(db_path: Path, args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    phases: dict[str, float] = {}
    failure = None
    database_result = None
    origin = None
    args = argparse.Namespace(**vars(args))
    if (
        not getattr(args, "bundle", None)
        and (
            getattr(args, "case_id", None) is not None
            or getattr(args, "continuation_id", None) is not None
        )
    ):
        rejected = continuation_rejection({}, "continuation_bundle_required", command="details")
        rejected = finish_detail_continuation(rejected, args, origin)
        return attach_command_timing(rejected, "details", started, phases)
    if getattr(args, "bundle", None):
        bundle_started = time.monotonic()
        try:
            header, resolved, continuation_error = resolve_bundle_continuation(args.bundle, args)
        except (OSError, ValueError, KeyError, UnicodeError, json.JSONDecodeError) as exc:
            header, resolved, continuation_error = {}, [], f"bundle_error: {exc}"
        phases["bundle_read"] = _seconds_since(bundle_started)
        origin = header.get("origin")
        if continuation_error:
            rejected = continuation_rejection(header, continuation_error, command="details")
            rejected = finish_detail_continuation(rejected, args, origin)
            return attach_command_timing(rejected, "details", started, phases)
        args.result_ids = resolved
    database_started = time.monotonic()
    try:
        db = connect(db_path, read_only=True, timeout=READER_LOCK_WAIT_SECONDS)
        try:
            db.execute(f"PRAGMA busy_timeout={int(READER_LOCK_WAIT_SECONDS * 1000)}")
            db.execute("BEGIN")
            database_result = detail_events(db, args)
        finally:
            db.close()
        if database_result["complete"]:
            phases["database_read"] = _seconds_since(database_started)
            finished = finish_detail_continuation(database_result, args, origin)
            return attach_command_timing(finished, "details", started, phases)
        failure = database_result.get("reason", "database_incomplete")
    except (OSError, sqlite3.Error, ValueError, KeyError, RuntimeError, UnicodeError, zstd.ZstdError) as exc:
        failure = f"database_error: {exc}"
    phases["database_read"] = _seconds_since(database_started)
    if getattr(args, "manifest", None):
        fallback_started = time.monotonic()
        result = raw_manifest_details(args.manifest, args, failure or "database_incomplete")
        phases["fallback"] = _seconds_since(fallback_started)
    else:
        result = database_result or {
            "protocol_version": RETRIEVAL_PROTOCOL_VERSION, "complete": False,
            "backend": "database", "events": [], "reason": failure,
        }
    result = finish_detail_continuation(result, args, origin)
    return attach_command_timing(result, "details", started, phases)


def verify_projection_pack(db: sqlite3.Connection) -> dict[str, Any]:
    path = projection_pack_path(db)
    if not path.exists():
        return {"pass": False, "reason": "missing", "blocks": 0, "bytes": 0}
    size = path.stat().st_size
    failures = []
    covered = 0
    live_bytes = 0
    prior_end = 0
    refs_by_block: dict[int, list[sqlite3.Row]] = {}
    for ref in db.execute(
        "SELECT event_rowid,block_key,projection_offset,projection_bytes,projection_hash,reference_hash "
        "FROM search_projection_refs ORDER BY event_rowid"
    ):
        refs_by_block.setdefault(ref["block_key"], []).append(ref)
    blocks = db.execute(
        "SELECT * FROM search_projection_blocks ORDER BY pack_offset,block_key"
    ).fetchall()
    with path.open("rb") as handle:
        for block in blocks:
            start = block["pack_offset"]
            end = start + block["compressed_bytes"]
            if start < prior_end or end > size:
                failures.append({"block_key": block["block_key"], "reason": "invalid_extent"})
                continue
            handle.seek(start)
            compressed = handle.read(block["compressed_bytes"])
            if len(compressed) != block["compressed_bytes"] or digest_bytes(compressed) != block["compressed_hash"]:
                failures.append({"block_key": block["block_key"], "reason": "compressed_hash_or_length_mismatch"})
                continue
            try:
                raw = zstd.decompress(compressed, max_output_size=block["raw_bytes"])
            except zstd.ZstdError:
                failures.append({"block_key": block["block_key"], "reason": "decompression_failed"})
                continue
            if len(raw) != block["raw_bytes"] or digest_bytes(raw) != block["block_digest"]:
                failures.append({"block_key": block["block_key"], "reason": "hash_or_length_mismatch"})
            refs = refs_by_block.get(block["block_key"], [])
            if refs:
                live_bytes += len(compressed)
            for ref in refs:
                if ref["reference_hash"] != projection_reference_hash(
                    ref["event_rowid"], block["block_digest"], ref["projection_offset"],
                    ref["projection_bytes"], ref["projection_hash"],
                ):
                    failures.append({
                        "event_rowid": ref["event_rowid"], "reason": "reference_metadata_hash_mismatch",
                    })
                ref_start = ref["projection_offset"]
                ref_end = ref_start + ref["projection_bytes"]
                projection = raw[ref_start:ref_end]
                if len(projection) != ref["projection_bytes"] or digest_bytes(projection) != ref["projection_hash"]:
                    failures.append({
                        "event_rowid": ref["event_rowid"], "reason": "reference_hash_or_length_mismatch",
                    })
            prior_end = end
            covered += len(compressed)
    invalid_refs = db.execute(
        "SELECT COUNT(*) FROM search_projection_refs r "
        "LEFT JOIN search_projection_blocks b USING(block_key) "
        "WHERE b.block_key IS NULL OR r.projection_offset<0 OR r.projection_bytes<0 "
        "OR r.projection_offset+r.projection_bytes>b.raw_bytes "
        "OR b.compressed_hash IS NULL OR r.reference_hash IS NULL"
    ).fetchone()[0]
    if invalid_refs:
        failures.append({"reason": "invalid_references", "count": invalid_refs})
    file_orphan_bytes = max(0, size - covered)
    unreachable_block_bytes = max(0, covered - live_bytes)
    return {
        "pass": not failures, "blocks": len(blocks), "bytes": size,
        "live_bytes": live_bytes, "unreachable_block_bytes": unreachable_block_bytes,
        "file_orphan_bytes": file_orphan_bytes,
        "orphan_bytes": file_orphan_bytes + unreachable_block_bytes, "failures": failures,
    }


def verify_pack(db: sqlite3.Connection) -> dict[str, Any]:
    path = pack_path(db)
    pack_bytes = path.stat().st_size if path.exists() else 0
    cursor = 0
    gaps = []
    failures = []
    checked = 0
    for row in db.execute("SELECT * FROM objects ORDER BY pack_offset,object_hash"):
        start = row["pack_offset"]
        end = start + row["compressed_bytes"]
        if start < cursor:
            failures.append({"hash": row["object_hash"], "error": "overlapping pack extent"})
            continue
        if start > cursor:
            gaps.append({"offset": cursor, "bytes": start - cursor})
        if end > pack_bytes:
            failures.append({"hash": row["object_hash"], "error": "pack extent beyond file"})
        else:
            try:
                read_stored_object(db, row["object_hash"])
                checked += 1
            except Exception as exc:
                failures.append({"hash": row["object_hash"], "error": str(exc)})
        cursor = max(cursor, end)
    if cursor < pack_bytes:
        gaps.append({"offset": cursor, "bytes": pack_bytes - cursor})
    return {
        "pack_bytes": pack_bytes, "objects_checked": checked, "failures": failures,
        "orphan_extents": gaps, "orphan_bytes": sum(gap["bytes"] for gap in gaps),
        "pass": not failures,
    }


def report(db: sqlite3.Connection, db_path: Path) -> dict[str, Any]:
    scalar = lambda sql: db.execute(sql).fetchone()[0]
    raw_source = scalar("SELECT COALESCE(SUM(size_bytes),0) FROM sources")
    content_refs = scalar("SELECT COALESCE(SUM(o.raw_bytes),0) FROM event_content c JOIN objects o USING(object_hash)")
    unique_content = scalar(
        "SELECT COALESCE(SUM(raw_bytes),0) FROM objects WHERE object_hash IN "
        "(SELECT DISTINCT object_hash FROM event_content)"
    )
    shared_refs = scalar(
        "SELECT COALESCE(SUM(o.raw_bytes),0) FROM object_references r "
        "JOIN objects o ON o.object_hash=r.child_hash"
    )
    unique_shared = scalar(
        "SELECT COALESCE(SUM(raw_bytes),0) FROM objects WHERE object_hash IN "
        "(SELECT DISTINCT child_hash FROM object_references)"
    )
    pack_bytes = pack_path(db).stat().st_size if pack_path(db).exists() else 0
    retrieval_pack = projection_pack_path(db)
    retrieval_pack_bytes = retrieval_pack.stat().st_size if retrieval_pack.exists() else 0
    owned_retrieval_packs = sorted(db_path.parent.glob(db_path.name + ".retrieval.*"))
    retrieval_owned_bytes = sum(path.stat().st_size for path in owned_retrieval_packs)
    database_bytes = db_path.stat().st_size
    total_storage = pack_bytes + retrieval_owned_bytes + database_bytes
    pack_verification = verify_pack(db)
    retrieval_pack_verification = verify_projection_pack(db)
    return {
        "sources": scalar("SELECT COUNT(*) FROM sources"), "sessions": scalar("SELECT COUNT(*) FROM sessions"),
        "events": scalar("SELECT COUNT(*) FROM events"), "issues": scalar("SELECT COUNT(*) FROM events WHERE supported=0"),
        "raw_source_bytes": raw_source, "content_reference_bytes": content_refs,
        "unique_content_bytes": unique_content, "object_pack_bytes": pack_bytes,
        "retrieval_pack_bytes": retrieval_pack_bytes,
        "retrieval_owned_pack_bytes": retrieval_owned_bytes,
        "retrieval_owned_packs": [path.name for path in owned_retrieval_packs],
        "database_bytes": database_bytes, "total_storage_bytes": total_storage,
        "storage_change_bytes": total_storage - raw_source,
        "storage_change_percent": round((total_storage / raw_source - 1) * 100, 2) if raw_source else 0,
        "content_dedup_saved_bytes": max(content_refs - unique_content, 0),
        "known_instruction_objects": scalar("SELECT COUNT(*) FROM objects WHERE kind='known-instruction'"),
        "known_instruction_references": scalar("SELECT COUNT(*) FROM object_references WHERE purpose='global-claude-md'"),
        "shared_text_objects": scalar("SELECT COUNT(*) FROM objects WHERE kind IN ('known-instruction','shared-text')"),
        "shared_text_references": scalar("SELECT COUNT(*) FROM object_references"),
        "shared_text_reference_bytes": shared_refs,
        "shared_text_unique_bytes": unique_shared,
        "shared_text_dedup_saved_bytes": max(shared_refs - unique_shared, 0),
        "pack_verification": pack_verification,
        "retrieval_pack_verification": retrieval_pack_verification,
    }


def json_print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def write_record_stream(db: sqlite3.Connection, path: Path, args: argparse.Namespace) -> dict[str, Any]:
    """Write a resumable, snapshot-bound stream of retained provider records."""
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    snapshot_id = projection_snapshot_id(db)
    after_rowid = max(0, getattr(args, "after_rowid", 0))
    expected_snapshot = getattr(args, "snapshot_id", None)
    if after_rowid and not expected_snapshot:
        raise ValueError("record-stream resume requires --snapshot-id")
    if expected_snapshot and expected_snapshot != snapshot_id:
        raise ValueError("record-stream snapshot changed; restart without --after-rowid")
    clauses, values = _index_filter_clauses(args)
    if getattr(args, "category", None):
        clauses.append("e.category=?")
        values.append(args.category)
    if after_rowid:
        clauses.append("e.rowid>?")
        values.append(after_rowid)
    scope = " AND ".join(clauses) if clauses else "1=1"
    coverage_row = db.execute(
        "SELECT COUNT(*) AS records,"
        "SUM(CASE WHEN e.supported=0 THEN 1 ELSE 0 END) AS unsupported,"
        "SUM(CASE WHEN e.category='unreadable' THEN 1 ELSE 0 END) AS unreadable "
        "FROM events e WHERE " + scope,
        values,
    ).fetchone()
    retained_complete = not (coverage_row["unsupported"] or coverage_row["unreadable"])
    snapshot_only = bool(getattr(args, "snapshot_only", False))
    live_coverage: dict[str, Any] | str | None = None
    live_complete = False
    if not snapshot_only:
        live_coverage = query_coverage(db, args)
        live_complete = coverage_is_current(live_coverage)
    coverage = {
        "complete": retained_complete and (snapshot_only or live_complete),
        "scope": "retained_snapshot" if snapshot_only else "live_corpus",
        "retained_snapshot_complete": retained_complete,
        "live_corpus_complete": None if snapshot_only else live_complete,
        "live": live_coverage,
        "records": coverage_row["records"],
        "unsupported_records": coverage_row["unsupported"] or 0,
        "unreadable_records": coverage_row["unreadable"] or 0,
    }
    sql = (
        "SELECT e.rowid AS event_rowid,e.*,src.path AS source_path FROM events e "
        "JOIN sources src USING(source_id) WHERE "
        + scope + " ORDER BY e.rowid"
    )
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    count = 0
    last_rowid = after_rowid
    payload_hash = hashlib.sha256()
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            header = {
                "format": "sessiondb-record-stream-v1", "snapshot_id": snapshot_id,
                "redacted": not getattr(args, "exact", False),
                "after_rowid": last_rowid, "coverage": coverage,
            }
            line = json.dumps({"header": header}, ensure_ascii=False, separators=(",", ":")) + "\n"
            handle.write(line)
            payload_hash.update(line.encode())
            with forbid_source_transcript_reads() as source_reads:
                for row in db.execute(sql, values):
                    if row["category"] == "unreadable":
                        if getattr(args, "exact", False):
                            retained = read_event_content(db, row)
                            if not isinstance(retained, dict) or retained.get("encoding") != "base64":
                                raise ValueError(f"unreadable event is not retained losslessly: {row['event_id']}")
                            raw = base64.b64decode(retained["unreadable_bytes_base64"], validate=True)
                            if digest(raw) != row["raw_hash"]:
                                raise ValueError(f"retained unreadable event hash failed: {row['event_id']}")
                            structure = {
                                "encoding": "base64",
                                "unreadable_bytes_base64": retained["unreadable_bytes_base64"],
                                "sha256": row["raw_hash"],
                            }
                        else:
                            structure = {"unreadable": True}
                    else:
                        structure = read_event_structure(db, row)
                        if not getattr(args, "exact", False):
                            structure = redact_value(structure)
                    record = {
                        "rowid": row["event_rowid"], "event_id": row["event_id"],
                        "provider": row["provider"], "session": row["session_id"],
                        "project": row["project"], "timestamp": row["timestamp"],
                        "category": row["category"], "role": row["role"], "tool": row["tool"],
                        "supported": bool(row["supported"]), "issue": row["issue"],
                        "source": {"path": row["source_path"], "line": row["line_number"], "hash": row["raw_hash"]},
                        "structure": structure,
                    }
                    line = json.dumps({"record": record}, ensure_ascii=False, separators=(",", ":")) + "\n"
                    handle.write(line)
                    payload_hash.update(line.encode())
                    count += 1
                    last_rowid = row["event_rowid"]
            footer = {
                "records": count, "last_rowid": last_rowid,
                "payload_sha256": payload_hash.hexdigest(), "source_reads": source_reads["attempts"],
                "coverage": coverage,
            }
            handle.write(json.dumps({"footer": footer}, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            Path(temporary).unlink()
        except FileNotFoundError:
            pass
    return {"complete": coverage["complete"], "path": str(path), "snapshot_id": snapshot_id,
            "records": count, "last_rowid": last_rowid, "coverage": coverage,
            "source_reads": source_reads["attempts"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    p_ingest = commands.add_parser("ingest")
    p_ingest.add_argument("--provider", choices=ADAPTER_VERSION, required=True)
    p_ingest.add_argument("paths", nargs="+", type=Path)
    p_query = commands.add_parser("query")
    for name in ("text", "provider", "project", "session", "role", "tool", "lineage", "since", "until"):
        p_query.add_argument("--" + name)
    p_query.add_argument("--raw", action="store_true")
    p_query.add_argument("--clean", action="store_true")
    p_query.add_argument("--before", type=int, default=0, help="include preceding source events around each hit")
    p_query.add_argument("--after", type=int, default=0, help="include following source events around each hit")
    p_query.add_argument("--limit", type=int, default=50)
    p_query.add_argument("--offset", type=int, default=0)
    p_query.add_argument("--include-page", action="store_true", help="include page and context-boundary metadata")
    p_query.add_argument(
        "--page-envelope", action="store_true",
        help="wrap expanded events as {events, page} with has_more and next_offset; "
             "with --clean, include the full page",
    )
    p_query.add_argument("--include-hashes", action="store_true", help="include exact content-object hashes")
    p_query.add_argument(
        "--include-structure", action="store_true",
        help="include each stored event's complete structure; combine with --raw for unredacted values",
    )
    p_session = commands.add_parser("session")
    p_session.add_argument("--provider", choices=ADAPTER_VERSION, required=True)
    p_session.add_argument("session_id")
    p_session.add_argument("--raw", action="store_true")
    p_build = commands.add_parser("build-search-index")
    p_build.add_argument("--readiness-receipt", type=Path)
    p_upgrade = commands.add_parser("upgrade-phrase-index")
    p_upgrade.add_argument("--readiness-receipt", type=Path)
    p_fts = commands.add_parser("rebuild-search-fts")
    p_fts.add_argument("--detail", choices=("column", "full"), required=True)
    p_fts.add_argument("--accents", choices=("kept", "folded"), default="kept",
                       help="kept indexes words as written (default); folded is the older index")
    p_fts.add_argument("--offline", action="store_true",
                       help="rebuild in one locked transaction (for a copy); default builds beside the serving index")
    p_index = commands.add_parser("search-index")
    p_index.add_argument("--text", required=True)
    p_index.add_argument("--mode", choices=("ranked", "literal", "all_words"), default="ranked",
                         help=argparse.SUPPRESS)
    p_index.add_argument("--order", choices=("relevance", "chronological"), default="relevance")
    for name in ("provider", "project", "session", "role", "tool", "lineage", "since", "until"):
        p_index.add_argument("--" + name)
    p_index.add_argument("--bundle", type=Path)
    p_index.add_argument("--case-id", help="caller-owned case identity for safe continuations")
    p_index.add_argument("--manifest", type=Path, help="bounded frozen-source fallback manifest")
    p_index.add_argument("--fallback-max-bytes", type=int, default=FALLBACK_MAX_SOURCE_BYTES)
    p_index.add_argument("--fallback-deadline-seconds", type=float, default=FALLBACK_DEADLINE_SECONDS)
    p_index.add_argument("--deadline-seconds", type=float, default=SEARCH_DEADLINE_SECONDS)
    # Display-only: sets the inline passage count; the bundle and total keep every result.
    p_index.add_argument(
        "--limit", dest="passage_limit", type=_positive_int, default=INLINE_RESULT_LIMIT,
    )
    p_bundle = commands.add_parser("bundle-page")
    p_bundle.add_argument("--bundle", type=Path, required=True)
    p_bundle.add_argument("--case-id")
    p_bundle.add_argument("--continuation-id")
    p_bundle.add_argument("--snapshot-id")
    for name in ("provider", "project", "session", "role", "tool"):
        p_bundle.add_argument("--" + name)
    p_bundle.add_argument("--offset", type=int, default=0)
    p_bundle.add_argument("--limit", type=int, default=50)
    p_details = commands.add_parser("details")
    p_details.add_argument("--snapshot-id", required=True)
    p_details.add_argument("--result-id", dest="result_ids", action="append", required=True)
    p_details.add_argument("--bundle", type=Path, help="result bundle used to resolve r1/r2 aliases")
    p_details.add_argument("--case-id")
    p_details.add_argument("--continuation-id")
    p_details.add_argument("--before", type=int, default=0)
    p_details.add_argument("--after", type=int, default=0)
    p_details.add_argument("--readable", action="store_true",
                           help="show redacted content text without duplicated provider envelope; default retains full structure")
    p_details.add_argument("--readable-query", help="focus the first readable page near this search text")
    p_details.add_argument("--readable-offset", type=int,
                           help="continue readable text from this Unicode character offset")
    p_details.add_argument("--previous-detail-fingerprint",
                           help="detect an unchanged repeat of previously returned detail evidence")
    p_details.add_argument("--manifest", type=Path, help="bounded frozen-source fallback manifest")
    p_details.add_argument("--fallback-max-bytes", type=int, default=FALLBACK_MAX_SOURCE_BYTES)
    p_details.add_argument("--fallback-deadline-seconds", type=float, default=FALLBACK_DEADLINE_SECONDS)
    p_records = commands.add_parser("record-stream")
    p_records.add_argument("--output", type=Path, required=True)
    for name in ("provider", "project", "session", "role", "tool", "lineage", "since", "until", "category"):
        p_records.add_argument("--" + name)
    p_records.add_argument("--after-rowid", type=int, default=0)
    p_records.add_argument("--snapshot-id", help="required snapshot identity when resuming")
    p_records.add_argument(
        "--snapshot-only", action="store_true",
        help="declare analysis complete for this retained snapshot without claiming live-source freshness",
    )
    p_records.add_argument("--exact", action="store_true", help="include exact retained values instead of redacted values")
    commands.add_parser("report")
    args = parser.parse_args(argv)
    if args.command == "query" and args.clean and args.page_envelope:
        # Clean output already carries its page; the envelope request means "show all of it".
        args.include_page = True
    if args.command == "bundle-page":
        json_print(result_bundle_page(args.bundle, args, args.db))
        return 0
    if args.command == "search-index":
        result = search_index_entrypoint(args.db, args)
        json_print(bundle_if_oversized(args.db, result, args.bundle))
        return 0
    if args.command == "details":
        json_print(details_entrypoint(args.db, args))
        return 0
    if args.command == "record-stream":
        db = connect(args.db, read_only=True)
        try:
            db.execute("BEGIN")
            json_print(write_record_stream(db, args.output, args))
        finally:
            db.close()
        return 0
    args.db.parent.mkdir(parents=True, exist_ok=True)
    read_only = args.command in {"report", "query", "session"}
    database_started = time.monotonic()
    db = connect(args.db, read_only=read_only)
    if args.command == "init":
        json_print({"database": str(args.db), "status": "initialized"})
    elif args.command == "ingest":
        json_print(ingest_many(db, args.provider, args.paths))
    elif args.command == "query":
        with source_transcript_read_reason("sessiondb query --raw"):
            # One read snapshot for results and their freshness coverage.
            db.execute("BEGIN")
            connection_snapshot_seconds = _seconds_since(database_started)
            query_started = time.monotonic()
            if args.clean:
                result = query_clean(db, args)
                query_seconds = _seconds_since(query_started)
                delivery_started = time.monotonic()
                result = deliver_clean_query(args.db, result)
                delivery_seconds = _seconds_since(delivery_started)
            else:
                events, page = query_window(db, args)
                result = {"events": events, "page": page} if args.page_envelope else events
            if args.clean:
                json_print(attach_command_timing(
                    result, "query", database_started,
                    {
                        "connection_snapshot": connection_snapshot_seconds,
                        "query_hydration": query_seconds,
                        "bundle_delivery": delivery_seconds,
                    },
                ))
                notice = clean_query_truncation_notice(result.get("page"))
                if notice:
                    sys.stdout.flush()
                    print(notice, file=sys.stderr, flush=True)
            else:
                json_print(result)
                notice = None if args.page_envelope else expanded_query_truncation_notice(page)
                if notice:
                    # After stdout, so the bare list stays first in combined captures.
                    sys.stdout.flush()
                    print(notice, file=sys.stderr, flush=True)
    elif args.command == "session":
        with source_transcript_read_reason("sessiondb session --raw"):
            json_print(reconstruct(db, args.provider, args.session_id, args.raw))
    elif args.command == "build-search-index":
        json_print(build_search_projections(db, readiness_receipt=args.readiness_receipt))
    elif args.command == "upgrade-phrase-index":
        json_print(upgrade_phrase_index(db, readiness_receipt=args.readiness_receipt))
    elif args.command == "rebuild-search-fts":
        report_progress = lambda item: print(json.dumps(item), file=sys.stderr, flush=True)
        json_print(
            rebuild_search_fts(
                db, args.detail, progress=report_progress, keep_accents=args.accents == "kept",
            ) if args.offline
            else rebuild_search_fts_online(
                db, args.detail, progress=report_progress, keep_accents=args.accents == "kept",
            )
        )
    elif args.command == "report":
        json_print(report(db, args.db))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
