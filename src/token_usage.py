"""Token-usage projection: normalized token counts derived from stored provider records.

Accounting is applied row by row so ingestion can extend a source with only its appended rows:

- Claude inclusive input = base input + cache write + cache read. One provider message id is one
  call; a streamed restatement of it replaces the stored figures only when every growing figure
  (output, thinking) is at least the stored one. Each call's inclusive input is also a context
  snapshot, and `compact_boundary` contributes numeric pre/post snapshots only.
- Codex `token_usage_record` rows (native per-response usage) are additive and, once present in a
  rollout, replace its cumulative `token_count` accounting. Otherwise cumulative counters become
  per-rollout deltas, reconciled to the total. `last_token_usage` is a separate context snapshot;
  `context_compacted` is the compaction marker, and older compaction rows count only when the
  rollout has none.
- Lineage comes from provider identities only: a Claude child names its launcher through the
  launcher's `toolUseResult.agentId` row; a Codex rollout names its parent in `session_meta`.

Rows are kept per source (`source_key`) so a re-import or a removed transcript replaces exactly
its own rows. A call copied into another transcript (a fork or resume) is one call: the
`token_usage_events` and `token_context_events` views keep the observation from the earliest
source. No prompt, message, tool argument or output is stored, only counts and identifiers.

A failure in one source never fails ingestion: its new rows are discarded, the source is marked
`error` in `token_source_state`, and it stays frozen until `rebuild-token-usage` re-derives it.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional

TOKEN_PROJECTION_VERSION = "token-usage-v1"

TOKEN_SCHEMA = """
CREATE TABLE IF NOT EXISTS token_source_state (
  source_key INTEGER PRIMARY KEY, projection_version TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('ok','error')), error TEXT,
  last_line INTEGER NOT NULL, state_json TEXT NOT NULL
);
-- One source's view of a run. A Claude child run can be described by its own transcript and by
-- the launcher's; token_runs_effective merges them.
CREATE TABLE IF NOT EXISTS token_runs (
  source_key INTEGER NOT NULL, run_id TEXT NOT NULL, provider TEXT NOT NULL,
  session_id TEXT, root_run_id TEXT NOT NULL, parent_run_id TEXT, run_kind TEXT NOT NULL,
  model TEXT, reasoning_effort TEXT, started_at TEXT, ended_at TEXT, status TEXT NOT NULL,
  attributes_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY(source_key, run_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS token_runs_run_idx ON token_runs(run_id, source_key);
-- usage_stream: 'message' (Claude), 'responses' (Codex native) or 'cumulative' (Codex deltas).
CREATE TABLE IF NOT EXISTS token_usage (
  source_key INTEGER NOT NULL, event_id TEXT NOT NULL, run_id TEXT NOT NULL,
  provider TEXT NOT NULL, observed_at TEXT, sequence INTEGER NOT NULL, line_number INTEGER NOT NULL,
  input_tokens INTEGER, output_tokens INTEGER, cache_read_input_tokens INTEGER,
  cache_write_input_tokens INTEGER, cache_write_5m_input_tokens INTEGER,
  cache_write_1h_input_tokens INTEGER, reasoning_output_tokens INTEGER, total_tokens INTEGER,
  usage_stream TEXT NOT NULL, model TEXT, source_event_id TEXT,
  PRIMARY KEY(source_key, event_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS token_usage_event_idx ON token_usage(event_id, source_key);
CREATE INDEX IF NOT EXISTS token_usage_run_idx ON token_usage(run_id);
CREATE TABLE IF NOT EXISTS token_context (
  source_key INTEGER NOT NULL, event_id TEXT NOT NULL, run_id TEXT NOT NULL,
  provider TEXT NOT NULL, observed_at TEXT, sequence INTEGER NOT NULL, line_number INTEGER NOT NULL,
  input_context_tokens INTEGER, cached_input_context_tokens INTEGER, model_context_window INTEGER,
  event_kind TEXT NOT NULL CHECK(event_kind IN ('normal','compaction')),
  compaction_phase TEXT CHECK(compaction_phase IS NULL OR compaction_phase IN ('pre','post','marker')),
  fallback INTEGER NOT NULL DEFAULT 0, source_event_id TEXT,
  PRIMARY KEY(source_key, event_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS token_context_event_idx ON token_context(event_id, source_key);
CREATE INDEX IF NOT EXISTS token_context_run_idx ON token_context(run_id);
-- Codex cumulative snapshots already seen in a rollout (timestamp + counter digest).
CREATE TABLE IF NOT EXISTS token_codex_snapshots (
  source_key INTEGER NOT NULL, snapshot_key TEXT NOT NULL, PRIMARY KEY(source_key, snapshot_key)
) WITHOUT ROWID;
CREATE VIEW IF NOT EXISTS token_usage_events AS
  SELECT u.* FROM token_usage u
  WHERE u.source_key=(SELECT min(o.source_key) FROM token_usage o WHERE o.event_id=u.event_id);
CREATE VIEW IF NOT EXISTS token_context_events AS
  SELECT c.* FROM token_context c
  WHERE c.source_key=(SELECT min(o.source_key) FROM token_context o WHERE o.event_id=c.event_id);
CREATE VIEW IF NOT EXISTS token_runs_transcript AS
  SELECT r.run_id AS run_id, min(r.provider) AS provider, min(r.session_id) AS session_id,
    coalesce(max(CASE WHEN r.parent_run_id IS NOT NULL THEN r.root_run_id END), min(r.root_run_id))
      AS root_run_id,
    max(r.parent_run_id) AS parent_run_id,
    CASE WHEN max(r.run_kind='subagent') THEN 'subagent' ELSE min(r.run_kind) END AS run_kind,
    (SELECT x.model FROM token_runs x WHERE x.run_id=r.run_id AND x.model IS NOT NULL
      ORDER BY x.source_key LIMIT 1) AS model,
    (SELECT x.reasoning_effort FROM token_runs x WHERE x.run_id=r.run_id
      AND x.reasoning_effort IS NOT NULL ORDER BY x.source_key LIMIT 1) AS reasoning_effort,
    min(r.started_at) AS started_at, max(r.ended_at) AS ended_at,
    CASE max(CASE r.status WHEN 'completed' THEN 4 WHEN 'failed' THEN 3 WHEN 'cancelled' THEN 2
      WHEN 'timed_out' THEN 2 WHEN 'running' THEN 1 ELSE 0 END)
      WHEN 4 THEN 'completed' WHEN 3 THEN 'failed' WHEN 1 THEN 'running'
      WHEN 2 THEN (SELECT x.status FROM token_runs x WHERE x.run_id=r.run_id
        AND x.status IN ('cancelled','timed_out') ORDER BY x.source_key LIMIT 1)
      ELSE 'unknown' END AS status,
    (SELECT s.path FROM token_runs x JOIN sources s ON s.source_key=x.source_key
      WHERE x.run_id=r.run_id ORDER BY (x.started_at IS NULL), x.source_key LIMIT 1) AS source_ref
  FROM token_runs r GROUP BY r.run_id;
-- Lineage a delegation launcher recorded (`sessiondb record-launch`) for a child whose own
-- transcript does not name its parent (a Codex `exec` child reads as a root session).
CREATE TABLE IF NOT EXISTS token_launches (
  child_run_id TEXT PRIMARY KEY, parent_run_id TEXT NOT NULL, root_run_id TEXT NOT NULL,
  launched_at TEXT NOT NULL, model TEXT, reasoning_effort TEXT, context_mode TEXT, purpose TEXT,
  launcher TEXT NOT NULL, recorded_at TEXT NOT NULL
) WITHOUT ROWID;
"""

# Transcript lineage wins; a launch record fills the parent only for a run no transcript gives one.
# The root is the top of the parent chain (a child of a child belongs to the first session), taken
# as that ancestor's own root when SessionDB holds it; a cycle or an over-deep chain stops at 32.
RUNS_EFFECTIVE_VIEW = """CREATE VIEW token_runs_effective AS
  WITH RECURSIVE linked AS (
    SELECT b.run_id AS run_id, b.provider AS provider, b.session_id AS session_id,
      b.root_run_id AS own_root, coalesce(b.parent_run_id, l.parent_run_id) AS parent_run_id,
      CASE WHEN b.parent_run_id IS NULL AND l.child_run_id IS NOT NULL AND b.run_kind='root'
        THEN 'delegated' ELSE b.run_kind END AS run_kind,
      coalesce(b.model, l.model) AS model,
      coalesce(b.reasoning_effort, l.reasoning_effort) AS reasoning_effort,
      b.started_at AS started_at, b.ended_at AS ended_at, b.status AS status, b.source_ref AS source_ref
    FROM token_runs_transcript b LEFT JOIN token_launches l ON l.child_run_id=b.run_id
  ), chain(run_id, ancestor, depth) AS (
    SELECT run_id, run_id, 0 FROM linked
    UNION ALL
    SELECT c.run_id, p.parent_run_id, c.depth+1 FROM chain c JOIN linked p ON p.run_id=c.ancestor
    WHERE p.parent_run_id IS NOT NULL AND c.depth < 32
  ), top(run_id, ancestor, depth) AS (
    SELECT run_id, ancestor, max(depth) FROM chain GROUP BY run_id
  )
  SELECT k.run_id AS run_id, k.provider AS provider, k.session_id AS session_id,
    CASE WHEN t.depth=0 THEN k.own_root ELSE coalesce(a.own_root, t.ancestor) END AS root_run_id,
    k.parent_run_id AS parent_run_id, k.run_kind AS run_kind, k.model AS model,
    k.reasoning_effort AS reasoning_effort, k.started_at AS started_at, k.ended_at AS ended_at,
    k.status AS status, k.source_ref AS source_ref
  FROM linked k JOIN top t ON t.run_id=k.run_id LEFT JOIN linked a ON a.run_id=t.ancestor"""

TOKEN_TABLES = ("token_source_state", "token_runs", "token_usage", "token_context", "token_codex_snapshots")

_STATUS_RANK = {"unknown": 0, "running": 1, "cancelled": 2, "timed_out": 2, "failed": 3, "completed": 4}
# Claude reports a subagent launch outcome on the launching row; only the lifecycle word is read.
_LAUNCH_STATUS = {
    "completed": "completed", "success": "completed", "async_launched": "running",
    "running": "running", "in_progress": "running", "error": "failed", "failed": "failed",
    "cancelled": "cancelled", "canceled": "cancelled", "timed_out": "timed_out",
}
_CODEX_COUNTERS = (
    "input_tokens", "output_tokens", "cached_input_tokens", "cache_read_input_tokens",
    "cache_write_input_tokens", "reasoning_output_tokens", "total_tokens",
)
USAGE_COLUMNS = (
    "source_key", "event_id", "run_id", "provider", "observed_at", "sequence", "line_number",
    "input_tokens", "output_tokens", "cache_read_input_tokens", "cache_write_input_tokens",
    "cache_write_5m_input_tokens", "cache_write_1h_input_tokens", "reasoning_output_tokens",
    "total_tokens", "usage_stream", "model", "source_event_id",
)
CONTEXT_COLUMNS = (
    "source_key", "event_id", "run_id", "provider", "observed_at", "sequence", "line_number",
    "input_context_tokens", "cached_input_context_tokens", "model_context_window", "event_kind",
    "compaction_phase", "fallback", "source_event_id",
)
RUN_COLUMNS = (
    "source_key", "run_id", "provider", "session_id", "root_run_id", "parent_run_id", "run_kind",
    "model", "reasoning_effort", "started_at", "ended_at", "status", "attributes_json",
)


class TokenProjectionError(ValueError):
    """A record cannot be accounted safely; the source's token rows are left unchanged."""


def stable_id(*parts: object) -> str:
    return hashlib.sha256("\x1f".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def payload_hash(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def rfc3339(value: Any, *, required: bool = False) -> Optional[str]:
    if value is None or value == "":
        if required:
            raise TokenProjectionError("timestamp is required")
        return None
    if isinstance(value, bool):
        raise TokenProjectionError("invalid timestamp")
    if isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(value, timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise TokenProjectionError(f"invalid timestamp: {value}") from exc
    elif isinstance(value, str):
        match = re.fullmatch(r"(.+:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})", value)
        if not match:
            raise TokenProjectionError(f"invalid RFC 3339 timestamp: {value}")
        fraction = (match.group(2) or "").ljust(6, "0")[:6]
        offset = "+00:00" if match.group(3) == "Z" else match.group(3)
        try:
            parsed = datetime.fromisoformat(f"{match.group(1)}.{fraction}{offset}")
        except ValueError as exc:
            raise TokenProjectionError(f"invalid RFC 3339 timestamp: {value}") from exc
    else:
        raise TokenProjectionError("invalid timestamp")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _token(source: Any, name: str) -> Optional[int]:
    if not isinstance(source, dict) or source.get(name) is None:
        return None
    value = source[name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TokenProjectionError(f"invalid token value for {name}")
    return value


def _sum_present(*values: Optional[int]) -> Optional[int]:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _dominates(later: tuple, earlier: tuple) -> bool:
    for new_value, old_value in zip(later, earlier):
        if old_value is None:
            continue
        if new_value is None or new_value < old_value:
            return False
    return True


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(TOKEN_SCHEMA)
    current = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='view' AND name='token_runs_effective'").fetchone()
    if current is None or current[0] != RUNS_EFFECTIVE_VIEW:
        # Views hold no data; an older definition (before launch lineage) is replaced in place.
        db.execute("DROP VIEW IF EXISTS token_runs_effective")
        db.execute(RUNS_EFFECTIVE_VIEW)


def record_launch(db: sqlite3.Connection, child: str, parent: str, root: str | None, launched_at: str,
                  launcher: str, model: str | None = None, reasoning_effort: str | None = None,
                  context_mode: str | None = None, purpose: str | None = None) -> dict[str, Any]:
    """Record one launch; repeating it is a no-op, a different parent for the same child is refused."""
    root = root or parent
    existing = db.execute(
        "SELECT parent_run_id, root_run_id FROM token_launches WHERE child_run_id=?", (child,)).fetchone()
    if existing is not None:
        if (existing[0], existing[1]) != (parent, root):
            raise ValueError(f"launch for {child} already recorded with parent {existing[0]}")
        return {"recorded": False, "child_run_id": child, "reason": "already recorded"}
    db.execute(
        "INSERT INTO token_launches(child_run_id,parent_run_id,root_run_id,launched_at,model,"
        "reasoning_effort,context_mode,purpose,launcher,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (child, parent, root, launched_at, model, reasoning_effort, context_mode, purpose, launcher,
         datetime.now(timezone.utc).isoformat(timespec="seconds")))
    return {"recorded": True, "child_run_id": child, "parent_run_id": parent, "root_run_id": root}


def import_ledger_launches(db: sqlite3.Connection, ledger: sqlite3.Connection) -> dict[str, Any]:
    """Copy registered launch links (one per child); transcript lineage still wins in the view."""
    rows = ledger.execute(
        "SELECT child_run_id, parent_run_id, root_run_id, min(launched_at), model, reasoning_effort,"
        " context_mode, purpose FROM delegations GROUP BY child_run_id").fetchall()
    counts = {"ledger_launches": len(rows), "recorded": 0, "already": 0, "conflicts": []}
    for child, parent, root, launched_at, model, effort, mode, purpose in rows:
        try:
            result = record_launch(db, child, parent, root, launched_at, "token-ledger-import",
                                   model, effort, mode, purpose)
        except ValueError as exc:
            counts["conflicts"].append(str(exc))
            continue
        counts["recorded" if result["recorded"] else "already"] += 1
    return counts


def reset_source(db: sqlite3.Connection, source_key: int) -> None:
    for table in TOKEN_TABLES:
        db.execute(f"DELETE FROM {table} WHERE source_key=?", (source_key,))


class SourceProjector:
    """Accumulate one source's token rows for one ingest batch, then write them atomically.

    `feed` reads only; `finish` writes inside a savepoint. Any error during either discards the
    batch and records the source as `error` instead of raising.
    """

    def __init__(self, db: sqlite3.Connection, provider: str, source_key: int, path: Path,
                 reset: bool) -> None:
        self.db = db
        self.provider = provider
        self.source_key = source_key
        self.path = Path(path)
        self.reset = reset
        self.error: Optional[str] = None
        self.disabled = False
        self.last_line = 0
        self.runs: dict[str, dict[str, Any]] = {}
        self.usage: dict[str, dict[str, Any]] = {}
        self.context: dict[str, dict[str, Any]] = {}
        self.snapshots: set[str] = set()
        self.drop_streams: set[str] = set()
        self.drop_fallback_context = False
        stored = None if reset else db.execute(
            "SELECT * FROM token_source_state WHERE source_key=?", (source_key,),
        ).fetchone()
        if stored is not None and (
            stored["status"] != "ok" or stored["projection_version"] != TOKEN_PROJECTION_VERSION
        ):
            # A failed or outdated source stays frozen until it is re-derived whole.
            self.disabled = True
            self.state: dict[str, Any] = {}
            return
        if stored is None and not reset:
            # Never projected whole (it predates the projection): appended rows alone would
            # give wrong Codex deltas, so wait for re-derivation.
            self.disabled = True
            self.state = {}
            return
        self.state = json.loads(stored["state_json"]) if stored is not None else {}
        self.last_line = stored["last_line"] if stored is not None else 0
        if stored is not None:
            for row in db.execute("SELECT * FROM token_runs WHERE source_key=?", (source_key,)):
                run = dict(row)
                run["attributes"] = json.loads(run.pop("attributes_json"))
                self.runs[run["run_id"]] = run

    def feed(self, line_number: int, row: Any) -> None:
        if self.disabled or self.error is not None or not isinstance(row, dict):
            return
        try:
            if self.provider == "claude":
                self._claude_row(line_number, row)
            else:
                self._codex_row(line_number, row)
            self.last_line = max(self.last_line, line_number)
        except (TokenProjectionError, ValueError, TypeError, KeyError) as exc:
            self.error = f"line {line_number}: {exc}"[:500]

    def finish(self) -> dict[str, Any]:
        if self.disabled:
            return {"status": "frozen"}
        db = self.db
        db.execute("SAVEPOINT token_projection")
        try:
            if self.error is None:
                if self.provider == "codex":
                    self._codex_finish()
                self._write()
            if self.error is not None:
                raise TokenProjectionError(self.error)
            db.execute("RELEASE token_projection")
            return {"status": "ok", "usage": len(self.usage), "context": len(self.context)}
        except Exception as exc:  # isolate: ingestion of the source itself must still commit
            db.execute("ROLLBACK TO token_projection")
            db.execute("RELEASE token_projection")
            message = self.error or f"{type(exc).__name__}: {exc}"[:500]
            if self.reset:
                reset_source(db, self.source_key)
            db.execute(
                "INSERT OR REPLACE INTO token_source_state VALUES(?,?,?,?,?,?)",
                (self.source_key, TOKEN_PROJECTION_VERSION, "error", message, self.last_line, "{}"),
            )
            return {"status": "error", "error": message}

    def _stored_usage(self, event_id: str) -> Optional[dict[str, Any]]:
        if event_id in self.usage:
            return self.usage[event_id]
        if self.reset:
            return None
        row = self.db.execute(
            "SELECT * FROM token_usage WHERE source_key=? AND event_id=?", (self.source_key, event_id),
        ).fetchone()
        return dict(row) if row else None

    def _has_context(self, event_id: str) -> bool:
        if event_id in self.context:
            return True
        if self.reset:
            return False
        return self.db.execute(
            "SELECT 1 FROM token_context WHERE source_key=? AND event_id=?", (self.source_key, event_id),
        ).fetchone() is not None

    def _snapshot_seen(self, key: str) -> bool:
        if key in self.snapshots:
            return True
        if not self.reset and self.db.execute(
            "SELECT 1 FROM token_codex_snapshots WHERE source_key=? AND snapshot_key=?",
            (self.source_key, key),
        ).fetchone():
            return True
        self.snapshots.add(key)
        return False

    def _add_context(self, **values: Any) -> None:
        if self._has_context(values["event_id"]):
            return
        self.state["context_sequence"] = self.state.get("context_sequence", 0) + 1
        values.setdefault("fallback", 0)
        self.context[values["event_id"]] = dict(
            values, source_key=self.source_key, provider=self._provider_name(),
            sequence=self.state["context_sequence"],
        )

    def _provider_name(self) -> str:
        return "anthropic" if self.provider == "claude" else "codex"

    def _write(self) -> None:
        db = self.db
        if self.reset:
            reset_source(db, self.source_key)
        for stream in self.drop_streams:
            db.execute(
                "DELETE FROM token_usage WHERE source_key=? AND usage_stream=?", (self.source_key, stream),
            )
        if self.drop_fallback_context:
            db.execute("DELETE FROM token_context WHERE source_key=? AND fallback=1", (self.source_key,))
        if self.usage:
            db.executemany(
                f"INSERT OR REPLACE INTO token_usage({','.join(USAGE_COLUMNS)}) "
                f"VALUES({','.join('?' * len(USAGE_COLUMNS))})",
                [tuple(item.get(column) for column in USAGE_COLUMNS) for item in self.usage.values()],
            )
        if self.context:
            db.executemany(
                f"INSERT OR IGNORE INTO token_context({','.join(CONTEXT_COLUMNS)}) "
                f"VALUES({','.join('?' * len(CONTEXT_COLUMNS))})",
                [tuple(item.get(column) for column in CONTEXT_COLUMNS) for item in self.context.values()],
            )
        if self.snapshots:
            db.executemany(
                "INSERT OR IGNORE INTO token_codex_snapshots VALUES(?,?)",
                [(self.source_key, key) for key in self.snapshots],
            )
        for run in self.runs.values():
            values = dict(run, source_key=self.source_key,
                          attributes_json=json.dumps(run.get("attributes") or {}, sort_keys=True))
            db.execute(
                f"INSERT OR REPLACE INTO token_runs({','.join(RUN_COLUMNS)}) "
                f"VALUES({','.join('?' * len(RUN_COLUMNS))})",
                tuple(values.get(column) for column in RUN_COLUMNS),
            )
        db.execute(
            "INSERT OR REPLACE INTO token_source_state VALUES(?,?,?,?,?,?)",
            (self.source_key, TOKEN_PROJECTION_VERSION, "ok", None, self.last_line,
             json.dumps(self.state, sort_keys=True, separators=(",", ":"))),
        )

    def _claude_run(self, run_id: str, session_id: str, child: bool, observed_at: Optional[str]) -> dict:
        run = self.runs.get(run_id)
        if run is None:
            run = {
                "run_id": run_id, "provider": "anthropic", "session_id": session_id,
                "root_run_id": run_id if child else session_id, "parent_run_id": None,
                "run_kind": "subagent" if child else "root", "model": None, "reasoning_effort": None,
                "started_at": observed_at, "ended_at": observed_at, "status": "unknown",
                "attributes": {"claude_session_id": session_id},
            }
            self.runs[run_id] = run
        return run

    def _claude_row(self, line_number: int, row: dict[str, Any]) -> None:
        session_id = row.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            return
        agent_id = row.get("agentId")
        child = row.get("isSidechain") is True or (isinstance(agent_id, str) and bool(agent_id))
        if child:
            discriminator = agent_id if isinstance(agent_id, str) and agent_id else "sidechain"
            run_id = stable_id("claude", "agent", session_id, discriminator)
        else:
            run_id = session_id
        observed_at = rfc3339(row.get("timestamp"))
        message = row.get("message")
        model = message.get("model") if isinstance(message, dict) else None
        run = self._claude_run(run_id, session_id, child, observed_at)
        if isinstance(model, str) and model and run["model"] is None:
            run["model"] = model
        if observed_at:
            if run["started_at"] is None or observed_at < run["started_at"]:
                run["started_at"] = observed_at
            if run["ended_at"] is None or observed_at > run["ended_at"]:
                run["ended_at"] = observed_at
        status = row.get("status")
        if isinstance(status, str) and status in {"completed", "failed", "cancelled", "running", "unknown"}:
            run["status"] = status
        self._claude_launch(row, session_id, run_id)

        compact = row.get("compactMetadata")
        if row.get("type") == "system" and row.get("subtype") == "compact_boundary" and isinstance(compact, dict):
            boundary_id = row.get("uuid")
            if not isinstance(boundary_id, str) or not boundary_id:
                boundary_id = stable_id("claude-boundary", session_id, line_number)
            for phase, name in (("pre", "preTokens"), ("post", "postTokens")):
                value = _token(compact, name)
                if value is None:
                    continue
                self._add_context(
                    event_id=stable_id("claude", "context", boundary_id, phase), run_id=run_id,
                    observed_at=observed_at, line_number=line_number, input_context_tokens=value,
                    event_kind="compaction", compaction_phase=phase,
                    source_event_id=boundary_id + ":" + phase,
                )

        if not isinstance(message, dict) or not isinstance(message.get("usage"), dict):
            return
        usage = message["usage"]
        message_id = message.get("id")
        if not isinstance(message_id, str) or not message_id:
            message_id = row.get("uuid")
        if not isinstance(message_id, str) or not message_id:
            message_id = stable_id("claude-row", session_id, line_number)
        input_base = _token(usage, "input_tokens")
        cache_write = _token(usage, "cache_creation_input_tokens")
        cache_read = _token(usage, "cache_read_input_tokens")
        output = _token(usage, "output_tokens")
        details = usage.get("output_tokens_details")
        thinking = _token(details, "thinking_tokens") if isinstance(details, dict) else None
        created = usage.get("cache_creation")
        write_5m = _token(created, "ephemeral_5m_input_tokens") if isinstance(created, dict) else None
        write_1h = _token(created, "ephemeral_1h_input_tokens") if isinstance(created, dict) else None
        normalized_input = _sum_present(input_base, cache_write, cache_read)
        event_id = stable_id("claude", "usage", message_id)
        stored = self._stored_usage(event_id)
        if stored is not None:
            fixed = (stored["input_tokens"], stored["cache_read_input_tokens"],
                     stored["cache_write_input_tokens"], stored["run_id"])
            if fixed != (normalized_input, cache_read, cache_write, run_id):
                raise TokenProjectionError(f"conflicting usage for Claude provider message {message_id!r}")
            stored_growth = (stored["output_tokens"], stored["reasoning_output_tokens"])
            incoming = (output, thinking)
            if incoming == stored_growth or _dominates(stored_growth, incoming):
                return
            if not _dominates(incoming, stored_growth):
                raise TokenProjectionError(f"incomparable streamed usage for Claude provider message {message_id!r}")
            updated = dict(stored)
            updated.update(
                observed_at=observed_at or stored["observed_at"], output_tokens=output,
                reasoning_output_tokens=thinking, total_tokens=_sum_present(normalized_input, output),
            )
            self.usage[event_id] = updated
            return
        self.state["usage_sequence"] = self.state.get("usage_sequence", 0) + 1
        self.usage[event_id] = {
            "source_key": self.source_key, "event_id": event_id, "run_id": run_id,
            "provider": "anthropic", "observed_at": observed_at,
            "sequence": self.state["usage_sequence"], "line_number": line_number,
            "input_tokens": normalized_input, "output_tokens": output,
            "cache_read_input_tokens": cache_read, "cache_write_input_tokens": cache_write,
            "cache_write_5m_input_tokens": write_5m, "cache_write_1h_input_tokens": write_1h,
            "reasoning_output_tokens": thinking, "total_tokens": _sum_present(normalized_input, output),
            "usage_stream": "message", "model": model if isinstance(model, str) and model else None,
            "source_event_id": message_id,
        }
        self._add_context(
            event_id=stable_id("claude", "context", message_id), run_id=run_id,
            observed_at=observed_at, line_number=line_number, input_context_tokens=normalized_input,
            cached_input_context_tokens=cache_read, event_kind="normal", compaction_phase=None,
            source_event_id=message_id,
        )

    def _claude_launch(self, row: dict[str, Any], session_id: str, launcher_run_id: str) -> None:
        """A child named by the launcher's own row (`toolUseResult.agentId`) gains its parent."""
        result = row.get("toolUseResult")
        if not isinstance(result, dict):
            return
        child_agent_id = result.get("agentId")
        if not isinstance(child_agent_id, str) or not child_agent_id:
            return
        launcher_agent_id = row.get("agentId")
        if row.get("isSidechain") is True and not (isinstance(launcher_agent_id, str) and launcher_agent_id):
            return
        child_run_id = stable_id("claude", "agent", session_id, child_agent_id)
        if child_run_id == launcher_run_id:
            return
        launcher = self.runs[launcher_run_id]
        child = self._claude_run(child_run_id, session_id, True, None)
        child["parent_run_id"] = launcher_run_id
        child["root_run_id"] = launcher["root_run_id"]
        child["run_kind"] = "subagent"
        status = result.get("status")
        run_status = _LAUNCH_STATUS.get(status if isinstance(status, str) else "", "unknown")
        if _STATUS_RANK.get(run_status, 0) > _STATUS_RANK.get(child["status"], 0):
            child["status"] = run_status

    def _codex_identity(self) -> dict[str, Any]:
        """Provider metadata, from the streaming state."""
        state = self.state
        meta = state.get("meta") or {}
        session_id = str(meta.get("id") or meta.get("session_id") or self.path.stem)
        parent_id = str(meta["parent_thread_id"]) if meta.get("parent_thread_id") else None
        source = meta.get("source")
        kind = "unclassified"
        root = meta.get("thread_source") == "user" and not parent_id
        if isinstance(source, dict):
            subagent = source.get("subagent")
            if isinstance(subagent, dict):
                if subagent.get("other") == "guardian":
                    kind = "guardian"
                spawn = subagent.get("thread_spawn")
                if isinstance(spawn, dict):
                    kind = "deliberate_subagent"
                    if spawn.get("parent_thread_id"):
                        parent_id = str(spawn["parent_thread_id"])
        if meta.get("thread_source") == "subagent" and source == "vscode" and not parent_id:
            delegated = state.get("delegated_parent")
            if delegated:
                parent_id = delegated
                kind = "delegated_task"
        if kind == "unclassified" and root:
            kind = "root"
        if (kind == "unclassified" and meta.get("thread_source") is None and source == "vscode"
                and not parent_id and state.get("has_user_message")):
            kind = "root"
        explicit_root = meta.get("root_thread_id") or meta.get("root_session_id") or meta.get("root_id")
        root_id = str(explicit_root) if explicit_root else (parent_id or session_id)
        normalized = {"root": "root", "deliberate_subagent": "subagent", "delegated_task": "delegated",
                      "guardian": "guardian", "unclassified": "unknown"}[kind]
        return {"session_id": session_id, "parent_id": parent_id, "root_id": root_id, "kind": normalized}

    def _codex_row(self, line_number: int, record: dict[str, Any]) -> None:
        state = self.state
        record_type = record.get("type")
        payload = record.get("payload")
        location = f"line {line_number}"
        if "first_timestamp" not in state and record.get("timestamp") is not None:
            state["first_timestamp"] = rfc3339(record.get("timestamp"))
        if record_type == "session_meta" and isinstance(payload, dict):
            state["meta_count"] = state.get("meta_count", 0) + 1
            if "meta" not in state:
                keys = ("id", "session_id", "parent_thread_id", "thread_source", "source",
                        "root_thread_id", "root_session_id", "root_id", "model", "reasoning_effort")
                state["meta"] = {key: payload[key] for key in keys if key in payload}
                model = payload.get("model")
                if isinstance(model, str):
                    self._codex_add_model(model)
                    if state.get("context_model") is None and not state.get("context_seen"):
                        state["context_model"] = model
        if _codex_is_user_message(record):
            state["has_user_message"] = True
        if (record_type == "event_msg" and isinstance(payload, dict) and payload.get("type") == "user_message"
                and not state.get("first_user_message_seen")):
            state["first_user_message_seen"] = True
            message = payload.get("message")
            if isinstance(message, str):
                marker, closing = "<source_thread_id>", "</source_thread_id>"
                if message.lstrip().startswith("<codex_delegation>") and marker in message and closing in message:
                    candidate = message.split(marker, 1)[1].split(closing, 1)[0].strip()
                    if candidate:
                        state["delegated_parent"] = candidate
        if record_type == "turn_context" and isinstance(payload, dict):
            model = payload.get("model")
            if isinstance(model, str):
                self._codex_add_model(model)
            state["context_seen"] = True
            state["context_model"] = model if isinstance(model, str) else None
            state["service_tier"] = payload.get("service_tier") if isinstance(payload.get("service_tier"), str) else None
            return
        if record_type == "event_msg" and isinstance(payload, dict) and payload.get("type") == "task_complete":
            state["completed"] = True
        if record_type == "token_usage_record":
            self._codex_native(line_number, record, payload, location)
            return
        if record_type == "event_msg" and isinstance(payload, dict):
            stamp = _codex_timestamp(record, location)
            event_type = payload.get("type")
            if event_type == "token_count":
                if not state.get("native"):
                    self._codex_cumulative(line_number, payload, stamp, location)
                self._codex_context(line_number, payload, stamp, location)
            elif event_type == "context_compacted":
                if not state.get("preferred_compaction"):
                    state["preferred_compaction"] = True
                    self.drop_fallback_context = True
                    for key in [k for k, v in self.context.items() if v.get("fallback")]:
                        del self.context[key]
                self._codex_compaction(line_number, payload, stamp, location, fallback=False, source=payload)
                return
        is_fallback = record_type == "compacted" or (
            record_type == "event_msg" and isinstance(payload, dict)
            and payload.get("type") in {"compacted", "context_compaction"}
        )
        if is_fallback and not state.get("preferred_compaction"):
            stamp = _codex_timestamp(record, location)
            source = payload if isinstance(payload, dict) else record
            self._codex_compaction(line_number, payload, stamp, location, fallback=True, source=source)

    def _codex_add_model(self, model: str) -> None:
        models = set(self.state.get("models") or [])
        models.add(model)
        self.state["models"] = sorted(models)

    def _codex_run_id(self) -> str:
        return self._codex_identity()["session_id"]

    def _codex_native(self, line_number: int, record: dict[str, Any], payload: Any, location: str) -> None:
        state = self.state
        if not state.get("native"):
            state["native"] = True
            self.drop_streams.add("cumulative")
            for key in [k for k, v in self.usage.items() if v["usage_stream"] == "cumulative"]:
                del self.usage[key]
        if not isinstance(payload, dict):
            return
        run_id = self._codex_run_id()
        if payload.get("thread_id", payload.get("session_id")) != run_id:
            return
        response_id = payload.get("response_id")
        usage = payload.get("usage")
        if not isinstance(response_id, str) or not response_id or not isinstance(usage, dict):
            raise TokenProjectionError("missing response identity/usage at " + location)
        safe = {key: _codex_int(usage.get(key), key, location) for key in _CODEX_COUNTERS}
        if safe["cache_read_input_tokens"] is None:
            safe["cache_read_input_tokens"] = safe["cached_input_tokens"]
        event_id = stable_id("codex", run_id, "response", response_id)
        stored = self._stored_usage(event_id)
        if stored is not None:
            previous = (stored["input_tokens"], stored["output_tokens"], stored["cache_read_input_tokens"],
                        stored["cache_write_input_tokens"], stored["reasoning_output_tokens"], stored["total_tokens"])
            current = (safe["input_tokens"], safe["output_tokens"], safe["cache_read_input_tokens"],
                       safe["cache_write_input_tokens"], safe["reasoning_output_tokens"], safe["total_tokens"])
            if previous != current:
                raise TokenProjectionError("conflicting response usage at " + location)
            return
        state["usage_sequence"] = state.get("usage_sequence", 0) + 1
        self.usage[event_id] = {
            "source_key": self.source_key, "event_id": event_id, "run_id": run_id, "provider": "codex",
            "observed_at": _codex_timestamp(record, location), "sequence": state["usage_sequence"],
            "line_number": line_number, "input_tokens": safe["input_tokens"],
            "output_tokens": safe["output_tokens"], "cache_read_input_tokens": safe["cache_read_input_tokens"],
            "cache_write_input_tokens": safe["cache_write_input_tokens"],
            "reasoning_output_tokens": safe["reasoning_output_tokens"], "total_tokens": safe["total_tokens"],
            "usage_stream": "responses", "model": state.get("context_model"), "source_event_id": response_id,
        }

    def _codex_cumulative(self, line_number: int, payload: dict, stamp: str, location: str) -> None:
        info = payload.get("info")
        if info is None:
            return
        if not isinstance(info, dict):
            raise TokenProjectionError("invalid token_count info at " + location)
        totals = info.get("total_token_usage")
        if not isinstance(totals, dict):
            raise TokenProjectionError("missing total token usage at " + location)
        if "total_tokens" not in totals:
            raise TokenProjectionError("missing total_tokens at " + location)
        raw = {key: _codex_int(totals.get(key), key, location) for key in _CODEX_COUNTERS}
        cached = raw["cache_read_input_tokens"]
        if cached is None:
            cached = raw["cached_input_tokens"]
        raw["cache_read_input_tokens"] = cached
        safe_snapshot = {key: raw[key] for key in _CODEX_COUNTERS if raw[key] is not None}
        snapshot_id = payload_hash(safe_snapshot)
        if self._snapshot_seen(stamp + "\x1f" + snapshot_id):
            return
        previous = self.state.setdefault("previous", {})
        delta: dict[str, Optional[int]] = {}
        for key, current in raw.items():
            if current is None:
                delta[key] = None
                continue
            before = previous.get(key)
            delta[key] = current if before is None or current < before else current - before
            previous[key] = current
        total, input_tokens, output_tokens = delta["total_tokens"], delta["input_tokens"], delta["output_tokens"]
        if total is not None and input_tokens is not None and output_tokens is not None:
            output_tokens = min(output_tokens, total)
            input_tokens = total - output_tokens
            delta["input_tokens"], delta["output_tokens"] = input_tokens, output_tokens
        if input_tokens is not None:
            for key in ("cache_read_input_tokens", "cache_write_input_tokens"):
                if delta[key] is not None:
                    delta[key] = min(delta[key], input_tokens)
        if output_tokens is not None and delta["reasoning_output_tokens"] is not None:
            delta["reasoning_output_tokens"] = min(delta["reasoning_output_tokens"], output_tokens)
        if not any(value for value in delta.values() if value is not None):
            return
        run_id = self._codex_run_id()
        self.state["usage_sequence"] = self.state.get("usage_sequence", 0) + 1
        event_id = stable_id("codex", run_id, stamp, snapshot_id)
        self.usage[event_id] = {
            "source_key": self.source_key, "event_id": event_id, "run_id": run_id, "provider": "codex",
            "observed_at": stamp, "sequence": self.state["usage_sequence"], "line_number": line_number,
            "input_tokens": delta["input_tokens"], "output_tokens": delta["output_tokens"],
            "cache_read_input_tokens": delta["cache_read_input_tokens"],
            "cache_write_input_tokens": delta["cache_write_input_tokens"],
            "reasoning_output_tokens": delta["reasoning_output_tokens"], "total_tokens": delta["total_tokens"],
            "usage_stream": "cumulative", "model": self.state.get("context_model"),
            "source_event_id": str(payload["id"]) if payload.get("id") is not None else None,
        }

    def _codex_context(self, line_number: int, payload: dict, stamp: str, location: str) -> None:
        info = payload.get("info")
        last = info.get("last_token_usage") if isinstance(info, dict) else None
        if not isinstance(last, dict):
            return
        input_tokens = _codex_int(last.get("input_tokens"), "last_token_usage.input_tokens", location)
        cached = _codex_int(last.get("cache_read_input_tokens"), "last_token_usage.cache_read_input_tokens", location)
        if cached is None:
            cached = _codex_int(last.get("cached_input_tokens"), "last_token_usage.cached_input_tokens", location)
        window = _codex_int(info.get("model_context_window"), "info.model_context_window", location)
        if input_tokens is None and cached is None and window is None:
            return
        digest = payload_hash({"input_context_tokens": input_tokens, "cached_input_context_tokens": cached,
                               "model_context_window": window})
        self._add_context(
            event_id=stable_id("codex", self._codex_run_id(), "context", stamp, digest),
            run_id=self._codex_run_id(), observed_at=stamp, line_number=line_number,
            input_context_tokens=input_tokens, cached_input_context_tokens=cached,
            model_context_window=window, event_kind="normal", compaction_phase=None,
            source_event_id=str(payload["id"]) if payload.get("id") is not None else None,
        )

    def _codex_compaction(self, line_number: int, payload: Any, stamp: str, location: str,
                          fallback: bool, source: dict) -> None:
        info = source.get("info") if isinstance(source.get("info"), dict) else source
        safe = {
            "input_context_tokens": _codex_int(info.get("input_tokens"), "compaction.input_tokens", location),
            "cached_input_context_tokens": _codex_int(
                info.get("cache_read_input_tokens", info.get("cached_input_tokens")),
                "compaction.cached_input_tokens", location),
            "model_context_window": _codex_int(info.get("model_context_window"),
                                               "compaction.model_context_window", location),
        }
        digest = payload_hash(safe)
        run_id = self._codex_run_id()
        self._add_context(
            event_id=stable_id("codex", run_id, "compaction", stamp, digest), run_id=run_id,
            observed_at=stamp, line_number=line_number, event_kind="compaction",
            compaction_phase="marker", fallback=1 if fallback else 0,
            source_event_id=str(source["id"]) if source.get("id") is not None else None, **safe,
        )

    def _codex_finish(self) -> None:
        state = self.state
        if "meta" not in state and not state.get("usage_sequence") and not state.get("context_sequence"):
            # Not a rollout (the Codex session index) or nothing accountable yet: no run.
            self.runs = {}
            if not self.reset:
                self.db.execute("DELETE FROM token_runs WHERE source_key=?", (self.source_key,))
            return
        identity = self._codex_identity()
        meta = state.get("meta") or {}
        models = state.get("models") or []
        effort = meta.get("reasoning_effort")
        self.runs = {identity["session_id"]: {
            "run_id": identity["session_id"], "provider": "codex", "session_id": identity["session_id"],
            "root_run_id": identity["root_id"], "parent_run_id": identity["parent_id"],
            "run_kind": identity["kind"], "model": models[0] if len(models) == 1 else None,
            "reasoning_effort": effort if isinstance(effort, str) else None,
            "started_at": state.get("first_timestamp"), "ended_at": None,
            "status": "completed" if state.get("completed") else "running",
            "attributes": {"session_meta_count": state.get("meta_count", 0),
                           "usage_stream": "responses" if state.get("native") else "cumulative",
                           "models": models},
        }}
        if not self.reset:
            # The run identity is derived from the whole rollout; drop a row stored under an
            # earlier identity (a rollout whose session_meta arrived late).
            self.db.execute(
                "DELETE FROM token_runs WHERE source_key=? AND run_id!=?", (self.source_key, identity["session_id"]),
            )


def _codex_int(value: Any, field: str, location: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TokenProjectionError(f"invalid {field} at {location}")
    return value


def _codex_timestamp(record: dict[str, Any], location: str) -> str:
    value = record.get("timestamp")
    if not isinstance(value, str) or not value:
        raise TokenProjectionError("missing timestamp at " + location)
    return rfc3339(value, required=True)


def _codex_is_user_message(record: dict[str, Any]) -> bool:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return False
    if record.get("type") == "event_msg":
        return payload.get("type") == "user_message"
    return (record.get("type") == "response_item" and payload.get("type") == "message"
            and payload.get("role") == "user")


def stored_source_rows(db: sqlite3.Connection, source_key: int,
                       read_raw_event: Callable[[sqlite3.Connection, sqlite3.Row], Any],
                       event_columns: str, event_join: str) -> Iterator[tuple[int, Any]]:
    """Yield (line, exact provider row) for one source from SessionDB's own retained records."""
    rows = db.execute(
        f"SELECT {event_columns} FROM {event_join} WHERE e.source_key=? AND e.category!='unreadable' "
        "ORDER BY e.line_number", (source_key,),
    )
    for row in rows:
        yield row["line_number"], read_raw_event(db, row)


def project_source(db: sqlite3.Connection, provider: str, source_key: int, path: str,
                   rows: Iterable[tuple[int, Any]]) -> dict[str, Any]:
    projector = SourceProjector(db, provider, source_key, Path(path), reset=True)
    for line_number, row in rows:
        projector.feed(line_number, row)
    return projector.finish()


def status(db: sqlite3.Connection) -> dict[str, Any]:
    """How complete the projection is: sources without token state, and failed sources."""
    sources = db.execute("SELECT count(*) FROM sources").fetchone()[0]
    rows = db.execute(
        "SELECT status, projection_version, count(*) n FROM token_source_state GROUP BY 1,2"
    ).fetchall()
    ok = sum(row["n"] for row in rows if row["status"] == "ok" and row["projection_version"] == TOKEN_PROJECTION_VERSION)
    errors = [dict(row) for row in db.execute(
        "SELECT s.source_key, s.path, t.error FROM token_source_state t JOIN sources s USING(source_key) "
        "WHERE t.status='error' ORDER BY s.source_key LIMIT 20"
    )]
    error_count = sum(row["n"] for row in rows if row["status"] == "error")
    return {
        "projection_version": TOKEN_PROJECTION_VERSION, "sources": sources, "ok": ok,
        "error": error_count, "missing": sources - sum(row["n"] for row in rows),
        "complete": ok == sources, "errors": errors,
    }


def usage_summary(db: sqlite3.Connection, since: str, until: Optional[str] = None,
                  limit: int = 10) -> dict[str, Any]:
    """Context size per model call and the heaviest sessions in a window.

    A call's context is its inclusive input (everything the model read for that call). Only calls
    made by root sessions count for the context figures, because child runs start from their own,
    smaller prompts. A session's total includes the calls of every run under it.
    """
    window = "u.observed_at >= ?" + (" AND u.observed_at <= ?" if until else "")
    values: tuple[Any, ...] = (since, until) if until else (since,)
    context = [dict(row) for row in db.execute(
        "SELECT u.provider, count(*) AS calls, avg(u.input_tokens) AS avg_context_tokens, "
        "avg(u.input_tokens > 100000) AS share_over_100k, max(u.input_tokens) AS max_context_tokens "
        "FROM token_usage_events u JOIN token_runs_effective r ON r.run_id=u.run_id "
        f"WHERE r.run_kind='root' AND u.input_tokens IS NOT NULL AND {window} GROUP BY u.provider",
        values,
    )]
    sessions = [dict(row) for row in db.execute(
        "SELECT r.root_run_id AS session_id, min(r.provider) AS provider, min(u.observed_at) AS started_at, "
        "sum(coalesce(u.input_tokens,0)+coalesce(u.output_tokens,0)) AS tokens, count(*) AS calls, "
        "count(DISTINCT r.run_id) AS runs FROM token_usage_events u "
        f"JOIN token_runs_effective r ON r.run_id=u.run_id WHERE {window} "
        "GROUP BY r.root_run_id ORDER BY tokens DESC", values,
    )]
    total = sum(row["tokens"] or 0 for row in sessions)
    top_count = max(1, len(sessions) // 10) if sessions else 0
    top_share = (sum(row["tokens"] for row in sessions[:top_count]) / total) if total else None
    for row in sessions:
        row["share"] = (row["tokens"] / total) if total else None
    return {
        "since": since, "until": until, "context_per_call": context,
        "sessions": len(sessions), "tokens": total,
        "top_tenth": {"sessions": top_count, "share": top_share},
        "heaviest_sessions": sessions[:limit],
    }
