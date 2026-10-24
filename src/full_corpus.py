#!/usr/bin/env python3
"""Discover, incrementally sync, and verify the complete local SessionDB corpus."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import random
import re
import shutil
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

import cloud_fetch
import sessiondb
import token_usage


CLOUD_THREADS_ROOT = Path.home() / ".local/share/sessiondb-public" / sessiondb.CLOUD_THREADS_DIR_NAME

ROOTS = {
    "claude": (Path.home() / ".claude/projects", CLOUD_THREADS_ROOT),
    # Codex keeps thread titles only in its session index, beside the session folders.
    "codex": (
        Path.home() / ".codex/sessions", Path.home() / ".codex/archived_sessions",
        Path.home() / ".codex" / sessiondb.CODEX_SESSION_INDEX_NAME,
    ),
}

COMPACTION_MIN_RECLAIM_BYTES = 256 * 1024 * 1024
COMPACTION_MIN_RECLAIM_PERCENT = 15.0
COMPACTION_FREE_SPACE_MULTIPLIER = 2
COMPACTION_MAX_P95_REGRESSION_PERCENT = 10.0


def home_path(path: Path) -> str:
    resolved = str(path.resolve())
    home = str(Path.home())
    return "${HOME}" + resolved[len(home):] if resolved.startswith(home + os.sep) else resolved


def discover() -> list[tuple[str, Path, str]]:
    return sessiondb.discover_transcript_sources(ROOTS)


def discover_changed(paths: Iterable[str]) -> list[tuple[str, Path, str]]:
    return sessiondb.discover_changed_transcript_sources(ROOTS, paths)


def needs_full_discovery(db: sqlite3.Connection) -> bool:
    """A pending adapter migration or an unfinished source must be selected from every file."""
    return any(
        row["status"] != "complete" or row["adapter_version"] != sessiondb.ADAPTER_VERSION[row["provider"]]
        for row in db.execute("SELECT provider,adapter_version,status FROM sources")
    )


def discovery_roots() -> dict[str, list[str]]:
    return {provider: [str(root) for root in roots] for provider, roots in ROOTS.items()}


CLOUD_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
CLOUD_MESSAGE_KINDS = ("user", "assistant", "system")


def cloud_transcript_rows(events: Any, session_id: str, title: str | None) -> list[dict[str, Any]]:
    """Map a cloud session's list_events export (oldest first) to Claude transcript rows.

    The mapping is deterministic, so a later export of the same session with more events
    produces a file that only appends to the earlier one.
    """
    if not isinstance(events, list) or not events:
        raise ValueError("cloud export must be a non-empty JSON array of events")
    rows = []
    for number, event in enumerate(events, 1):
        kinds = [key for key in event if key != "created_at"] if isinstance(event, dict) else []
        if len(kinds) != 1 or not isinstance(event.get("created_at"), str):
            raise ValueError(f"cloud event {number} is not {{created_at, <kind>}}")
        kind = kinds[0]
        body = event[kind]
        inner = body.get("internal_anthropic_catchall") if isinstance(body, dict) else None
        inner = inner if isinstance(inner, dict) else {}
        # SessionDB marks an unknown Claude row type unsupported, and one unsupported row
        # makes every search touching the session incomplete; keep cloud-only stream events
        # (result, control, rate-limit, environment logs) as system rows named by their kind.
        row_type = kind if kind in CLOUD_MESSAGE_KINDS else "system"
        row: dict[str, Any] = {
            "type": row_type, "sessionId": session_id,
            "timestamp": inner.get("timestamp") or event["created_at"],
        }
        if isinstance(body, dict) and body.get("uuid"):
            row["uuid"] = body["uuid"]
        for key, value in inner.items():
            if key == "tool_use_result":
                row["toolUseResult"] = value
            elif key not in ("session_id", "timestamp"):
                row[key] = value
        if row_type != kind:
            row["subtype"] = kind
            row["content"] = body
        rows.append(row)
    if title:
        rows.insert(0, {"type": "ai-title", "sessionId": session_id, "aiTitle": title,
                        "timestamp": events[0]["created_at"]})
    return rows


def import_cloud_export(
    export: Path, session_id: str, project: str, title: str | None = None,
    root: Path | None = None, replace: bool = False,
) -> dict[str, Any]:
    _check_cloud_names(session_id, project)
    events = json.loads(export.read_text(encoding="utf-8"))
    return import_cloud_events(events, session_id, project, title, root, replace)


def _check_cloud_names(session_id: str, project: str) -> None:
    for label, value in (("session", session_id), ("project", project)):
        if not CLOUD_NAME.fullmatch(value):
            raise ValueError(f"{label} must be letters, digits, '.', '_' or '-': {value!r}")


def import_cloud_events(
    events: Any, session_id: str, project: str, title: str | None = None,
    root: Path | None = None, replace: bool = False,
) -> dict[str, Any]:
    _check_cloud_names(session_id, project)
    data = "".join(
        json.dumps(row, ensure_ascii=False) + "\n"
        for row in cloud_transcript_rows(events, session_id, title)
    ).encode("utf-8")
    target = (root or CLOUD_THREADS_ROOT) / project / f"{session_id}.jsonl"
    previous = target.read_bytes() if target.exists() else b""
    if previous == data:
        return {"complete": True, "path": str(target), "rows": data.count(b"\n"), "changed": False}
    # A rewritten source is re-ingested from scratch; refuse to shrink or rewrite history
    # (for example a truncated export) unless the caller asks for it.
    if previous and not data.startswith(previous) and not replace:
        raise ValueError(f"{target} holds records the new export does not start with; pass --replace")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(data)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return {"complete": True, "path": str(target), "rows": data.count(b"\n"), "changed": True}


def sync_cloud_session(
    session_id: str, project: str, title: str | None = None, root: Path | None = None,
    replace: bool = False, fetch: Callable[[str], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Download a cloud session from the claude.ai API and save it as a transcript, without a model."""
    _check_cloud_names(session_id, project)
    if fetch is None:
        token, organization = cloud_fetch.credentials()
        fetch = lambda sid: cloud_fetch.fetch_session_events(sid, token, organization)
    events = cloud_fetch.events_to_export(fetch(session_id))
    return import_cloud_events(events, session_id, project, title, root, replace)


CLOUD_SYNC_STATE = Path.home() / ".local/share/sessiondb-public/cloud-sync/state.json"
# Only sessions that ran in Anthropic's cloud; Remote Control sessions on this Mac
# already have a local transcript, and importing them again would duplicate them.
CLOUD_ENVIRONMENT_KINDS = {"anthropic_cloud"}
CLOUD_DEFAULT_PROJECT = "claude-cloud"


def _cloud_placement(session_id: str, root: Path, title: str | None) -> tuple[str, str | None]:
    """Return (project folder, title row) for a session.

    A session keeps the folder and title it was first imported with: a second folder would
    duplicate it, and a changed title rewrites the first row, which a later download may not do.
    """
    for existing in sorted(root.glob(f"*/{session_id}.jsonl")):
        with existing.open(encoding="utf-8") as handle:
            try:
                first = json.loads(handle.readline() or "{}")
            except ValueError:
                first = {}
        return existing.parent.name, first.get("aiTitle") if first.get("type") == "ai-title" else None
    return CLOUD_DEFAULT_PROJECT, title


def sync_all_cloud_sessions(
    root: Path | None = None, state_path: Path | None = None, dry_run: bool = False,
    list_sessions: Callable[[], list[dict[str, Any]]] | None = None,
    fetch: Callable[[str], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Download every finished cloud session whose events changed since the last run.

    A session that fails is reported and retried on the next run; it never stops the others.
    """
    root = root or CLOUD_THREADS_ROOT
    state_path = state_path or CLOUD_SYNC_STATE
    if list_sessions is None or fetch is None:
        token, organization = cloud_fetch.credentials()
        list_sessions = list_sessions or (lambda: cloud_fetch.list_sessions(token, organization))
        fetch = fetch or (lambda sid: cloud_fetch.fetch_session_events(sid, token, organization))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path.with_suffix(".lock"), "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"complete": False, "error": "another cloud sync is running"}
        try:
            state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
        except ValueError:
            raise ValueError(f"{state_path} is not valid JSON; fix or remove it") from None
        synced = state.setdefault("sessions", {})
        report: dict[str, Any] = {"complete": True, "skipped_kinds": {}, "busy": [], "imported": [],
                                  "unchanged": 0, "failed": {}}
        for session in list_sessions():
            sid, kind = session["id"], session.get("environment_kind")
            if kind not in CLOUD_ENVIRONMENT_KINDS:
                report["skipped_kinds"][str(kind)] = report["skipped_kinds"].get(str(kind), 0) + 1
                continue
            if session.get("worker_status") != "idle":
                report["busy"].append(sid)
                continue
            marker = session.get("last_event_at")
            if marker and synced.get(sid, {}).get("last_event_at") == marker:
                report["unchanged"] += 1
                continue
            if dry_run:
                report["imported"].append(sid)
                continue
            try:
                project, title = _cloud_placement(sid, root, session.get("title") or None)
                result = sync_cloud_session(sid, project, title, root=root, fetch=fetch)
            except (OSError, ValueError, cloud_fetch.CloudFetchError) as exc:
                report["failed"][sid] = str(exc)
                synced.setdefault(sid, {})["error"] = str(exc)
                continue
            synced[sid] = {"last_event_at": marker, "path": result["path"], "rows": result["rows"]}
            report["imported"].append(sid)
        if not dry_run:
            temporary = state_path.with_name(f".{state_path.name}.{os.getpid()}.tmp")
            temporary.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
            os.replace(temporary, state_path)
        report["complete"] = not report["failed"]
        return report


def cloud_sync_main() -> int:
    parser = argparse.ArgumentParser(
        description="Download finished cloud Claude sessions into SessionDB's cloud-threads folder, without a model.")
    parser.add_argument("--dry-run", action="store_true", help="list what would be downloaded; write nothing")
    args = parser.parse_args()
    try:
        result = sync_all_cloud_sessions(dry_run=args.dry_run)
    except (OSError, ValueError, cloud_fetch.CloudFetchError) as exc:
        result = {"complete": False, "error": str(exc)}
    result["finished_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["complete"] else 1


def _remove_source(db: sqlite3.Connection, source_id: str) -> None:
    key = db.execute("SELECT source_key FROM sources WHERE source_id=?", (source_id,)).fetchone()
    if key is not None:
        token_usage.reset_source(db, key[0])
    db.execute(f"DELETE FROM events WHERE source_key={sessiondb.SOURCE_KEY_OF}", (source_id,))
    db.execute("DELETE FROM sessions WHERE source_id=?", (source_id,))
    db.execute("DELETE FROM sources WHERE source_id=?", (source_id,))


DESKTOP_DELETE_MARKER = ".desktop-released.json"


def desktop_delete_marker(path: Path) -> Path | None:
    """The Claude desktop app's delete marker for a transcript or its sub-agent files."""
    candidates = [path.parent / f"{path.stem}{DESKTOP_DELETE_MARKER}"]
    # Sub-agent transcripts live under <project>/<session-id>/; the marker sits beside that folder.
    candidates += [
        ancestor.parent / f"{ancestor.name}{DESKTOP_DELETE_MARKER}" for ancestor in list(path.parents)[:3]
    ]
    for marker in candidates:
        try:
            value = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(value, dict) and value.get("reason") == "delete":
            return marker
    return None


def purge_derived_copies(db_path: Path, needles: set[str]) -> list[str]:
    """Delete saved result bundles and nightly snapshots that mention a removed session."""
    encoded = [needle.encode("utf-8") for needle in needles if needle]
    if not encoded:
        return []

    def mentions(file: Path) -> bool:
        try:
            data = file.read_bytes()
        except OSError:
            return False
        return any(needle in data for needle in encoded)

    removed = []
    for directory in sessiondb.result_bundle_directories(db_path):
        for bundle in sorted(directory.glob("*.jsonl")) if directory.is_dir() else []:
            if mentions(bundle):
                bundle.unlink(missing_ok=True)
                removed.append(str(bundle))
    snapshots = db_path.resolve().parent / "shadow" / "snapshots"
    for snapshot in sorted(snapshots.iterdir()) if snapshots.is_dir() else []:
        if snapshot.is_dir() and any(mentions(file) for file in snapshot.rglob("*") if file.is_file()):
            shutil.rmtree(snapshot)
            removed.append(str(snapshot))
    return removed


def remove_desktop_deleted_sources(db: sqlite3.Connection, db_path: Path) -> list[dict[str, Any]]:
    """Forget sessions the Claude desktop app deleted: transcript gone and a delete marker present.

    A transcript that merely moved is matched by relocation first, and one that vanished
    without a delete marker stays indexed, so cold-storage moves never lose records.
    """
    doomed = []
    for row in db.execute("SELECT source_id,path FROM sources WHERE provider='claude'").fetchall():
        path = Path(row["path"])
        if os.path.lexists(path):
            continue
        marker = desktop_delete_marker(path)
        if marker is not None:
            doomed.append((row, marker))
    if not doomed:
        return []
    projection_was_current = (
        scalar(db, "SELECT COUNT(*) FROM events") == 0 or sessiondb.projection_status(db)["complete"]
    )
    removed = []
    db.execute("BEGIN IMMEDIATE")
    try:
        for row, marker in doomed:
            sessions = sorted(
                item[0] for item in db.execute(
                    f"SELECT DISTINCT session_id FROM events WHERE source_key={sessiondb.SOURCE_KEY_OF}",
                    (row["source_id"],),
                ) if item[0]
            )
            events = scalar(
                db, f"SELECT COUNT(*) FROM events WHERE source_key={sessiondb.SOURCE_KEY_OF}",
                (row["source_id"],),
            )
            _remove_source(db, row["source_id"])
            db.execute("DELETE FROM metadata WHERE key=?", (sessiondb.adapter_state_key(row["source_id"]),))
            removed.append({
                "path": row["path"], "source_id": row["source_id"], "marker": str(marker),
                "sessions": sessions, "events": events,
            })
        if projection_was_current:
            sessiondb.refresh_projection_metadata(db)
        db.commit()
    except Exception:
        db.rollback()
        raise
    needles = {session for item in removed for session in item["sessions"]}
    needles |= {item["path"] for item in removed}
    derived = purge_derived_copies(db_path, needles)
    for item in removed:
        item["derived_copies_removed"] = derived
    return removed


def reconcile_source_relocations(
    db: sqlite3.Connection, discovered: list[tuple[str, Path, str]],
    candidate_paths: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Relocate an indexed transcript when its moved copy was later appended.

    With `candidate_paths` (a changed-paths sync), only indexed paths in it can have moved.
    """
    rows = db.execute(
        "SELECT source_id,provider,path,source_hash,size_bytes FROM sources"
    ).fetchall()
    discovered_paths = {str(path) for _, path, _ in discovered}
    live_by_name: dict[tuple[str, str], list[Path]] = {}
    for provider, path, _ in discovered:
        live_by_name.setdefault((provider, path.name), []).append(path)

    candidates: list[tuple[sqlite3.Row, Path]] = []
    for stale in rows:
        if stale["path"] in discovered_paths:
            continue
        if candidate_paths is not None and stale["path"] not in candidate_paths:
            continue
        matches = []
        for path in live_by_name.get((stale["provider"], Path(stale["path"]).name), []):
            stat, data = sessiondb.stable_source_snapshot(path)
            exact = (
                stat.st_size == stale["size_bytes"]
                and sessiondb.digest(data) == stale["source_hash"]
            )
            appended = (
                stat.st_size > stale["size_bytes"]
                and data[:stale["size_bytes"]].endswith(b"\n")
                and sessiondb.digest(data[:stale["size_bytes"]]) == stale["source_hash"]
            )
            if exact or appended:
                matches.append(path)
        if len(matches) == 1:
            candidates.append((stale, matches[0]))

    target_counts = Counter(str(path) for _, path in candidates)
    current_by_path = {row["path"]: row for row in rows}
    relocated = []
    for stale, target in candidates:
        target_path = str(target)
        if target_counts[target_path] != 1:
            continue
        existing = current_by_path.get(target_path)
        if existing and existing["source_id"] != stale["source_id"]:
            _remove_source(db, stale["source_id"])
            mode = "deduplicated_appended_move"
        else:
            db.execute(
                "UPDATE sources SET path=? WHERE source_id=?",
                (target_path, stale["source_id"]),
            )
            mode = "relocated"
        relocated.append({
            "from": stale["path"], "to": target_path,
            "source_id": stale["source_id"], "mode": mode,
        })
    if relocated:
        db.commit()
    return relocated


def scalar(db: sqlite3.Connection, sql: str, values: tuple[Any, ...] = ()) -> Any:
    return db.execute(sql, values).fetchone()[0]


def compaction_analysis(db_path: Path, target_parent: Path | None = None) -> dict[str, Any]:
    db = sessiondb.connect(db_path)
    reachable = _load_reachable(db)
    page_size = scalar(db, "PRAGMA page_size")
    database_reclaimable = page_size * scalar(db, "PRAGMA freelist_count")
    pack_bytes = sessiondb.pack_path(db).stat().st_size
    # A block with any reachable member stays whole; only blocks and frames with none reclaim.
    reachable_pack_bytes = scalar(
        db,
        "SELECT COALESCE(SUM(o.compressed_bytes),0) FROM objects o "
        "JOIN temp.sessiondb_reachable r ON r.object_hash=o.object_hash WHERE o.block_key IS NULL",
    ) + scalar(
        db,
        "SELECT COALESCE(SUM(b.compressed_bytes),0) FROM object_blocks b WHERE EXISTS ("
        "SELECT 1 FROM objects o JOIN temp.sessiondb_reachable r ON r.object_hash=o.object_hash "
        "WHERE o.block_key=b.block_key)",
    )
    db.execute("DROP TABLE IF EXISTS temp.sessiondb_reachable")
    pack_reclaimable = max(pack_bytes - reachable_pack_bytes, 0)
    retrieval_verification = sessiondb.verify_projection_pack(db)
    active_retrieval = sessiondb.projection_pack_path(db)
    owned_retrieval = sorted(db_path.parent.glob(db_path.name + ".retrieval.*"))
    retrieval_bytes = sum(path.stat().st_size for path in owned_retrieval)
    retained_retrieval_bytes = sum(
        path.stat().st_size for path in owned_retrieval if path != active_retrieval
    )
    retrieval_reclaimable = (
        retrieval_verification.get("orphan_bytes", 0) + retained_retrieval_bytes
    )
    database_bytes = db_path.stat().st_size
    total_bytes = database_bytes + pack_bytes + retrieval_bytes
    reclaimable_bytes = database_reclaimable + pack_reclaimable + retrieval_reclaimable
    reclaimable_percent = (reclaimable_bytes / total_bytes * 100) if total_bytes else 0.0
    db.close()
    free_bytes = shutil.disk_usage(target_parent or db_path.parent).free
    minimum_free_bytes = total_bytes * COMPACTION_FREE_SPACE_MULTIPLIER
    threshold_met = (
        reclaimable_bytes >= COMPACTION_MIN_RECLAIM_BYTES
        and reclaimable_percent >= COMPACTION_MIN_RECLAIM_PERCENT
    )
    return {
        "database": str(db_path),
        "database_bytes": database_bytes,
        "pack_bytes": pack_bytes,
        "total_bytes": total_bytes,
        "database_reclaimable_bytes": database_reclaimable,
        "pack_reclaimable_bytes": pack_reclaimable,
        "retrieval_pack_bytes": retrieval_bytes,
        "retrieval_reclaimable_bytes": retrieval_reclaimable,
        "reclaimable_bytes": reclaimable_bytes,
        "reclaimable_percent": reclaimable_percent,
        "reachable_objects": reachable,
        "minimum_reclaim_bytes": COMPACTION_MIN_RECLAIM_BYTES,
        "minimum_reclaim_percent": COMPACTION_MIN_RECLAIM_PERCENT,
        "free_bytes": free_bytes,
        "minimum_free_bytes": minimum_free_bytes,
        "threshold_met": threshold_met,
        "headroom_ok": free_bytes >= minimum_free_bytes,
        "due": threshold_met and free_bytes >= minimum_free_bytes,
    }


MAX_REPAIR_EVIDENCE_EVENTS = 20
MAX_REPAIR_EVIDENCE_BYTES = 1_000_000


def issue_groups(
    db: sqlite3.Connection, *, include_repair_evidence: bool = False,
) -> list[dict[str, Any]]:
    groups = [dict(row) for row in db.execute(
        "SELECT e.provider,e.category,e.issue,COUNT(DISTINCT e.event_id) AS events,"
        "COALESCE(SUM(o.raw_bytes),0) AS content_bytes,MIN(src.path || ':' || e.line_number) AS example "
        ",MIN(CASE WHEN e.category<>'unreadable' AND e.content_hash IS NULL "
        "AND e.role IS NULL AND e.tool IS NULL AND e.tool_event=0 "
        "AND e.structure_bytes>0 AND length(e.raw_hash)=32 AND src.status='complete' "
        "THEN 1 ELSE 0 END) AS noncritical_adapter_gap "
        f"FROM {sessiondb.EVENT_JOIN} "
        "LEFT JOIN event_content ec ON ec.event_rowid=e.event_rowid "
        "LEFT JOIN objects o ON o.object_hash=ec.object_hash "
        "WHERE e.supported=0 GROUP BY e.provider,e.category,e.issue "
        "ORDER BY events DESC,e.provider,e.category"
    )]
    for item in groups:
        events = db.execute(
            f"SELECT {sessiondb.EVENT_COLUMNS},src.path FROM {sessiondb.EVENT_JOIN} "
            "WHERE e.supported=0 AND e.provider=? AND e.category=? AND e.issue IS ?",
            (item["provider"], item["category"], item["issue"]),
        ).fetchall()
        paths = sorted({row["path"] for row in events})
        fallback_readable = bool(paths) and all(
            Path(path).is_file() and os.access(path, os.R_OK) for path in paths
        )
        repair_events = []
        repair_bytes = 0
        repair_evidence_complete = True
        if include_repair_evidence:
            for event in events:
                try:
                    structure = sessiondb.read_event_structure(db, event)
                    evidence_source = "sessiondb"
                except (KeyError, OSError, ValueError):
                    try:
                        raw, structure = sessiondb.source_row(
                            db, event["source_id"], event["line_number"],
                        )
                        actual = (
                            sessiondb.digest(sessiondb.stored_json(structure))
                            if isinstance(structure, dict) else sessiondb.digest(raw)
                        )
                        if actual != event["raw_hash"]:
                            raise ValueError("raw event hash mismatch")
                        evidence_source = "raw_transcript"
                    except (KeyError, OSError, ValueError):
                        repair_evidence_complete = False
                        continue
                encoded = sessiondb.stored_json(structure)
                if (
                    len(repair_events) >= MAX_REPAIR_EVIDENCE_EVENTS
                    or repair_bytes + len(encoded) > MAX_REPAIR_EVIDENCE_BYTES
                ):
                    repair_evidence_complete = False
                    continue
                repair_bytes += len(encoded)
                repair_events.append({
                    "event_id": event["event_id"],
                    "provider": event["provider"],
                    "category": event["category"],
                    "issue": event["issue"],
                    "source_path": event["path"],
                    "line_number": event["line_number"],
                    "raw_hash": event["raw_hash"],
                    "evidence_source": evidence_source,
                    "structure": structure,
                })
            repair_evidence_complete = (
                repair_evidence_complete and len(repair_events) == len(events)
            )
        auto_repairable = bool(item["noncritical_adapter_gap"])
        if auto_repairable:
            try:
                auto_repairable = all(
                    sessiondb.auto_metadata_admission_signature(
                        event["provider"], event["category"],
                        sessiondb.read_event_structure(db, event), Path(event["path"]),
                    ) is not None
                    for event in events
                )
            except (KeyError, OSError, ValueError):
                auto_repairable = False
        item["fallback_readable"] = int(fallback_readable)
        if include_repair_evidence:
            item["repair_evidence_complete"] = int(repair_evidence_complete)
            item["_repair_events"] = repair_events
        item["noncritical_adapter_gap"] = int(
            auto_repairable and fallback_readable
        )
    return groups


def write_json_atomic(path: Path, value: Any, sort_keys: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=sort_keys) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def migration_id(provider: str, from_version: str, to_version: str) -> str:
    return f"{provider}:{from_version}->{to_version}"


def emit_migration(
    callback: Callable[[dict[str, Any]], None] | None, migration: dict[str, Any],
) -> None:
    if callback is not None:
        callback(dict(migration))


def apply_registered_migrations(
    db: sqlite3.Connection, discovered: list[tuple[str, Path, str]],
    progress_callback: Callable[[dict[str, Any]], None] | None,
    source_watermarks: dict[str, Any] | None = None,
) -> tuple[
    list[dict[str, Any]], list[dict[str, Any]],
    dict[str, dict[str, Any]], set[str],
]:
    source_watermarks = source_watermarks or {}
    paths = {str(path) for _, path, _ in discovered}
    rows = db.execute(
        "SELECT source_id,provider,path,adapter_version,status FROM sources ORDER BY provider,path"
    ).fetchall()
    groups: dict[tuple[str, str, str], list[sqlite3.Row]] = {}
    for row in rows:
        target = sessiondb.ADAPTER_VERSION[row["provider"]]
        if row["path"] in paths and row["status"] == "complete" and row["adapter_version"] != target:
            groups.setdefault((row["provider"], row["adapter_version"], target), []).append(row)
    migrations: list[dict[str, Any]] = []
    early_results: list[dict[str, Any]] = []
    pending_by_path: dict[str, dict[str, Any]] = {}
    deferred_paths: set[str] = set()
    for (provider, from_version, to_version), group in sorted(groups.items()):
        plan = sessiondb.ADAPTER_MIGRATIONS.get((provider, from_version, to_version))
        mode = plan.mode if plan else "provider-wide"
        item: dict[str, Any] = {
            "migration_id": migration_id(provider, from_version, to_version),
            "provider": provider,
            "from_version": from_version,
            "to_version": to_version,
            "mode": mode,
            "state": "running",
            "total_sources": len(group),
            "completed_sources": 0,
            "remaining_sources": len(group),
            "started_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        if plan and plan.mode == "category-scoped":
            item["selection"] = "unsupported-only" if plan.unsupported_only else "all-category-sources"
        migrations.append(item)
        emit_migration(progress_callback, item)
        if plan and plan.mode == "metadata-only":
            try:
                result = sessiondb.apply_metadata_migration(
                    db, provider, from_version, to_version,
                    (row["source_id"] for row in group), plan,
                )
                item.update({
                    "state": "complete", "completed_sources": len(group), "remaining_sources": 0,
                    "migrated_events": result["events"],
                })
                emit_migration(progress_callback, item)
                continue
            except Exception as exc:
                db.rollback()
                item["mode"] = "provider-wide"
                item["fallback_reason"] = str(exc)
        elif plan and plan.mode == "category-scoped":
            affected = []
            for row in group:
                placeholders = ",".join("?" for _ in plan.categories)
                support_filter = " AND supported=0" if plan.unsupported_only else ""
                hit = db.execute(
                    f"SELECT 1 FROM events WHERE source_key={sessiondb.SOURCE_KEY_OF} "
                    f"AND category IN ({placeholders}){support_filter} LIMIT 1",
                    (row["source_id"], *sorted(plan.categories)),
                ).fetchone()
                if hit:
                    affected.append(row)
            failed = None
            deferred = False
            for row in affected:
                started = time.monotonic()
                try:
                    result = sessiondb.ingest(
                        db, provider, Path(row["path"]),
                        source_watermark=source_watermarks.get(row["path"]),
                    )
                except sessiondb.SourceBoundaryChanged:
                    deferred = True
                    deferred_paths.add(row["path"])
                    continue
                except Exception as exc:
                    db.rollback()
                    failed = exc
                    break
                result["elapsed_seconds"] = time.monotonic() - started
                result["round"] = 0
                early_results.append(result)
                item["completed_sources"] += 1
                item["remaining_sources"] -= 1
                emit_migration(progress_callback, item)
            if failed is None:
                affected_ids = {row["source_id"] for row in affected}
                unaffected = [row["source_id"] for row in group if row["source_id"] not in affected_ids]
                try:
                    db.execute("BEGIN IMMEDIATE")
                    db.executemany(
                        "UPDATE sources SET adapter_version=? WHERE source_id=? AND adapter_version=?",
                        ((to_version, source_id, from_version) for source_id in unaffected),
                    )
                    if not deferred:
                        invalid = sum(
                            db.execute(
                                "SELECT adapter_version<>? FROM sources WHERE source_id=?",
                                (to_version, row["source_id"]),
                            ).fetchone()[0]
                            for row in group
                        )
                        if invalid:
                            raise ValueError("category migration validation failed")
                    db.commit()
                    if not deferred:
                        item.update({
                            "state": "complete", "completed_sources": len(group), "remaining_sources": 0,
                        })
                        emit_migration(progress_callback, item)
                        continue
                except Exception as exc:
                    db.rollback()
                    failed = exc
            if failed is not None:
                item["mode"] = "provider-wide"
                item["fallback_reason"] = str(failed)
        elif plan and plan.mode not in {"provider-wide"}:
            item["mode"] = "provider-wide"
            item["fallback_reason"] = f"unknown migration mode: {plan.mode}"
        for row in group:
            current = db.execute(
                "SELECT adapter_version FROM sources WHERE source_id=?", (row["source_id"],),
            ).fetchone()
            if not current or current["adapter_version"] != to_version:
                pending_by_path[row["path"]] = item
        item["remaining_sources"] = sum(1 for row in group if row["path"] in pending_by_path)
        item["completed_sources"] = item["total_sources"] - item["remaining_sources"]
        emit_migration(progress_callback, item)
    return migrations, early_results, pending_by_path, deferred_paths


def appended_only(report: dict[str, Any]) -> bool:
    """Whether a sync's own writes only appended events (no deletion, rewrite, or relocation)."""
    return bool(
        all(item["status"] in ("appended", "unchanged") for item in report["results"])
        and not report["relocated_sources"] and not report["desktop_deleted_sources"]
        and not report["migrations"] and not report["auto_adapter_updates"]["admitted"]
    )


def prepare_incremental_receipt(
    db: sqlite3.Connection, baseline: dict[str, Any], data_version: int,
) -> dict[str, Any] | None:
    """Build a receipt covering only rows added since `baseline`, or None to require a full check.

    The baseline bound the live files when this connection opened; an unchanged data_version
    proves no other connection committed since, so every change since the baseline is this
    sync's own appends.
    """
    try:
        sessiondb.checkpoint_without_waiting(db)
        db.execute("BEGIN")
        db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
        if db.execute("PRAGMA data_version").fetchone()[0] != data_version:
            return None
        if not sessiondb.connection_path_is_current(db):
            return None
        return sessiondb.build_projection_integrity_receipt(db, baseline=baseline)
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError):
        return None
    finally:
        db.rollback()


def fast_sync(
    db_path: Path, report_path: Path, max_rounds: int = 1,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    incremental_receipt: bool = False,
    changed_paths: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Catch up changed transcript sources without full-corpus verification.

    `changed_paths` limits discovery to files the caller saw change; None lists every file.
    """
    started = time.monotonic()
    db = sessiondb.connect(db_path)
    if changed_paths is not None and needs_full_discovery(db):
        changed_paths = None
    if changed_paths is None:
        list_sources, candidate_paths = discover, None
    else:
        changed_paths = list(changed_paths)
        candidate_paths = {str(Path(path).resolve()) for path in changed_paths}
        list_sources = lambda: discover_changed(changed_paths)
    receipt_baseline = sessiondb.projection_receipt_baseline(db) if incremental_receipt else None
    data_version = db.execute("PRAGMA data_version").fetchone()[0]
    results: list[dict[str, Any]] = []
    relocated: list[dict[str, Any]] = []
    desktop_deleted: list[dict[str, Any]] = []
    drifted: list[str] = []
    missing: list[str] = []
    rounds = 0
    migrations: list[dict[str, Any]] = []
    pending_migrations: dict[str, dict[str, Any]] = {}
    deferred_paths: set[str] = set()
    auto_admissions: list[dict[str, Any]] = []
    auto_rejected_groups = 0
    try:
        for rounds in range(1, max_rounds + 1):
            discovered = list_sources()
            source_watermarks = {}
            for _provider, path, _source_class in discovered:
                try:
                    source_watermarks[str(path)] = path.stat()
                except OSError:
                    continue
            relocated.extend(reconcile_source_relocations(db, discovered, candidate_paths))
            desktop_deleted.extend(remove_desktop_deleted_sources(db, db_path))
            indexed_rows = db.execute(
                "SELECT source_id,provider,path,source_hash,size_bytes,mtime_ns,adapter_version,status FROM sources"
            ).fetchall()
            indexed = {row["path"]: row for row in indexed_rows}
            if rounds == 1:
                migrations, early_results, pending_migrations, deferred_paths = apply_registered_migrations(
                    db, discovered, progress_callback, source_watermarks,
                )
                results.extend(early_results)
            indexed_rows = db.execute(
                "SELECT source_id,provider,path,source_hash,size_bytes,mtime_ns,adapter_version,status FROM sources"
            ).fetchall()
            indexed = {row["path"]: row for row in indexed_rows}

            for provider, path, _source_class in discovered:
                if str(path) in deferred_paths:
                    continue
                item_started = time.monotonic()
                try:
                    stat = path.stat()
                    row = indexed.get(str(path))
                    if row and row["adapter_version"] == sessiondb.ADAPTER_VERSION[provider] and row["status"] == "complete" and (
                        row["size_bytes"], row["mtime_ns"]
                    ) == (stat.st_size, stat.st_mtime_ns):
                        continue
                    result = sessiondb.ingest(
                        db, provider, path,
                        source_watermark=source_watermarks.get(str(path)),
                    )
                except FileNotFoundError:
                    # A lifecycle move is expected while a live session is
                    # being archived. Rediscovery below will find its durable
                    # location without turning the transient path into a fault.
                    continue
                except sessiondb.SourceBoundaryChanged:
                    # The next filesystem event owns the new boundary. Do not
                    # reread a live source synchronously inside this run.
                    continue
                except Exception as exc:
                    db.rollback()
                    result = {
                        "path": str(path), "status": "failed", "events": 0,
                        "issues": 1, "error": str(exc),
                    }
                result["elapsed_seconds"] = time.monotonic() - item_started
                result["round"] = rounds
                results.append(result)
                migration = pending_migrations.get(str(path))
                if migration:
                    if result["status"] == "failed":
                        migration["state"] = "failed"
                        migration["failure"] = result.get("error", "source rebuild failed")
                    else:
                        migration["completed_sources"] += 1
                        migration["remaining_sources"] -= 1
                        if migration["remaining_sources"] == 0:
                            migration["state"] = "complete"
                    emit_migration(progress_callback, migration)

            admission = sessiondb.apply_auto_metadata_admissions(db)
            auto_admissions.extend(admission["admitted"])
            auto_rejected_groups = admission["rejected_groups"]

            final = list_sources()
            final_paths = {str(path) for _, path, _ in final}
            final_indexed = {
                row["path"]: (row["size_bytes"], row["mtime_ns"])
                for row in db.execute("SELECT path,size_bytes,mtime_ns FROM sources")
            }
            missing = sorted(final_paths - set(final_indexed))
            drifted = []
            for path in sorted(final_paths & set(final_indexed)):
                try:
                    stat = Path(path).stat()
                except OSError:
                    drifted.append(path)
                    continue
                if final_indexed[path] != (stat.st_size, stat.st_mtime_ns):
                    drifted.append(path)
            if not missing and not drifted:
                break
        failures = [item for item in results if item["status"] == "failed"]
        issues = issue_groups(db, include_repair_evidence=True)
        evidence_groups = []
        for issue in issues:
            evidence_groups.append({
                key: issue[key] for key in ("provider", "category", "issue", "events")
            } | {"records": issue.pop("_repair_events")})
        repair_evidence = None
        if issues:
            evidence_payload = {
                "version": 1,
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "groups": evidence_groups,
            }
            evidence_identity = json.dumps(
                evidence_groups, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()
            evidence_id = hashlib.sha256(evidence_identity).hexdigest()
            evidence_payload["id"] = evidence_id
            evidence_path = report_path.with_name(f"repair-evidence-{evidence_id}.json")
            # Structures must keep provider field order so raw_hash still verifies.
            write_json_atomic(evidence_path, evidence_payload, sort_keys=False)
            repair_evidence = {
                "id": evidence_id,
                "path": str(evidence_path),
                "complete": all(item["repair_evidence_complete"] for item in issues),
                "events": sum(len(group["records"]) for group in evidence_groups),
            }
        # Keep projection metadata current so readers skip the query-scoped fallback scan.
        projection_metadata_refreshed = sessiondb.repair_projection_metadata(db)
        report = {
            "version": 1,
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "database": str(db_path),
            "elapsed_seconds": time.monotonic() - started,
            "rounds": rounds,
            "discovered_sources": len(final),
            "discovery": "full" if candidate_paths is None else "changed_paths",
            "source_boundary": "run-start-watermark",
            "projection_metadata_refreshed": projection_metadata_refreshed,
            "status_counts": dict(Counter(item["status"] for item in results)),
            "results": results,
            "relocated_sources": relocated,
            "desktop_deleted_sources": desktop_deleted,
            "hard_failures": failures,
            "missing_discovered_sources": missing,
            "sources_drifted_after_sync": drifted,
            "issues": issues,
            "repair_evidence": repair_evidence,
            "migrations": migrations,
            "auto_adapter_updates": {
                "admitted": auto_admissions,
                "admitted_groups": len(auto_admissions),
                "admitted_events": sum(item["events"] for item in auto_admissions),
                "rejected_groups": auto_rejected_groups,
            },
        }
        report["complete_except_drift"] = (
            not failures and not missing
            and all(item["state"] == "complete" for item in migrations)
        )
        report["complete"] = report["complete_except_drift"] and not drifted
        report["discovery_roots"] = discovery_roots()
        write_json_atomic(report_path, report)
        if receipt_baseline is not None and not failures and appended_only(report):
            report["incremental_receipt"] = prepare_incremental_receipt(
                db, receipt_baseline, data_version,
            )
        return report
    finally:
        db.close()


def reconstruction_sample(db: sqlite3.Connection, limit: int) -> dict[str, Any]:
    rows = db.execute(
        f"SELECT {sessiondb.EVENT_COLUMNS} FROM {sessiondb.EVENT_JOIN} "
        "WHERE e.supported=1 ORDER BY e.event_id LIMIT ?", (limit,)
    ).fetchall()
    failures = []
    for row in rows:
        try:
            sessiondb.read_event_structure(db, row)
        except Exception as exc:
            failures.append({"event_id": row["event_id"], "error": str(exc)})
            if len(failures) >= 20:
                break
    sessions = db.execute(
        "SELECT provider,session_id FROM events WHERE content_hash IS NOT NULL "
        "GROUP BY provider,session_id ORDER BY MIN(event_id) LIMIT 100"
    ).fetchall()
    session_failures = []
    for provider, session_id in sessions:
        try:
            sessiondb.reconstruct(db, provider, session_id)
        except Exception as exc:
            session_failures.append({"provider": provider, "session_id": session_id, "error": str(exc)})
    return {
        "parsed_events_checked": len(rows), "parsed_event_failures": failures,
        "sessions_checked": len(sessions), "session_failures": session_failures,
        "pass": not failures and not session_failures,
    }


def manifest(db: sqlite3.Connection, classes: dict[str, str]) -> dict[str, Any]:
    sources = []
    for row in db.execute(
        "SELECT s.*,COUNT(e.event_id) AS events FROM sources s LEFT JOIN events e USING(source_key) "
        "GROUP BY s.source_id ORDER BY s.provider,s.path"
    ):
        sources.append({
            "provider": row["provider"], "class": classes.get(row["path"], "unknown"),
            "path": home_path(Path(row["path"])), "size": row["size_bytes"],
            "mtime_ns": row["mtime_ns"], "sha256": row["source_hash"],
            "adapter_version": row["adapter_version"], "status": row["status"],
            "events": row["events"], "issues": row["issue_count"],
        })
    return {
        "version": 1, "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "sources": sources,
        "summary": {
            "sources": len(sources),
            "providers": dict(Counter(source["provider"] for source in sources)),
            "classes": dict(Counter(f"{source['provider']}:{source['class']}" for source in sources)),
            "bytes": sum(source["size"] for source in sources),
            "events": sum(source["events"] for source in sources),
            "issues": sum(source["issues"] for source in sources),
        },
    }


def sync(db_path: Path, manifest_path: Path, report_path: Path, sample: int) -> dict[str, Any]:
    started = time.monotonic()
    initial = discover()
    classes = {str(path): source_class for _, path, source_class in initial}
    db = sessiondb.connect(db_path)
    relocated = reconcile_source_relocations(db, initial)
    desktop_deleted = remove_desktop_deleted_sources(db, db_path)
    results = []
    expected_lines: dict[str, int] = {}
    for provider, path, _ in initial:
        try:
            result = sessiondb.ingest(db, provider, path)
        except Exception as exc:
            db.rollback()
            result = {"path": str(path), "status": "failed", "events": 0, "issues": 1, "error": str(exc)}
        results.append(result)
        if result["status"] != "failed":
            expected_lines[str(path)] = result["nonblank_lines"]
    auto_adapter_updates = sessiondb.apply_auto_metadata_admissions(db)
    final = discover()
    final_paths = {str(path) for _, path, _ in final}
    indexed = {row[0]: (row[1], row[2]) for row in db.execute("SELECT path,size_bytes,mtime_ns FROM sources")}
    failed = [row for row in results if row["status"] == "failed"]
    missing = sorted(final_paths - set(indexed))
    stale = sorted(set(indexed) - final_paths)
    drifted = []
    for path in sorted(final_paths & set(indexed)):
        stat = Path(path).stat()
        if (stat.st_size, stat.st_mtime_ns) != indexed[path]:
            drifted.append(path)
    accounting_failures = []
    for path, expected in expected_lines.items():
        actual = scalar(
            db, "SELECT COUNT(*) FROM events e JOIN sources s USING(source_key) WHERE s.path=?", (path,)
        )
        if expected != actual:
            accounting_failures.append({"path": path, "expected": expected, "actual": actual})
    sample_result = reconstruction_sample(db, sample)
    integrity = scalar(db, "PRAGMA integrity_check")
    corpus_manifest = manifest(db, classes)
    manifest_path.write_text(json.dumps(corpus_manifest, indent=2, sort_keys=True) + "\n")
    status_counts = dict(Counter(row["status"] for row in results))
    report = {
        "version": 1, "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "database": str(db_path), "elapsed_seconds": time.monotonic() - started,
        "discovered_before": len(initial), "discovered_after": len(final),
        "status_counts": status_counts, "hard_failures": failed,
        "missing_discovered_sources": missing, "stale_indexed_sources": stale,
        "relocated_sources": relocated,
        "desktop_deleted_sources": desktop_deleted,
        "sources_drifted_after_sync": drifted, "line_accounting_failures": accounting_failures,
        "issues": issue_groups(db), "reconstruction_sample": sample_result,
        "auto_adapter_updates": auto_adapter_updates,
        "integrity_check": integrity, "storage": sessiondb.report(db, db_path),
    }
    report["complete"] = (
        not failed and not missing and not stale and not drifted and not accounting_failures
        and sample_result["pass"] and integrity == "ok"
    )
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    db.close()
    return report


def _file_is_prefix(prefix: Path, path: Path) -> bool:
    try:
        with prefix.open("rb") as short, path.open("rb") as long:
            while chunk := short.read(1 << 20):
                if long.read(len(chunk)) != chunk:
                    return False
    except FileNotFoundError:
        return False
    return True


def backup(db_path: Path, target: Path) -> dict[str, Any]:
    target.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(target, 0o700)
    database_copy = target / db_path.name
    pack_copy = Path(str(database_copy) + ".objects")
    retrieval_copy = Path(str(database_copy) + ".retrieval." + os.urandom(12).hex())
    database_tmp = target / f".{db_path.name}.{os.getpid()}.tmp"
    pack_tmp = Path(str(database_tmp) + ".objects")
    retrieval_tmp = Path(str(database_tmp) + ".retrieval")
    source = None
    destination = restored = None
    retrieval_handle = None
    source_lease = target_lease = None
    old_retrieval = None
    published = False
    try:
        try:
            source_lease = sessiondb.acquire_projection_pack_lease_path(
                db_path, exclusive=False,
            )
        except FileNotFoundError:
            source_lease = None
        source = sessiondb.connect(db_path)
        source.execute("BEGIN")
        source_projection_status = sessiondb.projection_status(source)
        source_retrieval = sessiondb.projection_pack_path(source)
        if source_retrieval.exists():
            retrieval_handle = source_retrieval.open("rb")
        lock_path = sessiondb.projection_pack_lock_path_for_database(database_copy)
        descriptor = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(descriptor)
        target_lease = sessiondb.acquire_projection_pack_lease_path(
            database_copy, exclusive=True,
        )
        if database_copy.exists():
            old = sessiondb.connect(database_copy, read_only=True)
            try:
                old_retrieval = sessiondb.projection_pack_path(old)
            finally:
                old.close()
        destination = sqlite3.connect(database_tmp)
        source.backup(destination)
        if retrieval_handle is not None:
            destination.execute(
                "INSERT OR REPLACE INTO metadata(key,value) VALUES('projection_pack_generation',?)",
                (retrieval_copy.name,),
            )
        destination.commit()
        destination.close()
        destination = None
        with database_tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        shutil.copy2(sessiondb.pack_path(source), pack_tmp)
        with pack_tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        if retrieval_handle is not None:
            with retrieval_tmp.open("wb") as handle:
                shutil.copyfileobj(retrieval_handle, handle)
                handle.flush()
                os.fsync(handle.fileno())
        os.chmod(database_tmp, 0o600)
        os.chmod(pack_tmp, 0o600)
        if retrieval_handle is not None:
            os.chmod(retrieval_tmp, 0o600)
            os.replace(retrieval_tmp, retrieval_copy)
        # Two renames cannot publish the pair atomically. An append-only pack still serves the
        # previous database, but after compaction it would not, so that database goes first and
        # an interruption leaves a visibly incomplete backup instead of a mismatched pair.
        if database_copy.exists() and not _file_is_prefix(pack_copy, pack_tmp):
            database_copy.unlink()
        os.replace(pack_tmp, pack_copy)
        os.replace(database_tmp, database_copy)
        published = True
        if (
            old_retrieval is not None and old_retrieval != retrieval_copy
            and old_retrieval.exists()
            and sessiondb.owned_projection_pack(database_copy, old_retrieval)
        ):
            old_retrieval.unlink()
        restored = sessiondb.connect(database_copy)
        integrity = scalar(restored, "PRAGMA integrity_check")
        report = sessiondb.report(restored, database_copy)
        source_fingerprint = [tuple(row) for row in source.execute(
            "SELECT object_hash,stored_hash,raw_bytes,stored_bytes FROM objects ORDER BY object_hash"
        )]
        restored_fingerprint = [tuple(row) for row in restored.execute(
            "SELECT object_hash,stored_hash,raw_bytes,stored_bytes FROM objects ORDER BY object_hash"
        )]
        projection_preserved = (
            not source_projection_status["complete"]
            or (
                sessiondb.projection_status(restored)["complete"]
                and report["retrieval_pack_verification"]["pass"]
            )
        )
        complete = (
            integrity == "ok" and report["pack_verification"]["pass"]
            and source_fingerprint == restored_fingerprint and projection_preserved
        )
        return {
            "database": str(database_copy), "integrity_check": integrity,
            "report": report, "complete": complete,
        }
    finally:
        if retrieval_handle is not None:
            retrieval_handle.close()
        if source_lease is not None:
            sessiondb.release_projection_pack_lease(source_lease)
        if target_lease is not None:
            sessiondb.release_projection_pack_lease(target_lease)
        if destination is not None:
            destination.close()
        if restored is not None:
            restored.close()
        if source is not None:
            source.close()
        database_tmp.unlink(missing_ok=True)
        pack_tmp.unlink(missing_ok=True)
        retrieval_tmp.unlink(missing_ok=True)
        if not published:
            retrieval_copy.unlink(missing_ok=True)


def compact_copy(
    db_path: Path, target_db: Path, readiness_receipt: Path | None = None,
) -> dict[str, Any]:
    """Create a verified compact copy; never replace the source database or pack.

    A full-corpus candidate still requires and consumes a one-use readiness receipt.
    """
    preflight = sessiondb.connect(db_path, read_only=True)
    try:
        preflight.execute("BEGIN")
        sessiondb.preflight_full_rebuild_readiness(preflight, readiness_receipt)
        event_count = scalar(preflight, "SELECT COUNT(*) FROM events")
    finally:
        preflight.close()
    result = build_storage_copy(db_path, target_db)
    if readiness_receipt is not None and event_count >= sessiondb.FULL_REBUILD_RECEIPT_MIN_EVENTS:
        candidate = sessiondb.connect(target_db, read_only=True)
        try:
            result["readiness_receipt"] = str(
                sessiondb.consume_rebuild_readiness_receipt(candidate, readiness_receipt, event_count)
            )
        finally:
            candidate.close()
        sessiondb.discard_consumed_receipt_file(readiness_receipt)
    return result


STORAGE_COPY_WORKERS = max(1, min(6, (os.cpu_count() or 2) - 2))
STRUCTURE_DICTIONARY_BYTES = 512 * 1024
STRUCTURE_COPY_LEVEL = 19
STORAGE_COPY_BATCH_BYTES = 64 * 1024 * 1024
# Tables copied verbatim; positional_projection_fts shadow tables keep the search index byte-identical.
VERBATIM_TABLES = (
    "sessions", "adapter_extensions", "search_term_variants", "search_projection_refs",
    "positional_projection_fts_data", "positional_projection_fts_idx",
    "positional_projection_fts_docsize", "positional_projection_fts_config",
)


class StorageCopyVerificationError(ValueError):
    def __init__(self, verification: dict[str, Any]) -> None:
        super().__init__(
            "storage copy verification failed: "
            + json.dumps({key: value for key, value in verification.items() if key != "fingerprints"},
                         sort_keys=True, default=str)[:4000]
        )
        self.verification = verification


def _compress_storage_block(item: tuple[bytes, int]) -> bytes:
    data, level = item
    return sessiondb.zstd.compress(data, level=level)


class _SourcePack:
    """Read stored bytes from a v10 or v11 snapshot through pack handles opened at snapshot time."""

    def __init__(self, db: sqlite3.Connection, handle: Any, layout: str) -> None:
        self.db = db
        self.handle = handle
        self.layout = layout
        self.block_key: int | None = None
        self.block: bytes = b""

    def read(self, row: sqlite3.Row) -> bytes:
        if row["compression"] == sessiondb.COMPRESSION:
            self.handle.seek(row["pack_offset"])
            compressed = self.handle.read(row["compressed_bytes"])
            if len(compressed) != row["compressed_bytes"]:
                raise ValueError(f"source pack extent missing: {row['object_hash']}")
            data = sessiondb.zstd.decompress(compressed, max_output_size=row["stored_bytes"])
        elif row["compression"] == sessiondb.BLOCK_COMPRESSION:
            if row["block_key"] != self.block_key:
                block = self.db.execute(
                    "SELECT pack_offset,compressed_bytes,raw_bytes,compressed_hash,block_digest "
                    "FROM object_blocks WHERE block_key=?", (row["block_key"],),
                ).fetchone()
                self.handle.seek(block["pack_offset"])
                compressed = self.handle.read(block["compressed_bytes"])
                if sessiondb.digest_bytes(compressed) != block["compressed_hash"]:
                    raise ValueError(f"source object block integrity failed: {row['block_key']}")
                self.block = sessiondb.zstd.decompress(compressed, max_output_size=block["raw_bytes"])
                if sessiondb.digest_bytes(self.block) != block["block_digest"]:
                    raise ValueError(f"source object block integrity failed: {row['block_key']}")
                self.block_key = row["block_key"]
            data = self.block[row["block_offset"]:row["block_offset"] + row["stored_bytes"]]
        else:
            raise ValueError(f"unsupported object compression: {row['compression']}")
        if len(data) != row["stored_bytes"] or sessiondb.digest(data) != row["stored_hash"]:
            raise ValueError(f"source object verification failed: {row['object_hash']}")
        return data

    def frame(self, row: sqlite3.Row) -> bytes | None:
        """The object's existing single Zstandard frame, reusable verbatim."""
        if row["compression"] != sessiondb.COMPRESSION:
            return None
        self.handle.seek(row["pack_offset"])
        return self.handle.read(row["compressed_bytes"])


def _snapshot_layout(db: sqlite3.Connection, schema: str = "main") -> str:
    row = db.execute(f"SELECT value FROM {schema}.metadata WHERE key='schema_version'").fetchone()
    if not row or row[0] not in (
        sessiondb.V10_SCHEMA_VERSION, sessiondb.V11_SCHEMA_VERSION, sessiondb.SCHEMA_VERSION,
    ):
        raise ValueError(f"storage copy supports schema v10 to v12 sources, not {row[0] if row else None}")
    return row[0]


def _source_object_columns(layout: str) -> str:
    if layout == sessiondb.V10_SCHEMA_VERSION:
        return (
            "o.object_hash AS object_hash,o.kind,o.raw_bytes,o.stored_bytes,o.stored_hash,"
            "o.compression,o.pack_offset,o.compressed_bytes,NULL AS block_key,NULL AS block_offset"
        )
    return (
        "lower(hex(o.object_hash)) AS object_hash,o.kind,o.raw_bytes,o.stored_bytes,"
        "lower(hex(coalesce(o.stored_hash,o.object_hash))) AS stored_hash,o.compression,"
        "o.pack_offset,o.compressed_bytes,o.block_key,o.block_offset"
    )


def _source_event_columns(layout: str) -> str:
    """Source events in v10 form (hex hashes and source_id) for either layout."""
    if layout == sessiondb.V10_SCHEMA_VERSION:
        return (
            "e.rowid AS event_rowid,e.event_id,e.source_id,e.provider,e.session_id,e.project,e.sequence,"
            "e.timestamp,e.timestamp_utc,e.order_timestamp,e.order_kind,e.category,e.role,e.tool,"
            "e.tool_event,e.parent_session_id,e.raw_hash,e.content_hash,e.content_shape,e.supported,"
            "e.structure_offset,e.structure_bytes,e.issue,e.line_number"
        )
    return sessiondb.EVENT_COLUMNS


def _source_event_join(layout: str, schema: str) -> str:
    if layout == sessiondb.V10_SCHEMA_VERSION:
        return f"{schema}.events e"
    return f"{schema}.events e JOIN {schema}.sources src ON src.source_key=e.source_key"


def logical_fingerprints(db: sqlite3.Connection, schema: str = "main") -> dict[str, str]:
    """Order-stable digests of every logical record, comparable across v10 and v11 layouts.

    Objects and object references cover only objects reachable from events: a verified copy
    drops unreachable objects by design. Hashes are compared in hex, events in v10 form.
    """
    layout = _snapshot_layout(db, schema)
    v10 = layout == sessiondb.V10_SCHEMA_VERSION

    def fold(sql: str, values: tuple[Any, ...] = ()) -> str:
        hasher = hashlib.sha256()
        count = 0
        for row in db.execute(sql, values):
            hasher.update(sessiondb.canonical([
                value.hex() if isinstance(value, bytes) else value for value in tuple(row)
            ]))
            hasher.update(b"\n")
            count += 1
        return f"{count}:{hasher.hexdigest()}"

    result = {
        "events": fold(
            f"SELECT {_source_event_columns(layout)} FROM {_source_event_join(layout, schema)} "
            "ORDER BY event_rowid"
        ),
        "sources": _fold_sources(db, schema, v10),
        "event_content": fold(
            "SELECT e.rowid,c.ordinal,c.object_hash FROM "
            + (f"{schema}.event_content c JOIN {schema}.events e USING(event_id)" if v10
               else f"{schema}.event_content c JOIN {schema}.events e ON e.event_rowid=c.event_rowid")
            + " ORDER BY e.rowid,c.ordinal"
        ),
        "metadata": fold(
            f"SELECT key,value FROM {schema}.metadata WHERE key NOT IN "
            "('schema_version','projection_pack_generation') ORDER BY key"
        ),
        "search_projection_blocks": fold(
            f"SELECT block_key,block_digest,raw_bytes FROM {schema}.search_projection_blocks b "
            f"WHERE EXISTS (SELECT 1 FROM {schema}.search_projection_refs r WHERE r.block_key=b.block_key) "
            "ORDER BY block_key"
        ),
    }
    for table in VERBATIM_TABLES:
        key = {
            "positional_projection_fts_idx": "segid,term",
            "positional_projection_fts_config": "k",
            "sessions": "provider,session_id,source_id",
            "adapter_extensions": "provider,category,structural_signature",
            "search_term_variants": "folded,term",
        }.get(table, "1")
        result[table] = fold(f"SELECT * FROM {schema}.{table} ORDER BY {key}")
    reachable = _load_reachable(db, schema)
    result["objects"] = fold(
        f"SELECT {_source_object_columns(layout).split(',o.compression')[0]} FROM {schema}.objects o "
        "JOIN temp.sessiondb_reachable r ON r.object_hash=o.object_hash ORDER BY 1"
    )
    result["object_references"] = fold(
        "SELECT " + ("x.parent_hash,x.ordinal,x.child_hash" if v10 else
                     "lower(hex(x.parent_hash)),x.ordinal,lower(hex(x.child_hash))")
        + f",x.purpose FROM {schema}.object_references x "
        "JOIN temp.sessiondb_reachable r ON r.object_hash=x.parent_hash ORDER BY 1,2"
    )
    result["reachable_objects"] = str(reachable)
    db.execute("DROP TABLE IF EXISTS temp.sessiondb_reachable")
    tables = {row[0] for row in db.execute(f"SELECT name FROM {schema}.sqlite_master WHERE type='table'")}
    for table in (*token_usage.TOKEN_TABLES, "token_launches"):
        if table in tables:
            result[table] = fold(f"SELECT * FROM {schema}.{table} ORDER BY 1,2")
    return result


def _fold_sources(db: sqlite3.Connection, schema: str, v10: bool) -> str:
    """Digest of every source row with its structure bundle decompressed, whatever its codec."""
    hasher = hashlib.sha256()
    count = 0
    for row in db.execute(
        "SELECT source_id,provider,path,size_bytes,mtime_ns,source_hash,adapter_version,status,"
        "issue_count,structure_raw_bytes,structure_zstd,"
        + ("NULL" if v10 else "structure_dict_id") + f" FROM {schema}.sources ORDER BY source_id"
    ):
        structure = sessiondb.decompress_structure_bundle(db, row[10], row[9], row[11])
        hasher.update(sessiondb.canonical([*row[:10], sessiondb.digest(structure)]))
        hasher.update(b"\n")
        count += 1
    return f"{count}:{hasher.hexdigest()}"


def _train_structure_dictionary(bundles: list[bytes], size: int) -> bytes | None:
    """A Zstandard dictionary trained on chunks of a deterministic sample of structure bundles.

    A size of 0 disables dictionaries; bundles then stay plain Zstandard frames."""
    if size <= 0 or sum(len(bundle) for bundle in bundles) < 8 * size:
        return None
    chooser = random.Random(len(bundles))
    samples = [
        bundle[start:start + 16 * 1024]
        for bundle in chooser.sample(bundles, min(1500, len(bundles)))
        for start in range(0, min(len(bundle), 256 * 1024), 16 * 1024)
    ]
    try:
        return sessiondb.zstd.train_dictionary(size, samples, k=1024, d=8, f=20, accel=1, threads=-1).as_bytes()
    except sessiondb.zstd.ZstdError:
        return None


_WORKER_DICTIONARY: Any = None


def _init_dictionary_worker(dictionary: bytes) -> None:
    global _WORKER_DICTIONARY
    _WORKER_DICTIONARY = sessiondb.zstd.ZstdCompressor(
        level=STRUCTURE_COPY_LEVEL, dict_data=sessiondb.zstd.ZstdCompressionDict(dictionary),
    )


def _compress_with_worker_dictionary(data: bytes) -> bytes:
    return _WORKER_DICTIONARY.compress(data)


def _recompress_structures(
    db: sqlite3.Connection, table: str, v10: bool, workers: int, dictionary_bytes: int,
) -> dict[str, Any]:
    """Recompress every structure bundle in `table` with a newly trained dictionary, if one trains."""
    if v10:
        db.execute(_schema_table_sql("compression_dictionaries", "compression_dictionaries"))
    keys = [row[0] for row in db.execute(f"SELECT source_key FROM {table} ORDER BY source_key")]

    def bundles_for(chunk: list[int]) -> list[tuple[int, bytes]]:
        rows = db.execute(
            f"SELECT source_key,structure_raw_bytes,structure_zstd,structure_dict_id FROM {table} "
            f"WHERE source_key IN ({','.join('?' * len(chunk))}) ORDER BY source_key", chunk,
        ).fetchall()
        result = []
        for row in rows:
            data = sessiondb.decompress_structure_bundle(db, row[2], row[1], row[3])
            if len(data) != row[1]:
                raise ValueError(f"structure bundle length verification failed: {row[0]}")
            result.append((row[0], data))
        return result

    chooser = random.Random(len(keys))
    sampled = sorted(chooser.sample(keys, min(1500, len(keys))))
    samples = [data for start in range(0, len(sampled), 200) for _key, data in bundles_for(sampled[start:start + 200])]
    before = db.execute(f"SELECT COALESCE(SUM(length(structure_zstd)),0) FROM {table}").fetchone()[0]
    dictionary = _train_structure_dictionary(samples, dictionary_bytes) if keys else None
    samples.clear()
    if dictionary is None:
        return {"dictionary": None, "sources": len(keys), "bytes_before": before, "bytes_after": before}
    dict_id = db.execute("SELECT COALESCE(MAX(dict_id),0)+1 FROM compression_dictionaries").fetchone()[0]
    db.execute(
        "INSERT INTO compression_dictionaries(dict_id,purpose,dictionary,dictionary_hash,created_at) "
        "VALUES(?,?,?,?,?)",
        (dict_id, sessiondb.STRUCTURE_DICTIONARY_PURPOSE, dictionary, sessiondb.digest_bytes(dictionary),
         datetime.now().astimezone().isoformat(timespec="seconds")),
    )
    import multiprocessing
    with multiprocessing.get_context("spawn").Pool(
        max(1, workers), initializer=_init_dictionary_worker, initargs=(dictionary,),
    ) as pool:
        for start in range(0, len(keys), 200):
            batch = bundles_for(keys[start:start + 200])
            compressed = pool.map(_compress_with_worker_dictionary, [data for _key, data in batch], chunksize=4)
            db.executemany(
                f"UPDATE {table} SET structure_zstd=?,structure_dict_id=? WHERE source_key=?",
                ((blob, dict_id, key) for (key, _data), blob in zip(batch, compressed)),
            )
    db.execute(
        f"DELETE FROM compression_dictionaries WHERE dict_id NOT IN "
        f"(SELECT structure_dict_id FROM {table} WHERE structure_dict_id IS NOT NULL)"
    )
    after = db.execute(f"SELECT COALESCE(SUM(length(structure_zstd)),0) FROM {table}").fetchone()[0]
    return {"dictionary": dict_id, "dictionary_bytes": len(dictionary), "sources": len(keys),
            "bytes_before": before, "bytes_after": after}


def _load_reachable(db: sqlite3.Connection, schema: str = "main") -> int:
    """Fill temp.sessiondb_reachable with every object reachable from events; return its size.

    Roots are each event's content objects and the objects its structure bundle references
    directly; manifests' child references are followed transitively."""
    v10 = _snapshot_layout(db, schema) == sessiondb.V10_SCHEMA_VERSION
    db.execute("DROP TABLE IF EXISTS temp.sessiondb_reachable")
    db.execute("CREATE TEMP TABLE sessiondb_reachable(object_hash PRIMARY KEY) WITHOUT ROWID")
    db.execute(
        f"INSERT OR IGNORE INTO temp.sessiondb_reachable SELECT object_hash FROM {schema}.event_content"
    )
    for raw_bytes, bundle, dict_id in db.execute(
        f"SELECT structure_raw_bytes,structure_zstd,{'NULL' if v10 else 'structure_dict_id'} FROM {schema}.sources"
    ).fetchall():
        hashes = sessiondb.structure_object_hashes(
            sessiondb.decompress_structure_bundle(db, bundle, raw_bytes, dict_id)
        )
        db.executemany(
            "INSERT OR IGNORE INTO temp.sessiondb_reachable VALUES(" + ("?" if v10 else "unhex(?)") + ")",
            ((value,) for value in hashes),
        )
    while True:
        added = db.execute(
            f"INSERT OR IGNORE INTO temp.sessiondb_reachable SELECT r.child_hash "
            f"FROM {schema}.object_references r JOIN temp.sessiondb_reachable x ON x.object_hash=r.parent_hash"
        ).rowcount
        if not added:
            break
    return db.execute("SELECT COUNT(*) FROM temp.sessiondb_reachable").fetchone()[0]


def _write_batches(
    pool: Any, batches: list[tuple[list[tuple[Any, ...]], bytes]], level: int,
) -> list[bytes]:
    if pool is None:
        return [_compress_storage_block((raw, level)) for _members, raw in batches]
    return pool.map(_compress_storage_block, [(raw, level) for _members, raw in batches], chunksize=1)


def _schema_table_sql(name: str, new_name: str) -> str:
    """One CREATE TABLE statement from the current schema, under another name."""
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS {name} \(\n.*?\n\)( WITHOUT ROWID)?;", sessiondb.SCHEMA, re.DOTALL,
    )
    if not match:
        raise ValueError(f"table missing from schema: {name}")
    return match.group(0).replace(f"CREATE TABLE IF NOT EXISTS {name} (", f"CREATE TABLE {new_name} (", 1)


def build_storage_copy(
    db_path: Path, target_db: Path, *, block_target: int = sessiondb.OBJECT_BLOCK_TARGET_BYTES,
    workers: int = STORAGE_COPY_WORKERS, progress: Callable[[dict[str, Any]], None] | None = None,
    structure_dictionary_bytes: int = STRUCTURE_DICTIONARY_BYTES,
    small_object_max: int = sessiondb.SMALL_OBJECT_MAX_BYTES,
    large_block_target: int = sessiondb.LARGE_OBJECT_BLOCK_TARGET_BYTES,
    single_object_min: int = sessiondb.SINGLE_OBJECT_MIN_BYTES,
) -> dict[str, Any]:
    """Write a verified current-schema copy of a v10 or v11 database; never modify the source.

    The copy keeps every event, its row identity, the search index (FTS tables are never
    rewritten), projection references, and every reachable object. Objects are grouped into solid
    blocks in pack order: small objects into `block_target` blocks, larger ones into
    `large_block_target` blocks; objects of `single_object_min` or more keep their own frame.
    Retrieval blocks are recompressed at a higher level without changing their contents, and only
    unreachable objects and unreferenced retrieval blocks are dropped.
    """
    started = time.monotonic()
    target_db = target_db.absolute()
    target_pack = Path(str(target_db) + ".objects")
    if target_db.exists() or target_pack.exists() or list(target_db.parent.glob(target_db.name + ".retrieval.*")):
        raise FileExistsError(f"storage copy target already exists: {target_db}")
    target_db.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Everything is built and verified under the final names inside a private work directory on
    # the target's filesystem, then renamed into place; a failure publishes nothing.
    work = target_db.parent / f".{target_db.name}.{os.getpid()}.build"
    work.mkdir(mode=0o700)
    generation = target_db.name + ".retrieval." + os.urandom(12).hex()
    build_path = work / "build.sqlite"
    final_tmp = work / target_db.name
    pack_tmp = Path(str(final_tmp) + ".objects")
    retrieval_tmp = work / generation

    def note(phase: str, **values: Any) -> None:
        if progress:
            progress({"phase": phase, "elapsed_seconds": round(time.monotonic() - started, 1), **values})

    source_lease = None
    source = None
    object_handle = retrieval_handle = None
    build = None
    pool = None
    published = False
    try:
        # Snapshot under a short read lock: SQLite through the backup API, packs through handles
        # opened inside the same transaction. Packs are append-only, so every extent the snapshot
        # references stays readable through these handles while the live database keeps writing.
        try:
            source_lease = sessiondb.acquire_projection_pack_lease_path(db_path, exclusive=False)
        except FileNotFoundError:
            source_lease = None
        source = sqlite3.connect(sessiondb.read_only_uri(db_path.resolve()), uri=True, timeout=60)
        source.execute("BEGIN")
        layout = _snapshot_layout(source)
        source_generation = source.execute(
            "SELECT value FROM metadata WHERE key='projection_pack_generation'"
        ).fetchone()
        object_handle = Path(str(db_path.resolve()) + ".objects").open("rb")
        if source_generation:
            retrieval_handle = (db_path.resolve().parent / source_generation[0]).open("rb")
        snapshot = sqlite3.connect(build_path)
        source.backup(snapshot)
        snapshot.close()
        source.rollback()
        source.close()
        source = None
        if source_lease is not None:
            sessiondb.release_projection_pack_lease(source_lease)
            source_lease = None
        note("snapshot", layout=layout)

        # The build file is a second snapshot transformed in place, so the search index tables
        # are never rewritten; VACUUM INTO then writes the compact final file.
        build = sqlite3.connect(build_path, isolation_level=None)
        build.row_factory = sqlite3.Row
        build.execute("PRAGMA journal_mode=OFF")
        build.execute("PRAGMA synchronous=OFF")
        build.execute("PRAGMA foreign_keys=OFF")
        build.execute("PRAGMA cache_size=-262144")
        source_prints = logical_fingerprints(build)
        note("source_fingerprints")
        v10 = layout == sessiondb.V10_SCHEMA_VERSION
        build.execute("BEGIN")
        replaced = ["objects", "object_references"] + (["sources", "events", "event_content"] if v10 else [])
        if not v10:
            replaced.append("object_blocks")
        for table in replaced:
            build.execute(_schema_table_sql(table, f"{table}_v11"))
        if v10:
            build.execute(
                "INSERT INTO sources_v11(source_key,source_id,provider,path,size_bytes,mtime_ns,"
                "source_hash,adapter_version,status,issue_count,structure_raw_bytes,structure_zstd) "
                "SELECT rowid,source_id,provider,path,size_bytes,mtime_ns,source_hash,adapter_version,"
                "status,issue_count,structure_raw_bytes,structure_zstd FROM sources ORDER BY rowid"
            )
            build.execute(
                "INSERT INTO events_v11(event_rowid,event_id,source_key,provider,session_id,project,"
                "sequence,timestamp,timestamp_utc,order_timestamp,order_kind,category,role,tool,"
                "tool_event,parent_session_id,raw_hash,content_hash,content_shape,supported,"
                "structure_offset,structure_bytes,issue,line_number) "
                "SELECT e.rowid,unhex(e.event_id),s.rowid,e.provider,e.session_id,e.project,e.sequence,"
                "e.timestamp,e.timestamp_utc,e.order_timestamp,e.order_kind,e.category,e.role,e.tool,"
                "e.tool_event,e.parent_session_id,unhex(e.raw_hash),unhex(e.content_hash),"
                "e.content_shape,e.supported,e.structure_offset,e.structure_bytes,e.issue,e.line_number "
                "FROM events e JOIN sources s USING(source_id) ORDER BY e.rowid"
            )
            build.execute(
                "INSERT INTO event_content_v11 SELECT e.rowid,c.ordinal,unhex(c.object_hash) "
                "FROM event_content c JOIN events e USING(event_id) ORDER BY e.rowid,c.ordinal"
            )
        note("tables")

        # Objects: every reachable object, in source pack order. Objects of at least single_object_min bytes
        # (64 MiB by default) keep their existing frame; smaller ones are grouped into solid blocks.
        reachable = _load_reachable(build)
        order = (
            "o.pack_offset,o.object_hash" if v10
            else "coalesce(b.pack_offset,o.pack_offset),o.block_offset,o.object_hash"
        )
        block_join = "" if v10 else "LEFT JOIN object_blocks b ON b.block_key=o.block_key "
        reader = _SourcePack(build, object_handle, layout)
        object_rows: list[tuple[Any, ...]] = []
        block_rows: list[tuple[Any, ...]] = []
        level = sessiondb.OBJECT_BLOCK_LEVEL
        if workers > 1:
            import multiprocessing
            pool = multiprocessing.get_context("spawn").Pool(workers)
        stats = {"reachable": reachable, "objects": 0, "single_frames": 0, "block_members": 0, "blocks": 0}
        with pack_tmp.open("wb") as pack_out:
            pending: list[tuple[list[tuple[Any, ...]], bytes]] = []
            pending_bytes = 0
            # Two block streams in pack order: small objects and large objects.
            streams: dict[str, tuple[list[tuple[Any, ...]], bytearray]] = {
                "small": ([], bytearray()), "large": ([], bytearray()),
            }

            def close_block(name: str) -> None:
                nonlocal pending_bytes
                members, buffer = streams[name]
                if members:
                    pending.append((list(members), bytes(buffer)))
                    pending_bytes += len(buffer)
                    members.clear()
                    buffer.clear()

            def drain() -> None:
                nonlocal pending_bytes
                for (block_members, raw), compressed in zip(pending, _write_batches(pool, pending, level)):
                    block_key = len(block_rows) + 1
                    offset = pack_out.tell()
                    pack_out.write(compressed)
                    block_rows.append((
                        block_key, offset, len(compressed), len(raw),
                        sessiondb.digest_bytes(compressed), sessiondb.digest_bytes(raw),
                    ))
                    for member in block_members:
                        object_rows.append((*member[:5], sessiondb.BLOCK_COMPRESSION, None, None,
                                            block_key, member[5]))
                    stats["blocks"] += 1
                pending.clear()
                pending_bytes = 0

            for row in build.execute(
                f"SELECT {_source_object_columns(layout)} FROM objects o "
                f"JOIN temp.sessiondb_reachable r ON r.object_hash=o.object_hash {block_join}"
                f"ORDER BY {order}"
            ):
                data = reader.read(row)
                stored_hash = None if row["stored_hash"] == row["object_hash"] else bytes.fromhex(row["stored_hash"])
                identity = (bytes.fromhex(row["object_hash"]), row["kind"], row["raw_bytes"],
                            row["stored_bytes"], stored_hash)
                stats["objects"] += 1
                if len(data) >= single_object_min:
                    frame = reader.frame(row)
                    if frame is None:
                        frame = sessiondb.zstd.compress(data, level=sessiondb.ZSTD_LEVEL)
                    offset = pack_out.tell()
                    pack_out.write(frame)
                    object_rows.append((*identity, sessiondb.COMPRESSION, offset, len(frame), None, None))
                    stats["single_frames"] += 1
                    continue
                name = "small" if len(data) < small_object_max else "large"
                members, buffer = streams[name]
                if members and len(buffer) + len(data) > (block_target if name == "small" else large_block_target):
                    close_block(name)
                    if pending_bytes >= STORAGE_COPY_BATCH_BYTES:
                        drain()
                members.append((*identity, len(buffer)))
                buffer.extend(data)
                stats["block_members"] += 1
            close_block("small")
            close_block("large")
            drain()
            pack_out.flush()
            os.fsync(pack_out.fileno())
        note("objects", **stats)
        object_blocks_table = "object_blocks" if v10 else "object_blocks_v11"
        if v10:
            build.execute(_schema_table_sql("object_blocks", "object_blocks"))
        build.executemany(f"INSERT INTO {object_blocks_table} VALUES(?,?,?,?,?,?)", block_rows)
        object_rows.sort(key=lambda item: item[0])
        build.executemany(
            "INSERT INTO objects_v11(object_hash,kind,raw_bytes,stored_bytes,stored_hash,compression,"
            "pack_offset,compressed_bytes,block_key,block_offset) VALUES(?,?,?,?,?,?,?,?,?,?)",
            object_rows,
        )
        object_rows.clear()
        build.execute(
            "INSERT INTO object_references_v11 SELECT "
            + ("unhex(x.parent_hash),x.ordinal,unhex(x.child_hash),x.purpose" if v10 else "x.*")
            + " FROM object_references x JOIN temp.sessiondb_reachable r ON r.object_hash=x.parent_hash "
            "ORDER BY x.parent_hash,x.ordinal"
        )
        build.execute("DROP TABLE temp.sessiondb_reachable")

        # Structure bundles: recompressed with a dictionary trained on this corpus's bundles.
        structure_stats = _recompress_structures(
            build, "sources_v11" if v10 else "sources", v10, workers, structure_dictionary_bytes,
        )
        note("structures", **structure_stats)

        # Retrieval blocks: same contents and block keys, recompressed, unreferenced blocks dropped.
        retrieval_blocks = build.execute(
            "SELECT block_key,block_digest,pack_offset,compressed_bytes,raw_bytes,compressed_hash "
            "FROM search_projection_blocks b WHERE EXISTS "
            "(SELECT 1 FROM search_projection_refs r WHERE r.block_key=b.block_key) "
            "ORDER BY pack_offset,block_key"
        ).fetchall()
        if retrieval_blocks and retrieval_handle is None:
            raise ValueError("source retrieval pack is missing")
        updates: list[tuple[Any, ...]] = []
        with retrieval_tmp.open("wb") as retrieval_out:
            for start in range(0, len(retrieval_blocks), 256):
                chunk = retrieval_blocks[start:start + 256]
                raws = []
                for block in chunk:
                    retrieval_handle.seek(block["pack_offset"])
                    compressed = retrieval_handle.read(block["compressed_bytes"])
                    if sessiondb.digest_bytes(compressed) != block["compressed_hash"]:
                        raise ValueError(f"source projection block integrity failed: {block['block_key']}")
                    raw = sessiondb.zstd.decompress(compressed, max_output_size=block["raw_bytes"])
                    if len(raw) != block["raw_bytes"] or sessiondb.digest_bytes(raw) != block["block_digest"]:
                        raise ValueError(f"source projection block integrity failed: {block['block_key']}")
                    raws.append(([], raw))
                for block, compressed in zip(chunk, _write_batches(pool, raws, sessiondb.RETRIEVAL_COPY_LEVEL)):
                    offset = retrieval_out.tell()
                    retrieval_out.write(compressed)
                    updates.append((offset, len(compressed), sessiondb.digest_bytes(compressed), block["block_key"]))
            retrieval_out.flush()
            os.fsync(retrieval_out.fileno())
        build.execute(
            "DELETE FROM search_projection_blocks WHERE NOT EXISTS "
            "(SELECT 1 FROM search_projection_refs r WHERE r.block_key=search_projection_blocks.block_key)"
        )
        build.executemany(
            "UPDATE search_projection_blocks SET pack_offset=?,compressed_bytes=?,compressed_hash=? "
            "WHERE block_key=?", updates,
        )
        note("retrieval", blocks=len(updates))
        if pool is not None:
            pool.close()
            pool.join()
            pool = None

        build.execute("DROP TRIGGER IF EXISTS events_projection_bd")
        for table in replaced:
            build.execute(f"DROP TABLE {table}")
        for table in replaced:
            build.execute(f"ALTER TABLE {table}_v11 RENAME TO {table}")
        build.execute(
            "UPDATE metadata SET value=? WHERE key='schema_version'", (sessiondb.SCHEMA_VERSION,),
        )
        build.execute(
            "INSERT OR REPLACE INTO metadata(key,value) VALUES('projection_pack_generation',?)",
            (generation,),
        )
        build.execute("COMMIT")
        build.executescript(sessiondb.SCHEMA.replace("PRAGMA foreign_keys=ON;", ""))
        token_usage.ensure_schema(build)
        foreign_key_failures = [tuple(row) for row in build.execute("PRAGMA foreign_key_check")]
        build.execute(f"PRAGMA page_size={sessiondb.PAGE_SIZE}")
        build.execute("VACUUM INTO ?", (str(final_tmp),))
        build.close()
        build = None
        build_path.unlink()
        with final_tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        note("vacuum")
        token_backfill = None
        if "token_source_state" not in source_prints:
            # A v10 or v11 source has no token projection; derive it in the copy from its own records.
            import rebuild_copy
            copy = sessiondb.connect(final_tmp)
            try:
                token_backfill = rebuild_copy.backfill_token_usage(copy)
                copy.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                copy.close()
            for suffix in ("-wal", "-shm"):
                Path(str(final_tmp) + suffix).unlink(missing_ok=True)
            note("token_backfill", sources=token_backfill["sources"], errors=token_backfill["error"])

        for path in (final_tmp, pack_tmp, retrieval_tmp):
            os.chmod(path, 0o600)
        verification = verify_storage_copy(source_prints, final_tmp)
        verification["foreign_key_failures"] = foreign_key_failures
        verification["complete"] = verification["complete"] and not foreign_key_failures
        note("verified", complete=verification["complete"])
        if not verification["complete"]:
            raise StorageCopyVerificationError(verification)
        storage = storage_bytes(final_tmp)
        lock_path = sessiondb.projection_pack_lock_path_for_database(target_db)
        descriptor = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(descriptor)
        os.replace(retrieval_tmp, target_db.parent / generation)
        os.replace(pack_tmp, target_pack)
        os.replace(final_tmp, target_db)
        sessiondb.fsync_directory(target_db.parent)
        published = True
        return {
            "database": str(target_db), "source_layout": layout, "objects": stats,
            "structures": structure_stats,
            "retrieval_blocks": len(updates), "elapsed_seconds": round(time.monotonic() - started, 1),
            "storage": storage, "token_backfill": token_backfill, **verification,
        }
    finally:
        if pool is not None:
            pool.terminate()
        if build is not None:
            build.close()
        if source is not None:
            source.close()
        if source_lease is not None:
            sessiondb.release_projection_pack_lease(source_lease)
        for handle in (object_handle, retrieval_handle):
            if handle is not None:
                handle.close()
        shutil.rmtree(work, ignore_errors=True)
        if not published:
            sessiondb.projection_pack_lock_path_for_database(target_db).unlink(missing_ok=True)


def storage_bytes(db_path: Path) -> dict[str, int]:
    """Core storage of one database: SQLite file, object pack, and active retrieval pack."""
    db = sessiondb.connect(db_path, read_only=True)
    try:
        retrieval = sessiondb.projection_pack_path(db)
    finally:
        db.close()
    sizes = {
        "database": db_path.stat().st_size,
        "objects": Path(str(db_path) + ".objects").stat().st_size,
        "retrieval": retrieval.stat().st_size,
    }
    sizes["total"] = sum(sizes.values())
    return sizes


def verify_storage_copy(source: Path | dict[str, str], target_db: Path) -> dict[str, Any]:
    """Prove a copy holds the same logical records as its source and passes every integrity check."""
    target = sessiondb.connect(target_db, read_only=True)
    try:
        integrity = scalar(target, "PRAGMA integrity_check")
        report = sessiondb.report(target, target_db)
        projection = sessiondb.projection_status(target)
        freelist = scalar(target, "PRAGMA freelist_count")
        target_prints = logical_fingerprints(target)
        # Every object an event, a manifest, or a structure bundle references must be present.
        _load_reachable(target)
        dangling = scalar(
            target,
            "SELECT COUNT(*) FROM temp.sessiondb_reachable r LEFT JOIN objects o ON o.object_hash=r.object_hash "
            "WHERE o.object_hash IS NULL",
        )
        target.execute("DROP TABLE temp.sessiondb_reachable")
    finally:
        target.close()
    if isinstance(source, dict):
        source_prints = source
    else:
        connection = sqlite3.connect(sessiondb.read_only_uri(source), uri=True)
        try:
            source_prints = logical_fingerprints(connection)
        finally:
            connection.close()
    mismatched = sorted(key for key in source_prints if source_prints[key] != target_prints.get(key))
    complete = (
        integrity == "ok" and report["pack_verification"]["pass"]
        and not report["pack_verification"]["orphan_bytes"]
        and report["retrieval_pack_verification"]["pass"]
        and not report["retrieval_pack_verification"]["orphan_bytes"]
        and projection["complete"] and freelist == 0 and not mismatched and dangling == 0
    )
    return {
        "complete": complete, "integrity_check": integrity, "freelist_pages": freelist,
        "dangling_references": dangling,
        "projection_complete": projection["complete"], "fingerprints": target_prints,
        "fingerprint_mismatches": mismatched,
        "pack_verification": {key: report["pack_verification"][key] for key in ("pass", "objects_checked", "orphan_bytes")}
        | {"failures": report["pack_verification"]["failures"][:20]},
        "retrieval_pack_verification": {
            key: report["retrieval_pack_verification"][key] for key in ("pass", "blocks", "orphan_bytes")
        } | {"failures": report["retrieval_pack_verification"]["failures"][:20]},
    }


def compact_if_due(
    db_path: Path, target_db: Path, force: bool = False,
    readiness_receipt: Path | None = None,
) -> dict[str, Any]:
    analysis = compaction_analysis(db_path, target_db.parent)
    if not force and not analysis["threshold_met"]:
        return {"complete": True, "created": False, "status": "not_due", "analysis": analysis}
    if not analysis["headroom_ok"]:
        return {"complete": False, "created": False, "status": "insufficient_headroom", "analysis": analysis}
    result = compact_copy(db_path, target_db, readiness_receipt)
    result.update({"created": True, "status": "candidate_created", "analysis": analysis, "forced": force})
    return result


def validate_promotion(baseline_path: Path, candidate_path: Path) -> dict[str, Any]:
    baseline = json.loads(baseline_path.read_text())
    candidate = json.loads(candidate_path.read_text())
    correctness_equal = (
        baseline.get("pass") is True and candidate.get("pass") is True
        and baseline.get("database_events") == candidate.get("database_events")
        and baseline.get("correctness") == candidate.get("correctness")
        and baseline.get("beyond_index") == candidate.get("beyond_index")
    )
    tokens_equal = baseline.get("tokens") == candidate.get("tokens")
    baseline_p95 = baseline.get("latency_ms", {}).get("bounded", {}).get("p95")
    candidate_p95 = candidate.get("latency_ms", {}).get("bounded", {}).get("p95")
    p95_regression_percent = None
    speed_pass = False
    if isinstance(baseline_p95, (int, float)) and isinstance(candidate_p95, (int, float)):
        p95_regression_percent = (
            (candidate_p95 / baseline_p95 - 1) * 100 if baseline_p95 else (0.0 if not candidate_p95 else float("inf"))
        )
        speed_pass = p95_regression_percent <= COMPACTION_MAX_P95_REGRESSION_PERCENT
    result = {
        "correctness_equal": correctness_equal,
        "tokens_equal": tokens_equal,
        "speed_pass": speed_pass,
        "baseline_p95_ms": baseline_p95,
        "candidate_p95_ms": candidate_p95,
        "p95_regression_percent": p95_regression_percent,
        "maximum_p95_regression_percent": COMPACTION_MAX_P95_REGRESSION_PERCENT,
    }
    result["promotion_allowed"] = all((correctness_equal, tokens_equal, speed_pass))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sync_parser = commands.add_parser("sync")
    sync_parser.add_argument("--db", type=Path, required=True)
    sync_parser.add_argument("--manifest", type=Path, required=True)
    sync_parser.add_argument("--report", type=Path, required=True)
    sync_parser.add_argument("--sample", type=int, default=2000)
    backup_parser = commands.add_parser("backup")
    backup_parser.add_argument("--db", type=Path, required=True)
    backup_parser.add_argument("--target", type=Path, required=True)
    status_parser = commands.add_parser("compact-status")
    status_parser.add_argument("--db", type=Path, required=True)
    compact_parser = commands.add_parser("compact")
    compact_parser.add_argument("--db", type=Path, required=True)
    compact_parser.add_argument("--target-db", type=Path, required=True)
    compact_parser.add_argument("--readiness-receipt", type=Path)
    compact_parser.add_argument("--force", action="store_true", help="create a test candidate below the policy threshold")
    migrate_parser = commands.add_parser(
        "migrate-storage", help="write a verified current-schema copy of a v10 or v11 database",
    )
    migrate_parser.add_argument("--db", type=Path, required=True)
    migrate_parser.add_argument("--target-db", type=Path, required=True)
    migrate_parser.add_argument("--block-bytes", type=int, default=sessiondb.OBJECT_BLOCK_TARGET_BYTES)
    migrate_parser.add_argument("--small-object-max", type=int, default=sessiondb.SMALL_OBJECT_MAX_BYTES)
    migrate_parser.add_argument("--large-block-bytes", type=int, default=sessiondb.LARGE_OBJECT_BLOCK_TARGET_BYTES)
    migrate_parser.add_argument("--single-object-min", type=int, default=sessiondb.SINGLE_OBJECT_MIN_BYTES)
    migrate_parser.add_argument("--structure-dictionary-bytes", type=int, default=STRUCTURE_DICTIONARY_BYTES,
                                help="trained dictionary size for structure bundles; 0 disables it")
    migrate_parser.add_argument("--workers", type=int, default=STORAGE_COPY_WORKERS)
    rebuild_parser = commands.add_parser(
        "rebuild-into-copy", help="write a verified current-schema copy (token projection included)",
    )
    rebuild_parser.add_argument("--db", type=Path, required=True)
    rebuild_parser.add_argument("--target-db", type=Path, required=True)
    rebuild_parser.add_argument("--rebuild-projections", action="store_true",
                                help="re-derive all search text with the current redaction")
    promote_parser = commands.add_parser(
        "promote-copy", help="swap a verified copy into the live location, keeping a rollback copy",
    )
    promote_parser.add_argument("--db", type=Path, required=True)
    promote_parser.add_argument("--copy-db", type=Path, required=True)
    promote_parser.add_argument("--rollback-dir", type=Path, required=True)
    promote_parser.add_argument("--execute", action="store_true", help="perform the swap; default prints the plan")
    promote_parser.add_argument("--no-jobs", action="store_true", help="do not stop or restart launchd jobs")
    promote_parser.add_argument("--no-install", action="store_true", help="do not run the installer")
    launches_parser = commands.add_parser(
        "import-ledger-launches", help="copy launcher-registered lineage from a compatible SQLite database",
    )
    launches_parser.add_argument("--db", type=Path, required=True)
    launches_parser.add_argument("--ledger", type=Path, required=True)
    token_status_parser = commands.add_parser("token-status")
    token_status_parser.add_argument("--db", type=Path, required=True)
    token_rebuild_parser = commands.add_parser(
        "rebuild-token-usage", help="re-derive failed or outdated sources' token rows from stored records",
    )
    token_rebuild_parser.add_argument("--db", type=Path, required=True)
    token_rebuild_parser.add_argument("--all", action="store_true", help="re-derive every source")
    cloud_parser = commands.add_parser(
        "import-cloud", help="convert a cloud session's list_events export into a transcript SessionDB reads",
    )
    cloud_parser.add_argument("--export", type=Path, required=True, help="JSON array of events, oldest first")
    cloud_parser.add_argument("--session", required=True)
    cloud_parser.add_argument("--project", required=True, help="folder name to group the session under")
    cloud_parser.add_argument("--title")
    cloud_parser.add_argument("--replace", action="store_true",
                              help="allow an export that does not extend the saved one")
    sync_cloud_parser = commands.add_parser(
        "sync-cloud", help="download a cloud session from claude.ai with this Mac's Claude Code sign-in and save it",
    )
    sync_cloud_parser.add_argument("--session", required=True)
    sync_cloud_parser.add_argument("--project", required=True, help="folder name to group the session under")
    sync_cloud_parser.add_argument("--title")
    sync_cloud_parser.add_argument("--replace", action="store_true",
                                   help="allow a download that does not extend the saved transcript")
    promotion_parser = commands.add_parser("validate-promotion")
    promotion_parser.add_argument("--baseline-evaluation", type=Path, required=True)
    promotion_parser.add_argument("--candidate-evaluation", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "sync":
        args.db.parent.mkdir(parents=True, exist_ok=True)
        result = sync(args.db, args.manifest, args.report, args.sample)
    elif args.command == "backup":
        result = backup(args.db, args.target)
    elif args.command == "compact-status":
        result = compaction_analysis(args.db)
    elif args.command == "migrate-storage":
        result = build_storage_copy(
            args.db, args.target_db, block_target=args.block_bytes, workers=args.workers,
            small_object_max=args.small_object_max, large_block_target=args.large_block_bytes,
            single_object_min=args.single_object_min,
            structure_dictionary_bytes=args.structure_dictionary_bytes,
            progress=lambda item: print(json.dumps(item, sort_keys=True), file=sys.stderr, flush=True),
        )
        result.pop("fingerprints", None)
    elif args.command == "import-cloud":
        try:
            result = import_cloud_export(args.export, args.session, args.project, args.title,
                                         replace=args.replace)
        except (OSError, ValueError) as exc:
            result = {"complete": False, "error": str(exc)}
    elif args.command == "sync-cloud":
        try:
            result = sync_cloud_session(args.session, args.project, args.title, replace=args.replace)
        except (OSError, ValueError, cloud_fetch.CloudFetchError) as exc:
            result = {"complete": False, "error": str(exc)}
    elif args.command == "validate-promotion":
        result = validate_promotion(args.baseline_evaluation, args.candidate_evaluation)
    elif args.command == "import-ledger-launches":
        ledger = sqlite3.connect(sessiondb.read_only_uri(args.ledger), uri=True)
        db = sessiondb.connect(args.db)
        try:
            with db:
                result = token_usage.import_ledger_launches(db, ledger)
        finally:
            db.close()
            ledger.close()
        result["complete"] = not result["conflicts"]
    elif args.command in ("rebuild-into-copy", "promote-copy", "token-status", "rebuild-token-usage"):
        import rebuild_copy
        progress = lambda item: print(json.dumps(item, sort_keys=True), file=sys.stderr, flush=True)
        if args.command == "rebuild-into-copy":
            result = rebuild_copy.rebuild_into_copy(
                args.db, args.target_db, progress, rebuild_projections=args.rebuild_projections,
            )
        elif args.command == "promote-copy":
            result = rebuild_copy.promote_copy(
                args.db, args.copy_db, args.rollback_dir, execute=args.execute,
                agents=(), install_command=None,
            )
        elif args.command == "token-status":
            db = sessiondb.connect(args.db, read_only=True)
            try:
                result = token_usage.status(db)
            finally:
                db.close()
        else:
            lock_path = args.db.resolve().parent / "ingest.lock"
            with lock_path.open("a+") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                db = sessiondb.connect(args.db)
                try:
                    result = rebuild_copy.backfill_token_usage(db, progress, only_incomplete=not args.all)
                    result["complete"] = token_usage.status(db)["complete"]
                finally:
                    db.close()
    else:
        result = compact_if_due(
            args.db, args.target_db, args.force, args.readiness_receipt,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    successful = result.get("complete", result.get("promotion_allowed", result.get("integrity_check") == "ok"))
    return 0 if successful else 1


if __name__ == "__main__":
    raise SystemExit(main())
