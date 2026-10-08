#!/usr/bin/env python3
"""Discover, incrementally sync, and verify the complete local SessionDB corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import sessiondb


ROOTS = {
    "claude": (Path.home() / ".claude/projects",),
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


def discovery_roots() -> dict[str, list[str]]:
    return {provider: [str(root) for root in roots] for provider, roots in ROOTS.items()}


def _remove_source(db: sqlite3.Connection, source_id: str) -> None:
    db.execute("DELETE FROM events WHERE source_id=?", (source_id,))
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
                    "SELECT DISTINCT session_id FROM events WHERE source_id=?", (row["source_id"],),
                ) if item[0]
            )
            events = scalar(db, "SELECT COUNT(*) FROM events WHERE source_id=?", (row["source_id"],))
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
) -> list[dict[str, Any]]:
    """Relocate an indexed transcript when its moved copy was later appended."""
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


def reachable_objects(db: sqlite3.Connection) -> set[str]:
    reachable = {row[0] for row in db.execute("SELECT DISTINCT object_hash FROM event_content")}
    frontier = set(reachable)
    while frontier:
        db.execute("DROP TABLE IF EXISTS temp.sessiondb_frontier")
        db.execute("CREATE TEMP TABLE sessiondb_frontier(object_hash TEXT PRIMARY KEY) WITHOUT ROWID")
        db.executemany("INSERT INTO sessiondb_frontier VALUES(?)", ((value,) for value in frontier))
        children = {
            row[0] for row in db.execute(
                "SELECT DISTINCT r.child_hash FROM object_references r JOIN sessiondb_frontier f "
                "ON r.parent_hash=f.object_hash"
            )
        }
        frontier = children - reachable
        reachable.update(frontier)
    db.execute("DROP TABLE IF EXISTS temp.sessiondb_frontier")
    return reachable


def load_reachable_table(db: sqlite3.Connection, reachable: set[str]) -> None:
    db.execute("DROP TABLE IF EXISTS temp.sessiondb_reachable")
    db.execute("CREATE TEMP TABLE sessiondb_reachable(object_hash TEXT PRIMARY KEY) WITHOUT ROWID")
    db.executemany("INSERT INTO sessiondb_reachable VALUES(?)", ((value,) for value in reachable))


def compaction_analysis(db_path: Path, target_parent: Path | None = None) -> dict[str, Any]:
    db = sessiondb.connect(db_path)
    reachable = reachable_objects(db)
    load_reachable_table(db, reachable)
    page_size = scalar(db, "PRAGMA page_size")
    database_reclaimable = page_size * scalar(db, "PRAGMA freelist_count")
    pack_bytes = sessiondb.pack_path(db).stat().st_size
    reachable_pack_bytes = scalar(
        db,
        "SELECT COALESCE(SUM(o.compressed_bytes),0) FROM objects o "
        "JOIN sessiondb_reachable r USING(object_hash)",
    )
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
        "reachable_objects": len(reachable),
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
        "AND e.structure_bytes>0 AND length(e.raw_hash)=64 AND src.status='complete' "
        "THEN 1 ELSE 0 END) AS noncritical_adapter_gap "
        "FROM events e JOIN sources src USING(source_id) "
        "LEFT JOIN event_content ec USING(event_id) LEFT JOIN objects o USING(object_hash) "
        "WHERE e.supported=0 GROUP BY e.provider,e.category,e.issue "
        "ORDER BY events DESC,e.provider,e.category"
    )]
    for item in groups:
        events = db.execute(
            "SELECT e.*,src.path FROM events e JOIN sources src USING(source_id) "
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
                    f"SELECT 1 FROM events WHERE source_id=? AND category IN ({placeholders})"
                    f"{support_filter} LIMIT 1",
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


def fast_sync(
    db_path: Path, report_path: Path, max_rounds: int = 1,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Catch up changed transcript sources without full-corpus verification."""
    started = time.monotonic()
    db = sessiondb.connect(db_path)
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
            discovered = discover()
            source_watermarks = {}
            for _provider, path, _source_class in discovered:
                try:
                    source_watermarks[str(path)] = path.stat()
                except OSError:
                    continue
            relocated.extend(reconcile_source_relocations(db, discovered))
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

            final = discover()
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
        return report
    finally:
        db.close()


def reconstruction_sample(db: sqlite3.Connection, limit: int) -> dict[str, Any]:
    rows = db.execute(
        "SELECT * FROM events WHERE supported=1 ORDER BY event_id LIMIT ?", (limit,)
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
        "SELECT s.*,COUNT(e.event_id) AS events FROM sources s LEFT JOIN events e USING(source_id) "
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
            db, "SELECT COUNT(*) FROM events e JOIN sources s USING(source_id) WHERE s.path=?", (path,)
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
    """Create a verified compact copy; never replace the source database or pack."""
    preflight = sessiondb.connect(db_path, read_only=True)
    try:
        preflight.execute("BEGIN")
        sessiondb.preflight_full_rebuild_readiness(preflight, readiness_receipt)
    finally:
        preflight.close()
    target_pack = Path(str(target_db) + ".objects")
    target_retrieval = Path(str(target_db) + ".retrieval.compact")
    if target_db.exists() or target_pack.exists() or target_retrieval.exists():
        raise FileExistsError(f"compact target already exists: {target_db}")
    target_db.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    database_tmp = target_db.with_name(f".{target_db.name}.{os.getpid()}.tmp")
    pack_tmp = Path(str(database_tmp) + ".objects")
    retrieval_tmp = Path(str(database_tmp) + ".retrieval.source")
    source = destination = compact = None
    retrieval_handle = None
    built_retrieval = None
    source_lease = None
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
        destination = sqlite3.connect(database_tmp)
        source.backup(destination)
        destination.close()
        destination = None
        compact = sessiondb.connect(database_tmp)
        with retrieval_tmp.open("wb") as handle:
            if retrieval_handle is not None:
                shutil.copyfileobj(retrieval_handle, handle)
            handle.flush()
            os.fsync(handle.fileno())
        compact.execute(
            "INSERT OR REPLACE INTO metadata(key,value) VALUES('projection_pack_generation',?)",
            (retrieval_tmp.name,),
        )
        compact.commit()
        reachable = reachable_objects(compact)
        load_reachable_table(compact, reachable)
        compact.execute(
            "DELETE FROM object_references WHERE NOT EXISTS "
            "(SELECT 1 FROM sessiondb_reachable r WHERE r.object_hash=object_references.parent_hash)"
        )
        compact.executescript("""
            DROP TABLE IF EXISTS compact_objects;
            CREATE TABLE compact_objects (
              object_hash TEXT PRIMARY KEY, kind TEXT NOT NULL, raw_bytes INTEGER NOT NULL,
              stored_bytes INTEGER NOT NULL, stored_hash TEXT NOT NULL,
              pack_offset INTEGER NOT NULL, compressed_bytes INTEGER NOT NULL,
              compression TEXT NOT NULL
            );
            INSERT INTO compact_objects
            SELECT o.object_hash,o.kind,o.raw_bytes,o.stored_bytes,o.stored_hash,
              COALESCE(SUM(o.compressed_bytes) OVER (
                ORDER BY o.pack_offset,o.object_hash
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ),0),
              o.compressed_bytes,o.compression
            FROM objects o JOIN sessiondb_reachable r USING(object_hash)
            ORDER BY o.pack_offset,o.object_hash;
        """)
        with sessiondb.pack_path(source).open("rb") as old_pack, pack_tmp.open("wb") as new_pack:
            for row in compact.execute(
                "SELECT o.object_hash,o.pack_offset AS old_offset,o.compressed_bytes "
                "FROM objects o JOIN compact_objects c USING(object_hash) "
                "ORDER BY c.pack_offset,c.object_hash"
            ):
                old_pack.seek(row["old_offset"])
                compressed = old_pack.read(row["compressed_bytes"])
                if len(compressed) != row["compressed_bytes"]:
                    raise ValueError(f"source pack extent missing: {row['object_hash']}")
                new_pack.write(compressed)
            new_pack.flush()
            os.fsync(new_pack.fileno())
        compact.commit()
        compact.execute("PRAGMA foreign_keys=OFF")
        compact.executescript("BEGIN; DROP TABLE objects; ALTER TABLE compact_objects RENAME TO objects; COMMIT;")
        compact.execute("VACUUM")
        compact.execute("PRAGMA foreign_keys=ON")
        sessiondb.build_search_projections(
            compact, readiness_receipt=readiness_receipt,
        )
        built_retrieval = sessiondb.projection_pack_path(compact)
        foreign_key_failures = [tuple(row) for row in compact.execute("PRAGMA foreign_key_check")]
        integrity = scalar(compact, "PRAGMA integrity_check")
        report = sessiondb.report(compact, database_tmp)
        freelist_bytes = scalar(compact, "PRAGMA page_size") * scalar(compact, "PRAGMA freelist_count")
        complete = (
            integrity == "ok" and not foreign_key_failures
            and report["pack_verification"]["pass"]
            and not report["pack_verification"]["orphan_bytes"]
            and report["retrieval_pack_verification"]["pass"]
            and not report["retrieval_pack_verification"]["orphan_bytes"]
            and (not source_projection_status["complete"] or sessiondb.projection_status(compact)["complete"])
            and freelist_bytes == 0
        )
        if not complete:
            raise ValueError("compact candidate verification failed")
        compact.execute(
            "INSERT OR REPLACE INTO metadata(key,value) VALUES('projection_pack_generation',?)",
            (target_retrieval.name,),
        )
        compact.commit()
        compact.close()
        compact = None
        source.close()
        source = None
        os.chmod(database_tmp, 0o600)
        os.chmod(pack_tmp, 0o600)
        os.chmod(built_retrieval, 0o600)
        target_lock = sessiondb.projection_pack_lock_path_for_database(target_db)
        descriptor = os.open(target_lock, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(descriptor)
        os.replace(built_retrieval, target_retrieval)
        built_retrieval = None
        os.replace(pack_tmp, target_pack)
        os.replace(database_tmp, target_db)
        verified = sessiondb.connect(target_db)
        integrity = scalar(verified, "PRAGMA integrity_check")
        report = sessiondb.report(verified, target_db)
        freelist_bytes = scalar(verified, "PRAGMA page_size") * scalar(verified, "PRAGMA freelist_count")
        verified.close()
        complete = (
            integrity == "ok" and not foreign_key_failures
            and report["pack_verification"]["pass"]
            and not report["pack_verification"]["orphan_bytes"]
            and report["retrieval_pack_verification"]["pass"]
            and not report["retrieval_pack_verification"]["orphan_bytes"]
            and freelist_bytes == 0
        )
        return {
            "database": str(target_db), "integrity_check": integrity,
            "foreign_key_failures": foreign_key_failures, "freelist_bytes": freelist_bytes,
            "report": report, "complete": complete,
        }
    finally:
        if destination is not None:
            destination.close()
        if compact is not None:
            compact.close()
        if source is not None:
            source.close()
        if source_lease is not None:
            sessiondb.release_projection_pack_lease(source_lease)
        if retrieval_handle is not None:
            retrieval_handle.close()
        if built_retrieval is not None:
            built_retrieval.unlink(missing_ok=True)
        if not target_db.exists():
            database_tmp.unlink(missing_ok=True)
            pack_tmp.unlink(missing_ok=True)
            retrieval_tmp.unlink(missing_ok=True)
            target_pack.unlink(missing_ok=True)
            target_retrieval.unlink(missing_ok=True)
            sessiondb.projection_pack_lock_path_for_database(target_db).unlink(missing_ok=True)
        sessiondb.projection_pack_lock_path_for_database(database_tmp).unlink(missing_ok=True)


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
    elif args.command == "validate-promotion":
        result = validate_promotion(args.baseline_evaluation, args.candidate_evaluation)
    else:
        result = compact_if_due(
            args.db, args.target_db, args.force, args.readiness_receipt,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    successful = result.get("complete", result.get("promotion_allowed", result.get("integrity_check") == "ok"))
    return 0 if successful else 1


if __name__ == "__main__":
    raise SystemExit(main())
