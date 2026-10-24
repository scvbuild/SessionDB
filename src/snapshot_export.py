#!/usr/bin/env python3
"""Export a complete, portable, redacted SessionDB snapshot for a caller-selected window."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import full_corpus
import sessiondb


FORMAT = "sessiondb-portable-snapshot"
SCHEMA_VERSION = 1


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def write_json(path: Path, value: Any) -> int:
    data = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path.write_bytes(data)
    os.chmod(path, 0o600)
    return len(data)


def write_jsonl(path: Path, values: Iterable[Any]) -> int:
    data = b"".join(canonical(value) + b"\n" for value in values)
    path.write_bytes(data)
    os.chmod(path, 0o600)
    return len(data)


def source_reads_in_window(db_path: Path, start: str, end: str) -> list[dict[str, Any]]:
    path = Path(str(db_path) + ".source-reads.jsonl")
    if not path.exists():
        return []
    start_utc = sessiondb.normalize_timestamp(start) or start
    end_utc = sessiondb.normalize_timestamp(end) or end
    result = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            result.append({"timestamp": None, "reason": "malformed source-read audit record"})
            continue
        timestamp = sessiondb.normalize_timestamp(item.get("timestamp"))
        if timestamp and start_utc <= timestamp < end_utc:
            result.append(item)
    return result


def raw_structure(db: Any, event: Any, bundles: dict[str, bytes]) -> Any:
    source_id = event["source_id"]
    if source_id not in bundles:
        source = db.execute(
            "SELECT structure_raw_bytes,structure_zstd,structure_dict_id FROM sources WHERE source_id=?",
            (source_id,),
        ).fetchone()
        bundles[source_id] = sessiondb.decompress_structure_bundle(
            db, source["structure_zstd"], source["structure_raw_bytes"], source["structure_dict_id"],
        )
    start = event["structure_offset"]
    data = bundles[source_id][start:start + event["structure_bytes"]]
    if len(data) != event["structure_bytes"]:
        raise ValueError(f"parsed event structure missing: {event['event_id']}")
    return json.loads(data)


def extra_object_hashes(value: Any) -> set[str]:
    result: set[str] = set()
    if isinstance(value, dict):
        if set(value) == {sessiondb.EVENT_OBJECT_KEY}:
            result.add(value[sessiondb.EVENT_OBJECT_KEY])
        else:
            for child in value.values():
                result.update(extra_object_hashes(child))
    elif isinstance(value, list):
        for child in value:
            result.update(extra_object_hashes(child))
    return result


class PortableObjects:
    def __init__(self, db: Any) -> None:
        self.db = db
        self.source_to_export: dict[str, str] = {}
        self.exported: dict[str, dict[str, Any]] = {}
        self.active: set[str] = set()

    def add(self, source_hash: str) -> str:
        if source_hash in self.source_to_export:
            return self.source_to_export[source_hash]
        if source_hash in self.active:
            raise ValueError(f"object reference cycle detected: {source_hash}")
        self.active.add(source_hash)
        row, value = sessiondb.read_stored_object(self.db, source_hash)

        def rewrite(item: Any) -> Any:
            if isinstance(item, dict) and set(item) == {sessiondb.REF_KEY}:
                return {sessiondb.REF_KEY: self.add(item[sessiondb.REF_KEY])}
            if isinstance(item, list):
                return [rewrite(child) for child in item]
            if isinstance(item, dict):
                return {key: rewrite(child) for key, child in item.items()}
            return item

        value = sessiondb.redact_value(rewrite(value))
        export_id = hashlib.sha256(canonical(value)).hexdigest()
        self.source_to_export[source_hash] = export_id
        target = self.exported.setdefault(export_id, {
            "id": export_id, "kind": row["kind"], "source_hashes": [], "value": value,
        })
        target["source_hashes"].append(source_hash)
        self.active.remove(source_hash)
        return export_id

    def rewrite_structure(self, value: Any) -> Any:
        if isinstance(value, dict) and set(value) == {sessiondb.EVENT_OBJECT_KEY}:
            return {sessiondb.REF_KEY: self.add(value[sessiondb.EVENT_OBJECT_KEY])}
        if isinstance(value, list):
            return [self.rewrite_structure(child) for child in value]
        if isinstance(value, dict):
            return {key: self.rewrite_structure(child) for key, child in value.items()}
        return value

    def rows(self) -> list[dict[str, Any]]:
        rows = []
        for object_id in sorted(self.exported):
            item = dict(self.exported[object_id])
            item["source_hashes"] = sorted(set(item["source_hashes"]))
            rows.append(item)
        return rows


def session_metadata(db: Any, provider: str, session_id: str) -> Any:
    if provider != "codex":
        return None
    row = db.execute(
        f"SELECT {sessiondb.EVENT_COLUMNS} FROM {sessiondb.EVENT_JOIN} "
        "WHERE e.provider=? AND e.session_id=? AND e.category='session_meta' "
        "ORDER BY e.sequence LIMIT 1", (provider, session_id),
    ).fetchone()
    if not row:
        return None
    value = sessiondb.redact_value(sessiondb.read_event_structure(db, row))
    payload = value.get("payload") if isinstance(value, dict) else None
    if not isinstance(payload, dict):
        return None
    return {key: payload.get(key) for key in (
        "thread_source", "source", "cwd", "model", "model_provider", "originator", "cli_version",
    ) if payload.get(key) is not None}


def export_snapshot(
    db_path: Path, output_dir: Path, start: str, end: str, sync_report: dict[str, Any],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(output_dir, 0o700)
    db = sessiondb.connect(db_path, read_only=True)
    start_utc = sessiondb.normalize_timestamp(start) or start
    end_utc = sessiondb.normalize_timestamp(end) or end
    events = db.execute(
        f"SELECT {sessiondb.EVENT_COLUMNS},src.path AS source_path FROM {sessiondb.EVENT_JOIN} "
        "WHERE e.order_timestamp>=? AND e.order_timestamp<? "
        "ORDER BY e.provider,e.session_id,e.order_timestamp,src.source_id,e.line_number",
        (start_utc, end_utc),
    ).fetchall()
    bundles: dict[str, bytes] = {}
    structures: dict[str, Any] = {}
    direct_hashes: set[str] = set()
    for event in events:
        structure = raw_structure(db, event, bundles)
        structures[event["event_id"]] = structure
        direct_hashes.update(extra_object_hashes(structure))
        direct_hashes.update(row[0] for row in db.execute(
            "SELECT lower(hex(object_hash)) FROM event_content WHERE event_rowid=? ORDER BY ordinal",
            (event["event_rowid"],),
        ))

    portable = PortableObjects(db)
    for object_hash in sorted(direct_hashes):
        portable.add(object_hash)

    session_groups: dict[tuple[str, str], list[Any]] = defaultdict(list)
    event_rows = []
    unresolved = []
    unsupported = []
    for event in events:
        session_groups[(event["provider"], event["session_id"])].append(event)
        refs = [portable.add(row[0]) for row in db.execute(
            "SELECT lower(hex(object_hash)) FROM event_content WHERE event_rowid=? ORDER BY ordinal",
            (event["event_rowid"],),
        )]
        if event["tool_event"] and not event["tool"]:
            unresolved.append({
                "provider": event["provider"], "session_id": event["session_id"],
                "event_id": event["event_id"], "source_path": event["source_path"],
                "line": event["line_number"],
            })
        if not event["supported"]:
            unsupported.append({
                "provider": event["provider"], "session_id": event["session_id"],
                "event_id": event["event_id"], "source_path": event["source_path"],
                "line": event["line_number"], "issue": event["issue"],
            })
        event_rows.append({
            "event_id": event["event_id"], "provider": event["provider"],
            "session_id": event["session_id"], "project": event["project"],
            "parent_session_id": event["parent_session_id"], "timestamp": event["timestamp"],
            "timestamp_utc": event["timestamp_utc"], "order_timestamp": event["order_timestamp"],
            "order_kind": event["order_kind"], "category": event["category"],
            "role": event["role"], "tool": event["tool"], "tool_event": bool(event["tool_event"]),
            "supported": bool(event["supported"]), "issue": event["issue"],
            "content_shape": event["content_shape"], "content_refs": refs,
            "structure": sessiondb.redact_value(portable.rewrite_structure(structures[event["event_id"]])),
            "source": {"id": event["source_id"], "path": event["source_path"],
                       "line": event["line_number"], "hash": event["raw_hash"]},
        })

    session_rows = []
    for (provider, session_id), rows in sorted(session_groups.items()):
        categories = Counter(row["category"] for row in rows)
        roles = Counter(row["role"] for row in rows if row["role"])
        tools = Counter(row["tool"] for row in rows if row["tool"])
        stored_session = db.execute(
            "SELECT project,parent_session_id,root_session_id FROM sessions "
            "WHERE provider=? AND session_id=? ORDER BY source_id LIMIT 1", (provider, session_id),
        ).fetchone()
        session_rows.append({
            "provider": provider, "session_id": session_id,
            "project": stored_session["project"] if stored_session else rows[0]["project"],
            "parent_session_id": stored_session["parent_session_id"] if stored_session else rows[0]["parent_session_id"],
            "root_session_id": stored_session["root_session_id"] if stored_session else None,
            "first_timestamp": rows[0]["order_timestamp"], "last_timestamp": rows[-1]["order_timestamp"],
            "events": len(rows), "categories": dict(categories), "roles": dict(roles),
            "tools": dict(tools), "source_paths": sorted({row["source_path"] for row in rows}),
            "metadata": session_metadata(db, provider, session_id),
        })

    raw_source_reads = source_reads_in_window(db_path, start_utc, end_utc)
    ingestion_complete = bool(sync_report.get("complete"))
    sync_issues = list(sync_report.get("issues") or [])
    coverage = "complete" if ingestion_complete and not unsupported and not sync_issues else "partial"
    tool_events_by_provider = dict(Counter(
        event["provider"] for event in events if event["tool_event"]
    ))
    unresolved_by_provider = dict(Counter(item["provider"] for item in unresolved))
    exact_timestamps = sum(event["order_kind"] == "exact" for event in events)
    fallback_sources = sorted({
        (item["provider"], item["session_id"], item["source_path"], item.get("issue") or "unsupported_event")
        for item in unsupported
    })
    fallback_scope = "fixed_window" if (not ingestion_complete or sync_issues) else (
        "sources" if fallback_sources else "none"
    )
    fallback_providers = (
        ["claude", "codex"] if fallback_scope == "fixed_window" else
        sorted({item[0] for item in fallback_sources})
    )

    session_bytes = write_jsonl(output_dir / "sessions.jsonl", session_rows)
    event_bytes = write_jsonl(output_dir / "events.jsonl", event_rows)
    object_rows = portable.rows()
    object_bytes = write_jsonl(output_dir / "objects.jsonl", object_rows)
    manifest = {
        "schema_version": SCHEMA_VERSION, "format": FORMAT,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "window": {"start_utc": start, "end_utc": end},
        "coverage": coverage, "ingestion_complete": ingestion_complete,
        "export_complete": ingestion_complete, "attention_required": bool(
            unsupported or sync_issues or raw_source_reads or not ingestion_complete
        ),
        "files": {"sessions": "sessions.jsonl", "events": "events.jsonl", "objects": "objects.jsonl"},
        "counts": {
            "sessions": len(session_rows), "events": len(event_rows), "objects": len(object_rows),
            "providers": dict(Counter(row["provider"] for row in session_rows)),
            "session_bytes": session_bytes, "event_bytes": event_bytes, "object_bytes": object_bytes,
            "snapshot_bytes_excluding_manifest": session_bytes + event_bytes + object_bytes,
            "direct_object_references": len(direct_hashes),
            "unresolved_tool_attribution_events": len(unresolved),
            "tool_attribution_events_by_provider": tool_events_by_provider,
            "unresolved_tool_attribution_by_provider": unresolved_by_provider,
            "events_with_exact_timestamp": exact_timestamps,
            "events_with_inferred_timestamp": len(events) - exact_timestamps,
            "unsupported_events": len(unsupported), "raw_source_reads": len(raw_source_reads),
        },
        "unresolved_tool_attribution": unresolved, "unsupported_events": unsupported,
        "raw_source_reads": raw_source_reads,
        "fallback": {
            "required": coverage != "complete", "scope": fallback_scope,
            "providers": fallback_providers,
            "sources": [{"provider": p, "session_id": s, "path": path, "reason": reason}
                        for p, s, path, reason in fallback_sources],
            "window": {"start_utc": start, "end_utc": end},
        },
        "sync": {key: sync_report.get(key) for key in (
            "hard_failures", "missing_discovered_sources", "sources_drifted_after_sync",
            "stale_indexed_sources", "line_accounting_failures", "integrity_check",
        )},
        "sync_issues": sync_issues,
    }
    write_json(output_dir / "manifest.json", manifest)
    db.close()
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    sync_report = full_corpus.fast_sync(args.db, args.state_dir / "last_fast_sync_report.json")
    manifest = export_snapshot(args.db, args.output_dir, args.start, args.end, sync_report)
    print(json.dumps({
        "coverage": manifest["coverage"], "counts": manifest["counts"],
        "window": manifest["window"],
    }, indent=2, sort_keys=True))
    return 0 if manifest["ingestion_complete"] and manifest["export_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
