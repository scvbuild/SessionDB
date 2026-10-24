"""Rebuild SessionDB into a verified copy, then promote the copy with a rollback copy kept.

`rebuild_into_copy` snapshots the live store (database, object pack, retrieval pack) under a short
read lock, upgrades the snapshot to the current schema in a private work directory, proves that
every pre-existing logical record is unchanged (except search text, when asked to re-derive it),
and only then publishes the copy beside a verification report. The live store is opened read-only and never written.

`promote_copy` swaps a verified copy into the live location. It stops the SessionDB jobs, holds
the ingestion lock while it renames the live files into a rollback folder and the copy's files
into place, installs the current code (which restarts the jobs), and checks the result. Any
failure before the check passes restores the live files and the previous jobs. The rollback
folder is kept; deleting it is a separate decision. Without `--execute` it only prints the plan.

The copy is a snapshot: the first sync after promotion re-ingests whatever the live database
received after it, because every transcript's size and modification time are compared with the
copied watermarks.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import random
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

import full_corpus
import sessiondb
import token_usage

REPORT_SUFFIX = ".rebuild-report.json"
HEADROOM_MULTIPLIER = 1.5
IDEMPOTENCY_SAMPLE = 60
LAUNCH_AGENTS: tuple[str, ...] = ()
# Rebuilt when the copy re-derives search text; everything else must still match the source.
PROJECTION_FINGERPRINTS = (
    "metadata", "search_projection_blocks", "search_projection_refs", "search_term_variants",
    "positional_projection_fts_data", "positional_projection_fts_idx",
    "positional_projection_fts_docsize", "positional_projection_fts_config",
)


def comparable_fingerprints(db: sqlite3.Connection, rebuild_projections: bool) -> dict[str, str]:
    fingerprints = full_corpus.logical_fingerprints(db)
    if not rebuild_projections:
        return fingerprints
    for key in PROJECTION_FINGERPRINTS:
        fingerprints.pop(key, None)
    hasher = hashlib.sha256()
    for row in db.execute(
        "SELECT key,value FROM metadata WHERE key NOT IN "
        "('schema_version','projection_pack_generation','projection_snapshot_id') ORDER BY key"
    ):
        hasher.update(sessiondb.canonical(list(row)) + b"\n")
    fingerprints["metadata_except_projection"] = hasher.hexdigest()
    return fingerprints


def _note(progress: Optional[Callable[[dict[str, Any]], None]], started: float, phase: str, **values: Any) -> None:
    if progress:
        progress({"phase": phase, "elapsed_seconds": round(time.monotonic() - started, 1), **values})


def store_files(db_path: Path) -> dict[str, Path]:
    """The three files that make one SessionDB store, read without opening it for writing."""
    connection = sqlite3.connect(sessiondb.read_only_uri(db_path.resolve()), uri=True, timeout=60)
    try:
        generation = connection.execute(
            "SELECT value FROM metadata WHERE key='projection_pack_generation'"
        ).fetchone()
    finally:
        connection.close()
    files = {"database": db_path, "objects": Path(str(db_path) + ".objects")}
    if generation:
        files["retrieval"] = db_path.parent / generation[0]
    return files


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(16 * 1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


@contextlib.contextmanager
def migration_open() -> Iterator[None]:
    sessiondb._MIGRATION_OPEN.allowed = True
    try:
        yield
    finally:
        sessiondb._MIGRATION_OPEN.allowed = False


def backfill_token_usage(db: sqlite3.Connection, progress: Optional[Callable[[dict[str, Any]], None]] = None,
                         only_incomplete: bool = False) -> dict[str, Any]:
    """Derive every source's token rows from SessionDB's own retained records.

    Reading a record verifies its order-sensitive raw hash, so this pass also proves that every
    supported event still reconstructs exactly.
    """
    started = time.monotonic()
    where = (
        "WHERE NOT EXISTS (SELECT 1 FROM token_source_state t WHERE t.source_key=s.source_key "
        "AND t.status='ok' AND t.projection_version=?)" if only_incomplete else ""
    )
    sources = db.execute(
        f"SELECT s.source_key, s.provider, s.path FROM sources s {where} ORDER BY s.source_key",
        (token_usage.TOKEN_PROJECTION_VERSION,) if only_incomplete else (),
    ).fetchall()
    counts = {"sources": len(sources), "ok": 0, "error": 0, "records_read": 0}
    errors: list[dict[str, Any]] = []
    db.execute("BEGIN")
    for index, source in enumerate(sources, 1):
        def rows() -> Iterator[tuple[int, Any]]:
            for item in token_usage.stored_source_rows(
                db, source["source_key"], sessiondb.read_raw_event, sessiondb.EVENT_COLUMNS, sessiondb.EVENT_JOIN,
            ):
                counts["records_read"] += 1
                yield item
        result = token_usage.project_source(db, source["provider"], source["source_key"], source["path"], rows())
        counts[result["status"]] += 1
        if result["status"] == "error":
            errors.append({"source_key": source["source_key"], "path": source["path"], "error": result["error"]})
        if index % 100 == 0:
            db.commit()
            sessiondb.clear_caches()
            db.execute("BEGIN")
            _note(progress, started, "token_backfill", done=index, total=len(sources))
    db.commit()
    sessiondb.clear_caches()
    return {**counts, "errors": errors[:50], "elapsed_seconds": round(time.monotonic() - started, 1)}


def _token_rows(db: sqlite3.Connection, source_key: int) -> list[list[tuple[Any, ...]]]:
    return [
        [tuple(row) for row in db.execute(f"SELECT * FROM {table} WHERE source_key=? ORDER BY 1,2", (source_key,))]
        for table in token_usage.TOKEN_TABLES
    ]


def idempotency_check(db: sqlite3.Connection, sample: int = IDEMPOTENCY_SAMPLE) -> dict[str, Any]:
    """Re-derive a fixed sample of sources (the largest and a seeded random set) and compare."""
    largest = [row[0] for row in db.execute(
        "SELECT source_key FROM sources ORDER BY size_bytes DESC LIMIT ?", (sample // 2,))]
    keys = [row[0] for row in db.execute("SELECT source_key FROM sources ORDER BY source_key")]
    chosen = sorted(set(largest) | set(random.Random(20261014).sample(keys, min(len(keys), sample // 2))))
    changed = []
    for key in chosen:
        source = db.execute("SELECT provider, path FROM sources WHERE source_key=?", (key,)).fetchone()
        before = _token_rows(db, key)
        db.execute("BEGIN")
        token_usage.project_source(
            db, source["provider"], key, source["path"],
            token_usage.stored_source_rows(db, key, sessiondb.read_raw_event, sessiondb.EVENT_COLUMNS,
                                           sessiondb.EVENT_JOIN),
        )
        after = _token_rows(db, key)
        db.rollback()
        if before != after:
            changed.append(key)
    sessiondb.clear_caches()
    return {"sources_checked": len(chosen), "changed": changed, "pass": not changed}


def verify_rebuilt_copy(target_db: Path, before: dict[str, str], projection_was_complete: bool,
                        rebuild_projections: bool = False) -> dict[str, Any]:
    db = sessiondb.connect(target_db, read_only=True)
    try:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = [tuple(row) for row in db.execute("PRAGMA foreign_key_check")]
        report = sessiondb.report(db, target_db)
        projection = sessiondb.projection_status(db)
        tokens = token_usage.status(db)
        schema = db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0]
    finally:
        db.close()
    raw = sqlite3.connect(sessiondb.read_only_uri(target_db), uri=True)
    raw.row_factory = sqlite3.Row
    try:
        after = comparable_fingerprints(raw, rebuild_projections)
    finally:
        raw.close()
    mismatched = sorted(key for key in before if before[key] != after.get(key))
    complete = (
        integrity == "ok" and not foreign_keys and schema == sessiondb.SCHEMA_VERSION
        and report["pack_verification"]["pass"] and report["retrieval_pack_verification"]["pass"]
        and (projection["complete"] or (not projection_was_complete and not rebuild_projections))
        and not mismatched and tokens["missing"] == 0
    )
    return {
        "complete": complete, "integrity_check": integrity, "foreign_key_failures": foreign_keys[:20],
        "schema_version": schema, "fingerprint_mismatches": mismatched, "fingerprints_compared": sorted(before),
        "search_projection_complete": projection["complete"],
        "pack_verification": {k: report["pack_verification"][k] for k in ("pass", "objects_checked", "orphan_bytes")},
        "retrieval_pack_verification": {
            k: report["retrieval_pack_verification"][k] for k in ("pass", "blocks", "orphan_bytes")},
        "events": report["events"], "sources": report["sources"], "token_usage": tokens,
    }


def rebuild_into_copy(db_path: Path, target_db: Path,
                      progress: Optional[Callable[[dict[str, Any]], None]] = None,
                      *, rebuild_projections: bool = False) -> dict[str, Any]:
    """Write a verified current-schema copy of `db_path` at `target_db`; never modify the source.

    `rebuild_projections` re-derives every event's search text with the current redaction.
    """
    started = time.monotonic()
    db_path = db_path.resolve()
    target_db = target_db.absolute()
    if target_db.exists() or Path(str(target_db) + ".objects").exists() or list(
            target_db.parent.glob(target_db.name + ".retrieval.*")):
        raise FileExistsError(f"copy target already exists: {target_db}")
    target_db.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    live_files = store_files(db_path)
    live_bytes = sum(path.stat().st_size for path in live_files.values())
    free = shutil.disk_usage(target_db.parent).free
    if free < HEADROOM_MULTIPLIER * live_bytes:
        raise RuntimeError(f"need {HEADROOM_MULTIPLIER}x the store ({live_bytes} bytes) free; have {free}")
    work = target_db.parent / f".{target_db.name}.{os.getpid()}.rebuild"
    work.mkdir(mode=0o700)
    work_db = work / target_db.name
    generation = target_db.name + ".retrieval." + os.urandom(12).hex()
    published = False
    lease = source = None
    try:
        # Snapshot: SQLite through the backup API inside one read transaction; packs through handles
        # opened in that transaction. Packs are append-only, so every extent the snapshot references
        # is inside the bytes copied afterwards.
        try:
            lease = sessiondb.acquire_projection_pack_lease_path(db_path, exclusive=False)
        except FileNotFoundError:
            lease = None
        source = sqlite3.connect(sessiondb.read_only_uri(db_path), uri=True, timeout=60)
        source.execute("BEGIN")
        source_schema = source.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0]
        if source_schema not in (sessiondb.V11_SCHEMA_VERSION, sessiondb.SCHEMA_VERSION):
            raise ValueError(f"rebuild-into-copy supports schema v11 and v12 sources, not {source_schema}")
        generation_row = source.execute(
            "SELECT value FROM metadata WHERE key='projection_pack_generation'").fetchone()
        object_handle = Path(str(db_path) + ".objects").open("rb")
        retrieval_handle = (db_path.parent / generation_row[0]).open("rb") if generation_row else None
        snapshot = sqlite3.connect(work_db)
        source.backup(snapshot)
        snapshot.close()
        source.rollback()
        source.close()
        source = None
        if lease is not None:
            sessiondb.release_projection_pack_lease(lease)
            lease = None
        with object_handle, open(Path(str(work_db) + ".objects"), "wb") as out:
            shutil.copyfileobj(object_handle, out, 16 * 1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        if retrieval_handle is not None:
            with retrieval_handle, open(work / generation, "wb") as out:
                shutil.copyfileobj(retrieval_handle, out, 16 * 1024 * 1024)
                out.flush()
                os.fsync(out.fileno())
        _note(progress, started, "snapshot", source_schema=source_schema)

        raw = sqlite3.connect(work_db, isolation_level=None)
        raw.row_factory = sqlite3.Row
        try:
            if generation_row:
                raw.execute("UPDATE metadata SET value=? WHERE key='projection_pack_generation'", (generation,))
            before = comparable_fingerprints(raw, rebuild_projections)
        finally:
            raw.close()
        _note(progress, started, "source_fingerprints")

        with migration_open():
            db = sessiondb.connect(work_db)
        try:
            projection_was_complete = sessiondb.projection_status(db, deep=False)["complete"]
            db.execute("UPDATE metadata SET value=? WHERE key='schema_version'", (sessiondb.SCHEMA_VERSION,))
            db.commit()
            backfill = backfill_token_usage(db, progress, only_incomplete=True)
            _note(progress, started, "token_backfill_done", **{k: backfill[k] for k in ("sources", "ok", "error")})
            projections = None
            if rebuild_projections:
                projections = sessiondb.build_search_projections(db, progress, require_receipt=False)
                generation = db.execute(
                    "SELECT value FROM metadata WHERE key='projection_pack_generation'").fetchone()[0]
                generation_row = (generation,)
                _note(progress, started, "projections_rebuilt", events=projections["events"])
            idempotency = idempotency_check(db)
            _note(progress, started, "idempotency", passed=idempotency["pass"])
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            db.close()
        verification = verify_rebuilt_copy(work_db, before, projection_was_complete, rebuild_projections)
        verification["complete"] = verification["complete"] and idempotency["pass"]
        _note(progress, started, "verified", complete=verification["complete"])
        result = {
            "database": str(target_db), "source": str(db_path), "source_schema": source_schema,
            "token_backfill": backfill, "idempotency": idempotency, "projection_rebuild": projections,
            **verification,
            "elapsed_seconds": round(time.monotonic() - started, 1),
        }
        if not verification["complete"]:
            result["published"] = False
            return result
        for suffix in ("-wal", "-shm"):
            Path(str(work_db) + suffix).unlink(missing_ok=True)
        for path in work.iterdir():
            os.chmod(path, 0o600)
        result["copy_files"] = {
            "database": {"name": target_db.name, "bytes": work_db.stat().st_size, "sha256": file_sha256(work_db)},
            "objects_bytes": Path(str(work_db) + ".objects").stat().st_size,
            "retrieval": generation if generation_row else None,
        }
        lock_path = sessiondb.projection_pack_lock_path_for_database(target_db)
        os.close(os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o600))
        if generation_row:
            os.replace(work / generation, target_db.parent / generation)
        os.replace(Path(str(work_db) + ".objects"), Path(str(target_db) + ".objects"))
        os.replace(work_db, target_db)
        sessiondb.fsync_directory(target_db.parent)
        published = True
        result["published"] = True
        report_path = Path(str(target_db) + REPORT_SUFFIX)
        report_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.chmod(report_path, 0o600)
        return result
    finally:
        if source is not None:
            source.close()
        if lease is not None:
            sessiondb.release_projection_pack_lease(lease)
        shutil.rmtree(work, ignore_errors=True)
        if not published:
            sessiondb.projection_pack_lock_path_for_database(target_db).unlink(missing_ok=True)


def _launch_agent_plist(label: str) -> Path:
    return Path.home() / "Library/LaunchAgents" / f"{label}.plist"


def _launchctl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["/bin/launchctl", *args], capture_output=True, text=True, timeout=120)


def _agent_loaded(label: str) -> bool:
    return _launchctl("print", f"gui/{os.getuid()}/{label}").returncode == 0


def check_copy(live_db: Path, copy_db: Path) -> dict[str, Any]:
    """Everything that must hold before a swap; nothing is changed."""
    problems = []
    report_path = Path(str(copy_db) + REPORT_SUFFIX)
    report = json.loads(report_path.read_text()) if report_path.exists() else None
    if report is None:
        problems.append("verification report missing")
    elif not (report.get("complete") and report.get("published")):
        problems.append("verification report does not record a complete, published copy")
    elif report["copy_files"]["database"]["sha256"] != file_sha256(copy_db):
        problems.append("copy database changed since it was verified")
    if report is not None and Path(report.get("source", "")).resolve() != live_db.resolve():
        problems.append("copy was made from a different database")
    connection = sqlite3.connect(sessiondb.read_only_uri(copy_db), uri=True)
    try:
        schema = connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0]
    finally:
        connection.close()
    if schema != sessiondb.SCHEMA_VERSION:
        problems.append(f"copy schema {schema} is not this code's schema {sessiondb.SCHEMA_VERSION}")
    copy_files = store_files(copy_db)
    live_files = store_files(live_db)
    for path in list(copy_files.values()) + list(live_files.values()):
        if not path.exists():
            problems.append(f"missing file: {path}")
    if live_db.parent.stat().st_dev != copy_db.parent.stat().st_dev:
        problems.append("copy and live store are on different file systems; renames would not be atomic")
    return {"ok": not problems, "problems": problems, "copy_files": {k: str(v) for k, v in copy_files.items()},
            "live_files": {k: str(v) for k, v in live_files.items()}}


def promote_copy(live_db: Path, copy_db: Path, rollback_dir: Path, *, execute: bool = False,
                 agents: tuple[str, ...] = LAUNCH_AGENTS, install_command: Optional[list[str]] = None,
                 lock_timeout: float = 300.0) -> dict[str, Any]:
    """Swap a verified copy into the live location; keep the live files in `rollback_dir`."""
    live_db = live_db.absolute()
    copy_db = copy_db.absolute()
    rollback_dir = rollback_dir.absolute()
    check = check_copy(live_db, copy_db)
    if rollback_dir.exists() and any(rollback_dir.iterdir()):
        check["problems"].append(f"rollback folder is not empty: {rollback_dir}")
        check["ok"] = False
    plan = [
        "stop jobs: " + ", ".join(agents) if agents else "stop jobs: none",
        f"hold {live_db.parent / 'ingest.lock'}",
        f"move live files into {rollback_dir}",
        f"move copy files into {live_db.parent}",
        "install current code: " + (" ".join(install_command) if install_command else "none"),
        "check the promoted store",
    ]
    result: dict[str, Any] = {"plan": plan, "check": check, "executed": False}
    if not execute or not check["ok"]:
        result["complete"] = check["ok"] and not execute
        return result
    result["executed"] = True
    steps: list[str] = []
    result["steps"] = steps
    stopped: list[str] = []
    moved_live: list[tuple[Path, Path]] = []
    moved_copy: list[tuple[Path, Path]] = []
    lock_handle = None
    installed = False
    try:
        for label in agents:
            if _agent_loaded(label):
                outcome = _launchctl("bootout", f"gui/{os.getuid()}/{label}")
                if outcome.returncode != 0 and _agent_loaded(label):
                    raise RuntimeError(f"could not stop {label}: {outcome.stderr.strip()}")
                stopped.append(label)
        steps.append("jobs stopped: " + (", ".join(stopped) or "none were loaded"))
        lock_handle = (live_db.parent / "ingest.lock").open("a+")
        deadline = time.monotonic() + lock_timeout
        while True:
            try:
                fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise RuntimeError("ingest lock still held; a sync is running")
                time.sleep(1)
        steps.append("ingest lock held")
        rollback_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        live_files = [Path(p) for p in check["live_files"].values()]
        live_files += [Path(str(live_db) + suffix) for suffix in ("-wal", "-shm")
                       if Path(str(live_db) + suffix).exists()]
        for path in live_files:
            destination = rollback_dir / path.name
            os.replace(path, destination)
            moved_live.append((path, destination))
        for path in [Path(p) for p in check["copy_files"].values()]:
            destination = live_db if path == copy_db else live_db.parent / (
                live_db.name + ".objects" if path.name.endswith(".objects") else path.name.replace(copy_db.name, live_db.name, 1))
            os.replace(path, destination)
            moved_copy.append((path, destination))
        renamed = [(source, dest) for source, dest in moved_copy
                   if ".retrieval." in dest.name and source.name != dest.name]
        if renamed:
            # A copy stored under another file name named its retrieval pack after itself.
            new_generation = renamed[0][1].name
            connection = sqlite3.connect(live_db)
            try:
                connection.execute("UPDATE metadata SET value=? WHERE key='projection_pack_generation'",
                                   (new_generation,))
                connection.commit()
            finally:
                connection.close()
        sessiondb.fsync_directory(live_db.parent)
        steps.append("files swapped")
        fcntl.flock(lock_handle, fcntl.LOCK_UN)
        lock_handle.close()
        lock_handle = None
        if install_command:
            outcome = subprocess.run(install_command, capture_output=True, text=True, timeout=1800)
            if outcome.returncode != 0:
                raise RuntimeError("install failed: " + (outcome.stderr or outcome.stdout)[-2000:])
            installed = True
            steps.append("code installed and jobs restarted")
        db = sessiondb.connect(live_db, read_only=True)
        try:
            schema = db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0]
            integrity = db.execute("PRAGMA quick_check").fetchone()[0]
            tokens = token_usage.status(db)
            events = db.execute("SELECT count(*) FROM events").fetchone()[0]
        finally:
            db.close()
        if schema != sessiondb.SCHEMA_VERSION or integrity != "ok" or tokens["missing"]:
            raise RuntimeError(f"promoted store check failed: schema={schema} quick_check={integrity}")
        steps.append("promoted store checked")
        result.update({"complete": True, "events": events, "token_usage": tokens,
                       "rollback_dir": str(rollback_dir)})
        return result
    except Exception as exc:
        result["complete"] = False
        result["error"] = str(exc)
        result["rollback"] = _roll_back(live_db, moved_live, moved_copy, stopped, installed)
        return result
    finally:
        if lock_handle is not None:
            lock_handle.close()


def _roll_back(live_db: Path, moved_live: list[tuple[Path, Path]], moved_copy: list[tuple[Path, Path]],
               stopped: list[str], installed: bool) -> list[str]:
    done = []
    if installed:
        done.append("new code is installed; reinstall the previous revision before restarting jobs")
        for label in LAUNCH_AGENTS:
            if _agent_loaded(label):
                _launchctl("bootout", f"gui/{os.getuid()}/{label}")
    for source, destination in reversed(moved_copy):
        if destination.exists():
            os.replace(destination, source)
    if moved_copy:
        done.append("copy files moved back")
    for source, destination in reversed(moved_live):
        if destination.exists():
            os.replace(destination, source)
    if moved_live:
        done.append("live files restored")
    if not installed:
        for label in stopped:
            _launchctl("bootstrap", f"gui/{os.getuid()}", str(_launch_agent_plist(label)))
        if stopped:
            done.append("jobs restarted: " + ", ".join(stopped))
    return done
