#!/usr/bin/env python3
"""Keep a local SessionDB current; report failures without launching repair agents."""
from __future__ import annotations
import argparse
import fcntl
import json
import queue
import signal
import sqlite3
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
import full_corpus
import sessiondb

SYNC_LOCK_RETRY_DELAYS = (5.0, 15.0)
SYNC_ERROR_RETRY_SECONDS = 60.0


def publish_status(result: dict[str, Any], state_dir: Path) -> dict[str, Any]:
    """Report sync health locally; query coverage remains independently scoped."""
    attention = bool(
        result.get("error") or result.get("issues") or result.get("hard_failures")
        or result.get("blocking_drift_sources")
        or any(item.get("state") == "failed" for item in result.get("migrations", []))
        or result.get("missing_discovered_sources")
    )
    result["attention_required"] = attention
    result["recovery_state"] = "action_required" if attention else (
        "catching_up" if result.get("catching_up_sources") else "operational"
    )
    result["automatic_repair"] = False
    if attention:
        result["pass"] = False
    write_json(state_dir / "watcher_status.json", result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return result


def scheduled_recheck(latest: dict[str, Any], now: float) -> tuple[float | None, str]:
    if latest.get("error") or latest.get("hard_failures") or latest.get("missing_discovered_sources"):
        return now + SYNC_ERROR_RETRY_SECONDS, "error_retry"
    return None, "filesystem_event"

def write_json(path: Path, value: Any) -> None:
    full_corpus.write_json_atomic(path, value)

def publish_projection_integrity_receipt(db_path: Path) -> dict[str, Any]:
    """Publish a private optimization receipt; absence always means deep verification."""
    path = sessiondb.projection_integrity_receipt_path(db_path)
    db = None
    try:
        db = sessiondb.connect(db_path, read_only=True)
        db.execute("BEGIN")
        receipt = sessiondb.build_projection_integrity_receipt(db)
        if not sessiondb.connection_path_is_current(db):
            raise ValueError("database was replaced during receipt verification")
        write_json(path, receipt)
        return {
            "state": "current", "created_at": receipt["created_at"],
            "snapshot_id": receipt["snapshot_id"],
        }
    except Exception as exc:
        path.unlink(missing_ok=True)
        return {"state": "unavailable", "error": f"{type(exc).__name__}: {exc}"}
    finally:
        if db is not None:
            db.close()

def drift_health(items: list[Any]) -> dict[str, list[dict[str, Any]]]:
    """Separate normal active-tail catch-up from user-actionable source drift."""
    active = []
    blocking = []
    now = time.time()
    for item in items:
        path = item if isinstance(item, str) else item.get("path") if isinstance(item, dict) else None
        status = {"path": path}
        if not path:
            status.update({"reason": "unclassified_drift", "blocking": True})
            blocking.append(status)
            continue
        try:
            quiet_seconds = max(0.0, now - Path(path).stat().st_mtime)
        except OSError:
            status.update({"reason": "drift_source_unreadable", "blocking": True})
            blocking.append(status)
            continue
        status["quiet_seconds"] = round(quiet_seconds, 3)
        if quiet_seconds > full_corpus.sessiondb.ACTIVE_DRIFT_GRACE_SECONDS:
            status.update({"reason": "persistent_source_drift", "blocking": True})
            blocking.append(status)
        else:
            status.update({"reason": "active_source_tail", "blocking": False})
            active.append(status)
    return {"catching_up_sources": active, "blocking_drift_sources": blocking}

def migration_progress_publisher(
    state_dir: Path,
) -> Callable[[dict[str, Any]], None]:
    """Publish one resumable migration status without polling the worker."""
    status_path = state_dir / "migration_status.json"
    previous: dict[str, Any] = {}
    if status_path.exists():
        try:
            loaded = json.loads(status_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                previous = loaded
        except (OSError, ValueError):
            pass
    baselines: dict[str, dict[str, Any]] = {}

    def publish(progress: dict[str, Any]) -> None:
        identity = str(progress["migration_id"])
        if identity not in baselines:
            resumed = previous.get("migration_id") == identity and previous.get("state") == "running"
            prior_total = int(previous.get("total_sources") or 0) if resumed else 0
            prior_completed = int(previous.get("completed_sources") or 0) if resumed else 0
            current_total = int(progress.get("total_sources") or 0)
            baselines[identity] = {
                "resumed": resumed,
                "total_sources": max(prior_total, prior_completed + current_total),
                "started_at": previous.get("started_at") if resumed else progress.get("started_at"),
            }
        baseline = baselines[identity]
        remaining = int(progress.get("remaining_sources") or 0)
        total = int(baseline["total_sources"])
        now = datetime.now().astimezone().isoformat(timespec="seconds")
        payload = {
            **progress,
            "total_sources": total,
            "completed_sources": max(total - remaining, 0),
            "remaining_sources": remaining,
            "started_at": baseline["started_at"],
            "resumed": baseline["resumed"],
            "heartbeat_at": now,
            "generated_at": now,
        }
        if payload.get("state") in {"complete", "failed"}:
            payload["completed_at"] = now
        write_json(status_path, payload)

    return publish

def fail_running_migration(state_dir: Path, failure: str) -> None:
    status_path = state_dir / "migration_status.json"
    if not status_path.exists():
        return
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if not isinstance(status, dict) or status.get("state") != "running":
        return
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    status.update({
        "state": "failed", "failure": failure, "heartbeat_at": now,
        "generated_at": now, "completed_at": now,
    })
    write_json(status_path, status)

def fast_sync_with_lock_retry(
    *args: Any, sleep: Callable[[float], None] = time.sleep, **kwargs: Any,
) -> dict[str, Any]:
    """Retry a sync that lost a brief database-lock race instead of publishing an error."""
    for delay in (*SYNC_LOCK_RETRY_DELAYS, None):
        try:
            return full_corpus.fast_sync(*args, **kwargs)
        except sqlite3.OperationalError as exc:
            if delay is None or "database is locked" not in str(exc):
                raise
            sleep(delay)
    raise AssertionError("unreachable")

def fsevents_recovery_summary(events: list[Any], watch_path: str) -> dict[str, Any] | None:
    """Summarize native FSEvents signals that require a recursive rescan."""
    affected = [
        event for event in events
        if event.must_scan_subdirs or event.is_user_dropped or event.is_kernel_dropped
    ]
    if not affected:
        return None
    event_ids = [int(event.event_id) for event in affected]
    return {
        "watch": watch_path,
        "events": len(affected),
        "must_scan_subdirs": sum(bool(event.must_scan_subdirs) for event in affected),
        "user_dropped": sum(bool(event.is_user_dropped) for event in affected),
        "kernel_dropped": sum(bool(event.is_kernel_dropped) for event in affected),
        "first_event_id": min(event_ids),
        "last_event_id": max(event_ids),
    }

def recovery_fsevents_observer(
    changed: threading.Event, recovery_events: queue.SimpleQueue[dict[str, Any]],
) -> Any:
    """Build a native observer that preserves Apple's mandatory rescan signals."""
    from watchdog.observers.fsevents import FSEventsEmitter, FSEventsObserver

    class RecoveryFSEventsEmitter(FSEventsEmitter):
        def queue_events(self, timeout: float, events: list[Any]) -> None:
            summary = fsevents_recovery_summary(events, str(self.watch.path))
            if summary is not None:
                recovery_events.put(summary)
                changed.set()
            super().queue_events(timeout, events)

    observer = FSEventsObserver()
    observer._emitter_class = RecoveryFSEventsEmitter
    return observer

def drain_recovery_events(
    recovery_events: queue.SimpleQueue[dict[str, Any]],
) -> list[dict[str, Any]]:
    result = []
    while True:
        try:
            result.append(recovery_events.get_nowait())
        except queue.Empty:
            return result

def observer_thread_failure(observer: Any) -> str | None:
    if not observer.is_alive():
        return "FSEvents observer dispatcher stopped"
    dead = sum(not emitter.is_alive() for emitter in observer.emitters)
    if dead:
        return f"{dead} FSEvents emitter thread(s) stopped"
    return None

def loaded_code_fingerprint() -> tuple[tuple[str, int, int], ...]:
    """Identify the SessionDB modules this process imported, by path, size, and mtime."""
    fingerprint = []
    for module in (sessiondb, full_corpus, sys.modules[__name__]):
        path = Path(module.__file__).resolve()
        try:
            stat = path.stat()
            fingerprint.append((str(path), stat.st_size, stat.st_mtime_ns))
        except OSError:
            fingerprint.append((str(path), -1, -1))
    return tuple(fingerprint)

def run_sync(
    db_path: Path, state_dir: Path, reason: str,
    trigger_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = state_dir.parent / "ingest.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        # The previous receipt stays during the sync: readers reject it once any write changes the
        # database or WAL identity it binds. A tail still being written (drift) leaves the committed
        # generation fully verified; per-query live coverage reports that tail separately.
        receipt_path = sessiondb.projection_integrity_receipt_path(db_path)
        try:
            report = fast_sync_with_lock_retry(
                db_path, state_dir / "last_fresh_sync.json",
                progress_callback=migration_progress_publisher(state_dir),
            )
            if report["complete"] or report.get("complete_except_drift"):
                receipt = publish_projection_integrity_receipt(db_path)
            else:
                receipt_path.unlink(missing_ok=True)
                receipt = {"state": "withheld", "reason": "sync_incomplete"}
            issues = report["issues"]
            hard_failures = report["hard_failures"]
            blocking_issues = issues
            drift = drift_health(report["sources_drifted_after_sync"])
            failed_migrations = [
                item for item in report.get("migrations", []) if item.get("state") == "failed"
            ]
            result = {
                "version": 1,
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "reason": reason,
                "pass": bool(report["complete"] and not blocking_issues and not hard_failures),
                "coverage_complete": report["complete"],
                "coverage_complete_except_drift": report.get("complete_except_drift"),
                "discovery_roots": report.get("discovery_roots"),
                "elapsed_seconds": report["elapsed_seconds"],
                "status_counts": report["status_counts"],
                "hard_failures": hard_failures,
                "missing_discovered_sources": report.get("missing_discovered_sources", []),
                "sources_drifted_after_sync": report["sources_drifted_after_sync"],
                **drift,
                "issues": issues,
                "repair_evidence": report.get("repair_evidence"),
                "migrations": report.get("migrations", []),
                "auto_adapter_updates": report.get("auto_adapter_updates", {
                    "admitted": [], "admitted_groups": 0,
                    "admitted_events": 0, "rejected_groups": 0,
                }),
                "projection_integrity_receipt": receipt,
                "attention_required": bool(
                    blocking_issues or hard_failures or drift["blocking_drift_sources"]
                    or failed_migrations
                ),
            }
        except Exception as exc:
            receipt_path.unlink(missing_ok=True)
            fail_running_migration(state_dir, str(exc))
            result = {
                "version": 1,
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "reason": reason,
                "pass": False,
                "coverage_complete": False,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
    if trigger_evidence is not None:
        result["trigger_evidence"] = trigger_evidence
    return publish_status(result, state_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path,
                        help="default: freshness beside the database")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--debounce", type=float, default=2.0)
    parser.add_argument("--max-debounce", type=float, default=5.0)
    args = parser.parse_args()
    if args.debounce <= 0 or args.max_debounce <= 0:
        parser.error("debounce values must be positive")
    state_dir = args.state_dir or args.db.resolve().parent / "freshness"
    if state_dir.resolve() != args.db.resolve().parent / "freshness":
        parser.error("state-dir must be freshness beside the database so queries see coverage")
    if args.once:
        return 0 if run_sync(args.db, state_dir, "manual")["pass"] else 1
    if sys.platform != "darwin":
        parser.error("continuous watching is supported on macOS only")
    from watchdog.events import FileSystemEventHandler
    changed = threading.Event()
    stopping = threading.Event()
    recovery_events: queue.SimpleQueue[dict[str, Any]] = queue.SimpleQueue()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event: Any) -> None:
            paths = (event.src_path, getattr(event, "dest_path", ""))
            if event.is_directory or any(path.endswith((".jsonl", ".desktop-released.json")) for path in paths):
                changed.set()

    class FileHandler(FileSystemEventHandler):
        """Changes to one named file in a folder whose other files are not history."""

        def __init__(self, name: str) -> None:
            super().__init__()
            self.name = name

        def on_any_event(self, event: Any) -> None:
            paths = (event.src_path, getattr(event, "dest_path", ""))
            if not event.is_directory and any(Path(path).name == self.name for path in paths if path):
                changed.set()

    observer = recovery_fsevents_observer(changed, recovery_events)
    handler = Handler()
    roots = [root for group in full_corpus.ROOTS.values() for root in group if root.exists()]
    if not roots:
        publish_status({"pass": False, "coverage_complete": False,
                        "error": "no transcript roots exist"}, state_dir)
        return 1
    for root in roots:
        if root.is_file():
            observer.schedule(FileHandler(root.name), str(root.parent), recursive=False)
        else:
            observer.schedule(handler, str(root), recursive=True)

    def stop(_signum: int, _frame: Any) -> None:
        stopping.set()
        changed.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    observer.start()
    fingerprint = loaded_code_fingerprint()
    try:
        latest = run_sync(args.db, state_dir, "startup")
        due, reason = scheduled_recheck(latest, time.time())
        while not stopping.is_set():
            if loaded_code_fingerprint() != fingerprint:
                print("SessionDB code changed; restart the watcher", file=sys.stderr)
                return 0
            failure = observer_thread_failure(observer)
            if failure:
                publish_status({"pass": False, "coverage_complete": False,
                                "error": failure}, state_dir)
                return 1
            if not changed.wait(timeout=1.0):
                if due is not None and time.time() >= due:
                    latest = run_sync(args.db, state_dir, reason)
                    due, reason = scheduled_recheck(latest, time.time())
                continue
            changed.clear()
            deadline = time.monotonic() + args.max_debounce
            while not stopping.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not changed.wait(timeout=min(args.debounce, remaining)):
                    break
                changed.clear()
            if not stopping.is_set():
                recovery = drain_recovery_events(recovery_events)
                latest = run_sync(args.db, state_dir,
                                  "fsevents_recovery" if recovery else "filesystem_event",
                                  trigger_evidence={"fsevents_rescan": recovery} if recovery else None)
                due, reason = scheduled_recheck(latest, time.time())
    finally:
        observer.stop()
        observer.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
