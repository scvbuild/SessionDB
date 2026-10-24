#!/usr/bin/env python3
"""Keep local SessionDB current, with optional verified automatic parser repair."""
from __future__ import annotations
import argparse
import os
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
from typing import Any, Callable, Iterable
import full_corpus
import sessiondb

SYNC_LOCK_RETRY_DELAYS = (5.0, 15.0)
SYNC_ERROR_RETRY_SECONDS = 60.0


def publish_status(result: dict[str, Any], state_dir: Path,
                   repair_config: dict[str, Any] | None = None) -> dict[str, Any]:
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
    result["automatic_repair"] = repair_config is not None
    if attention:
        result["pass"] = False
    write_json(state_dir / "watcher_status.json", result)
    journal = Path(sessiondb.__file__).resolve().parents[1] / ".sessiondb-repair/activation.json"
    if repair_config is not None or (state_dir / "repair-state.json").exists() or journal.exists():
        import repair_agent
        result["repair"] = repair_agent.advance(result, state_dir, repair_config)
        if result["repair"]["state"] in {"confirming", "running", "busy", "restart_required"}:
            result["recovery_state"] = "repair_pending"
        if result["repair"]["state"] in {"blocked", "rolled_back", "no_safe_repair"}:
            result["attention_required"] = True
        write_json(state_dir / "watcher_status.json", result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return result


def scheduled_recheck(latest: dict[str, Any], now: float) -> tuple[float | None, str]:
    repair = latest.get("repair") or {}
    if repair.get("state") == "confirming":
        return float(repair["due"]), "repair_confirmation"
    if repair.get("state") in {"running", "busy", "restart_required"}:
        return now + 5.0, "repair_check"
    if latest.get("error") or latest.get("hard_failures") or latest.get("missing_discovered_sources") or (
        latest.get("automatic_repair") and latest.get("issues")
    ):
        return now + SYNC_ERROR_RETRY_SECONDS, "error_retry"
    return None, "filesystem_event"

def write_json(path: Path, value: Any) -> None:
    full_corpus.write_json_atomic(path, value)

def publish_projection_integrity_receipt(
    db_path: Path, prepared: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Publish a private optimization receipt; absence always means deep verification.

    `prepared` is an incremental receipt built by the sync itself. It is published only while
    the live files still match its binding; otherwise the full check runs.
    """
    path = sessiondb.projection_integrity_receipt_path(db_path)
    db = None
    try:
        db = sessiondb.connect(db_path, read_only=True)
        db.execute("BEGIN")
        if prepared is not None and sessiondb.sqlite_generation_identity(db) == prepared["sqlite"]:
            receipt = prepared
        else:
            receipt = sessiondb.build_projection_integrity_receipt(db)
        if not sessiondb.connection_path_is_current(db):
            raise ValueError("database was replaced during receipt verification")
        write_json(path, receipt)
        return {
            "state": "current", "created_at": receipt["created_at"],
            "snapshot_id": receipt["snapshot_id"],
            "verification": receipt.get("verification", "full"),
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
    from watchdog.observers.fsevents import FSEventsEmitter, FSEventsObserver, _fsevents, logger

    class RecoveryFSEventsEmitter(FSEventsEmitter):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._native_ready = threading.Event()
            self._native_started = False
            self._native_cleanup_lock = threading.Lock()

        def run(self) -> None:
            self.pathnames = [self.watch.path]
            self._start_time = time.monotonic()
            try:
                _fsevents.add_watch(self, self.watch, self.events_callback, self.pathnames)
                self._native_started = True
            except Exception:
                logger.exception("Cannot start SessionDB FSEvents watcher")
            finally:
                self._native_ready.set()
            if self._native_started and self.should_keep_running():
                try:
                    _fsevents.read_events(self)
                except Exception:
                    logger.exception("SessionDB FSEvents event loop failed")

        def on_thread_stop(self) -> None:
            if self.ident is None and not self._native_ready.is_set():
                return
            self._native_ready.wait()
            with self._native_cleanup_lock:
                # watchdog 6 retains a released stream after add_watch fails; removing it
                # again dereferences freed memory. Only clean up a successful start.
                if self._native_started:
                    super().on_thread_stop()
                    self._native_started = False

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

def store_journal_mode(db_path: Path) -> str | None:
    """Switch the store to WAL before writing; a failure leaves the sync to run in the old mode."""
    if not db_path.exists():
        return None
    try:
        return sessiondb.enable_wal(db_path)
    except sqlite3.Error as exc:
        print(f"WAL switch failed: {exc}", file=sys.stderr, flush=True)
        return None


class StoreKeeper:
    """Hold one idle read-only connection so no other program closes the store last.

    SQLite builds other than Apple's delete -wal and -shm when their last connection closes,
    and read-only opens elsewhere then fail until a writer recreates them. An idle connection
    holds no lock, so it never blocks writers or checkpoints.
    """

    def __init__(self) -> None:
        self.db: sqlite3.Connection | None = None
        self.error: str | None = None

    def refresh(self, db_path: Path) -> None:
        if self.db is not None and sessiondb.connection_path_is_current(self.db):
            return
        self.close()
        try:
            self.db = sessiondb.connect(db_path, read_only=True, timeout=1.0)
            self.error = None
        except (OSError, ValueError, sqlite3.Error) as exc:
            if str(exc) != self.error:
                print(f"store keeper could not open the store: {exc}", file=sys.stderr, flush=True)
            self.error = str(exc)

    def close(self) -> None:
        if self.db is not None:
            self.db.close()
            self.db = None


class ChangedPaths:
    """Transcript paths reported since the last sync; an event naming no file forces a full listing."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._paths: set[str] = set()
        self._full = True

    def add(self, paths: Iterable[str]) -> None:
        with self._lock:
            self._paths.update(path for path in paths if path)

    def require_full(self) -> None:
        with self._lock:
            self._full = True

    def take(self) -> list[str] | None:
        """Return the paths to sync and reset, or None when every file must be listed."""
        with self._lock:
            paths, full = sorted(self._paths), self._full
            self._paths, self._full = set(), False
        return None if full else paths


def record_transcript_event(event: Any, pending: ChangedPaths, changed: threading.Event) -> None:
    paths = [event.src_path, getattr(event, "dest_path", "")]
    if event.is_directory:
        # A moved or removed folder can carry transcripts that get no events of their own;
        # creating or modifying one only accompanies file events that are reported.
        if event.event_type in ("moved", "deleted"):
            pending.require_full()
        changed.set()
    elif any(path.endswith(".jsonl") for path in paths):
        pending.add(paths)
        changed.set()


def wait_for_sync_window(
    changed: threading.Event, stopping: threading.Event,
    recovery_events: queue.SimpleQueue[dict[str, Any]],
    not_before: float, debounce: float, max_debounce: float,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Hold an event-triggered sync until `not_before`, then apply the quiet-period debounce.

    Every sync re-verifies the whole database, so active sessions writing continuously would
    otherwise trigger one every few seconds. Queries serve unindexed tails themselves, so
    spacing syncs does not make answers incomplete. A stop or an FSEvents rescan signal ends
    the spacing wait at once.
    """
    while not stopping.is_set() and recovery_events.empty():
        remaining = not_before - clock()
        if remaining <= 0:
            break
        # Ordinary events are cleared here: the coming sync discovers every source anyway.
        changed.wait(timeout=remaining)
        changed.clear()
    deadline = clock() + max_debounce
    while not stopping.is_set():
        remaining = deadline - clock()
        if remaining <= 0 or not changed.wait(timeout=min(debounce, remaining)):
            break
        changed.clear()


def run_sync(
    db_path: Path, state_dir: Path, reason: str,
    trigger_evidence: dict[str, Any] | None = None,
    changed_paths: list[str] | None = None,
    repair_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = state_dir.parent / "ingest.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        # The previous receipt stays during the sync: readers reject it once any write changes the
        # database or WAL identity it binds. A tail still being written (drift) leaves the committed
        # generation fully verified; per-query live coverage reports that tail separately.
        receipt_path = sessiondb.projection_integrity_receipt_path(db_path)
        journal_mode = store_journal_mode(db_path)
        try:
            report = fast_sync_with_lock_retry(
                db_path, state_dir / "last_fresh_sync.json",
                progress_callback=migration_progress_publisher(state_dir),
                incremental_receipt=True, changed_paths=changed_paths,
            )
            prepared_receipt = report.pop("incremental_receipt", None)
            if journal_mode is None:
                journal_mode = store_journal_mode(db_path)
            if report["complete"] or report.get("complete_except_drift"):
                receipt = publish_projection_integrity_receipt(db_path, prepared_receipt)
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
                "journal_mode": journal_mode,
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
    return publish_status(result, state_dir, repair_config)


def restart_process() -> None:
    os.execv(sys.executable, [sys.executable, "-m", "freshness_service", *sys.argv[1:]])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path,
                        help="default: freshness beside the database")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--min-sync-interval", type=float, default=30.0)
    parser.add_argument("--debounce", type=float, default=2.0)
    parser.add_argument("--max-debounce", type=float, default=5.0)
    parser.add_argument("--auto-repair", choices=("codex", "claude"),
                        help="opt in to an agent and automatic activation of verified parser fixes")
    parser.add_argument("--repair-repo", type=Path, help="source checkout installed in editable mode")
    parser.add_argument("--repair-executable", help="path to the selected agent CLI")
    parser.add_argument("--repair-model", help="optional model override; default is the CLI default")
    args = parser.parse_args()
    repair_config = None
    if args.auto_repair:
        if args.once or args.repair_repo is None:
            parser.error("auto-repair requires continuous watching and --repair-repo")
        import repair_agent
        try:
            repo = repair_agent.check_repository(args.repair_repo)
        except ValueError as exc:
            parser.error(str(exc))
        repair_config = {"repo": str(repo), "provider": args.auto_repair,
                         "executable": args.repair_executable, "model": args.repair_model}
    repo = Path(sessiondb.__file__).resolve().parents[1]
    if (repo / ".sessiondb-repair/activation.json").exists():
        import repair_agent
        if repair_agent.recover(repo):
            restart_process()
    if args.debounce <= 0 or args.max_debounce <= 0 or args.min_sync_interval < 0:
        parser.error("debounce values must be positive")
    state_dir = args.state_dir or args.db.resolve().parent / "freshness"
    if state_dir.resolve() != args.db.resolve().parent / "freshness":
        parser.error("state-dir must be freshness beside the database so queries see coverage")
    if args.once:
        return 0 if run_sync(args.db, state_dir, "manual")["pass"] else 1
    if sys.platform != "darwin":
        parser.error("continuous watching is supported on macOS only")
    from watchdog.events import FileSystemEventHandler
    pending = ChangedPaths()
    changed = threading.Event()
    stopping = threading.Event()
    recovery_events: queue.SimpleQueue[dict[str, Any]] = queue.SimpleQueue()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event: Any) -> None:
            record_transcript_event(event, pending, changed)

    class FileHandler(FileSystemEventHandler):
        """Changes to one named file in a folder whose other files are not history."""

        def __init__(self, name: str) -> None:
            super().__init__()
            self.name = name

        def on_any_event(self, event: Any) -> None:
            paths = (event.src_path, getattr(event, "dest_path", ""))
            if not event.is_directory and any(Path(path).name == self.name for path in paths if path):
                pending.add(paths)
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
    keeper = StoreKeeper()
    try:
        pending.take()
        latest = run_sync(args.db, state_dir, "startup", repair_config=repair_config)
        last_sync_ended = time.monotonic()
        due, reason = scheduled_recheck(latest, time.time())
        while not stopping.is_set():
            if loaded_code_fingerprint() != fingerprint:
                print("SessionDB code changed; restarting the watcher", file=sys.stderr)
                break
            failure = observer_thread_failure(observer)
            if failure:
                publish_status({"pass": False, "coverage_complete": False,
                                "error": failure}, state_dir)
                return 1
            keeper.refresh(args.db)
            if not changed.wait(timeout=1.0):
                if due is not None and time.time() >= due:
                    latest = run_sync(args.db, state_dir, reason, repair_config=repair_config)
                    last_sync_ended = time.monotonic()
                    due, reason = scheduled_recheck(latest, time.time())
                continue
            changed.clear()
            wait_for_sync_window(changed, stopping, recovery_events,
                                 last_sync_ended + args.min_sync_interval,
                                 args.debounce, args.max_debounce)
            if not stopping.is_set():
                recovery = drain_recovery_events(recovery_events)
                if recovery:
                    pending.require_full()
                latest = run_sync(args.db, state_dir,
                                  "fsevents_recovery" if recovery else "filesystem_event",
                                  trigger_evidence={"fsevents_rescan": recovery} if recovery else None,
                                  changed_paths=pending.take(), repair_config=repair_config)
                last_sync_ended = time.monotonic()
                due, reason = scheduled_recheck(latest, time.time())
    finally:
        keeper.close()
        observer.stop()
        observer.join()
    if not stopping.is_set():
        restart_process()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
