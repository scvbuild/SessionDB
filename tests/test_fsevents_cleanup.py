import queue
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import freshness_service
from watchdog.observers.api import EventQueue, ObservedWatch
from watchdog.observers.fsevents import _fsevents


class FSEventsCleanupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        observer = freshness_service.recovery_fsevents_observer(threading.Event(), queue.SimpleQueue())
        self.emitter = observer._emitter_class(
            EventQueue(), ObservedWatch(self.temp.name, recursive=True),
        )

    def test_failed_start_never_removes_the_released_native_stream(self):
        with mock.patch.object(_fsevents, "add_watch", side_effect=SystemError("stream denied")), \
                mock.patch.object(_fsevents, "read_events") as read, \
                mock.patch.object(_fsevents, "remove_watch") as remove, \
                mock.patch.object(_fsevents, "stop") as stop, \
                self.assertLogs("fsevents", level="ERROR") as logs:
            self.emitter.start()
            self.emitter.join(timeout=2)
            self.assertFalse(self.emitter.is_alive())
            self.emitter.stop()
            self.emitter.stop()
        self.assertIn("stream denied", "\n".join(logs.output))
        read.assert_not_called()
        remove.assert_not_called()
        stop.assert_not_called()

    def test_successful_start_stops_and_removes_exactly_once(self):
        reading, released = threading.Event(), threading.Event()

        def read_events(_emitter):
            reading.set()
            released.wait(timeout=2)

        with mock.patch.object(_fsevents, "add_watch"), \
                mock.patch.object(_fsevents, "read_events", side_effect=read_events), \
                mock.patch.object(_fsevents, "remove_watch") as remove, \
                mock.patch.object(_fsevents, "stop", side_effect=lambda _: released.set()) as stop:
            self.emitter.start()
            self.assertTrue(reading.wait(timeout=2))
            self.emitter.stop()
            self.emitter.join(timeout=2)
            self.emitter.stop()
        self.assertFalse(self.emitter.is_alive())
        remove.assert_called_once_with(self.emitter.watch)
        stop.assert_called_once_with(self.emitter)

    def test_stop_waits_for_failed_start_before_deciding_cleanup(self):
        starting, release = threading.Event(), threading.Event()

        def add_watch(*_args):
            starting.set()
            release.wait(timeout=2)
            raise SystemError("stream denied")

        with mock.patch.object(_fsevents, "add_watch", side_effect=add_watch), \
                mock.patch.object(_fsevents, "remove_watch") as remove, \
                mock.patch.object(_fsevents, "stop") as stop, \
                self.assertLogs("fsevents", level="ERROR"):
            self.emitter.start()
            self.assertTrue(starting.wait(timeout=2))
            stopper = threading.Thread(target=self.emitter.stop)
            stopper.start()
            release.set()
            stopper.join(timeout=2)
            self.emitter.join(timeout=2)
        self.assertFalse(stopper.is_alive())
        self.assertFalse(self.emitter.is_alive())
        remove.assert_not_called()
        stop.assert_not_called()

    def test_stop_before_start_does_not_wait_or_call_native_cleanup(self):
        with mock.patch.object(_fsevents, "remove_watch") as remove, \
                mock.patch.object(_fsevents, "stop") as stop:
            self.emitter.stop()
        remove.assert_not_called()
        stop.assert_not_called()

    def test_native_observer_handles_denial_or_events_across_restarts(self):
        script = '''
import json, queue, tempfile, threading
from pathlib import Path
import freshness_service
from watchdog.events import FileSystemEventHandler

results = []
with tempfile.TemporaryDirectory() as folder:
    for cycle in range(6):
        changed = threading.Event()
        observer = freshness_service.recovery_fsevents_observer(changed, queue.SimpleQueue())
        class Handler(FileSystemEventHandler):
            def on_any_event(self, event):
                changed.set()
        for name in ('claude', 'codex'):
            path = Path(folder) / (name + str(cycle))
            path.mkdir()
            observer.schedule(Handler(), str(path), recursive=True)
        observer.start()
        emitters = list(observer.emitters)
        try:
            for emitter in emitters:
                assert emitter._native_ready.wait(2), 'native startup timed out'
            started = all(emitter._native_started for emitter in emitters)
            if started:
                changed.clear()
                (Path(folder) / ('claude' + str(cycle)) / 'record').write_text('fixture')
                assert changed.wait(5), 'native event missed'
            else:
                for emitter in emitters:
                    if not emitter._native_started:
                        emitter.join(2)
                assert freshness_service.observer_thread_failure(observer), 'failure not reported'
            results.append('events' if started else 'denied')
        finally:
            observer.stop()
            observer.join(2)
            assert not observer.is_alive(), 'dispatcher did not stop'
            assert all(not emitter.is_alive() for emitter in emitters), 'emitter did not stop'
print(json.dumps(results))
'''
        environment = dict(os.environ, PYTHONPATH=str(Path(freshness_service.__file__).parent),
                           PYTHONDONTWRITEBYTECODE="1")
        result = subprocess.run([sys.executable, "-c", script], env=environment,
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        results = json.loads(result.stdout)
        self.assertEqual(len(results), 6)
        self.assertTrue(all(value in ("events", "denied") for value in results))
        self.assertNotIn("already scheduled", result.stderr)
        self.assertNotIn("over-released FSEventStreamRef", result.stderr)
        self.assertNotIn("failed assertion", result.stderr)
