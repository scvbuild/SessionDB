import json
import tempfile
import time
import unittest
from pathlib import Path

import sessiondb


class RedactionTest(unittest.TestCase):
    def test_quoted_keys_and_any_url_scheme_are_redacted(self):
        self.assertNotIn("hunter2", sessiondb.redact('{"password": "hunter2"}'))
        self.assertNotIn("hunter2", sessiondb.redact("{'api_key': 'hunter2'}"))
        self.assertNotIn("hunter2", sessiondb.redact("postgres://alice:hunter2@db.example.test/app"))

    def test_escaped_newline_before_a_secret_does_not_hide_it(self):
        self.assertNotIn("hunter2", sessiondb.redact("line one\\nhttps://alice:hunter2@example.test/x"))
        key = "sk-" + "a1B2c3D4e5F6g7H8i9J0"
        self.assertNotIn(key, sessiondb.redact("export\\n" + key))
        self.assertNotIn(key, sessiondb.redact("value:\\t" + key))
        self.assertEqual(sessiondb.redact("task-" + "a1B2c3D4e5F6g7H8i9J0"), "task-" + "a1B2c3D4e5F6g7H8i9J0")

    def test_redaction_stays_linear_on_long_unbroken_text(self):
        for text in ("abc123" * 200_000, "a://" + "b:" * 300_000, "http://" + "x" * 1_200_000):
            with self.subTest(text=text[:12]):
                started = time.monotonic()
                sessiondb.redact(text)
                self.assertLess(time.monotonic() - started, 2.0)
        self.assertNotIn("hunter2", sessiondb.redact("x" * 100_000 + " git+ssh://alice:hunter2@host/repo"))

    def test_url_credentials_after_an_earlier_redaction_are_still_redacted(self):
        pem = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----"
        key = "sk-" + "a1B2c3D4e5F6g7H8i9J0"
        for prefix in (pem, key):
            with self.subTest(prefix=prefix[:5]):
                redacted = sessiondb.redact(prefix + " https://alice:hunter2@example.test/x tail")
                self.assertNotIn("hunter2", redacted)
                self.assertTrue(redacted.endswith("@example.test/x tail"))


class IngestFixesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = sessiondb.connect(self.root / "test.sqlite")

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def test_timestamps_with_any_fraction_length_parse(self):
        self.assertEqual(
            sessiondb.normalize_timestamp("2026-01-02T03:04:05.12Z"), "2026-01-02T03:04:05.120000Z",
        )
        self.assertEqual(
            sessiondb.normalize_timestamp("2026-01-02T03:04:05.123456789+00:00"),
            "2026-01-02T03:04:05.123456Z",
        )
        self.assertEqual(
            sessiondb._time_bound("2026-01-02T03:04:05.1Z", end=False), "2026-01-02T03:04:05.100000Z",
        )

    def test_lone_surrogate_line_is_kept_unreadable_without_failing_ingest(self):
        path = self.root / "surrogate.jsonl"
        good = json.dumps({"type": "user", "sessionId": "surrogate", "message": {"role": "user", "content": "ok"}})
        lone = '{"type":"user","sessionId":"surrogate","message":{"role":"user","content":"half \\ud83d"}}'
        path.write_text(good + "\n" + lone + "\n", encoding="utf-8")
        sessiondb.ingest(self.db, "claude", path)
        categories = [row[0] for row in self.db.execute("SELECT category FROM events ORDER BY line_number")]
        self.assertEqual(len(categories), 2)
        self.assertEqual(categories[1], "unreadable")


if __name__ == "__main__":
    unittest.main()
