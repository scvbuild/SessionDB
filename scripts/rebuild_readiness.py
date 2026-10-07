#!/usr/bin/env python3
"""Create a one-use, revision- and corpus-bound full-rebuild readiness receipt."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import sessiondb
import tiktoken


def file_sha256(path: Path) -> str:
    return sessiondb.digest(path.read_bytes())


def git_revision() -> str:
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT,
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=PROJECT_ROOT,
        check=True, capture_output=True, text=True,
    ).stdout
    if dirty:
        raise ValueError("rebuild readiness requires a clean committed revision")
    return revision


def git_root() -> Path:
    return Path(subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], cwd=PROJECT_ROOT,
        check=True, capture_output=True, text=True,
    ).stdout.strip()).resolve()


def run_unit_tests() -> dict[str, Any]:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    started = time.perf_counter()
    completed = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests"],
        cwd=PROJECT_ROOT, env=environment, capture_output=True, text=True,
    )
    output = completed.stdout + completed.stderr
    match = re.search(r"Ran ([0-9]+) tests?", output)
    result = {
        "pass": completed.returncode == 0,
        "tests": int(match.group(1)) if match else None,
        "seconds": round(time.perf_counter() - started, 3),
    }
    if not result["pass"]:
        raise ValueError("unit tests failed:\n" + output[-4000:])
    return result


def search_args(text: str, mode: str = "literal") -> SimpleNamespace:
    return SimpleNamespace(
        text=text, mode=mode, order="relevance", provider=None, project=None,
        session=None, role=None, tool=None, lineage=None, since=None, until=None,
        deadline_seconds=30.0,
    )


def representative_sample_validation() -> dict[str, Any]:
    rows = [
        {"type": "user", "sessionId": "sample", "message": {
            "role": "user", "content": "customer quote is ready",
        }},
        {"type": "assistant", "sessionId": "sample", "message": {
            "role": "assistant", "content": "khách hàng báo giá",
        }},
        {"type": "user", "sessionId": "sample", "message": {
            "role": "user", "content": "khach hang follow up",
        }},
        {"type": "assistant", "sessionId": "sample", "message": {
            "role": "assistant", "content": "đồng ý alpha one beta",
        }},
        {"type": "user", "sessionId": "sample", "message": {
            "role": "user", "content": "dong y alpha one two three beta",
        }},
        {"type": "assistant", "sessionId": "sample", "message": {
            "role": "assistant", "content": "--dangerously-skip-permissions",
        }},
        {"type": "user", "sessionId": "sample", "message": {
            "role": "user", "content": "--allow-dangerously-skip-permissions",
        }},
    ]
    expected = [
        ("english", "customer quote", "literal", 1),
        ("vietnamese-exact", "khách hàng", "literal", 1),
        ("vietnamese-folded", "khach hang", "literal", 2),
        ("d-stroke-exact", "đồng", "literal", 1),
        ("d-plain-exact", "dong", "literal", 1),
        ("proximity", "alpha NEAR/2 beta", "ranked", 1),
        ("structured-boundary", "--dangerously-skip-permissions", "literal", 1),
    ]
    with tempfile.TemporaryDirectory(prefix="sessiondb-rebuild-readiness-") as directory:
        root = Path(directory)
        source = root / "sample.jsonl"
        source.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
            encoding="utf-8",
        )
        database = root / "sample.sqlite"
        db = sessiondb.connect(database)
        try:
            sessiondb.ingest(db, "claude", source)
            build = sessiondb.build_search_projections(db)
            cases = []
            for case_id, text, mode, count in expected:
                result = sessiondb.search_index(db, search_args(text, mode))
                passed = (
                    result.get("complete") is True
                    and result.get("total") == count
                    and result.get("telemetry", {}).get("source_reads") == 0
                )
                cases.append({
                    "id": case_id, "pass": passed, "expected": count,
                    "actual": result.get("total"),
                })
            return {
                "pass": build.get("complete") is True and all(case["pass"] for case in cases),
                "projection_version": sessiondb.PROJECTION_VERSION,
                "source_reads": build.get("source_reads"), "cases": cases,
            }
        finally:
            db.close()


def load_material_review(path: Path, revision: str, plan_sha256: str) -> dict[str, Any]:
    review = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(review, dict) or (
        review.get("outcome") != "approved"
        or review.get("material_changes_reviewed") is not True
        or review.get("reviewed_revision") != revision
        or review.get("plan_sha256") != plan_sha256
        or not isinstance(review.get("reviewer"), str)
    ):
        raise ValueError("material review is missing, unapproved, or does not match the frozen revision")
    return {
        "reviewer": review["reviewer"], "outcome": review["outcome"],
        "material_changes_reviewed": True, "reviewed_revision": revision,
        "plan_sha256": plan_sha256, "receipt_sha256": file_sha256(path),
    }


def corpus_preflight(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    db = sessiondb.connect(path, read_only=True)
    try:
        quick_check = db.execute("PRAGMA quick_check").fetchone()[0]
        foreign_keys = db.execute("PRAGMA foreign_key_check").fetchall()
        if quick_check != "ok" or foreign_keys:
            raise ValueError("corpus database integrity preflight failed")
        events = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        snapshot_id = sessiondb.projection_snapshot_id(db)
        pack = sessiondb.projection_pack_path(db)
        pack_bytes = pack.stat().st_size if pack.is_file() else 0
        object_pack = sessiondb.verify_pack(db)
        if not object_pack["pass"]:
            raise ValueError("retained object-pack integrity preflight failed")
    finally:
        db.close()
    database_bytes = path.stat().st_size
    required_free = database_bytes + pack_bytes
    free = shutil.disk_usage(path.resolve().parent).free
    if free < required_free:
        raise ValueError("insufficient free disk for a side-by-side full rebuild")
    return (
        {
            "events": events, "snapshot_id": snapshot_id,
            "schema_version": sessiondb.SCHEMA_VERSION,
            "database_bytes": database_bytes, "retrieval_pack_bytes": pack_bytes,
            "quick_check": quick_check, "foreign_key_failures": 0,
            "object_pack": {
                "pass": True, "objects_checked": object_pack["objects_checked"],
                "pack_bytes": object_pack["pack_bytes"],
                "orphan_bytes": object_pack["orphan_bytes"],
            },
        },
        {"free_bytes": free, "required_free_bytes": required_free, "pass": True},
    )


def write_private_json(path: Path, value: dict[str, Any]) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    sessiondb.fsync_directory(path.parent)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--review-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    repository_root = git_root()
    revision = git_revision()
    plan = args.plan.resolve()
    try:
        plan_relative = plan.relative_to(repository_root)
    except ValueError as exc:
        raise ValueError("rebuild plan must be inside the frozen repository") from exc
    plan_sha256 = file_sha256(plan)
    review = load_material_review(args.review_receipt.resolve(), revision, plan_sha256)
    unit_tests = run_unit_tests()
    samples = representative_sample_validation()
    if not samples["pass"]:
        raise ValueError("representative English/Vietnamese sample validation failed")
    corpus, capacity = corpus_preflight(args.db.resolve())
    encoding = tiktoken.get_encoding("o200k_base")
    encoding.encode_ordinary("English và tiếng Việt <|endoftext|>")
    if git_revision() != revision or file_sha256(plan) != plan_sha256:
        raise ValueError("frozen revision or plan changed during readiness validation")
    receipt = {
        "schema": sessiondb.REBUILD_READINESS_SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "revision": revision,
        "repository": {"root": str(repository_root)},
        "module_sha256": file_sha256(Path(sessiondb.__file__).resolve()),
        "projection_version": sessiondb.PROJECTION_VERSION,
        "plan": {
            "path": str(plan), "relative_path": str(plan_relative),
            "sha256": plan_sha256,
        },
        "material_review": review,
        "unit_tests": unit_tests,
        "representative_samples": samples,
        "corpus": corpus,
        "capacity": capacity,
        "tokenizer": {"library": "tiktoken", "version": tiktoken.__version__,
                      "encoding": "o200k_base", "pass": True},
    }
    receipt["receipt_sha256"] = sessiondb.rebuild_receipt_digest(receipt)
    write_private_json(args.output, receipt)
    print(json.dumps({
        "ready": True, "receipt": str(args.output.resolve()),
        "receipt_sha256": receipt["receipt_sha256"],
        "events": corpus["events"], "revision": revision,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
