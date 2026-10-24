"""Opt-in parser repair in a separate candidate, with verified automatic activation."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterator
import fcntl

import full_corpus
import sessiondb

CONFIRM_SECONDS = 300.0
POLL_SECONDS = 5.0
AGENT_TIMEOUT = 900.0
TEST_TIMEOUT = 300.0
MAX_INPUT_BYTES = 256 * 1024
ALLOWED_DECLARATIONS = frozenset({
    "ADAPTER_VERSION", "ADAPTER_MIGRATIONS", "CLAUDE_TYPES", "CLAUDE_ARTIFACT_TYPES",
    "CODEX_TYPES", "CODEX_PAYLOAD_TYPES", "CODEX_REALTIME_PAYLOAD_TYPES",
    "CODEX_COMPLETED_DUPLICATE_TYPES", "CODEX_COMPLETED_TOOL_TYPES",
    "CODEX_COMPLETED_UNIQUE_TOOL_TYPES", "parse_claude", "parse_codex", "claude_tool",
    "codex_content", "codex_completed_item", "codex_compaction_supported",
    "codex_token_usage_record_supported", "codex_thread_name", "tool_from",
})
RESULT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["outcome", "summary"],
    "properties": {"outcome": {"type": "string", "enum": ["repaired", "no_safe_repair", "blocked"]},
                   "summary": {"type": "string", "maxLength": 500}},
}
LOADED_CODE_HASH = hashlib.sha256(Path(sessiondb.__file__).read_bytes()).hexdigest()


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    result = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(result, dict):
        raise ValueError(f"expected a JSON object: {path.name}")
    return result


def write_json(path: Path, value: dict[str, Any]) -> None:
    full_corpus.write_json_atomic(path, value)
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
    sessiondb.fsync_directory(path.parent)


@contextlib.contextmanager
def locked(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def atomic_source(path: Path, content: bytes) -> None:
    previous = path.stat()
    descriptor, name = tempfile.mkstemp(prefix=".sessiondb-repair-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, previous.st_mode & 0o777)
        # Bytecode caches compare whole-second mtime and size, not source identity.
        modified = max(time.time_ns(), previous.st_mtime_ns + 1_000_000_000)
        os.utime(temporary, ns=(modified, modified))
        os.replace(temporary, path)
        sessiondb.fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def issue_keys(result: dict[str, Any]) -> list[str]:
    return sorted({json.dumps({k: item.get(k) for k in ("provider", "category", "issue")},
                             sort_keys=True) for item in result.get("issues", [])})


def eligible(result: dict[str, Any]) -> bool:
    return bool(result.get("issues") and (result.get("repair_evidence") or {}).get("complete")) and not any((
        result.get("error"), result.get("hard_failures"), result.get("blocking_drift_sources"),
        result.get("missing_discovered_sources"),
        any(item.get("state") == "failed" for item in result.get("migrations", [])),
    ))


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: "[REDACTED]" if sessiondb.SECRET_FIELD_RE.search(key) else redact_value(item)
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    return sessiondb.redact(value) if isinstance(value, str) else value


def validate_repair_evidence(status_path: Path, evidence_path: Path) -> dict[str, Any]:
    """Validate one immutable repair bundle against the watcher observation that selected it."""
    status = json.loads(status_path.read_text(encoding="utf-8"))
    bundle = json.loads(evidence_path.read_text(encoding="utf-8"))
    metadata = status.get("repair_evidence") if isinstance(status, dict) else None
    if not isinstance(metadata, dict) or metadata.get("complete") is not True:
        raise ValueError("watcher repair evidence is not complete")
    if bundle.get("version") != 1 or not isinstance(bundle.get("groups"), list):
        raise ValueError("repair evidence schema is invalid")
    if bundle.get("id") != metadata.get("id"):
        raise ValueError("repair evidence identity mismatch")
    if evidence_path.resolve() != Path(str(metadata.get("path") or "")).resolve():
        raise ValueError("repair evidence path mismatch")
    identity = json.dumps(
        bundle["groups"], ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()
    if hashlib.sha256(identity).hexdigest() != bundle["id"]:
        raise ValueError("repair evidence content hash mismatch")

    def key(item: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(item.get(field) for field in ("provider", "category", "issue", "events"))

    watcher_issues = status.get("issues") or []
    if not all(isinstance(item, dict) for item in watcher_issues):
        raise ValueError("watcher issue schema is invalid")
    groups = bundle["groups"]
    if not all(isinstance(item, dict) for item in groups):
        raise ValueError("repair evidence group schema is invalid")
    if Counter(key(item) for item in watcher_issues) != Counter(key(item) for item in groups):
        raise ValueError("repair evidence issue mapping mismatch")
    required = {
        "event_id", "provider", "category", "issue", "source_path", "line_number",
        "raw_hash", "evidence_source", "structure",
    }
    record_count = 0
    for group in groups:
        records = group.get("records")
        if not isinstance(records, list) or len(records) != group.get("events"):
            raise ValueError("repair evidence record count mismatch")
        for record in records:
            if not isinstance(record, dict) or not required.issubset(record):
                raise ValueError("repair evidence record schema is invalid")
            if record["evidence_source"] not in {"sessiondb", "raw_transcript"}:
                raise ValueError("repair evidence source is invalid")
            if not isinstance(record["structure"], dict):
                raise ValueError("repair evidence structure is not an object")
            if full_corpus.sessiondb.digest(
                full_corpus.sessiondb.stored_json(record["structure"])
            ) != record["raw_hash"]:
                raise ValueError("repair evidence structure hash mismatch")
            if (
                record["provider"], record["category"], record["issue"]
            ) != (
                group.get("provider"), group.get("category"), group.get("issue")
            ):
                raise ValueError("repair evidence record group mismatch")
            record_count += 1
    if record_count != metadata.get("events"):
        raise ValueError("repair evidence total count mismatch")
    return bundle


def repair_input(bundle: dict[str, Any]) -> dict[str, Any]:
    records = [{"provider": record["provider"], "category": record["category"],
                "structure": redact_value(record["structure"])}
               for group in bundle["groups"] for record in group["records"]]
    if not records:
        raise ValueError("repair evidence contains no records")
    result = {"records": records, "allowed_declarations": sorted(ALLOWED_DECLARATIONS),
              "migration_selection": "category-scoped with unsupported_only=True",
              "test_contract": "Test behavior and migration entries, without referencing ADAPTER_VERSION; successful tests are retained for future repairs."}
    if len(json.dumps(result).encode()) > MAX_INPUT_BYTES:
        raise ValueError("redacted repair evidence exceeds the bounded input limit")
    return result


def check_repository(repo: Path) -> Path:
    repo = repo.resolve()
    if (repo / "src/sessiondb.py").resolve() != Path(sessiondb.__file__).resolve():
        raise ValueError("automatic repair requires an editable install of --repair-repo")
    for relative in ("src/sessiondb.py", "tests/test_sessiondb.py", "repair-agent-prompt.md"):
        if not (repo / relative).is_file() or (repo / relative).is_symlink():
            raise ValueError(f"repair checkout missing a regular {relative}")
    return repo


def declarations(text: str) -> list[tuple[str | None, str]]:
    result = []
    for node in ast.parse(text).body:
        name = node.name if isinstance(node, (ast.FunctionDef, ast.ClassDef)) else None
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
        result.append((name, ast.dump(node, include_attributes=False)))
    return result


def check_parser_change(before: str, after: str, providers: set[str]) -> None:
    old, new = declarations(before), declarations(after)
    if [name for name, _ in old] != [name for name, _ in new]:
        raise ValueError("repair added, removed or reordered module declarations")
    changed = {name for (name, a), (_, b) in zip(old, new) if a != b}
    if not changed or not changed <= ALLOWED_DECLARATIONS:
        raise ValueError("repair must change only allowed parser declarations")
    if not {"ADAPTER_VERSION", "ADAPTER_MIGRATIONS"} <= changed:
        raise ValueError("repair must update the adapter version and category migration")
    def versions(text: str) -> dict[str, str]:
        node = next(n for n in ast.parse(text).body if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "ADAPTER_VERSION" for t in n.targets))
        return ast.literal_eval(node.value)
    old_versions, new_versions = versions(before), versions(after)
    if set(old_versions) != set(new_versions):
        raise ValueError("repair changed provider identities")
    for provider, version in new_versions.items():
        if not isinstance(version, str) or (provider in providers) != (old_versions[provider] != version):
            raise ValueError("repair version changes must match affected providers")
    for name in changed - {"ADAPTER_VERSION", "ADAPTER_MIGRATIONS", "tool_from"}:
        owner = "claude" if "claude" in str(name).lower() else "codex"
        if owner not in providers:
            raise ValueError("repair changed an unaffected provider's declarations")
    def migrations(text: str) -> dict[tuple[str, str, str], ast.AST]:
        node = next(n for n in ast.parse(text).body if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "ADAPTER_MIGRATIONS" for t in n.targets))
        if not isinstance(node.value, ast.Dict):
            raise ValueError("migration declaration must remain a dictionary")
        return {ast.literal_eval(k): v for k, v in zip(node.value.keys, node.value.values)}
    prior, proposed = migrations(before), migrations(after)
    if any(key not in proposed or ast.dump(value) != ast.dump(proposed[key]) for key, value in prior.items()):
        raise ValueError("repair changed an existing migration")
    expected = {(p, old_versions[p], new_versions[p]) for p in providers}
    if proposed.keys() - prior.keys() != expected:
        raise ValueError("repair must add one migration per affected provider")
    for key in expected:
        node = proposed[key]
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != "AdapterMigration" \
                or len(node.args) != 2 or ast.literal_eval(node.args[0]) != "category-scoped":
            raise ValueError("repair migration must be category-scoped")
        categories = node.args[1]
        if not isinstance(categories, ast.Call) or not isinstance(categories.func, ast.Name) \
                or categories.func.id != "frozenset" or len(categories.args) != 1 \
                or categories.keywords or not ast.literal_eval(categories.args[0]):
            raise ValueError("repair migration needs explicit categories")
        if len(node.keywords) != 1 or any(k.arg != "unsupported_only" or ast.literal_eval(k.value) is not True for k in node.keywords):
            raise ValueError("repair migration cannot broaden its selection")


def tree_files(root: Path) -> dict[str, str]:
    result = {}
    for path in sorted(root.rglob("*")):
        if "__pycache__" in path.parts or path.suffix == ".pyc" or any(part.endswith(".egg-info") for part in path.parts):
            continue
        if path.is_symlink():
            raise ValueError("repair candidate contains a symbolic link")
        if path.is_file():
            result[str(path.relative_to(root))] = file_hash(path)
    return result


def copy_candidate(repo: Path, candidate: Path) -> dict[str, str]:
    candidate.mkdir(mode=0o700)
    for name in ("src", "tests", "examples", "scripts"):
        source = repo / name
        if source.exists():
            for path in source.rglob("*"):
                if path.is_symlink():
                    raise ValueError("repair source tree contains a symbolic link")
            shutil.copytree(source, candidate / name,
                            ignore=shutil.ignore_patterns("__pycache__", "*.egg-info", "*.pyc"))
    for name in ("README.md", "pyproject.toml", "LICENSE", "MANIFEST.in"):
        shutil.copy2(repo / name, candidate / name)
    return tree_files(candidate)


def check_candidate(candidate: Path, baseline: dict[str, str], before: str,
                    providers: set[str]) -> None:
    current = tree_files(candidate)
    permitted = {"src/sessiondb.py", "tests/test_repair_regression.py", "agent-result.json"}
    changed = {name for name in current.keys() | baseline.keys() if current.get(name) != baseline.get(name)}
    if not changed <= permitted or not {"src/sessiondb.py", "tests/test_repair_regression.py"} <= changed:
        raise ValueError("candidate changed files outside the parser and new regression test")
    if "tests/test_repair_regression.py" in baseline:
        raise ValueError("reserved regression test already exists")
    check_parser_change(before, (candidate / "src/sessiondb.py").read_text(), providers)
    tests = ast.parse((candidate / "tests/test_repair_regression.py").read_text())
    if len([n for n in ast.walk(tests) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")]) < 2:
        raise ValueError("candidate needs affected and unaffected regression tests")
    if any((isinstance(n, ast.Attribute) and n.attr == "ADAPTER_VERSION")
           or (isinstance(n, ast.Name) and n.id == "ADAPTER_VERSION") for n in ast.walk(tests)):
        raise ValueError("regression tests must not reference ADAPTER_VERSION; test the migration entry directly")


def check_checkout(repo: Path, baseline: dict[str, str]) -> None:
    current = {}
    for name in ("src", "tests", "examples", "scripts"):
        if (repo / name).exists():
            current.update({f"{name}/{path}": digest for path, digest in tree_files(repo / name).items()})
    for name in ("README.md", "pyproject.toml", "LICENSE", "MANIFEST.in"):
        current[name] = file_hash(repo / name)
    expected = {name: digest for name, digest in baseline.items() if name != "repair-input.json"}
    if current != expected:
        raise ValueError("checkout inputs changed during repair; candidate preserved")


def run_command(argv: list[str], cwd: Path, log: Path, timeout: float,
                *, input_text: str | None = None, env: dict[str, str] | None = None) -> None:
    with log.open("wb") as output:
        process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.PIPE if input_text else subprocess.DEVNULL,
                                   stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            process.communicate(input_text.encode() if input_text else None, timeout=timeout)
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    if process.returncode:
        raise RuntimeError(f"{Path(argv[0]).name} exited {process.returncode}; see {log.name}")


def agent_command(config: dict[str, Any], candidate: Path, schema: Path) -> list[str]:
    provider = config["provider"]
    executable = config.get("executable") or shutil.which(provider)
    if not executable:
        raise ValueError(f"{provider} executable unavailable; set --repair-executable")
    if provider == "codex":
        command = [str(executable), "exec", "--ephemeral", "--skip-git-repo-check",
                   "--ignore-user-config",
                   "--sandbox", "workspace-write", "-c", 'approval_policy="never"',
                   "--output-schema", str(schema), "-o", str(candidate / "agent-result.json"), "-"]
    elif provider == "claude":
        command = [str(executable), "-p", "--output-format", "json", "--json-schema", json.dumps(RESULT_SCHEMA),
                   "--permission-mode", "acceptEdits", "--tools", "Read,Edit,Write,Glob,Grep",
                   "--allowedTools", "Read,Edit,Write,Glob,Grep", "--no-session-persistence",
                   "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}',
                   "--settings", '{"disableAllHooks":true}', "--setting-sources", ""]
    else:
        raise ValueError("unknown repair provider")
    if config.get("model"):
        command.extend(["--model", config["model"]])
    return command


def agent_result(provider: str, candidate: Path, log: Path) -> dict[str, Any]:
    if provider == "codex":
        result = read_json(candidate / "agent-result.json")
    else:
        envelope = read_json(log)
        result = envelope.get("structured_output")
        if not isinstance(result, dict):
            result = json.loads(envelope.get("result", ""))
        if envelope.get("is_error"):
            raise ValueError("Claude reported an agent error")
    if not isinstance(result, dict) or result.get("outcome") not in ("repaired", "no_safe_repair", "blocked"):
        raise ValueError("agent returned no valid repair outcome")
    return result


def test_environment(candidate: Path, home: Path) -> dict[str, str]:
    home.mkdir(mode=0o700, exist_ok=True)
    temporary = home / "tmp"
    temporary.mkdir(mode=0o700, exist_ok=True)
    # No provider credentials or inherited test command path enter generated tests.
    env = {key: value for key, value in os.environ.items() if key in ("PATH", "SYSTEMROOT", "LANG", "LC_ALL", "TMPDIR")}
    env.update(HOME=str(home), TMPDIR=str(temporary), PYTHONPATH=str(candidate / "src"), PYTHONDONTWRITEBYTECODE="1",
               SESSIONDB_REPAIR_VERIFY="1")
    return env


def verification_command(argv: list[str], job: Path) -> list[str]:
    if sys.platform != "darwin" or not Path("/usr/bin/sandbox-exec").is_file():
        raise ValueError("automatic repair verification requires the macOS sandbox")
    protected = str(Path.home().resolve())
    allowed = {str(job.resolve()), str(Path(sys.prefix).resolve()), str(Path(sys.base_prefix).resolve())}
    exceptions = " ".join(f"(require-not (subpath {json.dumps(path)}))" for path in sorted(allowed))
    profile = (f"(version 1)(allow default)(deny network*)"
               f"(deny file-write* (require-all (require-not (subpath {json.dumps(str(job.resolve()))}))"
               f" (require-not (literal \"/dev/null\"))))"
               f"(deny file-read* (require-all (subpath {json.dumps(protected)}) {exceptions}))")
    return ["/usr/bin/sandbox-exec", "-p", profile, *argv]


def validate_candidate(candidate: Path, job: Path, data: dict[str, Any], attempt: int = 1) -> None:
    validation = job / f"validation-{attempt}"
    validation.mkdir(mode=0o700)
    env = test_environment(candidate, validation / "test-home")
    run_command(verification_command([sys.executable, "-m", "unittest", "discover", "-s", "tests"], job), candidate,
                validation / "tests.log", TEST_TIMEOUT, env=env)
    run_command(verification_command([sys.executable, "-m", "repair_agent", "replay", "--input", str(job / "input.json"),
                 "--output", str(validation / "replay")], job), candidate, validation / "replay.log", TEST_TIMEOUT, env=env)


def replay(input_path: Path, output: Path) -> None:
    data = read_json(input_path)
    output.mkdir(mode=0o700)
    db = sessiondb.connect(output / "fixture.sqlite")
    try:
        for index, record in enumerate(data["records"]):
            provider = record["provider"]
            source = output / f"{provider}-{index}.jsonl"
            source.write_text(json.dumps(record["structure"]) + "\n")
            result = sessiondb.ingest(db, provider, source)
            if result["issues"]:
                raise ValueError("candidate still reports an unsupported evidence record")
            event = db.execute(
                f"SELECT {sessiondb.EVENT_COLUMNS},src.path AS source_path FROM {sessiondb.EVENT_JOIN} "
                "WHERE src.path=?", (str(source.resolve()),)).fetchone()
            if event is None or sessiondb.read_event_structure(db, event) != record["structure"]:
                raise ValueError("candidate changed or lost evidence structure")
        sessiondb.build_search_projections(db)
    finally:
        db.close()


def activate(repo: Path, job: Path, before: str, after: str, spec: dict[str, Any]) -> None:
    source = repo / "src/sessiondb.py"
    if file_hash(source) != hashlib.sha256(before.encode()).hexdigest():
        raise ValueError("source changed while repair was running; candidate preserved")
    backup = job / "original-sessiondb.py"
    backup.write_text(before)
    with backup.open("rb") as handle:
        os.fsync(handle.fileno())
    sessiondb.fsync_directory(job)
    regression = job / "candidate/tests/test_repair_regression.py"
    regression_hash = file_hash(regression)
    test = repo / "tests" / f"test_repair_{regression_hash[:16]}.py"
    if test.exists() and file_hash(test) != regression_hash:
        raise ValueError("regression destination already contains different work")
    journal = {"phase": "prepared", "before": file_hash(backup), "after": hashlib.sha256(after.encode()).hexdigest(),
               "backup": str(backup), "job": str(job), "state_dir": spec["state_dir"], "issues": spec["issues"],
               "test": str(test), "test_hash": regression_hash, "test_created": not test.exists()}
    path = repo / ".sessiondb-repair/activation.json"
    write_json(path, journal)
    if journal["test_created"]:
        with test.open("xb") as handle:
            handle.write(regression.read_bytes())
            handle.flush()
            os.fsync(handle.fileno())
        sessiondb.fsync_directory(test.parent)
    atomic_source(source, after.encode())
    journal["phase"] = "activated"
    write_json(path, journal)


def rollback(repo: Path, journal: dict[str, Any]) -> bool:
    source, backup = repo / "src/sessiondb.py", Path(journal["backup"])
    current = file_hash(source)
    if current not in (journal["before"], journal["after"]):
        raise ValueError("source changed after activation; automatic rollback would overwrite newer work")
    if file_hash(backup) != journal["before"]:
        raise ValueError("rollback copy failed verification")
    if current == journal["after"]:
        atomic_source(source, backup.read_bytes())
    test = Path(journal["test"])
    if journal["test_created"] and test.exists() and file_hash(test) == journal["test_hash"]:
        test.unlink()
        sessiondb.fsync_directory(test.parent)
    journal["phase"] = "rolled_back"
    write_json(repo / ".sessiondb-repair/activation.json", journal)
    return current == journal["after"]


def recover(repo: Path) -> bool:
    path = repo / ".sessiondb-repair/activation.json"
    if not path.exists():
        return False
    try:
        with locked(repo / ".sessiondb-repair/lock"):
            journal = read_json(path)
            if journal["phase"] == "prepared":
                return rollback(repo, journal)
    except BlockingIOError:
        pass
    return False


def verify_activation(repo: Path, state_dir: Path, result: dict[str, Any]) -> str | None:
    path = repo / ".sessiondb-repair/activation.json"
    if not path.exists():
        return None
    with locked(repo / ".sessiondb-repair/lock"):
        journal = read_json(path)
        if journal["phase"] != "activated" or Path(journal["state_dir"]) != state_dir.resolve():
            return None
        if file_hash(repo / "src/sessiondb.py") != journal["after"]:
            raise ValueError("source changed after activation; verification stopped")
        if LOADED_CODE_HASH != journal["after"]:
            return "restart_required"
        failed = bool(set(journal["issues"]) & set(issue_keys(result))) or bool(
            result.get("error") or result.get("hard_failures")
            or any(m.get("state") == "failed" for m in result.get("migrations", [])))
        if failed:
            rollback(repo, journal)
            return "rolled_back"
        journal["phase"] = "verified"
        write_json(path, journal)
        return "verified"


def worker(job: Path) -> dict[str, Any]:
    spec = read_json(job / "job.json")
    config, repo = spec["config"], Path(spec["config"]["repo"])
    result: dict[str, Any] = {"state": "blocked"}
    try:
        with locked(repo / ".sessiondb-repair/lock"):
            journal_path = repo / ".sessiondb-repair/activation.json"
            if journal_path.exists() and read_json(journal_path)["phase"] in ("prepared", "activated"):
                raise ValueError("another activation still needs recovery or verification")
            candidate = job / "candidate"
            baseline = copy_candidate(repo, candidate)
            before = (candidate / "src/sessiondb.py").read_text()
            data = read_json(job / "input.json")
            nodes = ast.parse(before).body
            lines = before.splitlines()
            data["source_context"] = "\n\n".join(
                "\n".join(lines[node.lineno - 1:node.end_lineno]) for node in nodes
                if (getattr(node, "name", None) in ALLOWED_DECLARATIONS | {"Parsed", "AdapterMigration"})
                or (isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                    and target.id in ALLOWED_DECLARATIONS for target in node.targets)))
            (candidate / "repair-input.json").write_text(json.dumps(data, indent=2))
            baseline = tree_files(candidate)
            write_json(job / "schema.json", RESULT_SCHEMA)
            prompt = (repo / "repair-agent-prompt.md").read_text() + "\n\nrepair-input.json:\n" + json.dumps(data)
            remaining = float(config.get("timeout", AGENT_TIMEOUT))
            feedback = ""
            for attempt in (1, 2):
                log = job / f"agent-{attempt}.log"
                (candidate / "agent-result.json").unlink(missing_ok=True)
                started = time.monotonic()
                run_command(agent_command(config, candidate, job / "schema.json"), candidate, log,
                            max(0.1, remaining), input_text=prompt + feedback)
                remaining -= time.monotonic() - started
                reported = agent_result(config["provider"], candidate, log)
                if reported["outcome"] != "repaired":
                    result = {"state": reported["outcome"], "summary": reported.get("summary", "")}
                    break
                try:
                    check_candidate(candidate, baseline, before, {r["provider"] for r in data["records"]})
                    after = (candidate / "src/sessiondb.py").read_text()
                    validate_candidate(candidate, job, data, attempt)
                    # Tests are executable code too; reject a candidate changed by its own tests.
                    check_candidate(candidate, baseline, before, {r["provider"] for r in data["records"]})
                    if (candidate / "src/sessiondb.py").read_text() != after:
                        raise ValueError("tests changed the parser under verification")
                except (ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
                    if attempt == 2 or remaining <= 0:
                        raise
                    diagnostics = str(exc)
                    for name in ("tests.log", "replay.log"):
                        path = job / f"validation-{attempt}" / name
                        if path.exists():
                            diagnostics += "\n" + path.read_text(errors="replace")[-6000:]
                    feedback = "\n\nLauncher verification rejected the current candidate:\n" + diagnostics
                    continue
                check_checkout(repo, baseline)
                activate(repo, job, before, after, spec)
                result = {"state": "activated", "rounds": attempt, "summary": reported.get("summary", "")}
                break
    except Exception as exc:
        result = {"state": "blocked", "error": f"{type(exc).__name__}: {exc}"}
    write_json(job / "result.json", result)
    return result


def process_running(pid: int) -> bool:
    try:
        if os.waitpid(pid, os.WNOHANG)[0]:
            return False
    except ChildProcessError:
        pass
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def advance(result: dict[str, Any], state_dir: Path, config: dict[str, Any] | None,
            *, now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    path = state_dir / "repair-state.json"
    repo = Path(sessiondb.__file__).resolve().parents[1]
    try:
        if recover(repo):
            return {"state": "rolled_back", "error": "interrupted activation recovered; restart required"}
        verification = verify_activation(repo, state_dir, result)
        with locked(state_dir / "repair-state.lock"):
            state = read_json(path) if path.exists() else {"attempts": []}
            if state.get("job"):
                job = Path(state["job"])
                if verification in ("verified", "rolled_back"):
                    state["state"] = verification
                    state.pop("job")
                elif (job / "result.json").exists():
                    outcome = read_json(job / "result.json")
                    state.update(outcome)
                    if outcome["state"] == "activated":
                        state["state"] = "restart_required"
                    else:
                        state.pop("job")
                elif not process_running(state["pid"]):
                    state.update(state="blocked", error="repair process ended without a result")
                    state.pop("job")
                else:
                    state["state"] = "running"
                write_json(path, state)
                return {k: v for k, v in state.items() if k != "attempts"}
            if config is None:
                return {"state": verification or "disabled"}
            if not eligible(result):
                state.pop("pending", None)
                write_json(path, state)
                return {"state": "not_eligible" if result.get("issues") else "idle"}
            identity = hashlib.sha256(json.dumps([issue_keys(result), LOADED_CODE_HASH]).encode()).hexdigest()
            if identity in state["attempts"]:
                return {"state": state.get("state", "attempted"), "error": state.get("error")}
            if (state.get("pending") or {}).get("id") != identity:
                state["pending"] = {"id": identity, "since": now}
            if now - state["pending"]["since"] < CONFIRM_SECONDS:
                write_json(path, state)
                return {"state": "confirming", "due": state["pending"]["since"] + CONFIRM_SECONDS}
            state["attempts"].append(identity)
            state["state"] = "blocked"
            write_json(path, state)
            try:
                bundle = validate_repair_evidence(state_dir / "watcher_status.json", Path(result["repair_evidence"]["path"]))
                data = repair_input(bundle)
                job = state_dir / "repair-jobs" / identity
                job.mkdir(parents=True, mode=0o700)
                write_json(job / "input.json", data)
                write_json(job / "job.json", {"config": config, "state_dir": str(state_dir.resolve()),
                                              "issues": issue_keys(result)})
                with (job / "launcher.log").open("wb") as log:
                    process = subprocess.Popen([sys.executable, "-m", "repair_agent", "worker", "--job", str(job)],
                                               stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                               start_new_session=True)
            except Exception as exc:
                state.update(state="blocked", error=f"{type(exc).__name__}: {exc}")
                write_json(path, state)
                return {"state": "blocked", "error": state["error"]}
            state.update(state="running", job=str(job), pid=process.pid, started_at=now)
            state.pop("pending", None)
            write_json(path, state)
            return {"state": "running", "job": str(job)}
    except BlockingIOError:
        return {"state": "busy"}
    except Exception as exc:
        return {"state": "blocked", "error": f"{type(exc).__name__}: {exc}"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("worker")
    run.add_argument("--job", type=Path, required=True)
    replay_parser = commands.add_parser("replay")
    replay_parser.add_argument("--input", type=Path, required=True)
    replay_parser.add_argument("--output", type=Path, required=True)
    retry = commands.add_parser("retry", help="allow another attempt after a terminal failure")
    retry.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "worker":
        def interrupted(_signum: int, _frame: Any) -> None:
            raise InterruptedError("repair interrupted")
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        result = worker(args.job)
        print(json.dumps(result))
        return 0 if result["state"] == "activated" else 1
    if args.command == "replay":
        replay(args.input, args.output)
        return 0
    with locked(args.state_dir / "repair-state.lock"):
        path = args.state_dir / "repair-state.json"
        state = read_json(path)
        if state.get("job"):
            parser.error("repair is active; wait for its result")
        write_json(path, {"attempts": []})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
