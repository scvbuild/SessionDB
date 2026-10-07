# SessionDB

Local search and verifiable retrieval across Claude and Codex coding sessions.

SessionDB indexes local session records in SQLite and retains exact content in compressed object packs. Search supports phrases, Boolean expressions, metadata filters, and English/Vietnamese text. Results include provenance and coverage information so callers can distinguish complete evidence from missing or unsupported records.

This is the first standalone release candidate, `0.1.0rc1`. macOS is the tested platform. Continuous watching uses native macOS filesystem events. Linux and Windows compatibility have not been validated.

## Install from this checkout

Python 3.9 with SQLite FTS5 support is the tested runtime; the package requires Python 3.9 or newer. Install into a dedicated virtual environment to keep it separate from other SessionDB installations:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install .
```

The distribution is named `local-sessiondb-public`; the commands are `sessiondb`, `sessiondb-admin`, `sessiondb-export`, and `sessiondb-freshness`. No package-index publication is claimed. Installation does not register background jobs. Use the command paths inside this virtual environment when another SessionDB is already installed.

## Try the synthetic examples

The example files contain invented records, not real conversations:

```sh
mkdir -p demo-data
.venv/bin/sessiondb --db demo-data/demo.sqlite ingest --provider claude examples/claude.jsonl
.venv/bin/sessiondb --db demo-data/demo.sqlite ingest --provider codex examples/codex.jsonl
.venv/bin/sessiondb --db demo-data/demo.sqlite build-search-index
.venv/bin/sessiondb --db demo-data/demo.sqlite search-index --text '"release checklist"'
.venv/bin/sessiondb --db demo-data/demo.sqlite query --provider codex --clean
```

For manual ingestion, a missing watcher status means coverage is limited to the retained/indexed sources; it does not prove every conversation on the computer was discovered. Inspect the returned coverage rather than treating an empty result as proof of absence.

## Keep your own sessions current

Use a new database, separate from any existing installation. The watcher discovers `~/.claude/projects`, `~/.codex/sessions`, and `~/.codex/archived_sessions`:

```sh
mkdir -p "$HOME/.local/share/sessiondb-public/data"
.venv/bin/sessiondb-freshness --db "$HOME/.local/share/sessiondb-public/data/sessiondb.sqlite" --once
.venv/bin/sessiondb-freshness --db "$HOME/.local/share/sessiondb-public/data/sessiondb.sqlite"
```

The second command runs in the foreground. Stop it with Control-C. Watcher status is written to `freshness/watcher_status.json` beside the database and printed locally. No scheduling, messaging service, dashboard, Harness dependency, or automatic repair agent is included. Unsupported content-bearing shapes remain visible and may make affected queries incomplete. Bounded passive metadata admission and integrity-checked adapter migrations are retained; these do not launch agents or edit source code.

Live records may be reported as catching up while they are still being written. Persistent drift, unreadable evidence, failed migrations, and unsupported records require investigation. The watcher reports these conditions without claiming a repair is pending. Query coverage remains scoped independently of watcher attention state.

## Privacy and storage

- The database and object packs retain exact session content, which can include credentials and private customer information. Redacted search output does not make the retained storage safe to share.
- Keep databases, object packs, result bundles, snapshots, and status/evidence files private. Redaction is best-effort; inspect exported data before sharing it.
- Normal ingestion does not move or delete source transcripts. Confirmed provider deletion markers can remove indexed records and associated derived copies. Retain backups when deletion recovery matters.
- Rebuild receipts use `~/.local/share/sessiondb-public/data/rebuild-receipts`, separate from the private installation. The package never needs your private credentials.

## Export and backup

Use `sessiondb-export --help` for a bounded, redacted portable snapshot. It synchronizes discovered session roots first, so use it only with a database and account scope you intend to index. Use `sessiondb-admin backup --help` for a database backup including retained packs. Saved snapshots and backups still require a privacy review.

## Large rebuilds

Rebuilds containing at least 100,000 events require a current, one-use readiness receipt. The receipt binds the code revision, reviewed plan, corpus identity, tests, sample queries, tokenizer, and available capacity. That safeguard is retained in this release.

The generator is `scripts/rebuild_readiness.py` and runs from a clean, committed Git checkout containing the tests, not an installed wheel alone. Run its `--help` for inputs. The independent review JSON must contain `reviewer`, `outcome: approved`, `material_changes_reviewed: true`, `reviewed_revision`, and `plan_sha256`, matching that exact revision and plan. After the generator succeeds, pass its output with `build-search-index --readiness-receipt`. Do not fabricate a review receipt to bypass review.

## Development checks

```sh
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests
```

The tests use synthetic fixtures. For checking coexistence with another installation, run them in a separate test account or with an isolated process home and database. The retained test methods and omitted private-service tests are listed in `release/extraction-manifest.json`.

## First-release boundaries

Included: local ingestion, retained storage, search, evidence retrieval, export/backup, and a foreground macOS watcher.

Excluded: automatic agent repair, Telegram alerts, private dashboards, scheduled nightly audits, agent benchmarks, and the walkthrough app. The standalone candidate and private installation are maintained separately for now. Performance on a production corpus must be measured before replacing an existing installation.

MIT licensed. See `LICENSE`.
