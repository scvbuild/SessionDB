# Release candidate verification

Date: 2026-10-07. Candidate: `0.1.0rc1`.

## Confirmed

- A new Python 3.9 virtual environment installed the package and its declared dependencies. `pip check` reported no broken requirements.
- The suite passed all 229 tests in 17.386 seconds. This includes 223 retained synthetic test methods and six new release-boundary tests.
- Installed commands ingested Claude and Codex examples, built an index, searched, retrieved details, queried records, exported a portable snapshot, and created a backup.
- A native macOS filesystem watcher observed appended records and stopped cleanly across two starts, using an isolated process home and synthetic database. This check required running outside the execution sandbox because macOS refused to start FSEvents streams inside it.
- An unsupported secret-shaped event remained visible and actionable without spawning a process. Query coverage for the unaffected Codex provider remained complete. Removed source evidence was reported as unreadable rather than falsely remaining readable.
- Every retained test method matches its extraction source exactly. Two core modules match byte-for-byte; the search/storage module differs only in the rebuild-receipt directory namespace.
- The wheel contains only `sessiondb`, `full_corpus`, `snapshot_export`, and `freshness_service`. Targeted checks found no private user paths, local workspace lookup, private credentials path, dashboard reference, or automatic-repair launcher in the wheel.
- The large-rebuild gate remains at 100,000 events. The included readiness-generator command loads and exposes its inputs. Existing synthetic tests exercise gate rejection, receipt validation, frozen-input checks, consumption, and replay rejection. No real large-corpus rebuild or independent review receipt was created for this release.

## Performance evidence

The two copies returned identical ordered result IDs for three searches over the same 2,000 invented Claude records in six alternating runs. Each query had two warmups and ten measured repetitions per run.

| Measurement | Private median | Public median | Difference |
| --- | ---: | ---: | ---: |
| Phrase: release checklist | 112.605 ms | 118.084 ms | +4.87% |
| Boolean: backup AND verified | 34.529 ms | 33.728 ms | -2.32% |
| Vietnamese phrase: ghi chú | 135.270 ms | 138.603 ms | +2.46% |
| Ingestion | 0.4985 s | 0.5511 s | approximately +11% |
| Index build | 0.5638 s | 0.5355 s | approximately -5% |

These timings are limited evidence from one small synthetic corpus on one machine. Search code is unchanged, but production-corpus performance parity is unverified. Raw run medians and execution order are in `synthetic-performance.json`. The private installation remains the working version.

## Advisor review

A read-only Claude Opus consultation ran three plan-review rounds. Its material findings were private watcher/notification coupling, the missing rebuild generator, an agent-launching benchmark module, and a repository-layout-dependent audit module. The release excludes those services, includes the generator, and separates the receipt ledger. The final test-preservation objection is addressed by preserving methods rather than entire files and recording every omitted import/method.

## Remaining limits

No production data or original repository history was copied. Privacy checks were targeted source/package checks, not a guarantee that best-effort redaction catches all sensitive content. Linux, Windows, production-corpus latency, public adoption, GitHub publication, and OpenAI program acceptance remain unverified.

## Folder reorganization verification

The release records now live under `docs/release/`. Package configuration is consolidated in `pyproject.toml`; `MANIFEST.in` includes the new documentation location. The public root contains four visible files.

The reorganized candidate built both a source archive and a wheel using Python 3.9 and the minimum declared setuptools version, 61.0.0. The before/after wheel lists contain the same ten files; module and license bytes, parsed command entry points, dependency versions, Python requirement, and license metadata are preserved. The source archive keeps the release records under their new paths. Setuptools creates a minimal `setup.cfg` inside the source archive; the checkout does not require that file.

All 229 synthetic tests passed in 17.637 seconds, including the installed-command flows and native watcher checks. `pip check` found no broken requirements. Retrieval and storage source files and retained tests were not changed by this reorganization.

## Carry-across from the private build (8 October 2026)

The private source at revision `69ca922` was carried across: `sessiondb.py` (with the public receipt namespace), `full_corpus.py`, and `snapshot_export.py`, plus the watcher's handling of Codex's title index in the local-only watcher. The extraction manifest records the new source hashes and retained tests. Private test changes were applied to the retained tests; two changes touched only tests this release omits. Two private test files (`test_agent_reading.py`, `test_search_noise_and_titles.py`) were added; they use only the retained engine.

All 270 synthetic tests passed against a fresh install of this repository, including the installed-command flows and native watcher checks. `pip check` found no broken requirements.

## Second carry-across (8 October 2026)

Private revision `28b1e96` adds the `grep` and `sessions` commands, `record-stream --output -`, and one-line follow-up hints. Carried across with the public receipt namespace kept; the extraction manifest records the new hashes and retained tests. All 276 synthetic tests passed against a fresh install (one test, which checks the private agent guide, is skipped here).
