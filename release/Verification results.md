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
