# First public release scope

The first public release is a standalone copy. It does not replace or modify the private installation. Publication and program applications are separate from installation.

## Source boundary

The retained source modules are `sessiondb`, `full_corpus`, `snapshot_export`, and `freshness_service`. `full_corpus` and `snapshot_export` are copied unchanged. `sessiondb` changes only the rebuild-receipt storage namespace. The watcher retains the original ingestion, receipt verification, drift classification, migration reporting, bounded database-lock retry, and FSEvents recovery helpers; status reporting and the watch loop are adapted to local-only operation.

No source transcripts, databases, original Git history, credentials, evaluation corpora, evaluation result files, or historical reports are included. The only evaluation utility retained is the standalone rebuild-readiness generator.

## Impact map

| Component | Disposition |
| --- | --- |
| Private installation, database, schedules and repair launcher | Unaffected; no deployment or writes |
| Public retrieval/storage engine | Retained, with a separate receipt-ledger namespace |
| Public foreground watcher | Local status and errors; no repair agent or external notification |
| Public exports/admin commands | Retained; explicit database and output paths |
| Rebuild-readiness utility | Included with the frozen-code, corpus and review gates retained |
| Nightly service, retrieval audit, agent benchmark and walkthrough | Excluded from this release |
| Existing synthetic tests | Retained methods unchanged; removed imports/methods recorded |
| New release-boundary tests | Exercise local status, process isolation and installed commands |

## Verification boundary

Verification uses isolated homes and synthetic databases. Verify installed commands for both providers, unknown-shape reporting without agent launch, retry behavior, restart and native watcher events, query-scoped coverage, export, receipt namespace isolation, and equality of retained test methods. Compare retrieval and ingestion on identical synthetic records against the private engine. Production-corpus latency, broad platform compatibility, and public adoption are unverified.

The private source is the extraction baseline; after this release the two codebases are temporarily independent. Fixes must be consciously carried across until one source is selected. Work amplification is not applicable to a production rollout because no installed production unit is changed.
