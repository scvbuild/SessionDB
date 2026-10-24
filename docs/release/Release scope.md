# Release scope: v1.1.0

This release updates local storage and retrieval, provider record handling, token-usage derivation, and incremental watcher behavior. The package includes eight modules: sessiondb, full_corpus, snapshot_export, freshness_service, cloud_fetch, token_usage, rebuild_copy, and repair_agent.

## Component and failure boundaries

| Component | Behavior and scope |
| --- | --- |
| Local engine | Schema 12, searchable projections, reconstructable records, redacted output and scope-specific coverage |
| Foreground watcher | Incremental synchronization, batched changes, WAL connection maintenance, native macOS events and restart after source changes |
| Optional repair | Explicit provider selection; bounded evidence and candidate edits; verification before activation; rollback after failed ingestion |
| Optional cloud import | Explicit command using Claude login; separate per-session failures; no schedule |
| Copy tools | Verified copy and explicit promotion; no service management or installer |
| Package data | Synthetic tests and examples; no source transcripts, databases, credentials or model logs |

Repair follows this lifecycle: disabled → confirming → running → restart required → verified. Unsafe candidates stop as blocked or no-safe-repair; unsuccessful post-activation ingestion rolls back. Coverage remains a separate statement about indexed evidence. Parser repair cannot resolve missing credentials, unreadable source files, general infrastructure faults, or arbitrary code defects.

The public data namespace is `~/.local/share/sessiondb-public`. No running installation, database, background job or agent instruction is changed merely by publishing or installing the package. External notifications, scheduled audits, benchmarks and a walkthrough application are excluded.

## Verification limits

Checks use synthetic records and isolated databases. Real Codex and Claude calls exercise the optional repair handoff; they do not establish that every future format change is safely repairable. Linux, Windows, live cloud endpoints, production-corpus performance and unknown future provider formats are unverified. Historical performance records are retained with their original dates.
