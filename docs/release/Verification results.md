# Verification: v1.1.0 candidate

Date: 24 October 2026 (ICT). Tested on macOS with Python 3.9.

## Completed checks

- All 378 synthetic tests passed in 33.105 seconds, including installed commands, native filesystem events, source-change watcher restart and macOS verification sandbox restrictions.
- After correcting cloud-project detection for the public data directory and adding stale-candidate rejection coverage, all 40 affected cloud and repair tests passed. The new test brings the checked-in suite to 379 tests; the full suite was not repeated after these focused changes.
- The editable package installed successfully; dependency checking reported no broken requirements.
- A wheel built successfully. Its eight Python modules and metadata contain no test fixtures, databases, logs, or detected author-specific filesystem paths. Installing that wheel into a separate target loaded all six command entry points, indexed both example providers and returned two search passages.
- The source archive includes the reviewed prompt, tests and nested synthetic fixtures. Its 68 members include no Git history, databases, logs or detected author-specific filesystem paths.
- A schema-10 database created with v1.0.1 migrated into a schema-12 copy. Original database files remained byte-identical, and the migrated store returned the expected search results.
- Real Codex and Claude invocations produced repairs for a deliberately restored historical parser gap using invented evidence. Codex completed activation and fresh ingestion. The unchanged Claude candidate passed full verification on replay after a launcher sandbox defect was corrected, then completed activation and fresh ingestion.
- Missing evidence caused a real Claude invocation to return no-safe-repair without changing the candidate. An embedded instruction to delete an existing test remained data; that test was preserved.
- Failure tests cover malformed evidence, invalid results, failed and timed-out commands, concurrent locks, interrupted activation, rollback, source edits during repair and denied network or external-file access during verification.

## Repair prompt review

The required review passed on the final prompt. Wording changes split dense preservation and testing instructions into shorter bullets without changing authority or safeguards. An apparent output-key mismatch was resolved using the original agent response: the agent returned `outcome`; the launcher saved it as `state`. A clean final review confirmed the artifact-level necessity and conflict checks. No rejected findings or provisional forward checks remain.

## Limits

Cloud endpoints were tested with stand-ins, not a live account. Linux, Windows, production-corpus performance and future provider format changes are unverified. Model-generated code remains a risk despite edit restrictions, isolated verification and rollback; automatic repair is opt-in. Redaction is best-effort.

Historical performance files are retained unchanged and do not benchmark this version. No live transcript store or running installation was modified by these release checks.
