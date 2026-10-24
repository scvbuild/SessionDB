# Operations for v1.1.0

## Upgrade from v1.0.x

Stop the old watcher and keep a backup. Install v1.1.0 into the environment you intend to use. The v1.0.x database uses schema 10; v1.1.0 uses schema 12. Create a new database with the installed `sessiondb-admin migrate-storage --db <old database> --target-db <new database>` command. Wait for its successful verification, then use the new path for the watcher and all searches. Keep the original until you have verified your own searches. The migration writes a copy; it does not update the original in place.

`rebuild-into-copy` supports schema 11 and 12. It is not the schema-10 upgrade command. `promote-copy` uses a rebuild report; do not use it on `migrate-storage` output. The public tools do not stop your watcher or install code for you.

## Enable automatic parser repair

Repair is disabled by default. It needs macOS, a retained source checkout installed in editable mode, and a signed-in Codex or Claude CLI. From the checkout, upgrade pip and setuptools in the chosen virtual environment, then install with `python -m pip install -e .` using that environment's Python. A regular wheel installation can index and search but cannot automatically update its parser.

Start `sessiondb-freshness` with your database path, `--auto-repair codex` or `--auto-repair claude`, and `--repair-repo` pointing to the editable checkout. Use `--repair-executable` if the CLI is not on PATH. `--repair-model` is optional; otherwise the CLI selects its default model. Repair requires continuous watching and cannot be combined with `--once`.

Enabling repair authorizes model calls and automatic activation of verified parser edits. Redacted evidence is transmitted to the chosen provider; redaction is best-effort. Provider usage may be billed. The agent receives bounded evidence, not permission to read your live database or transcripts.

The watcher confirms a persistent, isolated adapter issue for five minutes, then starts one job per issue and parser version. The job permits at most two model rounds, sharing a fifteen-minute model-call limit. Candidate tests and evidence replay have separate five-minute limits. Unrelated ingestion continues while a job runs.

The agent edits a disposable copy. Only listed parser declarations and a new regression test may change; existing tests and the database schema are protected. The launcher checks the change, runs the synthetic suite and evidence replay in a restricted macOS environment, then atomically replaces the parser and saves its regression test. The watcher restarts and checks normal ingestion before marking the repair verified. A failed post-activation check restores the prior parser; newer source edits are not overwritten.

Activation changes the retained checkout and therefore every process using that editable installation. It does not commit or push Git changes. Review local repairs before updating the checkout or publishing them.

## Diagnose or retry a repair

`freshness/watcher_status.json` beside the database reports coverage separately from repair state. Jobs and logs are under `freshness/repair-jobs`; activation backups and the recovery journal are in `.sessiondb-repair` in the checkout. Treat all of these as private.

A blocked or unsuccessful repair leaves the coverage gap visible. Fix the reported prerequisite, then run `sessiondb-repair retry --state-dir <database directory>/freshness` to allow another attempt on the next watcher recheck. The command refuses while a repair is active. Disabling repair does not conceal unsupported records.

## Optional Claude cloud import

`sessiondb-cloud-sync --dry-run` lists finished cloud sessions eligible for download. It still contacts Claude using credentials from the local Claude login. Without `--dry-run`, the command writes cloud transcripts under the public SessionDB data directory for the watcher to ingest. No recurring download is installed.

Cloud import uses provider endpoints that can change; this release tests them with synthetic responses, not a live account. Individual download failures are reported without discarding successful imports. Local transcript indexing does not require cloud import.

## Verification utilities

The normal synthetic suite does not call a model. `scripts/verify_agent_repair.py` is a separate, explicit real-agent check: it creates a disposable historical parser regression, asks the selected provider to fix it, and verifies activation using invented records. It requires a signed-in CLI and may incur provider charges.
