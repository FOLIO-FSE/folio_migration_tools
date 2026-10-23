# Logging

When you run a migration task, the tools set up logging automatically. You get a Rich-formatted console stream plus log files under the iteration’s `reports/` folder. Third-party libraries (including `folio_data_import`) flow into the same handlers.

## What you see during a run
- Console: Rich output at INFO by default (DEBUG if enabled). Progress output and logs interleave cleanly.
- Files (per task run):
  - `reports/log_<object>_<timestamp>.log` — full run log at INFO/DEBUG.
  - `reports/data_issues_log_<timestamp>.tsv` — only data issues (custom level 26).
  - `reports/.raw/migration_<timestamp>_<task>.sqlite` — the report statistics and data issues in a database. See [Report Database](migration_reports.md#report-database).
- Noise control: `httpx` and `pymarc` are reduced to WARNING so they do not flood the console.

## Turning on DEBUG
- In your configuration (libraryInformation), set `logLevelDebug: true` to enable DEBUG for the run. This raises verbosity for both console and log files.
- Leave it `false` for the default INFO view.

## Where logs land
Logs are written inside the iteration you specify in `libraryInformation.iterationIdentifier`, under `base_folder/iterations/<iteration>/reports/`:
- `log_<object>_<timestamp>.log` — main log
- `data_issues_log_<timestamp>.tsv` — data issues only

## Third-party logs
Handlers are attached at the root logger, so module-level loggers inside dependencies (e.g., `folio_data_import`) are captured automatically. Suppression for chatty libraries remains in place; adjust levels manually if you need more detail.

## Data issue entries for array items
- Missing required fields in object-array items are logged as data issues (level 26) when the item has legacy-sourced content.
- The log format is `FIELD MAPPING FAILED\t<record_id>\t<message>\t<data>`.
- Items that only contain static mapped values and miss required fields are discarded without a field mapping log entry.

## Tips for noisy environments
- If you enable DEBUG and see too much from HTTP clients, you can lower them via env or a small shim before invoking the CLI (e.g., set `LOGGING_HTTPX_LEVEL=WARNING` in your wrapper). By default, they are already set to WARNING.

## Data issues in the report database
Data issues are also written to the task's report database, with the message stored as a translation key and parameters. The TSV above stays in the language the task ran in. To get it in another language, run `folio-migration-tools report <configuration_path> <task_name> --locale <locale> --issues-tsv <file>`. See [Report Database](migration_reports.md#report-database).

In both outputs the record id is normalized: a record with one legacy id shows that id, not a list such as `['b1234']`, and a record with several shows the first.
