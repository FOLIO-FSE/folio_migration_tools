# Migration Reports

Migration reports are a key output of the FOLIO Migration Tools, providing detailed statistics and diagnostics for each migration task. These reports help you understand what happened during a transformation or data load, identify data quality issues, and track progress across migration iterations.

## Overview

### Purpose

Every migration task generates a migration report upon completion. These reports serve several purposes:

- **Transparency**: See exactly what the tools did with your data
- **Data Quality**: Identify records that failed validation or mapping
- **Debugging**: Understand why certain records may not have migrated as expected
- **Documentation**: Create an audit trail of each migration run
- **Iteration Planning**: Use statistics to prioritize data cleaning efforts

### Report Formats

Each task generates three report outputs:

| Format | File Location | Purpose |
|--------|---------------|---------|
| **Markdown** | `reports/report_<timestamp>_<task_name>.md` | Human-readable report for review and documentation |
| **JSON** | `reports/.raw/raw_report_<timestamp>_<task_name>.json` | Machine-readable format for automated processing and analysis |
| **SQLite database** | `reports/.raw/migration_<timestamp>_<task_name>.sqlite` | The statistics and data issues in a queryable form. The markdown report and the issues log can be regenerated from it, in any language. See [Report Database](#report-database) |

```{note}
The raw JSON reports were introduced to enable downstream processing such as aggregating statistics across multiple runs, building dashboards, or integrating with data quality monitoring tools. SQLite databases were added later to support external query-based analysis and reporting tools.
```

### When Reports Are Generated

Reports are generated at the end of each task's execution, during the "wrap-up" phase. This means:

- For **transformation tasks**: The report reflects all records processed and any mapping issues encountered
- For **posting/loading tasks**: The report reflects API responses, successes, and failures
- For **migration tasks** (loans, requests, reserves): The report reflects transaction processing results

## Report Structure

### Markdown Report Sections

A typical markdown migration report contains the following sections:

#### 1. Header and Introduction
The report begins with a title and brief introduction explaining what the report contains.

#### 2. Timings
A timing breakdown showing:

| Measure | Description |
|---------|-------------|
| Time Started | UTC timestamp when the task began |
| Time Finished | UTC timestamp when the task completed |
| Elapsed time | Total duration of the task |

#### 3. General Statistics
High-level counts such as:
- Total records processed
- Records successfully transformed/posted
- Records that failed
- Files processed

#### 4. Task-Specific Sections
Each task type adds its own statistical sections. These appear as expandable `<details>` blocks containing tables of measures and counts. Examples include:

- **Mapping statistics**: Which fields were mapped and how often
- **Validation results**: Records failing validation rules
- **Reference data mapping**: How legacy values mapped to FOLIO reference data
- **Error breakdowns**: Categorized error types and counts

### JSON Report Structure

The raw JSON report contains the same data in a structured format suitable for programmatic access:

```json
{
  "GeneralStatistics": {
    "blurb_id": "GeneralStatistics",
    "Records processed": 10000,
    "Records successfully transformed": 9850,
    "Records with errors": 150
  },
  "MappedLocations": {
    "blurb_id": "MappedLocations",
    "Main Library": 5000,
    "Branch Library": 3000,
    "Special Collections": 1850
  }
}
```

Each top-level key represents a report section. The `blurb_id` field links to the translated section title, while all other keys are measure names with their counts.

## File Locations

Migration reports are stored in the iteration's `reports` folder:

```
iterations/
└── <iteration_identifier>/
    ├── reports/
    │   ├── report_<timestamp>_<task_name>.md      # Markdown report
    │   ├── data_issues_log_<timestamp>_<task_name>.tsv  # Detailed issues
    │   ├── log_<object_type>_<timestamp>_<task_name>.log  # Execution log
    │   └── .raw/
    │       ├── raw_report_<timestamp>_<task_name>.json  # JSON report
    │       └── migration_<timestamp>_<task_name>.sqlite  # Report database
    ├── results/
    └── source_data/
```

```{tip}
The `.raw` folder is created automatically and contains raw JSON reports. This folder can be used as input for reporting scripts or data quality dashboards.
```

## Task-Specific Reports

### Transformation Tasks

Transformation tasks convert legacy data into FOLIO format. Their reports focus on mapping statistics and data validation.

#### BibsTransformer
Transforms MARC bibliographic records to FOLIO Instance records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts, files processed |
| RecordStatus | Breakdown by MARC Leader/05 status |
| RecourceTypeMapping | Instance type mappings from 336 field |
| InstanceFormat | Format mappings |
| MappedIdentifierTypes | Identifier type usage |
| MappedContributorTypes | Contributor type mappings |
| MappedSubjectTypes | Subject type usage |
| MappedSubjectSources | Subject source usage |
| AuthoritySources | Heading/term sources from $2 and $0 |
| PrecedingSuccedingTitles | Linked title relationships |

#### HoldingsMarcTransformer
Transforms MFHD (MARC Holdings) records to FOLIO Holdings records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts |
| HoldingsTypeMapping | Holdings type assignments |
| LocationMapping | Location code mappings |
| CallNumberTypeMapping | Call number type usage |
| AuthoritySources | Heading/term sources from $2 and $0 |

#### HoldingsCsvTransformer
Transforms CSV/TSV holdings data to FOLIO Holdings records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts, merging statistics |
| LocationMapping | Location mappings |
| HoldingsTypeMapping | Holdings type assignments |
| HoldingsNoteTypeMapping | Holdings note type mappings |

#### ItemsTransformer
Transforms legacy item data to FOLIO Item records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts |
| MaterialTypeMapping | Material type mappings |
| LoanTypeMapping | Loan type mappings |
| ItemStatusMapping | Status mappings |
| LocationMapping | Effective location mappings |
| ItemNoteTypeMapping | Item note type mappings |

#### UserTransformer
Transforms patron/user data to FOLIO User records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts |
| PatronGroupMapping | Patron group assignments |
| AddressTypeMapping | Address type mappings |
| DepartmentMapping | Department mappings |

#### OrganizationTransformer
Transforms vendor/organization data for acquisitions.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts |
| OrganizationTypeMapping | Organization type assignments |

#### OrdersTransformer
Transforms purchase order data.

For Orders-specific address mapping behavior and validation/error handling for `billTo`/`shipTo`, see [OrdersTransformer](tasks/orders_transformer#validation-and-error-reporting-for-billtoshipto).

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Order and order line counts |
| OrderTypeMapping | Order type assignments |
| AcquisitionMethodMapping | Acquisition method mappings |
| OrderAddressMapping | `billTo`/`shipTo` names resolved to address IDs |

#### ManualFeeFinesTransformer
Transforms fee/fine data.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts |
| FeeFineTypeMapping | Fee/fine type mappings |

### Loading Tasks

#### BatchPoster
Posts transformed records to FOLIO via batch APIs.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Records processed, posted, failed |
| Details | Error message breakdowns |

```{attention}
If `rerun_failed_records` is enabled, the BatchPoster report will show statistics for both the initial run and the retry run.
```

### Circulation Migration Tasks

These tasks migrate active circulation transactions.

#### LoansMigrator
Migrates open loans from the legacy system.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Loans processed, checked out, failed |
| DiscardedLoans | Reasons for discarded loans |
| ServicePointMapping | Service point mappings, if a service point map file is configured |

#### RequestsMigrator
Migrates open hold/recall requests.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Requests processed, created, failed |
| DiscardedRequests | Reasons for discarded requests |
| ServicePointMapping | Service point mappings, if a service point map file is configured |

#### ReservesMigrator
Migrates course reserve relationships.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Reserves processed, created, failed |
| DiscardedReserves | Reasons for discarded reserves |

#### CoursesMigrator
Migrates course and course listing data.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Courses and listings processed |

## Additional Output Files

### MARC Decoding Diagnostics

For MARC-based transformation tasks, report outputs include decoding and recovery signals.

Common **GeneralStatistics** counters include:

- `Records in file before parsing`
- `Records successfully decoded from MARC21`
- `Records with encoding errors - repaired`
- `Records with encoding errors - parsing failed`
- `Records with text fidelity warnings`

In `data_issues_log_<task_name>.tsv`, common decoding messages include:

- `MARC-8 decoding warning`
- `MARC parsing issue repaired with MARC-8 leader heuristic`
- `MARC parsing issue repaired by converting MARCMaker dagger to subfield delimiter`
- `MARC parsing issue repaired with Latin-1 leader heuristic`
- `MARC parsing issue could not be repaired with configured heuristics`
- `MARC text fidelity warning`

Use these together to determine whether decode issues were non-blocking warnings,
automatically repaired, or unrecoverable failures.

Notes:

- `MARC-8 decoding warning` entries are captured from parser stderr output and are logged
  as informational diagnostics.
- `MARC text fidelity warning` is emitted only after a successful non-UTF8 recovery
  strategy (currently MARC-8 leader or Latin-1 heuristic).
- `Ã` and `Â` are only treated as mojibake when they are not followed by an ASCII letter, so
  genuine text such as "Moyen Âge" or "AÇÃO" does not trigger the warning, while `Ã©` or `Â°` does.

### Data Issues Log

In addition to the migration report, transformation tasks generate a **data issues log** as a TSV file:

```
reports/data_issues_log_<timestamp>_<task_name>.tsv
```

This file contains per-record issues with columns for:
- Record identifier (legacy ID, barcode or row number; a record with several legacy IDs shows the first)
- Issue type
- Issue description
- Affected field

Use this file to create targeted data cleaning task lists.

### Array object required-field handling

For mapping-file-based transformations, missing required fields inside object arrays are reported at item level:

- Items with legacy-sourced content but missing required fields are logged as field mapping issues and removed from the resulting record.
- Items containing only static mapped values (from `value`) and missing required fields are removed without creating a field mapping issue.

Where to look in reports:

- `FieldMappingErrors` section: item-level required-field problems with legacy content.
- `IncompleteSubPropertyRemoved` section: discarded incomplete array items.
- `Field Mapping Errors found` in `GeneralStatistics`: count of field mapping issues.

This behavior allows a record to continue transforming even when individual array items are invalid.

### Mapping Reports

Transformation tasks that use mapping files append a **mapping report** to the markdown report. This section shows:

- Total records processed
- FOLIO fields that were mapped and their frequency
- Legacy fields that were mapped and their frequency

This helps identify:
- Unmapped legacy fields that might contain valuable data
- FOLIO fields that were never populated

## Report Database

Alongside the markdown and JSON reports, every task writes a SQLite database,
`reports/.raw/migration_<timestamp>_<task_name>.sqlite`. It holds the report statistics, the
data issues, and, for the records that have issues, the legacy record itself. The database is
rebuilt each time the task runs, like the other report files. If it cannot be created, the task
logs a warning and carries on with the other outputs.

Statistics and issue messages are stored as translation keys and parameters, not as translated
text. The language of a report is therefore chosen when it is rendered, not when the task runs.

### Regenerating reports

The `report` subcommand renders a run from a task's report database. It does not need a FOLIO
connection or password. Give it the configuration file and the task name, as when running the task:

```bash
# The report for the task's latest run, written to the iteration's reports folder
# as report_<timestamp>_<task_name>_<locale>.md
folio-migration-tools report config.json transform_items --base_folder_path ./

# A specific run, by the timestamp in the database file name
folio-migration-tools report config.json transform_items --run 20261023-101500

# Several languages at once: writes ..._en.md and ..._fr.md
folio-migration-tools report config.json transform_items --locale en,fr

# The data issues as a tab-separated file, with messages in French
folio-migration-tools report config.json transform_items --locale fr --issues-tsv issues.tsv

# The statistics as JSON
folio-migration-tools report config.json transform_items --json-output stats.json

# A database file directly, with the report on the console
folio-migration-tools report --db reports/.raw/migration_<timestamp>_<task_name>.sqlite --output -
```

An unknown task name is an error that lists the tasks in the configuration. If the task has
several runs and neither `--run` nor `--db` is given, you are asked to choose one. Without a
terminal the command stops and lists the runs instead. The report never overwrites the one the
task wrote, because the file name includes the locale.

| Option | Description |
|--------|-------------|
| `--base_folder_path` | The base folder of the migration project, if not set in the configuration. Also read from `FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH` |
| `--run` | The run to use: the timestamp in the database file name, or `latest` |
| `--db` | Use this report database instead of looking up the task's |
| `--locale` | Comma-separated locales. Defaults to the configured report language. With several locales, `--output`, `--issues-tsv` or `--json-output` is required and the locale is added to each file name |
| `--output` | Write the markdown report to this file, or to the console if `-`, instead of the reports folder |
| `--issues-tsv` | Also write the data issues as TSV |
| `--json-output` | Also write the statistics as JSON |
| `--json-localized` | Write the JSON as `{section: {label: count}}` with labels in the locale, instead of locale-neutral keys and parameters |
| `--list-sections` | List the section ids in the run, with the number of measures and the total count, and exit |
| `--sections`, `--exclude-sections` | Comma-separated section ids to include or leave out |
| `--no-mapping` | Leave out the mapped FOLIO and legacy field tables |
| `--min-count` | Hide measures with a count below this |
| `--measure-contains` | Only show measures whose text contains this, ignoring case |
| `--config` | A JSON file with default values for the options above |

With no filters, the markdown report is identical to the one the task wrote, apart from the
finish time and elapsed time, which are the ones stored for the run.

The filter options can be kept in a JSON file and passed with `--config`. Keys are the option
names without the dashes, in snake_case or camelCase, and unknown keys are an error. Options
given on the command line take precedence over the file.

```json
{
  "locale": ["en", "fr"],
  "excludeSections": ["Trivia"],
  "minCount": 2,
  "mapping": false,
  "output": "report.md"
}
```

The locale-neutral JSON has this shape. `kind` is `key` for a translation key with its
parameters, `literal` for text that was never translated, and `composed` when `params.parts`
lists keys and text that are joined together.

```json
{
  "run": {"task_name": "transform_bibs", "started": "...", "finished": "...", "total_records": 10000},
  "sections": {
    "GeneralStatistics": [
      {"kind": "key", "key": "Records processed", "params": {}, "count": 10000}
    ]
  },
  "mapping": {"folio": {"title": [9900]}, "legacy": {"245": [10000, 9900]}}
}
```

### Tables

Every table except `schema_info` has a `run_id` that points to `run`. The database of a task
run holds one run. The renderer and the `report` subcommand use the latest one.

| Table | Contents |
|-------|----------|
| `schema_info` | The schema version |
| `run` | Task name and type, start and finish time, tool version, the locale the task ran in, the task configuration with credentials removed (`config_json`), and the report title |
| `report_stat` | One row per section and measure: `section_id`, `measure_key`, `params_json`, `count`. `translated` is 1 for a translation key with parameters, 2 for a message composed of several keys and text, and 0 for text that was never translated |
| `mapping_stat` | The mapped FOLIO fields (`side = 'folio'`) and legacy fields (`side = 'legacy'`) with how many records had and mapped each |
| `data_issue` | One row per data issue, described below |
| `legacy_record` | The legacy record behind one or more issues, stored once however many issues it has |
| `record_alias` | The ids of a record with several legacy ids, so that a record can be found by any of them |
| `reference_file`, `reference_content`, `reference_row` | The mapping and reference data files the task used, as they were when it ran |

#### Data issues

| Column | Description |
|--------|-------------|
| `issue_type` | The category, such as `FIELD MAPPING FAILED` |
| `message_key`, `params_json`, `translated` | The message as a translation key and parameters. When `translated` is 0, `message_key` holds the literal text |
| `legacy_value` | The offending value |
| `record_id`, `record_id_kind` | The normalized id of the record, and what it is: `legacy_id`, `row_number`, `file_index` or `none`. Barcodes passed by circulation tasks are stored as `legacy_id` |
| `record_id_raw` | The value as the task logged it, kept when it differs from `record_id` |
| `record_pk` | The `legacy_record` the issue belongs to, when one was captured |
| `location_kind`, `location_path` | Where in the record the problem is, if the task knows |
| `ts` | When the issue was logged |

A `location_path` for `location_kind = 'marc'` is `TAG[OCCURRENCE]$CODE/SPAN`. Only the tag is
required. The occurrence is the zero-based position among fields with the same tag, and the span
is a zero-based, inclusive character position or range in the value: `245`, `245$a`, `245[0]$a`,
`LDR/05`, `008/35-37`. For `location_kind = 'column'` the path is the column name.

`legacy_record.format` is `marc` for a record stored as MARC21 (ISO 2709) bytes and `json` for a
row or object stored as a JSON object of column or key to value. The record is identified by
`source_file` and `source_index`, the zero-based position of the record in the file.

#### Example queries

```sql
-- The most common data issue messages
SELECT issue_type, message_key, COUNT(*) AS n
FROM data_issue GROUP BY issue_type, message_key ORDER BY n DESC LIMIT 20;

-- The issues on one record, whichever of its ids you have
SELECT d.issue_type, d.message_key, d.location_path, d.legacy_value
FROM data_issue d
WHERE d.record_id = 'b1234567'
   OR d.record_pk IN (SELECT record_pk FROM record_alias WHERE legacy_id = 'b1234567');

-- MARC records with a problem in the 245 field
SELECT r.source_file, r.source_index, d.location_path, d.legacy_value
FROM data_issue d JOIN legacy_record r USING (record_pk)
WHERE d.location_kind = 'marc' AND d.location_path LIKE '245%';
```

The database is plain SQLite, so it can be read from any language. With
[polars](https://pola.rs):

```python
import polars as pl

issues = pl.read_database_uri(
    "SELECT issue_type, message_key, record_id FROM data_issue",
    "sqlite:///reports/.raw/migration_<timestamp>_<task_name>.sqlite",
)
print(issues.group_by("message_key").len().sort("len", descending=True))
```

### Privacy

The stored legacy records can contain personal data, for example in user migrations. The
database sits in the same iteration folder as the logs and the data issues log, and should be
handled in the same way. Two task configuration options under `reportPrivacy` limit what is
stored:

```json
{
  "name": "transform_users",
  "migrationTaskType": "UserTransformer",
  "reportPrivacy": {
    "captureLegacyRecords": true,
    "maskFields": ["ssn", "dateOfBirth"]
  }
}
```

- `captureLegacyRecords`: set to `false` to store no legacy record data. Statistics and issue
  messages are still stored.
- `maskFields`: column or key names for delimited and JSON data, or tags and subfields such as
  `100`, `245$a` or `008` for MARC. The values are replaced by a fixed token before anything is
  written, including the `legacy_value` and message parameters of issues raised on those fields.
  A masked `legacy_record` has `masked = 1` and a `mask_note` pointing to the source file and
  position of the original.

## Using Reports for Data Cleaning

### Recommended Workflow

1. **Run an initial transformation** on a sample or full dataset
2. **Review the migration report** for high-level statistics
3. **Check the data issues log** for specific record problems
4. **Prioritize fixes** based on frequency counts in the report
5. **Clean source data** or adjust mapping files
6. **Re-run the transformation** and compare reports
7. **Iterate** until data quality meets requirements

### Common Statistics to Watch

| Statistic | What to Look For |
|-----------|------------------|
| Failed records | Should be 0 or very low percentage |
| Unmapped values | May indicate missing reference data mappings |
| Default fallback usage | High counts may indicate mapping gaps |
| Validation errors | Records that don't meet FOLIO schema requirements |

### Automating Report Analysis

The raw JSON reports enable automated analysis. Example use cases:

- **Trend tracking**: Compare statistics across migration iterations
- **Quality gates**: Fail CI/CD pipelines if error rates exceed thresholds
- **Dashboards**: Aggregate statistics for project status reporting
- **Alerting**: Notify team when specific error types appear

```python
# Example: Reading a raw JSON report
import json

with open("reports/.raw/raw_report_20260124_transform_bibs.json") as f:
    report = json.load(f)

total = report["GeneralStatistics"].get("Records processed", 0)
failed = report["GeneralStatistics"].get("Records with errors", 0)
success_rate = ((total - failed) / total) * 100 if total > 0 else 0

print(f"Success rate: {success_rate:.2f}%")
```
