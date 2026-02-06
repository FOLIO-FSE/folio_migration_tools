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

Each task generates two report formats:

| Format | File Location | Purpose |
|--------|---------------|---------|
| **Markdown** | `reports/report_<timestamp>_<task_name>.md` | Human-readable report for review and documentation |
| **JSON** | `reports/.raw/raw_report_<timestamp>_<task_name>.json` | Machine-readable format for automated processing and analysis |

```{note}
The raw JSON reports were introduced to enable downstream processing such as aggregating statistics across multiple runs, building dashboards, or integrating with data quality monitoring tools.
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
    │       └── raw_report_<timestamp>_<task_name>.json  # JSON report
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
| PrecedingSuccedingTitles | Linked title relationships |

#### HoldingsMarcTransformer
Transforms MFHD (MARC Holdings) records to FOLIO Holdings records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts |
| HoldingsTypeMapping | Holdings type assignments |
| LocationMapping | Location code mappings |
| CallNumberTypeMapping | Call number type usage |

#### HoldingsCsvTransformer
Transforms CSV/TSV holdings data to FOLIO Holdings records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts, merging statistics |
| LocationMapping | Location mappings |
| HoldingsTypeMapping | Holdings type assignments |

#### ItemsTransformer
Transforms legacy item data to FOLIO Item records.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Record counts |
| MaterialTypeMapping | Material type mappings |
| LoanTypeMapping | Loan type mappings |
| ItemStatusMapping | Status mappings |
| LocationMapping | Effective location mappings |

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

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Order and order line counts |
| OrderTypeMapping | Order type assignments |
| AcquisitionMethodMapping | Acquisition method mappings |

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

#### RequestsMigrator
Migrates open hold/recall requests.

| Report Section | Description |
|----------------|-------------|
| GeneralStatistics | Requests processed, created, failed |
| DiscardedLoans | Reasons for discarded requests |

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

### Data Issues Log

In addition to the migration report, transformation tasks generate a **data issues log** as a TSV file:

```
reports/data_issues_log_<timestamp>_<task_name>.tsv
```

This file contains per-record issues with columns for:
- Record identifier (row number or legacy ID)
- Issue type
- Issue description
- Affected field

Use this file to create targeted data cleaning task lists.

### Mapping Reports

Transformation tasks that use mapping files append a **mapping report** to the markdown report. This section shows:

- Total records processed
- FOLIO fields that were mapped and their frequency
- Legacy fields that were mapped and their frequency

This helps identify:
- Unmapped legacy fields that might contain valuable data
- FOLIO fields that were never populated

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
