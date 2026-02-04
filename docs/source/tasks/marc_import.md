# MARCImportTask

Import MARC records into FOLIO using the Data Import (change-manager) APIs. This task leverages FOLIO's native Data Import capabilities with configurable job profiles.

This task uses the [folio_data_import](https://folio-data-import.readthedocs.io/en/latest) library.

## When to Use This Task

- Loading MARC bibliographic records directly via Data Import
- Using FOLIO's job profiles to control record creation/overlay
- Importing large MARC files with splitting and resume capabilities
- When you need MARC records to go through FOLIO's normal Data Import pipeline

```{tip}
MARCImportTask uses FOLIO's native Data Import APIs, which means records are processed according to your configured job profiles. This provides more control over match/overlay behavior than direct batch posting.
```

## Configuration

```json
{
    "name": "import_marc_bibs",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "batchSize": 10,
    "files": [
        {
            "file_name": "bibs.mrc"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | Yes | - | The name of this task |
| `migrationTaskType` | string | Yes | - | Must be `"MARCImportTask"` |
| `files` | array | Yes | - | List of MARC files to import from `source_data/` |
| `importProfileName` | string | Yes | - | Name of the FOLIO Data Import job profile to use |
| `batchSize` | integer | No | 10 | Records per batch sent to FOLIO (1-1000) |
| `batchDelay` | float | No | 0.0 | Seconds to wait between batches |
| `marcRecordPreprocessors` | array | No | `[]` | Preprocessor names to apply to each record |
| `preprocessorsArgs` | object/string | No | `{}` | Arguments for preprocessors (or path to JSON file) |
| `splitFiles` | boolean | No | `false` | Split large files into smaller jobs |
| `splitSize` | integer | No | 1000 | Records per split file |
| `splitOffset` | integer | No | 0 | Number of splits to skip (for resume) |
| `showFileNamesInDataImportLogs` | boolean | No | `false` | Show file names in FOLIO Data Import UI |
| `letSummaryFail` | boolean | No | `false` | Don't fail if job summary unavailable |
| `skipSummary` | boolean | No | `false` | Skip fetching final job statistics |
| `showProgress` | boolean | No | `true` | Display progress bars during import |

## Job Profiles

The `importProfileName` must match an existing Data Import job profile in your FOLIO tenant. Common profiles include:

| Profile Name | Description |
|--------------|-------------|
| `Default - Create instance and SRS MARC Bib` | Creates new instances and SRS records |
| `Default - Create Holdings and SRS MARC Holdings` | Creates holdings from MARC holdings |
| (Custom profiles) | Profiles you've configured for overlay, matching, etc. |

```{warning}
The job profile must exist in FOLIO before running this task. Use the FOLIO Data Import settings UI to create or verify profiles.
```

## MARC Record Preprocessing

Apply transformations to MARC records before they're sent to FOLIO. The `folio_data_import` library provides several built-in preprocessors:

| Preprocessor | Description |
|--------------|-------------|
| `prepend_prefix_001` | Prepend a custom prefix to the 001 field (requires `prefix` argument) |
| `prepend_ppn_prefix_001` | Prepend "(PPN)" to the 001 field (for ABES SUDOC records) |
| `prepend_abes_prefix_001` | Prepend "(ABES)" to the 001 field |
| `strip_999_ff_fields` | Remove 999 fields with ff indicators |
| `clean_999_fields` | Remove 999 ff fields, copy other 999s to 945 |
| `clean_non_ff_999_fields` | Move non-ff 999 fields to 945 with "99" indicators |
| `clean_empty_fields` | Remove empty fields and subfields |
| `fix_bib_leader` | Fix common leader issues |
| `mark_deleted` | Mark record as deleted in leader position 5 |

### Configuring Preprocessors

Specify preprocessors by name in the `marcRecordPreprocessors` array:

```json
{
    "name": "import_marc_bibs",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "marcRecordPreprocessors": ["strip_999_ff_fields", "clean_empty_fields"],
    "files": [{"file_name": "bibs.mrc"}]
}
```

### Preprocessor Arguments

Some preprocessors require arguments. Pass them via `preprocessorsArgs` as a dictionary keyed by preprocessor name:

```json
{
    "name": "import_marc_bibs",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "marcRecordPreprocessors": ["prepend_prefix_001"],
    "preprocessorsArgs": {
        "prepend_prefix_001": {
            "prefix": "LEGACY"
        }
    },
    "files": [{"file_name": "bibs.mrc"}]
}
```

You can also set default arguments that apply to all preprocessors:

```json
{
    "preprocessorsArgs": {
        "default": {
            "some_common_setting": "value"
        },
        "prepend_prefix_001": {
            "prefix": "LEGACY"
        }
    }
}
```

Preprocessor arguments can also be loaded from a JSON file in `mapping_files/`:

```json
{
    "name": "import_marc_bibs",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "marcRecordPreprocessors": ["prepend_prefix_001"],
    "preprocessorsArgs": "preprocessor_config.json",
    "files": [{"file_name": "bibs.mrc"}]
}
```

### Custom Preprocessors

You can use custom preprocessors by specifying the full module path:

```json
{
    "marcRecordPreprocessors": ["my_module.my_preprocessor"]
}
```

Custom preprocessor functions must accept a `pymarc.Record` as the first argument and return a `pymarc.Record`.

## Handling Large Files

For very large MARC files, use file splitting:

```json
{
    "name": "import_large_marc_file",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "splitFiles": true,
    "splitSize": 5000,
    "files": [{"file_name": "large_export.mrc"}]
}
```

### Resuming After Failure

If an import fails partway through, use `splitOffset` to skip already-processed splits:

```json
{
    "name": "resume_import",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "splitFiles": true,
    "splitSize": 5000,
    "splitOffset": 10,
    "files": [{"file_name": "large_export.mrc"}]
}
```

This skips the first 10 splits (50,000 records) and continues from split 11.

## Source Files

- **Location**: `iterations/<iteration>/source_data/instances/` (or appropriate subfolder)
- **Format**: Binary MARC (.mrc) files
- **Note**: Files should be valid MARC21 format

## Output Files

Files and job information are created in `iterations/<iteration>/results/`:

| Output | Description |
|--------|-------------|
| Job IDs | Job identifiers logged for monitoring in FOLIO UI |
| `bad_marc_records_*.mrc` | Records that couldn't be parsed |
| `failed_batches_*.mrc` | Batches that failed to import |
| Migration report | Statistics on records sent, jobs created |

## Examples

### Basic MARC Import

```json
{
    "name": "import_bibs",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "files": [
        {"file_name": "bibs.mrc"}
    ]
}
```

### Import with Throttling

For systems under load, add delays between batches:

```json
{
    "name": "import_bibs_throttled",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "batchSize": 50,
    "batchDelay": 1.0,
    "files": [
        {"file_name": "bibs.mrc"}
    ]
}
```

### Import Multiple Files

```json
{
    "name": "import_all_bibs",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "batchSize": 100,
    "files": [
        {"file_name": "bibs_part1.mrc"},
        {"file_name": "bibs_part2.mrc"},
        {"file_name": "bibs_part3.mrc"}
    ]
}
```

### Production Import with All Options

```json
{
    "name": "production_import",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Migration - Create or Update Instances",
    "batchSize": 100,
    "batchDelay": 0.5,
    "splitFiles": true,
    "splitSize": 10000,
    "showFileNamesInDataImportLogs": true,
    "letSummaryFail": true,
    "showProgress": true,
    "files": [
        {"file_name": "full_catalog_export.mrc"}
    ]
}
```

### Non-Interactive/CI Environment

```json
{
    "name": "ci_import",
    "migrationTaskType": "MARCImportTask",
    "importProfileName": "Default - Create instance and SRS MARC Bib",
    "showProgress": false,
    "skipSummary": true,
    "files": [
        {"file_name": "test_bibs.mrc"}
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json import_marc_bibs --base_folder ./
```

## Monitoring in FOLIO

With `showFileNamesInDataImportLogs: true`, you can monitor import progress in the FOLIO UI:

1. Go to **Data Import** in FOLIO
2. From the **Actions** menu, select "View all logs"
3. Find jobs by file name or job ID (logged in console output)

## See Also

- [BibsTransformer](bibs_transformer) - Alternative: Transform MARC to FOLIO Instance objects
- [BatchPoster](batch_poster) - Post transformed objects
- [InventoryBatchPoster](inventory_batch_poster) - Enhanced inventory posting
