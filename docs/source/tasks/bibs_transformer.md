# BibsTransformer

Transform MARC bibliographic records into FOLIO Instance records and prepare MARC data for loading to SRS via Data Import.

## When to Use This Task

- Migrating bibliographic data from a legacy ILS with MARC21 records
- Creating FOLIO Instances with corresponding SRS (Source Record Storage) records
- Supporting various ILS flavors: Voyager, Sierra, Aleph, Koha, and others

## Configuration

Configuration keys can be provided in either `camelCase` or `snake_case` in JSON files.
The examples below use `camelCase` for consistency.

```json
{
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavour": "tag001",
    "hridHandling": "preserve001",
    "updateHridSettings": false,
    "tagsToDelete": ["841", "852"],
    "statisticalCodesMapFileName": "statistical_codes.tsv",
    "statisticalCodeMappingFields": ["998$a$b"],
    "files": [
        {
            "file_name": "bibs.mrc",
            "discovery_suppressed": false
        },
        {
            "file_name": "bibs_suppressed.mrc",
            "discovery_suppressed": true
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. Used to identify the task and name output files. |
| `migrationTaskType` | string | Yes | Must be `"BibsTransformer"` |
| `ilsFlavour` | string | Yes | The ILS type for legacy ID handling. See [ILS Flavours](#ils-flavours) below. |
| `customBibIdField` | string | No | MARC field containing legacy ID when ILS flavour is `"custom"`. Example: `"991$a"` |
| `addAdministrativeNotesWithLegacyIds` | boolean | No | Add administrative notes containing legacy IDs. Default: `true` |
| `hridHandling` | string | No | How to handle HRIDs. `"default"` (generate new) or `"preserve001"` (use 001 value). Default: `"default"` |
| `deactivate035From001` | boolean | No | Disable adding MARC 035 from legacy 001/003 during HRID handling. Default: `false` |
| `dataImportMarc` | boolean | No | Generate MARC output for Data Import overlay workflow. Default: `true` |
| `parseCatalogedDate` | boolean | No | Parse mapped `catalogedDate` values into FOLIO date format. Default: `false` |
| `resetHridSettings` | boolean | No | Reset instance HRID counter before processing. Default: `false` |
| `updateHridSettings` | boolean | No | Whether to update FOLIO HRID settings after transformation. Default: `true` |
| `tagsToDelete` | array | No | MARC tags to remove before saving to output MARC file. |
| `statisticalCodesMapFileName` | string | No | TSV file mapping legacy codes to FOLIO statistical codes. |
| `statisticalCodeMappingFields` | array | No | MARC fields to extract statistical codes from (e.g., `["998$a$b"]`). |
| `createSourceRecords` | boolean | No | Task-level control for creating SRS records. Default: `false` |
| `files` | array | Yes | List of MARC files to process. See [File Configuration](#file-configuration). |

```{note}
Task-level `createSourceRecords` and per-file `create_source_records` are both applied.
SRS records are only created when both are `true`, and `dataImportMarc` is `false`.
```

```{note}
When `dataImportMarc` is `true`, BibsTransformer forces 035 generation from 001/003 off in the HRID flow.
```

### MARC Record Preprocessors

`BibsTransformer` can apply MARC preprocessors to each `pymarc.Record` before mapping it to FOLIO.

- `marcRecordPreprocessors`: ordered list of preprocessor names or full module paths.
- `preprocessorsArgs`: inline JSON object or the name of a JSON file in `mapping_files/` containing per-preprocessor arguments.

By default, the task includes `folio_migration_tools.marc_rules_transformation.marc_reader_wrapper.set_leader`, which normalizes leader bytes 09-11 and 20-23. If you provide your own `marcRecordPreprocessors` list and omit that preprocessor, it will not run.

All preprocessors are called with the task's `migration_report` object via keyword arguments. Custom preprocessors must accept `**kwargs`.

Example:

```json
{
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavour": "voyager",
    "marcRecordPreprocessors": [
        "folio_migration_tools.marc_rules_transformation.marc_reader_wrapper.set_leader",
        "folio_data_import.marc_preprocessors.clean_empty_fields"
    ],
    "preprocessorsArgs": {
        "default": {
            "log_level": "DEBUG"
        }
    },
    "files": [
        {
            "file_name": "bibs.mrc"
        }
    ]
}
```

### ILS Flavours

The `ilsFlavour` parameter determines how the legacy system identifier is extracted from MARC records:

| Value | Legacy ID Source |
|-------|-----------------|
| `aleph` | 001 (with processing) |
| `voyager` | 001 |
| `sierra` | 907$y |
| `millennium` | 907$y |
| `koha` | 999$c |
| `tag907y` | 907$y |
| `tag001` | 001 |
| `tagf990a` | 990$a |
| `custom` | Field specified in `customBibIdField` |
| `none` | UUID generated |

### File Configuration

Each file object in the `files` array supports:

| Property | Type | Description |
|----------|------|-------------|
| `file_name` | string | Name of the MARC file in `source_data/instances/` |
| `discovery_suppressed` | boolean | Mark all records from this file as discovery suppressed |
| `staff_suppressed` | boolean | Mark all records from this file as staff suppressed |
| `statistical_code` | string | Statistical code(s) to assign to all records. Use `multi_field_delimiter` for multiple codes. |
| `data_import_marc` | boolean | Include records in the output MARC file for Data Import. Default: `true` |
| `create_source_records` | boolean | Create SRS records for these bibs. Default: `true` |

## Source Data Requirements

- **Location**: Place MARC21 binary files (`.mrc`) in `iterations/<iteration>/source_data/instances/`
- **Format**: Standard MARC21 binary format
- **Encoding**: UTF-8 recommended

## Decoding Error Handling

`BibsTransformer` uses permissive MARC decoding with per-record diagnostics and recovery
heuristics.

- MARC-8 decoding warnings are logged but do not stop processing.
- Some decode failures are automatically repaired (for example, MARC-8 leader and MARCMaker
    dagger marker issues).
- Records that cannot be repaired are skipped and logged as failed.

Use the following outputs to review decoding behavior:

- `reports/data_issues_log_<task_name>.tsv` for warning and repair messages.
- `reports/report_<task_name>.md` for repaired vs failed decode counts.
- `results/failed_bib_records.mrc` for unrecoverable records.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_instances_<task_name>.json` | FOLIO Instance records (one per line) |
| `folio_marc_instances_<task_name>.mrc` | MARC21 file for loading via Data Import |
| `instance_id_map_<task_name>.json` | Legacy ID to FOLIO UUID mapping |

Reports are created in `iterations/<iteration>/reports/`:

| File | Description |
|------|-------------|
| `report_<task_name>.md` | Transformation statistics and mapping report |
| `data_issues_log_<task_name>.tsv` | Data issues requiring attention |

## Examples

### Basic Example

Transform MARC bibs with default HRID generation:

```json
{
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavour": "voyager",
    "files": [
        {
            "file_name": "bibs.mrc"
        }
    ]
}
```

### Preserving 001 as HRID

```json
{
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavour": "tag001",
    "hridHandling": "preserve001",
    "files": [
        {
            "file_name": "bibs.mrc"
        }
    ]
}
```

### Multiple Files with Different Settings

```json
{
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavour": "sierra",
    "tagsToDelete": ["9XX"],
    "files": [
        {
            "file_name": "regular_bibs.mrc",
            "discovery_suppressed": false
        },
        {
            "file_name": "suppressed_bibs.mrc",
            "discovery_suppressed": true
        },
        {
            "file_name": "equipment.mrc",
            "discovery_suppressed": false,
            "data_import_marc": false
        }
    ]
}
```

### With Statistical Code Mapping

```json
{
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavour": "tag001",
    "statisticalCodesMapFileName": "stat_codes.tsv",
    "statisticalCodeMappingFields": ["998$a", "998$b"],
    "files": [
        {
            "file_name": "bibs.mrc",
            "statistical_code": "migrated"
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_bibs --base_folder ./
```

## Next Steps

After running BibsTransformer:

1. **Post Instances**: Use [InventoryBatchPoster](inventory_batch_poster) or [BatchPoster](batch_poster) to load Instance records
2. **Load MARC to SRS**: Use [MARCImportTask](marc_import) to import the MARC file via Data Import
3. **Transform Holdings**: Use [HoldingsMarcTransformer](holdings_marc_transformer) or [HoldingsCsvTransformer](holdings_csv_transformer)

## See Also

- [MARC Rules Based Mapping](../marc_rule_based_mapping) - Customizing MARC-to-Instance mapping
- [Statistical Code Mapping](../statistical_codes) - Mapping statistical codes from MARC
- [InventoryBatchPoster](inventory_batch_poster) - Posting transformed instances
- [MARCImportTask](marc_import) - Loading MARC records to SRS
