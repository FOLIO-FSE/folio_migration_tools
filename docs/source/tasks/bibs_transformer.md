# BibsTransformer

Transform MARC bibliographic records into FOLIO Instance records and prepare MARC data for loading to SRS via Data Import.

## When to Use This Task

- Migrating bibliographic data from a legacy ILS with MARC21 records
- Creating FOLIO Instances with corresponding SRS (Source Record Storage) records
- Supporting various ILS flavors: Voyager, Sierra, Aleph, Koha, and others

## Configuration

```json
{
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavour": "tag001",
    "hridHandling": "preserve001",
    "updateHridSettings": false,
    "tagsToDelete": ["841", "852"],
    "statisticalCodeMapFileName": "statistical_codes.tsv",
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
| `hridHandling` | string | No | How to handle HRIDs. `"default"` (generate new) or `"preserve001"` (use 001 value). Default: `"default"` |
| `updateHridSettings` | boolean | No | Whether to update FOLIO HRID settings after transformation. Default: `false` |
| `tagsToDelete` | array | No | MARC tags to remove before saving to output MARC file. |
| `statisticalCodeMapFileName` | string | No | TSV file mapping legacy codes to FOLIO statistical codes. |
| `statisticalCodeMappingFields` | array | No | MARC fields to extract statistical codes from (e.g., `["998$a$b"]`). |
| `customBibIdField` | string | No | MARC field containing legacy ID when `ilsFlavour` is `"custom"`. Example: `"991$a"` |
| `files` | array | Yes | List of MARC files to process. See [File Configuration](#file-configuration). |

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
    "statisticalCodeMapFileName": "stat_codes.tsv",
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
