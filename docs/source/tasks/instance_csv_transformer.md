# InstanceCsvTransformer

Transform delimited (CSV/TSV) data into FOLIO Instance records using a JSON mapping file, for legacy data that is not available as MARC.

## When to Use This Task

- Migrating bibliographic or title-level data that exists only as delimited files
- Creating FOLIO Instances (source `FOLIO`) that holdings can then be attached to

The task writes instances, an instance id map and a migration report. It does not create SRS/MARC records. FOLIO assigns the HRID when the instances are posted, unless one is mapped.

## Configuration

```json
{
    "name": "transform_instances",
    "migrationTaskType": "InstanceCsvTransformer",
    "instancesMappingFileName": "instance_mapping.json",
    "instanceTypesMapFileName": "instance_types.tsv",
    "files": [
        {
            "file_name": "instances.tsv"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"InstanceCsvTransformer"` |
| `files` | array | Yes | Delimited files in the legacy records folder. Supports `discovery_suppressed`, `staff_suppressed` and `statistical_code`. |
| `instancesMappingFileName` | string | Yes | JSON mapping file. Must map `legacyIdentifier`, `title` and `instanceTypeId`. |
| `instanceTypesMapFileName` | string | Yes | TSV map from legacy values to FOLIO instance types (`folio_name` or `folio_code` column; the header decides which FOLIO field is matched). |
| `natureOfContentMapFileName` | string | No | TSV map for `natureOfContentTermIds`. |
| `instanceFormatsMapFileName` | string | No | TSV map for `instanceFormatIds`. |
| `modesOfIssuanceMapFileName` | string | No | TSV map for `modeOfIssuanceId`. |
| `contributorTypesMapFileName` | string | No | TSV map for `contributors[n].contributorTypeId` (`folio_name` or `folio_code` column). |
| `identifierTypesMapFileName` | string | No | TSV map with `legacy_identifier_type` and `folio_name` columns for `identifiers[n].identifierTypeId`. |
| `contributorNameTypesMapFileName` | string | No | TSV map with `legacy_contributor_name_type` and `folio_name` columns for `contributors[n].contributorNameTypeId`. |
| `instanceNoteTypesMapFileName` | string | No | TSV map with `legacy_note_type` and `folio_name` columns for `notes[n].instanceNoteTypeId`. |
| `statisticalCodesMapFileName` | string | No | TSV map with `legacy_stat_code` and `folio_code` columns. |
| `addAdministrativeNotesWithLegacyIds` | boolean | No | Add an administrative note with the legacy id. Default `true`. |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/instances/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row
- **Prerequisites**: None. Run this task before [HoldingsCsvTransformer](holdings_csv_transformer) or [HoldingsMarcTransformer](holdings_marc_transformer), which use the instance id map

### Instance Mapping File

Create a JSON mapping file in `mapping_files/`:

```json
{
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "id",
            "value": "",
            "description": "Legacy identifier for deterministic UUID"
        },
        {
            "folio_field": "title",
            "legacy_field": "Title",
            "value": "",
            "description": "Instance title"
        },
        {
            "folio_field": "instanceTypeId",
            "legacy_field": "Type",
            "value": "",
            "description": "Mapped via instanceTypesMapFileName"
        },
        {
            "folio_field": "contributors[0].name",
            "legacy_field": "Author",
            "value": "",
            "description": "Contributor name"
        },
        {
            "folio_field": "contributors[0].contributorNameTypeId",
            "legacy_field": "",
            "value": "Personal name",
            "description": "FOLIO name or UUID"
        }
    ]
}
```

```{important}
`legacyIdentifier`, `title` and `instanceTypeId` must be mapped. `legacyIdentifier` must map to a unique value in your source data.
```

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `instanceTypesMapFileName` | `folio_name` or `folio_code` | Instance type |
| `natureOfContentMapFileName` | `folio_name` | Nature of content term |
| `instanceFormatsMapFileName` | `folio_name` | Instance format |
| `modesOfIssuanceMapFileName` | `folio_name` | Mode of issuance |
| `contributorTypesMapFileName` | `folio_name` or `folio_code` | Contributor type |
| `statisticalCodesMapFileName` | `folio_code` | Statistical code |
| `identifierTypesMapFileName` | `folio_name` | Identifier type name |
| `contributorNameTypesMapFileName` | `folio_name` | Contributor name type name |
| `instanceNoteTypesMapFileName` | `folio_name` | Instance note type name |

For instance types and contributor types, the FOLIO-side header decides whether values are matched by name or by code. The legacy column headers must match the source file's headers exactly, including case.

```text
TypeofPublication	folio_code
Books	txt
*	txt
```

The instance types map needs a fallback row with `*`. Optional maps are only used when the file name is set; without them, the mapped value is used as is, so hardcoded UUIDs in the mapping file keep working.

## Identifier types, contributor name types and note types

By default these are resolved from the mapping file value, given as the FOLIO name (case-insensitive) or the UUID, as for note types in the holdings and items transformers. An optional TSV map file (see the parameters above) translates legacy codes to FOLIO names first. Unknown names fail the record, or fail start-up when hardcoded in the mapping file.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_instances_<task_name>.json` | FOLIO Instance records |
| `instances_id_map.json` | Maps each legacy ID to the instance UUID (and HRID, when mapped). Used by the holdings transformers |
| `extradata_<task_name>.extradata` | Extra data, when applicable |

The migration report is written to `iterations/<iteration>/reports/`.

## Examples

### Basic Instance Transformation

```json
{
    "name": "transform_instances",
    "migrationTaskType": "InstanceCsvTransformer",
    "instancesMappingFileName": "instance_mapping.json",
    "instanceTypesMapFileName": "instance_types.tsv",
    "files": [
        {
            "file_name": "instances.tsv"
        }
    ]
}
```

### With Optional Reference Data Mappings

```json
{
    "name": "transform_instances",
    "migrationTaskType": "InstanceCsvTransformer",
    "instancesMappingFileName": "instance_mapping.json",
    "instanceTypesMapFileName": "instance_types.tsv",
    "natureOfContentMapFileName": "nature_of_content.tsv",
    "instanceFormatsMapFileName": "instance_formats.tsv",
    "modesOfIssuanceMapFileName": "modes_of_issuance.tsv",
    "contributorTypesMapFileName": "contributor_types.tsv",
    "identifierTypesMapFileName": "identifier_types.tsv",
    "statisticalCodesMapFileName": "stat_codes.tsv",
    "addAdministrativeNotesWithLegacyIds": false,
    "files": [
        {
            "file_name": "instances.tsv",
            "discovery_suppressed": false,
            "staff_suppressed": false
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_instances --base_folder ./
```

## Next Steps

1. **Post Instances**: Use [InventoryBatchPoster](inventory_batch_poster) or [BatchPoster](batch_poster)
2. **Transform Holdings**: Use [HoldingsCsvTransformer](holdings_csv_transformer) or [HoldingsMarcTransformer](holdings_marc_transformer), which look up instances in the instance id map

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [Mapping Files for Inventory](../mapping_files_inventory) - Required mapping files
- [Reference Data Mapping](../reference_data_mapping) - Reference data map file format
