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
| `instanceTypesMapFileName` | string | Yes | TSV map from legacy values to FOLIO instance types (`folio_name` column). |
| `natureOfContentMapFileName` | string | No | TSV map for `natureOfContentTermIds`. |
| `instanceFormatsMapFileName` | string | No | TSV map for `instanceFormatIds`. |
| `modesOfIssuanceMapFileName` | string | No | TSV map for `modeOfIssuanceId`. |
| `contributorTypesMapFileName` | string | No | TSV map for `contributors[n].contributorTypeId`. |
| `statisticalCodesMapFileName` | string | No | TSV map with `legacy_stat_code` and `folio_code` columns. |
| `addAdministrativeNotesWithLegacyIds` | boolean | No | Add an administrative note with the legacy id. Default `true`. |

## Identifier types, contributor name types and note types

These are not mapped through a map file. In the mapping file, give the FOLIO name (case-insensitive) or the UUID, as for note types in the holdings and items transformers. Unknown names fail the transformation at start-up when hardcoded, or fail the record when they come from the data.

## Output

- Instances in the results folder (`folio_instances_<task>.json`)
- The instance id map, used by the holdings transformers
- The migration report
