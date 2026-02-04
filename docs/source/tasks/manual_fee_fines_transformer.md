# ManualFeeFinesTransformer

Transform delimited (CSV/TSV) data into FOLIO Manual Fees/Fines (accounts and fee/fine actions).

## When to Use This Task

- Migrating static, non-incrementing fees/fines from legacy systems
- Creating manual charges not tied to open loans (e.g., replacement costs for returned items)
- Preserving fee/fine history with partial payment records

```{important}
This task creates **manual** (static) fees/fines. For automatic fees/fines that increment over time (like overdue fines), FOLIO generates these from open loans. To avoid duplication, only migrate manual fees/fines for charges not related to open loans.
```

## Configuration

```json
{
    "name": "transform_feefines",
    "migrationTaskType": "ManualFeeFinesTransformer",
    "feefinesMap": "feefines_mapping.json",
    "feefinesOwnerMap": "feefine_owners.tsv",
    "feefinesTypeMap": "feefine_types.tsv",
    "servicePointMap": "service_points.tsv",
    "files": [
        {
            "file_name": "feefines.tsv"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"ManualFeeFinesTransformer"` |
| `feefinesMap` | string | Yes | JSON mapping file for fee/fine fields |
| `feefinesOwnerMap` | string | No | TSV file mapping fee/fine owners |
| `feefinesTypeMap` | string | No | TSV file mapping fee/fine types |
| `servicePointMap` | string | No | TSV file mapping service points |
| `files` | array | Yes | List of source data files to process |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/feefines/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row
- **Prerequisites**: Run [UserTransformer](user_transformer) and post users first

### Fee/Fine Mapping File

Behind the scenes, a manual fee/fine in FOLIO consists of one "account" and one or more "feeFineActions". The mapping file creates both:

```json
{
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "FEE_ID"
        },
        {
            "folio_field": "account.userId",
            "legacy_field": "PATRON_BARCODE",
            "description": "Matched to user UUID"
        },
        {
            "folio_field": "account.amount",
            "legacy_field": "ORIGINAL_AMOUNT"
        },
        {
            "folio_field": "account.remaining",
            "legacy_field": "BALANCE"
        },
        {
            "folio_field": "account.feeFineType",
            "legacy_field": "FEE_TYPE",
            "description": "Mapped via feefinesTypeMap"
        },
        {
            "folio_field": "account.ownerId",
            "legacy_field": "LOCATION",
            "description": "Mapped via feefinesOwnerMap"
        },
        {
            "folio_field": "account.feeFineOwner",
            "legacy_field": "LOCATION",
            "description": "Mapped via feefinesOwnerMap"
        },
        {
            "folio_field": "feefineaction.dateAction",
            "legacy_field": "CHARGE_DATE"
        },
        {
            "folio_field": "feefineaction.comments",
            "legacy_field": "NOTES"
        },
        {
            "folio_field": "feefineaction.createdAt",
            "legacy_field": "SERVICE_POINT",
            "description": "Mapped via servicePointMap"
        }
    ]
}
```

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work. All files are optional—you can alternatively specify UUIDs directly in the mapping file.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `ownerMapFileName` | `folio_owner` | Fee/fine owner name (the `owner` field) |
| `feefineTypeMapFileName` | `folio_name` | Fee/fine type name (the `feeFineType` field) |
| `servicePointMapFileName` | `folio_name` | Service point name |

```{attention}
- The fee/fine type must be associated with the fee/fine owner in FOLIO.
- FOLIO allows duplicate fee/fine type names, but the reference data mapping requires unique names.
```

## Fee/Fine Status

The overall Fee/Fine status is automatically determined:
- **Open**: If `remaining` amount > 0
- **Closed**: If `remaining` amount = 0

You can map any valid payment status, but the overall status is calculated from the remaining balance.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `extradata_<task_name>.extradata` | Fee/fine records for posting |
| `feefines_id_map_<task_name>.json` | Legacy ID to FOLIO UUID mapping |

## Examples

### Basic Fee/Fine Transformation

```json
{
    "name": "transform_feefines",
    "migrationTaskType": "ManualFeeFinesTransformer",
    "feefinesMap": "feefines_mapping.json",
    "files": [
        {
            "file_name": "feefines.tsv"
        }
    ]
}
```

### With All Reference Data Mappings

```json
{
    "name": "transform_feefines",
    "migrationTaskType": "ManualFeeFinesTransformer",
    "feefinesMap": "feefines_mapping.json",
    "feefinesOwnerMap": "feefine_owners.tsv",
    "feefinesTypeMap": "feefine_types.tsv",
    "servicePointMap": "service_points.tsv",
    "files": [
        {
            "file_name": "feefines.tsv"
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_feefines --base_folder ./
```

## Posting Fee/Fines

Fee/fine data is stored in an extradata file. Post using BatchPoster:

```json
{
    "name": "post_feefines",
    "migrationTaskType": "BatchPoster",
    "objectType": "Extradata",
    "batchSize": 1,
    "files": [
        {
            "file_name": "extradata_transform_feefines.extradata"
        }
    ]
}
```

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [UserTransformer](user_transformer) - Creating users (prerequisite)
- [BatchPoster](batch_poster) - Posting fee/fines
