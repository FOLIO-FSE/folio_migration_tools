# InventoryBatchPoster

Post inventory objects (Instances, Holdings, Items) to FOLIO with advanced upsert and field preservation capabilities. This task uses the [folio_data_import](https://folio-data-import.readthedocs.io/en/latest) library's BatchPoster for enhanced performance and features.

## When to Use This Task

- Loading inventory objects with upsert requirements
- Updating existing records while preserving specific fields
- Posting ShadowInstances for ECS/consortium environments
- When you need fine-grained control over which fields are updated

```{tip}
InventoryBatchPoster provides the same field preservation and upsert capabilities as [BatchPoster](batch_poster) for inventory objects. Both support statistical code preservation, admin notes preservation, temporary location/loan type preservation, selective field patching, and automatic retry of failed records.

The primary difference is that InventoryBatchPoster uses the `folio_data_import` library, which is where future development efforts will be focused. New features for inventory posting will be added to InventoryBatchPoster.
```

## Configuration

```json
{
    "name": "post_instances",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Instances",
    "batchSize": 100,
    "upsert": true,
    "files": [
        {
            "file_name": "folio_instances_transform_bibs.json"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | Yes | - | The name of this task |
| `migrationTaskType` | string | Yes | - | Must be `"InventoryBatchPoster"` |
| `objectType` | string | Yes | - | One of: `Instances`, `Holdings`, `Items`, `ShadowInstances` |
| `files` | array | Yes | - | List of files to post from the `results/` folder |
| `batchSize` | integer | No | 100 | Records per batch (1-1000) |
| `upsert` | boolean | No | `false` | Create new or update existing records |
| `preserveStatisticalCodes` | boolean | No | `false` | Keep existing statistical codes during upsert |
| `preserveAdministrativeNotes` | boolean | No | `false` | Keep existing admin notes during upsert |
| `preserveTemporaryLocations` | boolean | No | `false` | Keep temporary locations on items during upsert |
| `preserveTemporaryLoanTypes` | boolean | No | `false` | Keep temporary loan types on items during upsert |
| `preserveItemStatus` | boolean | No | `true` | Keep item status during upsert (items only) |
| `patchExistingRecords` | boolean | No | `false` | Enable selective field patching |
| `patchPaths` | array | No | `[]` | Field paths to update when patching |
| `rerunFailedRecords` | boolean | No | `true` | Retry failed records individually |
| `noProgress` | boolean | No | `false` | Disable progress reporting |

### Object Types

| Object Type | Description | API Endpoint |
|-------------|-------------|--------------|
| `Instances` | Bibliographic records | `/instance-storage/batch/synchronous-unsafe` |
| `Holdings` | Holdings records | `/holdings-storage/batch/synchronous-unsafe` |
| `Items` | Item records | `/item-storage/batch/synchronous-unsafe` |
| `ShadowInstances` | Consortium shadow copies | `/instance-storage/batch/synchronous-unsafe` |

## Field Preservation

When upserting records, you can preserve existing field values:

### Statistical Codes

```json
{
    "name": "update_instances",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Instances",
    "upsert": true,
    "preserveStatisticalCodes": true,
    "files": [{"file_name": "updated_instances.json"}]
}
```

When `preserveStatisticalCodes` is `true`, existing codes are retained and merged with new codes.

### Administrative Notes

```json
{
    "name": "update_instances",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Instances",
    "upsert": true,
    "preserveAdministrativeNotes": true,
    "files": [{"file_name": "updated_instances.json"}]
}
```

### Item-Specific Preservation

For items, additional preservation options are available:

```json
{
    "name": "update_items",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Items",
    "upsert": true,
    "preserveTemporaryLocations": true,
    "preserveTemporaryLoanTypes": true,
    "preserveItemStatus": true,
    "files": [{"file_name": "updated_items.json"}]
}
```

```{note}
`preserveItemStatus` defaults to `true` to prevent accidentally overwriting circulation-managed statuses like "Checked out" or "In transit".
```

## Selective Field Patching

When you only want to update specific fields while preserving all others:

```json
{
    "name": "patch_barcodes",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Items",
    "upsert": true,
    "patchExistingRecords": true,
    "patchPaths": ["barcode", "copyNumber"],
    "files": [{"file_name": "items_with_corrected_barcodes.json"}]
}
```

This will only update the `barcode` and `copyNumber` fields, leaving all other fields untouched.

## ShadowInstances for ECS/Consortium

In ECS (Enhanced Consortial Support) environments, use `ShadowInstances` to create shadow copies of shared records in the appropriate member/data tenants. This will significantly improve performance when posting new holdings records that are attached to shared instances:

```json
{
    "name": "post_shadow_instances",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "ShadowInstances",
    "batchSize": 100,
    "files": [{"file_name": "shadow_instances.json"}]
}
```

```{warning}
ShadowInstances are for advanced ECS configurations. Consult FOLIO documentation on consortium data sharing before using this feature. While this does improve holding load performance, it will require an offline sync process against the mod_consortia.sharing_instances table to ensure future updates to the central instance propagate to the shadow instance. 
```

## Source Files

- **Location**: `iterations/<iteration>/results/`
- **Format**: Newline-delimited JSON (one record per line)
- **Content**: Transformed FOLIO objects from a prior transformation task

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `failed_<objectType>_<timestamp>.json` | Records that failed to post |
| Migration report | Statistics on posted/failed records |

## Examples

### Simple Instance Posting

```json
{
    "name": "post_instances",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Instances",
    "batchSize": 250,
    "files": [
        {"file_name": "folio_instances_transform_bibs.json"}
    ]
}
```

### Upsert with All Preservation Options

```json
{
    "name": "upsert_items",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Items",
    "batchSize": 100,
    "upsert": true,
    "preserveStatisticalCodes": true,
    "preserveAdministrativeNotes": true,
    "preserveTemporaryLocations": true,
    "preserveTemporaryLoanTypes": true,
    "preserveItemStatus": true,
    "rerunFailedRecords": true,
    "files": [
        {"file_name": "folio_items_transform.json"}
    ]
}
```

### Update Only Specific Fields

```json
{
    "name": "update_call_numbers",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Holdings",
    "upsert": true,
    "patchExistingRecords": true,
    "patchPaths": ["callNumber", "callNumberPrefix", "callNumberSuffix"],
    "files": [
        {"file_name": "holdings_with_corrected_call_numbers.json"}
    ]
}
```

### High-Volume Posting with Larger Batches

```json
{
    "name": "post_holdings",
    "migrationTaskType": "InventoryBatchPoster",
    "objectType": "Holdings",
    "batchSize": 500,
    "rerunFailedRecords": true,
    "files": [
        {"file_name": "folio_holdings_transform_mfhd.json"}
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json post_instances --base_folder ./
```

## Error Handling

InventoryBatchPoster includes robust error handling:

1. **Batch-level errors**: When a batch fails, the error is logged and the task continues with the next batch
2. **Retry mechanism**: When `rerunFailedRecords` is `true` (default), failed records are retried individually after the main run
3. **Failed record output**: All failed records are written to a JSON file for review and reprocessing

## See Also

- [BatchPoster](batch_poster) - General-purpose posting for all object types
- [MARCImportTask](marc_import) - Loading MARC records via Data Import
- [BibsTransformer](bibs_transformer) - Creating instances for posting
