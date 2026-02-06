# BatchPoster

Post transformed FOLIO objects to FOLIO using batch APIs. This is the general-purpose posting task for loading migration data.

## When to Use This Task

- Loading transformed instances, holdings, items to FOLIO
- Posting SRS (Source Record Storage) records
- Loading users, organizations, orders, and other object types
- Posting extradata files (notes, contacts, credentials)

```{tip}
For inventory objects (Instances, Holdings, Items), you can use either BatchPoster or [InventoryBatchPoster](inventory_batch_poster). Both offer the same field preservation and upsert capabilities. InventoryBatchPoster uses the `folio_data_import` library, which is where future development will be focused.
```

## Configuration

```json
{
    "name": "post_instances",
    "migrationTaskType": "BatchPoster",
    "objectType": "Instances",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_instances_transform_bibs.json"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"BatchPoster"` |
| `objectType` | string | Yes | Type of object to post. See [Object Types](#object-types). |
| `batchSize` | integer | No | Records per batch. Default varies by object type. |
| `files` | array | Yes | List of files to post from the `results/` folder |

### Object Types

| Object Type | API Endpoint | Notes |
|-------------|--------------|-------|
| `Instances` | `/instance-storage/batch/synchronous` | Batch posting |
| `Holdings` | `/holdings-storage/batch/synchronous` | Batch posting |
| `Items` | `/item-storage/batch/synchronous` | Batch posting |
| `SRS` | `/source-storage/batch/records` | SRS records |
| `Users` | `/user-import` | Uses user-import API |
| `Organizations` | `/organizations-storage/organizations` | One at a time |
| `CompositeOrders` | `/orders/composite-orders` | One at a time |
| `Extradata` | Various | Routes to appropriate API based on content |

## Upsert Mode

Enable upsert to update existing records:

```json
{
    "name": "post_instances",
    "migrationTaskType": "BatchPoster",
    "objectType": "Instances",
    "batchSize": 250,
    "upsert": true,
    "files": [
        {
            "file_name": "folio_instances_transform_bibs.json"
        }
    ]
}
```

```{note}
When using `upsert=True`, you may want to adjust concurrent requests. Set the `FOLIO_MAX_CONCURRENT_REQUESTS` environment variable (default: 10).
```

## Source Files

- **Location**: Files should be in `iterations/<iteration>/results/`
- **Format**: Newline-delimited JSON (one record per line)

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `failed_records_<task_name>_<timestamp>.txt` | Records that failed to post |
| Report files | Posting statistics and error logs |

## Examples

### Post Instances

```json
{
    "name": "post_instances",
    "migrationTaskType": "BatchPoster",
    "objectType": "Instances",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_instances_transform_bibs.json"
        }
    ]
}
```

### Post Holdings

```json
{
    "name": "post_holdings",
    "migrationTaskType": "BatchPoster",
    "objectType": "Holdings",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_holdings_transform_mfhd.json"
        },
        {
            "file_name": "folio_holdings_transform_csv_holdings.json"
        }
    ]
}
```

### Post Items

```json
{
    "name": "post_items",
    "migrationTaskType": "BatchPoster",
    "objectType": "Items",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_items_transform_items.json"
        }
    ]
}
```

### Post SRS Records

```json
{
    "name": "post_srs",
    "migrationTaskType": "BatchPoster",
    "objectType": "SRS",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_srs_instances_transform_bibs.json"
        }
    ]
}
```

### Post Users

```json
{
    "name": "post_users",
    "migrationTaskType": "BatchPoster",
    "objectType": "Users",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_users_transform_users.json"
        }
    ]
}
```

### Post Extradata

```json
{
    "name": "post_extradata",
    "migrationTaskType": "BatchPoster",
    "objectType": "Extradata",
    "batchSize": 1,
    "files": [
        {
            "file_name": "extradata_transform_organizations.extradata"
        }
    ]
}
```

### Upsert with Custom Concurrency

```shell
# Set environment variable before running
export FOLIO_MAX_CONCURRENT_REQUESTS=5
folio-migration-tools mapping_files/config.json post_instances --base_folder ./
```

```json
{
    "name": "post_instances",
    "migrationTaskType": "BatchPoster",
    "objectType": "Instances",
    "batchSize": 250,
    "upsert": true,
    "files": [
        {
            "file_name": "folio_instances_transform_bibs.json"
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json post_instances --base_folder ./
```

## See Also

- [InventoryBatchPoster](inventory_batch_poster) - Enhanced posting for inventory with field preservation
- [MARCImportTask](marc_import) - Loading MARC records via Data Import
- [UserImportTask](user_import) - Enhanced user posting
