# RequestsMigrator

Migrate open requests from legacy systems into FOLIO using the circulation APIs.

## When to Use This Task

- Migrating active hold/recall requests from legacy ILS
- Preserving request queue positions
- Creating requests via FOLIO's circulation APIs

```{attention}
This task creates real circulation transactions. Ensure items and users have been posted to FOLIO before running.
```

## Configuration

```json
{
    "name": "migrate_requests",
    "migrationTaskType": "RequestsMigrator",
    "openRequestsFile": {
        "file_name": "requests.tsv"
    },
    "startingRow": 1,
    "itemFiles": [
        {
            "file_name": "folio_items_transform_items.json"
        }
    ],
    "patronFiles": [
        {
            "file_name": "folio_users_transform_users.json"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"RequestsMigrator"` |
| `openRequestsFile` | object | Yes | File definition with `file_name` for request data |
| `startingRow` | integer | No | Row number to start processing. Default: 1 |
| `itemFiles` | array | No | Transformed item files for pre-validation |
| `patronFiles` | array | No | Transformed user files for pre-validation |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/requests/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with specific required columns

### Required Columns

| Column | Description |
|--------|-------------|
| `item_barcode` | Barcode of the requested item |
| `patron_barcode` | Barcode of the requesting patron |
| `request_type` | Type of request (Hold, Recall, Page) |
| `request_date` | Date the request was placed |

### Optional Columns

| Column | Description |
|--------|-------------|
| `pickup_service_point_id` | UUID of the pickup location |
| `expiration_date` | Request expiration date |
| `request_level` | Item or Title level request |
| `fulfillment_preference` | Hold Shelf or Delivery |

### Example Data

```text
item_barcode	patron_barcode	request_type	request_date	pickup_service_point_id
1234567890	P001234	Hold	2024-11-01	a77b55e7-f9f3-40a1-83e0-241bc606a826
0987654321	P005678	Recall	2024-11-15	a77b55e7-f9f3-40a1-83e0-241bc606a826
```

## Pre-validation

If `itemFiles` and/or `patronFiles` are specified, the task validates requests before attempting to create them:

- **Missing items**: Rows with item barcodes not found in item files are set aside
- **Missing patrons**: Rows with patron barcodes not found in user files are set aside

Failed records are saved for review.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `failed_records_<task_name>_<timestamp>.txt` | Records that failed validation or posting |
| Report files | Migration statistics and error logs |

## Examples

### Basic Request Migration

```json
{
    "name": "migrate_requests",
    "migrationTaskType": "RequestsMigrator",
    "openRequestsFile": {
        "file_name": "requests.tsv"
    }
}
```

### With Pre-validation

```json
{
    "name": "migrate_requests",
    "migrationTaskType": "RequestsMigrator",
    "openRequestsFile": {
        "file_name": "requests.tsv"
    },
    "itemFiles": [
        {
            "file_name": "folio_items_transform_items.json"
        }
    ],
    "patronFiles": [
        {
            "file_name": "folio_users_transform_users.json"
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json migrate_requests --base_folder ./
```

## See Also

- [ItemsTransformer](items_transformer) - Creating items first
- [UserTransformer](user_transformer) - Creating users first
- [LoansMigrator](loans_migrator) - Migrating loans
