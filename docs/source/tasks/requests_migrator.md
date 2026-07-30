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
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openRequestsFile": {
        "file_name": "requests.tsv"
    },
    "startingRow": 1,
    "skipBarcodePrevalidation": false
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"RequestsMigrator"` |
| `fallbackServicePointId` | string | Yes | Service point UUID or code to use when a request has no pickup service point |
| `openRequestsFile` | object | Yes | File definition with `file_name` for request data |
| `servicePointMapFileName` | string | No | Filename of a TSV mapping file for translating legacy service point codes to FOLIO codes |
| `startingRow` | integer | No | Row number to start processing. Default: 1 |
| `skipBarcodePrevalidation` | boolean | No | Skip pre-validation of patron and item barcodes against FOLIO. Default: false |

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
| `pickup_servicepoint_id` | Pickup location service point (UUID or code) |
| `expiration_date` | Request expiration date |
| `request_level` | Item or Title level request |
| `fulfillment_preference` | Hold Shelf or Delivery |

### Service Point Values

The `pickup_servicepoint_id` column and `fallbackServicePointId` parameter accept either:

- A **UUID** of a FOLIO service point (e.g., `a77b55e7-f9f3-40a1-83e0-241bc606a826`)
- A **code** of a FOLIO service point (e.g., `circ-desk`)

All service point values are validated against FOLIO before barcode pre-validation begins. If any value cannot be resolved, the task halts with an error.

### Service Point Mapping File

When `servicePointMapFileName` is provided, the task loads a TSV file from the `mapping_files` folder that maps legacy service point codes to FOLIO service point codes.

**Format**: Tab-separated with headers `service_point_id` and `folio_code`:

```text
service_point_id	folio_code
MAIN_CIRC	circ-desk
BRANCH1	branch-desk
*	circ-desk
```

- `service_point_id` — The legacy code that appears in your source data
- `folio_code` — The corresponding FOLIO service point code
- `*` — Required wildcard row (must be present for file validation, but is not used as a runtime fallback; unmatched codes will produce an error)

### Example Data

```text
item_barcode	patron_barcode	request_type	request_date	pickup_servicepoint_id
1234567890	P001234	Hold	2024-11-01	circ-desk
0987654321	P005678	Recall	2024-11-15	a77b55e7-f9f3-40a1-83e0-241bc606a826
```

## Pre-validation

The task performs several validation steps before creating requests.

### Service point validation

All unique service point values (from source data and the fallback) are validated against FOLIO before any barcode checking occurs. Both UUIDs and codes are verified to exist. If any service point cannot be found, the task halts immediately. Service point codes are resolved to UUIDs for the circulation API.

### Barcode validation

By default, the task validates request barcodes directly against FOLIO before attempting to create requests.

- **Missing items**: Rows with item barcodes not found in FOLIO are set aside
- **Missing patrons**: Rows with patron barcodes not found in FOLIO are set aside

Failed records are saved for review.

Set `skipBarcodePrevalidation` to `true` to bypass this step.

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
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openRequestsFile": {
        "file_name": "requests.tsv"
    }
}
```

### Skip Pre-validation

```json
{
    "name": "migrate_requests",
    "migrationTaskType": "RequestsMigrator",
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openRequestsFile": {
        "file_name": "requests.tsv"
    },
    "skipBarcodePrevalidation": true
}
```

### Using Service Point Codes with a Mapping File

```json
{
    "name": "migrate_requests",
    "migrationTaskType": "RequestsMigrator",
    "fallbackServicePointId": "circ-desk",
    "servicePointMapFileName": "service_point_map.tsv",
    "openRequestsFile": {
        "file_name": "requests.tsv"
    }
}
```

In this example, both the `fallbackServicePointId` and values in the source data's `pickup_servicepoint_id` column can be FOLIO service point codes. The `service_point_map.tsv` file translates legacy codes to FOLIO codes.

## Running the Task

```shell
folio-migration-tools mapping_files/config.json migrate_requests --base_folder ./
```

## See Also

- [ItemsTransformer](items_transformer) - Creating items first
- [UserTransformer](user_transformer) - Creating users first
- [LoansMigrator](loans_migrator) - Migrating loans
