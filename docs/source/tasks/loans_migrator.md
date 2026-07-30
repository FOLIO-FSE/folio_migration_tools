# LoansMigrator

Migrate open loans from legacy systems into FOLIO using the circulation APIs. Unlike other tasks, this task creates transactions in real-time rather than transforming data for later posting.

## When to Use This Task

- Migrating active/open loans from legacy ILS
- Preserving loan due dates and renewal counts
- Creating loans via FOLIO's circulation APIs (with policy validation)

```{attention}
This task creates real circulation transactions and **can generate thousands of patron notices**. We strongly recommend disabling SMTP before running this task.
```

## Configuration

```json
{
    "name": "migrate_loans",
    "migrationTaskType": "LoansMigrator",
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openLoansFiles": [
        {
            "file_name": "loans.tsv",
            "service_point_id": "a77b55e7-f9f3-40a1-83e0-241bc606a826"
        }
    ],
    "startingRow": 1
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"LoansMigrator"` |
| `fallbackServicePointId` | string | Yes | Service point UUID or code for check-out transactions when not specified per-loan |
| `openLoansFiles` | array | Yes | List of loan data files with optional per-file service point |
| `servicePointMapFileName` | string | No | Filename of a TSV mapping file for translating legacy service point codes to FOLIO codes |
| `startingRow` | integer | No | Row number to start processing (for resuming). Default: 1 |
| `skipBarcodePrevalidation` | boolean | No | Skip pre-validation of patron and item barcodes against FOLIO. Default: false |
| `itemFiles` | array | No | **Deprecated.** No longer used for pre-validation. |
| `patronFiles` | array | No | **Deprecated.** No longer used for pre-validation. |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/loans/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with specific required columns

### Required Columns

| Column | Description |
|--------|-------------|
| `item_barcode` | Barcode of the item being loaned |
| `patron_barcode` | Barcode of the patron with the loan |
| `due_date` | Due date of the loan |
| `out_date` | Check-out date of the loan |

### Optional Columns

| Column | Description |
|--------|-------------|
| `proxy_patron_barcode` | Barcode of proxy borrower (if applicable) |
| `renewal_count` | Number of times the loan has been renewed |
| `next_item_status` | Item status to set after loan is created |
| `service_point_id` | Override service point for this specific loan (UUID or code) |

### Service Point Values

The `service_point_id` column and `fallbackServicePointId` parameter accept either:

- A **UUID** of a FOLIO service point (e.g., `a77b55e7-f9f3-40a1-83e0-241bc606a826`)
- A **code** of a FOLIO service point (e.g., `circ-desk`)

All service point values are validated against FOLIO before loan processing begins. If any value cannot be resolved, the task halts with an error.

### Service Point Mapping File

When `servicePointMapFileName` is provided, the task loads a TSV file from the `mapping_files` folder that maps legacy service point codes to FOLIO service point codes. This is useful when your legacy system uses different codes than FOLIO.

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
item_barcode	patron_barcode	due_date	out_date	renewal_count
1234567890	P001234	2024-12-15	2024-11-01	2
0987654321	P005678	2024-12-20	2024-11-15	0
```

### Date Formats

Dates should be in ISO 8601 format:
- `2024-12-15` (date only)
- `2024-12-15T10:30:00` (with time)
- `2024-12-15T10:30:00-05:00` (with timezone)

## Pre-validation

The task performs several validation steps before creating loans.

### Service point validation

All unique service point values (from source data and the fallback) are validated against FOLIO before any barcode checking occurs. Both UUIDs and codes are verified to exist. If any service point cannot be found, the task halts immediately. Service point codes are resolved to UUIDs for the circulation API.

### Barcode validation

By default, the task validates patron and item barcodes directly against the FOLIO tenant before attempting to create loans. This can be disabled by setting `skipBarcodePrevalidation` to `true`.

### Item barcode validation

Item barcodes from the loan data are validated in batches against FOLIO's `/item-storage/items/retrieve` endpoint. Barcodes that do not match an existing item are logged and the corresponding loans are set aside.

### Patron barcode validation

Patron barcodes (including proxy patron barcodes) are validated individually against FOLIO's `/users` endpoint using the tenant's configured preferred patron identifier fields (from `mod-configuration`). This ensures that lookup barcodes are resolved to the patron's actual FOLIO barcode before checkout.

Patrons that cannot be found, or where a lookup barcode matches multiple users, are logged and the corresponding loans are set aside.

### Validation outcomes

- **Missing items**: Loans with item barcodes not found in FOLIO are discarded
- **Missing patrons**: Loans with patron barcodes not found in FOLIO are discarded
- **Invalid dates**: Loans where `due_date` precedes `out_date` are discarded
- **Barcode rewriting**: If a patron is found via a non-barcode identifier (e.g., `externalSystemId`), the loan's patron barcode is rewritten to the patron's actual FOLIO barcode for checkout

Failed records are saved to `failed_records_<task_name>_<timestamp>.txt`.

## How It Works

1. **Check-out via API**: Creates loans via `/circulation/check-out-by-barcode` with all overrides enabled
2. **Update dates**: Updates `loanDate` and `dueDate` to match original values
3. **Handle special statuses**: If item status prevents checkout (Aged to lost, Declared lost, Claimed returned), temporarily changes status to Available
4. **Handle inactive patrons**: If patron is inactive, temporarily activates for checkout then deactivates

```{tip}
Migrate items with loanable statuses (like "Available" or "Checked out") rather than non-loanable statuses to improve performance. The task can handle non-loanable statuses but it's significantly slower.
```

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `failed_records_<task_name>_<timestamp>.txt` | Records that failed validation or posting |
| Report files | Migration statistics and error logs |

## Examples

### Basic Loan Migration

```json
{
    "name": "migrate_loans",
    "migrationTaskType": "LoansMigrator",
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openLoansFiles": [
        {
            "file_name": "loans.tsv"
        }
    ]
}
```

### Skipping Pre-validation

```json
{
    "name": "migrate_loans",
    "migrationTaskType": "LoansMigrator",
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openLoansFiles": [
        {
            "file_name": "loans.tsv"
        }
    ],
    "skipBarcodePrevalidation": true
}
```

### Multiple Files with Different Service Points

```json
{
    "name": "migrate_loans",
    "migrationTaskType": "LoansMigrator",
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openLoansFiles": [
        {
            "file_name": "main_library_loans.tsv",
            "service_point_id": "a77b55e7-f9f3-40a1-83e0-241bc606a826"
        },
        {
            "file_name": "branch_loans.tsv",
            "service_point_id": "b88c66f8-g4h5-51b2-94f1-352de707b937"
        }
    ]
}
```

### Resuming from a Specific Row

```json
{
    "name": "migrate_loans",
    "migrationTaskType": "LoansMigrator",
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "startingRow": 5001,
    "openLoansFiles": [
        {
            "file_name": "loans.tsv"
        }
    ]
}
```

### Using Service Point Codes with a Mapping File

```json
{
    "name": "migrate_loans",
    "migrationTaskType": "LoansMigrator",
    "fallbackServicePointId": "circ-desk",
    "servicePointMapFileName": "service_point_map.tsv",
    "openLoansFiles": [
        {
            "file_name": "loans.tsv"
        }
    ]
}
```

In this example, both the `fallbackServicePointId` and values in the source data's `service_point_id` column can be FOLIO service point codes. The `service_point_map.tsv` file translates legacy codes to FOLIO codes.

## Running the Task

```shell
folio-migration-tools mapping_files/config.json migrate_loans --base_folder ./
```

## Troubleshooting

### SMTP Warning

The task checks if SMTP is disabled before starting. If not, you'll get a 10-second warning before proceeding.

### Item Status Issues

Items in non-loanable statuses will be temporarily changed to "Available", have the loan created, then have their status reset. This is logged but significantly slows processing.

### Inactive Patrons

Inactive patrons will be temporarily activated for the checkout, then deactivated. This is handled automatically.

## See Also

- [ItemsTransformer](items_transformer) - Creating items first
- [UserTransformer](user_transformer) - Creating users first
- [RequestsMigrator](requests_migrator) - Migrating requests
