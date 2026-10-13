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
| `allowDueDateBeforeOutDate` | boolean | No | Load loans whose `due_date` is earlier than their `out_date`, keeping the legacy dates as given. Default: false. See [Due date before out date](#due-date-before-out-date) |
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

Item barcodes from the loan data are validated in batches against FOLIO's `/item-storage/items/retrieve` endpoint. Barcodes are matched case-insensitively. Barcodes that do not match an existing item are logged and the corresponding loans are set aside.

### Patron barcode validation

Patron barcodes (including proxy patron barcodes) are validated individually against FOLIO's `/users` endpoint using the tenant's configured preferred patron identifier fields (from `mod-configuration`). This ensures that lookup barcodes are resolved to the patron's actual FOLIO barcode before checkout.

Patrons that cannot be found, or where a lookup barcode matches multiple users, are logged and the corresponding loans are set aside.

### Lookup failures

If an item batch or a patron lookup fails, for example because FOLIO is unavailable, it is retried once. If the retry fails too, the task halts. A lookup that failed cannot be told apart from a missing record, so carrying on could set aside loans whose records exist.

### Validation outcomes

- **Missing items**: Loans with item barcodes not found in FOLIO are discarded
- **Missing patrons**: Loans with patron barcodes not found in FOLIO are discarded
- **Invalid dates**: Loans where `due_date` precedes `out_date` are discarded, unless `allowDueDateBeforeOutDate` is set. Loans with missing dates are always discarded
- **Barcode rewriting**: If a patron is found via a non-barcode identifier (e.g., `externalSystemId`), the loan's patron barcode is rewritten to the patron's actual FOLIO barcode for checkout

Failed records are saved to `failed_records_<task_name>_<timestamp>.txt`.

### Due date before out date

By default, a loan whose `due_date` is earlier than its `out_date` is discarded, since this usually points to mangled dates or timezone problems in the source data. Such a loan is a valid state in FOLIO and in many legacy systems, though. If your data legitimately contains them, set `allowDueDateBeforeOutDate` to `true`:

```json
{
    "name": "migrate_loans",
    "migrationTaskType": "LoansMigrator",
    "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
    "openLoansFiles": [{"file_name": "loans.tsv"}],
    "allowDueDateBeforeOutDate": true
}
```

With the option set:

- The legacy `due_date` and `out_date` are loaded exactly as given. The midnight adjustment that is applied to same-day loans is not applied.
- FOLIO refuses a check-out whose due date is not after the loan date, so these loans are checked out with a placeholder due date (one day after `out_date`). The legacy dates are then set in an update right after the check-out. If that update fails, the loan is listed in the follow-up file as `update_loan`.
- Each such loan is written to the data issues log.

Loans with the same due and out timestamp, or with missing or unparseable dates, are still discarded.

The migration report counts these loans in all cases, also when the option is off:

| Report line | Meaning |
|-------------|---------|
| Loans with due date before out date | Loans in the source data with `due_date` earlier than `out_date`, whether they were discarded or not |
| Updated dates for loan with due date before out date | Loans that got their legacy dates after check-out |
| Failed to update dates for loan with due date before out date | Loans where that update failed. See the follow-up file |

Inverted loans that also have a renewal count are counted under "Updated renewal count for loan" and "Failed to update renewal count for loan" instead. They are listed in "Applied legacy dates to loan with due date before out date" under Details.

## How It Works

1. **Check-out via API**: Creates loans via `/circulation/check-out-by-barcode` with all overrides enabled
2. **Update loan**: Updates `renewalCount` for loans with renewals, and `loanDate` and `dueDate` for loans with due date before out date. Other loans get their dates at check-out
3. **Handle special statuses**: If item status prevents checkout (Aged to lost, Declared lost, Claimed returned), temporarily changes status to Available
4. **Handle inactive patrons**: If patron is inactive, temporarily activates for checkout then deactivates
5. **Record follow-ups**: If a step after the checkout fails, the loan is listed in the follow-up file instead of the failed records file

```{tip}
Migrate items with loanable statuses (like "Available" or "Checked out") rather than non-loanable statuses to improve performance. The task can handle non-loanable statuses but it's significantly slower.
```

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `failed_records_<task_name>_<timestamp>.txt` | Records that failed validation or posting |
| `loans_needing_followup_<task_name>_<timestamp>.tsv` | Checked out loans that need a manual fix. Only created when there are any |
| Report files | Migration statistics and error logs |

### Follow-up File

A loan is listed in the follow-up file when the checkout succeeded but a later step failed. Do not re-run these loans: they already exist in FOLIO, and a re-run would try to check them out again. Fix them by hand, or with a script, using the columns below.

| Column | Description |
|--------|-------------|
| `item_barcode` | Item barcode from the source data |
| `patron_barcode` | Patron barcode from the source data |
| `loan_id` | FOLIO loan UUID, when known |
| `followup_type` | The step that failed (see below) |
| `detail` | What was not done, with the values to apply |

| `followup_type` | Meaning |
|-----------------|---------|
| `update_loan` | The loan's `renewalCount`, `dueDate` and `loanDate` could not be updated after check-out |
| `declare_lost` | The loan could not be declared lost |
| `claim_returned` | The loan could not be set to Claimed returned |
| `set_item_status` | The item status could not be set to `next_item_status` |
| `restore_item_status` | The checkout failed after the item was set to Available, and the item's original status could not be put back |
| `deactivate_user` | An inactive patron was activated for the checkout and could not be deactivated again |

The count of rows is reported as "Loan follow-up actions needed" in the migration report.

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

- If the item cannot be set to "Available", no checkout is attempted and the loan is written to the failed records file.
- If the checkout fails after the item was set to "Available", the item's original status is put back. If that also fails, the loan is listed in the follow-up file as `restore_item_status`.

### Inactive Patrons

Inactive patrons will be temporarily activated for the checkout, then deactivated. This is handled automatically.

- While the patron is active, their `expirationDate` is set to one day in the future. On deactivation the original `expirationDate` is restored, or removed if the patron had none.
- If the checkout after activation fails for a reason that can be handled, such as the item status (for example, Claimed returned), it is handled as described above before the patron is deactivated.
- If the checkout after activation fails because the patron is still inactive, it is not retried again, and the loan is written to the failed records file.
- If the patron cannot be found or activated, no checkout is attempted and the loan is written to the failed records file.
- If the patron cannot be deactivated again, it is logged as a data issue and listed in the follow-up file as `deactivate_user`. Check these patrons in FOLIO, since they are still active.

## See Also

- [ItemsTransformer](items_transformer) - Creating items first
- [UserTransformer](user_transformer) - Creating users first
- [RequestsMigrator](requests_migrator) - Migrating requests
