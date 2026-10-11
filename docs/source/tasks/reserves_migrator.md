# ReservesMigrator

Add items to course reserves in FOLIO by linking existing items to courses.

## When to Use This Task

- Adding reserve items to courses after courses have been created
- Migrating reserve item assignments from legacy systems
- Linking items to course listings

## Configuration

```json
{
    "name": "migrate_reserves",
    "migrationTaskType": "ReservesMigrator",
    "courseReserveFilePath": {
        "file_name": "reserves.tsv"
    }
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"ReservesMigrator"` |
| `courseReserveFilePath` | object | Yes | File definition with `file_name` for the reserves data file |
| `skipBarcodePrevalidation` | boolean | No | Skip pre-validation of item barcodes against FOLIO. Default: false |

## Source Data Requirements

- **Location**: Place the file in `iterations/<iteration>/source_data/reserves/`
- **Format**: Tab-separated (TSV) with header row
- **Prerequisites**:
  - Run [CoursesMigrator](courses_migrator) first, and post its output with [BatchPoster](batch_poster). CoursesMigrator only writes the course listings, courses and instructors to a file. ReservesMigrator posts to the course listings in FOLIO, so they must exist already
  - Items must exist in FOLIO (post items first)

### Required Columns

The reserves file must contain:

| Column | Description |
|--------|-------------|
| `legacy_identifier` | The legacy identifier of the course, as used by [CoursesMigrator](courses_migrator). The course listing ID is generated from it |
| `item_barcode` | Barcode of the item to place on reserve |

The task halts if either column is missing.

The course listing ID is generated the same way as in CoursesMigrator, using the library configuration's `useGatewayUrlForUuids` and ECS settings. Run ReservesMigrator with the same library configuration as the CoursesMigrator run that created the course listings, or the reserves will not find their course listings.

### Example Data

```text
legacy_identifier	item_barcode
CHEM101-F24	1234567890
CHEM101-F24	0987654321
```

## What Gets Posted

ReservesMigrator creates only the reserve records. Course listings, courses and instructors are created by [CoursesMigrator](courses_migrator).

For each row, the task posts `{id, courseListingId, copiedItem: {barcode}}` to `/coursereserves/courselistings/{courseListingId}/reserves`. The reserve `id` is a random UUID, so it is not stable between runs.

mod-courses then does the following for every reserve:

- Looks up the item by barcode, and fills in the rest of the `copiedItem` data (title, contributors, locations, call number and so on) from the item, its holdings record and its instance. These fields are not taken from the source file.
- Updates the item record in inventory. If the course listing has a location, it is set as the item's temporary location. Deleting the reserve later clears the item's temporary location.
- Rejects the reserve with a 422 if the same item is already reserved to the same course listing. A repeated `legacy_identifier` and `item_barcode` pair in the source file fails on its second occurrence.
- Fails the request if no item has the barcode.

Reserve start and end dates are inherited from the term of the course listing. Processing status, copyright tracking and temporary loan type are not set by this task.

## Pre-validation

By default, the item barcodes are checked against FOLIO before any reserves are posted. Barcodes are matched case-insensitively. Reserves whose barcode does not match an item in FOLIO are not posted. They are written to the failed records file and counted as "Reserves discarded by barcode pre-validation" in the migration report.

Items are looked up in batches. If a batch cannot be fetched, it is retried once. If the retry fails too, the task halts, because a lookup that failed cannot be told apart from a missing item.

Set `skipBarcodePrevalidation` to `true` to bypass this step.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `failed_records_<task_name>_<timestamp>.txt` | Reserves that were discarded by pre-validation, or failed to post |
| Report files | Migration statistics and error logs |

### Failed Records File

The failed records file is tab-separated, with the same `legacy_identifier` and `item_barcode` columns as the source file. Once the cause is fixed, the file can be used as the source file for another run.

The number of rows is reported as "Failed reserves" in the migration report.

## Examples

### Basic Reserves Migration

```json
{
    "name": "migrate_reserves",
    "migrationTaskType": "ReservesMigrator",
    "courseReserveFilePath": {
        "file_name": "reserves.tsv"
    }
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json migrate_reserves --base_folder ./
```

## See Also

- [CoursesMigrator](courses_migrator) - Creating courses first
- [BatchPoster](batch_poster) - Posting the course listings, courses and instructors
- [ItemsTransformer](items_transformer) - Creating items first
