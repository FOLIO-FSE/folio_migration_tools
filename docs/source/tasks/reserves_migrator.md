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
    "reservesFile": {
        "file_name": "reserves.tsv"
    }
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"ReservesMigrator"` |
| `reservesFile` | object | Yes | File definition with `file_name` for the reserves data file |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/reserves/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row
- **Prerequisites**:
  - Run [CoursesMigrator](courses_migrator) first
  - Items must exist in FOLIO (post items first)

### Required Columns

The reserves file must contain:

| Column | Description |
|--------|-------------|
| `item_barcode` | Barcode of the item to place on reserve |
| `course_listing_id` | UUID of the course listing |
| `term_id` | UUID of the term (optional, for new course listings) |

### Example Data

```text
item_barcode	course_listing_id
1234567890	a1b2c3d4-e5f6-7890-abcd-ef1234567890
0987654321	a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| Report files | Migration statistics and error logs |

## Examples

### Basic Reserves Migration

```json
{
    "name": "migrate_reserves",
    "migrationTaskType": "ReservesMigrator",
    "reservesFile": {
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
- [ItemsTransformer](items_transformer) - Creating items first
