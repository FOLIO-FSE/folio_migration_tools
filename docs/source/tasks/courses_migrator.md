# CoursesMigrator

Import course reserve courses into FOLIO from JSON data files.

## When to Use This Task

- Migrating course reserve courses from legacy systems
- Setting up course structures before adding reserve items
- Importing courses with terms, departments, and instructors

## Configuration

```json
{
    "name": "migrate_courses",
    "migrationTaskType": "CoursesMigrator",
    "courseFile": {
        "file_name": "courses.json"
    }
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"CoursesMigrator"` |
| `courseFile` | object | Yes | File definition with `file_name` for the courses JSON file |

## Source Data Requirements

- **Location**: Place JSON files in `iterations/<iteration>/source_data/courses/`
- **Format**: JSON with course data structured for FOLIO's course reserves API

### Course Data Format

The courses file should contain a JSON array or newline-delimited JSON objects with course data:

```json
{
    "name": "Introduction to Library Science",
    "courseNumber": "LIS 101",
    "sectionName": "Section A",
    "termId": "42093be3-d1e7-4bb6-b2b9-18e153d109b2",
    "departmentId": "7532e5ab-9812-496c-ab77-4fbb6a7e5dbf",
    "courseListingId": "existing-listing-uuid-if-any"
}
```

### Prerequisites

Ensure the following reference data exists in FOLIO before running:
- **Terms** (`/coursereserves/terms`) - Academic terms like "Fall 2024"
- **Departments** (`/coursereserves/departments`) - Academic departments

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| Report files | Migration statistics and error logs |

## Examples

### Basic Course Migration

```json
{
    "name": "migrate_courses",
    "migrationTaskType": "CoursesMigrator",
    "courseFile": {
        "file_name": "courses.json"
    }
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json migrate_courses --base_folder ./
```

## Next Steps

After migrating courses:
1. **Add Reserves**: Use [ReservesMigrator](reserves_migrator) to add items to courses

## See Also

- [ReservesMigrator](reserves_migrator) - Adding items to courses
