# CoursesMigrator

Transform legacy course data into FOLIO Course Reserves courses, course listings and instructors.

## When to Use This Task

- Migrating courses from a legacy system that will have items on reserve
- Setting up course structures before adding reserve items with [ReservesMigrator](reserves_migrator)
- Importing courses together with their terms, departments, instructors and notes

## How Course Reserves Are Migrated

Migrating course reserves takes three tasks, run in this order:

1. **CoursesMigrator** transforms each legacy course row into a course listing, a course, and zero or more instructors. It does not post anything to FOLIO. It writes the objects to an extradata file in the `results/` folder.
2. **[BatchPoster](batch_poster)** posts that extradata file to FOLIO, with `objectType` set to `Extradata`.
3. **[ReservesMigrator](reserves_migrator)** posts the reserve records that link items to the course listings.

In FOLIO, a course listing holds the data shared by a set of cross-listed courses (term, location, registrar ID). Each course belongs to exactly one course listing. Instructors and reserves exist only within a course listing. CoursesMigrator creates one course listing and one course for every row in the source file.

## Configuration

```json
{
    "name": "migrate_courses",
    "migrationTaskType": "CoursesMigrator",
    "compositeCourseMapPath": "courses_mapping.json",
    "coursesFile": {
        "file_name": "courses.tsv"
    },
    "termsMapPath": "terms_map.tsv",
    "departmentsMapPath": "departments_map.tsv"
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"CoursesMigrator"` |
| `compositeCourseMapPath` | string | Yes | Name of the course mapping file, in the `mapping_files` folder |
| `coursesFile` | object | Yes | File definition with `file_name` for the legacy courses file |
| `termsMapPath` | string | Yes | Name of the terms reference data mapping file, in the `mapping_files` folder |
| `departmentsMapPath` | string | Yes | Name of the departments reference data mapping file, in the `mapping_files` folder |
| `lookUpInstructor` | boolean | No | Look up instructors in FOLIO users. Default: false. See [Instructors](#instructors) |

## Source Data Requirements

- **Location**: Place the file in `iterations/<iteration>/source_data/courses/`
- **Format**: Tab-separated (`.tsv`) or comma-separated with a header row. A file name ending in `tsv` is read as tab-separated, any other name as comma-separated. The file is read as UTF-8, with or without a byte order mark.
- **One row per course**: Every row becomes one course and one course listing

### Prerequisites

The following must exist in FOLIO before you run [BatchPoster](batch_poster) on the output. CoursesMigrator maps your legacy values to existing records by name, and does not create them:

- **Terms** (`/coursereserves/terms`), for example "Fall 2024"
- **Departments** (`/coursereserves/departments`)
- **Course types**, **locations** and **service points**, if you map them in the course mapping file. Map these by UUID. There is no reference data mapping file for them
- **Note types**, if you migrate notes

## Mapping File

The course mapping file is a JSON file in the same format as other mapping files (see [Mapping File Based Mapping](../mapping_file_based_mapping)). Its FOLIO fields are prefixed with the object they belong to:

| FOLIO field prefix | FOLIO object | Required fields in FOLIO |
|--------------------|--------------|--------------------------|
| `courselisting.` | Course listing | `termId` |
| `course.` | Course | `name`, `departmentId` |
| `instructors[n].` | Instructor | None |
| `notes[n].` | Note, linked to the course | See the [notes mapping](../mapping_file_based_mapping) |

The mapping file must also contain the `legacyIdentifier` field. It is used to generate the UUIDs of the course, course listing and instructors. If you map it from more than one column, the values are joined with a space. The row fails if the identifier is empty.

```{important}
ReservesMigrator finds the course listing again by regenerating its UUID from the `legacy_identifier` column in the reserves file. The value in that column must be identical to the legacy identifier produced here.
```

### Example Mapping

```json
{
  "data": [
    {
      "folio_field": "legacyIdentifier",
      "legacy_field": "RECORD #(COURSE)",
      "value": "",
      "description": ""
    },
    {
      "folio_field": "courselisting.termId",
      "legacy_field": "",
      "value": "",
      "description": "Mapped with the terms map file"
    },
    {
      "folio_field": "courselisting.courseTypeId",
      "legacy_field": "",
      "value": "428ea311-c2c2-4805-a21e-1d198c7bcd58",
      "description": ""
    },
    {
      "folio_field": "courselisting.locationId",
      "legacy_field": "LOCATION",
      "value": "",
      "description": ""
    },
    {
      "folio_field": "course.name",
      "legacy_field": "COURSE",
      "value": "",
      "description": ""
    },
    {
      "folio_field": "course.description",
      "legacy_field": "COURSE NOTE",
      "value": "",
      "description": ""
    },
    {
      "folio_field": "course.departmentId",
      "legacy_field": "DEPT",
      "value": "",
      "description": "Mapped with the departments map file"
    },
    {
      "folio_field": "instructors[0].name",
      "legacy_field": "INSTRUCTOR",
      "value": "",
      "description": ""
    }
  ]
}
```

The available properties are those in the mod-courses [course](https://github.com/folio-org/mod-courses/blob/master/ramls/course.json), [course listing](https://github.com/folio-org/mod-courses/blob/master/ramls/courselisting.json) and [instructor](https://github.com/folio-org/mod-courses/blob/master/ramls/instructor.json) schemas.

Properties you do not map are counted under "UnmappedProperties" in the migration report. They do not fail the row.

### Course Listing Location

If you set `courselisting.locationId`, then whenever a reserve is posted to the course listing, FOLIO sets the temporary location of the reserved item to that location. Deleting the reserve clears the item's temporary location. Leave the field unmapped if reserved items should stay where they are.

## Terms and Departments Maps

Terms and departments are mapped with [reference data mapping files](../reference_data_mapping). Both files are required. The legacy columns must have the names of the legacy fields mapped to `courselisting.termId` and `course.departmentId` in the course mapping file. The FOLIO column is `folio_name`, which holds the name of the term or department in FOLIO. A row with `*` in the legacy columns is the fallback.

Terms map, where the term is determined from two legacy columns:

```text
BEGIN DATE	END DATE	folio_name
01-10-2022	05-06-2022	Fall 2022
*	*	Summer 2022
```

Departments map:

```text
DEPT	folio_name
dep1	Department One
*	Department Fallback
```

## Instructors

Instructors are created from the `instructors[n]` mappings, and each is linked to the course listing of its row. The instructor's `userId` is optional in FOLIO.

By default, the task creates the instructor from the mapped values only. If `lookUpInstructor` is `true` and `instructors[n].userId` is mapped, the value is used as the `externalSystemId` of a FOLIO user:

- If a user is found, the instructor's `userId` is replaced with the FOLIO user ID, and the user's barcode and patron group are copied to the instructor
- If no user is found, `userId` is removed from the instructor. The instructor is still created, with the other mapped values, such as `name`

Rows without any instructor are counted as "Missing Instructors" in the migration report.

## Notes

Notes mapped under `notes[n]` are written to the extradata file, linked to the course, with the domain `courses`. They are posted to FOLIO by BatchPoster along with the other objects.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `extradata_<timestamp>_<task_name>.extradata` | The course listings, courses, instructors and notes, one per line |
| Report files | Migration statistics and error logs |

Each line of the extradata file holds the object type and the JSON of the object, separated by a tab. For every row, the course listing is written first, then the course, then the instructors, so that BatchPoster posts a course listing before the objects that refer to it.

## Posting to FOLIO

Post the extradata file with BatchPoster:

```json
{
    "name": "post_courses",
    "migrationTaskType": "BatchPoster",
    "objectType": "Extradata",
    "batchSize": 1,
    "files": [
        {
            "file_name": "extradata_<timestamp>_migrate_courses.extradata"
        }
    ]
}
```

BatchPoster posts the objects one at a time, in file order, to these endpoints:

| Object | Endpoint |
|--------|----------|
| Course listing | `/coursereserves/courselistings` |
| Course | `/coursereserves/courses` |
| Instructor | `/coursereserves/courselistings/{courseListingId}/instructors` |
| Note | `/notes` |

The UUIDs are generated from the legacy identifier, so the same data produces the same IDs on every run. Posting the file again does not create duplicates. FOLIO rejects the objects that already exist, and BatchPoster does not add those to its failed records file. Use the same library configuration for the courses and reserves runs, including the gateway URL and ECS settings, because they are part of the UUID generation.

## Examples

### Basic Course Migration

```json
{
    "name": "migrate_courses",
    "migrationTaskType": "CoursesMigrator",
    "compositeCourseMapPath": "courses_mapping.json",
    "coursesFile": {
        "file_name": "courses.tsv"
    },
    "termsMapPath": "terms_map.tsv",
    "departmentsMapPath": "departments_map.tsv"
}
```

### With Instructor Lookup

```json
{
    "name": "migrate_courses",
    "migrationTaskType": "CoursesMigrator",
    "compositeCourseMapPath": "courses_mapping.json",
    "coursesFile": {
        "file_name": "courses.tsv"
    },
    "termsMapPath": "terms_map.tsv",
    "departmentsMapPath": "departments_map.tsv",
    "lookUpInstructor": true
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json migrate_courses --base_folder ./
```

## Next Steps

After migrating courses:
1. **Post the courses**: Run [BatchPoster](batch_poster) on the extradata file, as described above
2. **Add Reserves**: Use [ReservesMigrator](reserves_migrator) to add items to courses

## See Also

- [ReservesMigrator](reserves_migrator) - Adding items to courses
- [BatchPoster](batch_poster) - Posting the extradata file
- [Reference Data Mapping](../reference_data_mapping) - Terms and departments map files
- [Mapping File Based Mapping](../mapping_file_based_mapping) - The mapping file format
