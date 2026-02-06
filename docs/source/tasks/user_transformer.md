# UserTransformer

Transform delimited (CSV/TSV) data into FOLIO User records with support for patron groups, departments, addresses, and custom fields.

## When to Use This Task

- Migrating patron/user data from any legacy ILS
- Importing staff or patron records
- Creating users with multiple addresses, notes, and custom fields

## Configuration

```json
{
    "name": "transform_users",
    "migrationTaskType": "UserTransformer",
    "userMappingFileName": "user_mapping.json",
    "groupMapPath": "patron_groups.tsv",
    "useGroupMap": true,
    "userFile": {
        "file_name": "patrons.tsv"
    }
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"UserTransformer"` |
| `userMappingFileName` | string | Yes | JSON mapping file for user fields |
| `groupMapPath` | string | Yes | TSV file mapping patron groups |
| `useGroupMap` | boolean | Yes | Whether to use the group map file |
| `departmentsMapPath` | string | No | TSV file mapping user departments |
| `addressTypeMapPath` | string | No | TSV file mapping address types |
| `preferredContactTypeMapPath` | string | No | TSV file mapping preferred contact types |
| `removeRequestPreferences` | boolean | No | Remove request preference data from output |
| `userFile` | object | Yes | Source file definition with `file_name` |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/users/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row

### User Mapping File

Create a JSON mapping file in `mapping_files/`:

```json
{
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "PATRON_ID",
            "description": "Legacy identifier for deterministic UUID"
        },
        {
            "folio_field": "username",
            "legacy_field": "USERNAME"
        },
        {
            "folio_field": "barcode",
            "legacy_field": "BARCODE"
        },
        {
            "folio_field": "externalSystemId",
            "legacy_field": "EXTERNAL_ID"
        },
        {
            "folio_field": "patronGroup",
            "legacy_field": "PATRON_TYPE",
            "description": "Mapped via groupMapPath"
        },
        {
            "folio_field": "active",
            "legacy_field": "",
            "value": true
        },
        {
            "folio_field": "personal.lastName",
            "legacy_field": "LAST_NAME"
        },
        {
            "folio_field": "personal.firstName",
            "legacy_field": "FIRST_NAME"
        },
        {
            "folio_field": "personal.email",
            "legacy_field": "EMAIL"
        },
        {
            "folio_field": "expirationDate",
            "legacy_field": "EXPIRY_DATE"
        }
    ]
}
```

```{important}
The `legacyIdentifier` field is required and must map to a unique value in your source data.
```

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `groupMapFileName` | `folio_name` | Patron group name (the `group` field) |
| `departmentMapFileName` | `folio_name` | Department name |

## Mapping Multiple Addresses

Map multiple addresses using array indexing:

```json
{
    "folio_field": "personal.addresses[0].addressLine1",
    "legacy_field": "STREET_1"
},
{
    "folio_field": "personal.addresses[0].city",
    "legacy_field": "CITY"
},
{
    "folio_field": "personal.addresses[0].postalCode",
    "legacy_field": "ZIP"
},
{
    "folio_field": "personal.addresses[0].addressTypeId",
    "legacy_field": "",
    "value": "93d3d88d-499b-45d0-9bc7-ac73c3a19880"
},
{
    "folio_field": "personal.addresses[0].primaryAddress",
    "legacy_field": "",
    "value": true
}
```

## Mapping Multiple Departments

To map multiple departments for a user, include all legacy values in the same column, sub-delimited with the `multi_field_delimiter` value from your `libraryConfiguration`:

```json
{
    "folio_field": "departments[0]",
    "legacy_field": "DEPARTMENTS"
}
```

Source data:
```
DEPARTMENTS
CHEM<delimiter>PHYS
```

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_users_<task_name>.json` | FOLIO User records |
| `user_id_map_<task_name>.json` | Legacy ID to FOLIO UUID mapping |

## Examples

### Basic User Transformation

```json
{
    "name": "transform_users",
    "migrationTaskType": "UserTransformer",
    "userMappingFileName": "user_mapping.json",
    "groupMapPath": "patron_groups.tsv",
    "useGroupMap": true,
    "userFile": {
        "file_name": "patrons.tsv"
    }
}
```

### With Department Mapping

```json
{
    "name": "transform_users",
    "migrationTaskType": "UserTransformer",
    "userMappingFileName": "user_mapping.json",
    "groupMapPath": "patron_groups.tsv",
    "useGroupMap": true,
    "departmentsMapPath": "departments.tsv",
    "userFile": {
        "file_name": "patrons.tsv"
    }
}
```

### Without Request Preferences

```json
{
    "name": "transform_users",
    "migrationTaskType": "UserTransformer",
    "userMappingFileName": "user_mapping.json",
    "groupMapPath": "patron_groups.tsv",
    "useGroupMap": true,
    "removeRequestPreferences": true,
    "userFile": {
        "file_name": "patrons.tsv"
    }
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_users --base_folder ./
```

## Next Steps

1. **Post Users**: Use [UserImportTask](user_import) (recommended) or [BatchPoster](batch_poster)
2. **Migrate Loans**: Use [LoansMigrator](loans_migrator) after posting users

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [UserImportTask](user_import) - Enhanced user posting with upsert
- [BatchPoster](batch_poster) - Alternative posting method
