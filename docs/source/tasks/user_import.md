# UserImportTask

Import users to FOLIO with full relationship handling, field protection, and upsert capabilities. This task uses the [folio_data_import](https://folio-data-import.readthedocs.io/en/latest) library's UserImporter for enhanced features beyond the standard `/user-import` API.

## When to Use This Task

- Loading transformed users with request preferences and service point assignments
- Updating existing users while protecting specific fields
- When you need fine-grained control over which user fields are updated
- Handling complex user imports with concurrent processing

```{tip}
UserImportTask offers advantages over [BatchPoster](batch_poster) with `objectType: Users`:
- Field protection to prevent overwriting specific fields
- Configurable match key (externalSystemId, username, or barcode)
- Automatic handling of request preferences and service points users
- Better error handling and progress reporting
```

## Configuration

```json
{
    "name": "import_users",
    "migrationTaskType": "UserImportTask",
    "userMatchKey": "externalSystemId",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_users_transform_users.json"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | Yes | - | The name of this task |
| `migrationTaskType` | string | Yes | - | Must be `"UserImportTask"` |
| `files` | array | Yes | - | List of user files from `results/` folder |
| `batchSize` | integer | No | 250 | Users per concurrent batch (1-1000) |
| `userMatchKey` | string | No | `externalSystemId` | Match key: `externalSystemId`, `username`, or `barcode` |
| `onlyUpdatePresentFields` | boolean | No | `false` | Only update fields present in input |
| `defaultPreferredContactType` | string | No | `"002"` | Default contact type for users |
| `fieldsToProtect` | array | No | `[]` | Field paths to never overwrite |
| `limitSimultaneousRequests` | integer | No | 10 | Max concurrent HTTP requests (1-100) |
| `noProgress` | boolean | No | `false` | Disable progress reporting |

### User Match Keys

| Match Key | Description | Use Case |
|-----------|-------------|----------|
| `externalSystemId` | Match on external system ID | Default; most reliable for ILS migrations |
| `username` | Match on username | When usernames are stable identifiers |
| `barcode` | Match on barcode | When barcodes are the primary identifier |

### Preferred Contact Types

| ID | Name | Description |
|----|------|-------------|
| `001` | `mail` | Postal mail |
| `002` | `email` | Email (default) |
| `003` | `text` | Text/SMS message |
| `004` | `phone` | Phone call |
| `005` | `mobile` | Mobile phone |

## Field Protection

Protect specific fields from being overwritten during updates:

```json
{
    "name": "update_users",
    "migrationTaskType": "UserImportTask",
    "userMatchKey": "externalSystemId",
    "fieldsToProtect": [
        "personal.email",
        "barcode",
        "patronGroup"
    ],
    "files": [{"file_name": "user_updates.json"}]
}
```

Protected fields will retain their existing values in FOLIO, even if different values are provided in the input.

## Partial Updates

When you only want to update fields that are present in the input:

```json
{
    "name": "partial_user_update",
    "migrationTaskType": "UserImportTask",
    "userMatchKey": "externalSystemId",
    "onlyUpdatePresentFields": true,
    "files": [{"file_name": "user_partial_updates.json"}]
}
```

When `onlyUpdatePresentFields` is `true`:
- Fields in the input file will be updated
- Fields missing from the input will be left unchanged
- This is useful for targeted updates (e.g., only updating addresses)

## Source Files

- **Location**: `iterations/<iteration>/results/`
- **Format**: Newline-delimited JSON (one user record per line)
- **Content**: Output from [UserTransformer](user_transformer) with mod-user-import structure

### Expected File Structure

User objects should include the standard mod-user-import format with optional extensions:

```json
{
    "username": "jsmith",
    "externalSystemId": "12345",
    "barcode": "U123456",
    "active": true,
    "patronGroup": "faculty",
    "personal": {
        "lastName": "Smith",
        "firstName": "John",
        "email": "jsmith@example.edu"
    },
    "requestPreference": {
        "holdShelf": true,
        "delivery": false
    }
}
```

## Output Files

| Output | Description |
|--------|-------------|
| Migration report | Statistics on created/updated/failed users |
| Console output | Progress bars and summary information |
| Failed records | Logged in migration report with error details |

## Examples

### Basic User Import

```json
{
    "name": "import_users",
    "migrationTaskType": "UserImportTask",
    "files": [
        {"file_name": "folio_users_transform_users.json"}
    ]
}
```

### Update by Barcode with Field Protection

```json
{
    "name": "update_by_barcode",
    "migrationTaskType": "UserImportTask",
    "userMatchKey": "barcode",
    "fieldsToProtect": ["personal.email", "personal.phone"],
    "files": [
        {"file_name": "user_address_updates.json"}
    ]
}
```

### High-Volume Import with Throttling

```json
{
    "name": "bulk_user_import",
    "migrationTaskType": "UserImportTask",
    "batchSize": 100,
    "limitSimultaneousRequests": 5,
    "files": [
        {"file_name": "all_users.json"}
    ]
}
```

### Partial Update - Only Present Fields

```json
{
    "name": "update_expiration_dates",
    "migrationTaskType": "UserImportTask",
    "userMatchKey": "externalSystemId",
    "onlyUpdatePresentFields": true,
    "files": [
        {"file_name": "user_expirations.json"}
    ]
}
```

Input file might contain only the fields to update:

```json
{"externalSystemId": "12345", "expirationDate": "2025-12-31"}
{"externalSystemId": "67890", "expirationDate": "2025-12-31"}
```

### Non-Interactive/CI Environment

```json
{
    "name": "ci_user_import",
    "migrationTaskType": "UserImportTask",
    "noProgress": true,
    "files": [
        {"file_name": "test_users.json"}
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json import_users --base_folder ./
```

## Related Functionality

UserImportTask automatically handles:

- **Request Preferences**: Creates/updates `requestPreference` objects for users
- **Service Points Users**: Creates service point user records if service point data is present
- **Permission Users**: Creates permission user records as needed
- **Patron Group Mapping**: Maps patron group names to UUIDs

## See Also

- [UserTransformer](user_transformer) - Transform legacy user data to FOLIO format
- [BatchPoster](batch_poster) - Alternative posting method for users
