# OrganizationTransformer

Transform delimited (CSV/TSV) data into FOLIO Organization (vendor) records with support for contacts, addresses, emails, phone numbers, and notes.

## When to Use This Task

- Migrating vendor/supplier records from legacy acquisitions systems
- Creating organizations for ERM or acquisitions workflows
- Importing contact information with multiple addresses and communication methods

## Configuration

```json
{
    "name": "transform_organizations",
    "migrationTaskType": "OrganizationTransformer",
    "organizationMapPath": "organization_mapping.json",
    "organizationTypesMapPath": "org_types.tsv",
    "addressCategoriesMapPath": "address_categories.tsv",
    "emailCategoriesMapPath": "email_categories.tsv",
    "phoneCategoriesMapPath": "phone_categories.tsv",
    "files": [
        {
            "file_name": "vendors.tsv"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"OrganizationTransformer"` |
| `organizationMapPath` | string | Yes | JSON mapping file for organization fields |
| `organizationTypesMapPath` | string | No | TSV file mapping organization types |
| `addressCategoriesMapPath` | string | No | TSV file mapping address categories |
| `emailCategoriesMapPath` | string | No | TSV file mapping email categories |
| `phoneCategoriesMapPath` | string | No | TSV file mapping phone categories |
| `files` | array | Yes | List of source data files to process |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/organizations/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row

### Organization Mapping File

Create a JSON mapping file in `mapping_files/`:

```json
{
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "VENDOR_ID",
            "description": "Legacy identifier for deterministic UUID"
        },
        {
            "folio_field": "name",
            "legacy_field": "VENDOR_NAME"
        },
        {
            "folio_field": "code",
            "legacy_field": "VENDOR_CODE"
        },
        {
            "folio_field": "status",
            "legacy_field": "",
            "value": "Active"
        },
        {
            "folio_field": "isVendor",
            "legacy_field": "",
            "value": true
        },
        {
            "folio_field": "addresses[0].addressLine1",
            "legacy_field": "ADDRESS_1"
        },
        {
            "folio_field": "addresses[0].city",
            "legacy_field": "CITY"
        },
        {
            "folio_field": "addresses[0].stateRegion",
            "legacy_field": "STATE"
        },
        {
            "folio_field": "addresses[0].zipCode",
            "legacy_field": "ZIP"
        },
        {
            "folio_field": "addresses[0].country",
            "legacy_field": "COUNTRY"
        },
        {
            "folio_field": "emails[0].value",
            "legacy_field": "EMAIL"
        },
        {
            "folio_field": "phoneNumbers[0].phoneNumber",
            "legacy_field": "PHONE"
        }
    ]
}
```

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `organizationTypesMapFileName` | `folio_name` | Organization type name |
| `addressCategoriesMapFileName` | `folio_value` | Address category value |
| `emailCategoriesMapFileName` | `folio_value` | Email category value |
| `phoneCategoriesMapFileName` | `folio_value` | Phone category value |
| `urlCategoriesMapFileName` | `folio_value` | URL category value |

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_organizations_<task_name>.json` | FOLIO Organization records |
| `extradata_<task_name>.extradata` | Related objects (contacts, interfaces, credentials, notes) |
| `organization_id_map_<task_name>.json` | Legacy ID to FOLIO UUID mapping |

## Examples

### Basic Organization Transformation

```json
{
    "name": "transform_organizations",
    "migrationTaskType": "OrganizationTransformer",
    "organizationMapPath": "organization_mapping.json",
    "files": [
        {
            "file_name": "vendors.tsv"
        }
    ]
}
```

### With All Reference Data Mappings

```json
{
    "name": "transform_organizations",
    "migrationTaskType": "OrganizationTransformer",
    "organizationMapPath": "organization_mapping.json",
    "organizationTypesMapPath": "org_types.tsv",
    "addressCategoriesMapPath": "address_categories.tsv",
    "emailCategoriesMapPath": "email_categories.tsv",
    "phoneCategoriesMapPath": "phone_categories.tsv",
    "files": [
        {
            "file_name": "vendors.tsv"
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_organizations --base_folder ./
```

## Posting Organizations

After transformation, post organizations using BatchPoster:

```json
{
    "name": "post_organizations",
    "migrationTaskType": "BatchPoster",
    "objectType": "Organizations",
    "batchSize": 250,
    "files": [
        {
            "file_name": "folio_organizations_transform_organizations.json"
        }
    ]
}
```

Then post the extradata file for related objects:

```json
{
    "name": "post_organization_extradata",
    "migrationTaskType": "BatchPoster",
    "objectType": "Extradata",
    "batchSize": 1,
    "files": [
        {
            "file_name": "extradata_transform_organizations.extradata"
        }
    ]
}
```

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [BatchPoster](batch_poster) - Posting organizations
- [OrdersTransformer](orders_transformer) - Using organizations in orders
