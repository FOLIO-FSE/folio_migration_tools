# ItemsTransformer

Transform delimited (CSV/TSV) data into FOLIO Item records with support for material types, loan types, item statuses, and location mapping.

## When to Use This Task

- Migrating item-level data from any legacy ILS
- Creating FOLIO Items linked to existing Holdings records
- Mapping item statuses, material types, and loan types from legacy values

## Configuration

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "itemStatusesMapFileName": "item_statuses.tsv",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"ItemsTransformer"` |
| `itemsMappingFileName` | string | Yes | JSON mapping file for item fields |
| `locationMapFileName` | string | Yes | TSV file mapping legacy locations to FOLIO codes |
| `materialTypesMapFileName` | string | Yes | TSV file mapping material types |
| `loanTypesMapFileName` | string | Yes | TSV file mapping loan types |
| `itemStatusesMapFileName` | string | No | TSV file mapping item statuses |
| `tempLocationMapFileName` | string | No | TSV file for temporary location mapping |
| `tempLoanTypesMapFileName` | string | No | TSV file for temporary loan type mapping |
| `callNumberTypeMapFileName` | string | No | TSV file mapping call number types |
| `statisticalCodeMapFileName` | string | No | TSV file mapping statistical codes |
| `damagedStatusMapFileName` | string | No | TSV file mapping damaged statuses |
| `preventPermanentLocationMapDefault` | boolean | No | If `true`, don't use fallback for permanent location mapping |
| `boundwithRelationshipFilePath` | string | No | TSV file for boundwith relationships |
| `holdingsTypeUuidForBoundwiths` | string | No | UUID of holdings type for boundwith items |
| `files` | array | Yes | List of source data files to process |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/items/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row
- **Prerequisites**: 
  - Run [BibsTransformer](bibs_transformer) to create `instance_id_map`
  - Run [HoldingsCsvTransformer](holdings_csv_transformer) or [HoldingsMarcTransformer](holdings_marc_transformer) to create `holdings_id_map`

### Item Mapping File

Create a JSON mapping file in `mapping_files/`:

```json
{
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "ITEM_ID",
            "description": "Legacy identifier for deterministic UUID"
        },
        {
            "folio_field": "barcode",
            "legacy_field": "BARCODE",
            "description": "Item barcode"
        },
        {
            "folio_field": "holdingsRecordId",
            "legacy_field": "HOLDINGS_ID",
            "description": "Links to parent holdings"
        },
        {
            "folio_field": "materialTypeId",
            "legacy_field": "ITYPE",
            "description": "Mapped via materialTypesMapFileName"
        },
        {
            "folio_field": "permanentLoanTypeId",
            "legacy_field": "LOAN_TYPE",
            "description": "Mapped via loanTypesMapFileName"
        },
        {
            "folio_field": "status.name",
            "legacy_field": "STATUS",
            "description": "Mapped via itemStatusesMapFileName"
        },
        {
            "folio_field": "permanentLocationId",
            "legacy_field": "LOCATION",
            "description": "Mapped via locationMapFileName"
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
| `locationMapFileName` | `folio_code` | Location code |
| `tempLocationMapFileName` | `folio_code` | Temporary location code |
| `materialTypesMapFileName` | `folio_name` | Material type name |
| `loanTypesMapFileName` | `folio_name` | Loan type name |
| `tempLoanTypesMapFileName` | `folio_name` | Temporary loan type name |
| `callNumberTypeMapFileName` | `folio_name` | Call number type name |
| `statisticalCodeMapFileName` | `folio_code` | Statistical code |
| `damagedStatusMapFileName` | `folio_name` | Damaged status name |

#### Item Statuses (item_statuses.tsv)

Item status mapping has special requirements different from other reference data:

```text
legacy_code	folio_name
AVAILABLE	Available
CHECKED OUT	Checked out
IN TRANSIT	In transit
MISSING	Missing
```

```{important}
- Item status mapping requires the column names `legacy_code` and `folio_name` exactly as shown.
- The `folio_name` must be one of the valid FOLIO item statuses: `Available`, `Awaiting pickup`, `Awaiting delivery`, `Checked out`, `Claimed returned`, `Declared lost`, `In process`, `In process (non-requestable)`, `In transit`, `Intellectual item`, `Long missing`, `Lost and paid`, `Missing`, `On order`, `Paged`, `Restricted`, `Order closed`, `Unavailable`, `Unknown`, `Withdrawn`.
- Fallback rows with `*` are **not allowed** for item status mapping. If no match is found, the status defaults to `Available`.
```

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_items_<task_name>.json` | FOLIO Item records |
| `extradata_<task_name>.extradata` | Extra data including boundwith parts (when applicable) |

```{note}
Unlike BibsTransformer and the Holdings transformers, ItemsTransformer does not generate a legacy ID map file. If you need to look up item UUIDs by legacy ID, you can query the transformed items file directly using the `administrativeNotes` field which contains the legacy identifier.
```

## Examples

### Basic Item Transformation

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

### With All Reference Data Mappings

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "tempLocationMapFileName": "temp_locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "tempLoanTypesMapFileName": "temp_loan_types.tsv",
    "itemStatusesMapFileName": "item_statuses.tsv",
    "callNumberTypeMapFileName": "call_number_types.tsv",
    "statisticalCodeMapFileName": "stat_codes.tsv",
    "damagedStatusMapFileName": "damaged_statuses.tsv",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

### With Boundwith Support

For Sierra/III-style boundwiths where items link to multiple bibs:

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "boundwithRelationshipFilePath": "item_bib_links.tsv",
    "holdingsTypeUuidForBoundwiths": "1b6c62cf-034c-4972-ac80-fa595a9bfbde",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

### Multiple Files with Different Settings

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "files": [
        {
            "file_name": "regular_items.tsv",
            "discovery_suppressed": false
        },
        {
            "file_name": "suppressed_items.tsv",
            "discovery_suppressed": true
        },
        {
            "file_name": "special_collection.tsv",
            "statistical_code": "special-coll"
        }
    ]
}
```

## Item Notes

Map item notes using array syntax in the mapping file:

```json
{
    "folio_field": "notes[0].itemNoteTypeId",
    "legacy_field": "",
    "value": "c7bc292c-a318-43d3-9b03-7a40dfba046a"
},
{
    "folio_field": "notes[0].note",
    "legacy_field": "PUBLIC_NOTE"
},
{
    "folio_field": "notes[0].staffOnly",
    "legacy_field": "",
    "value": false
},
{
    "folio_field": "notes[1].itemNoteTypeId",
    "legacy_field": "",
    "value": "1dde7141-ec8a-4dae-9825-49ce14c728e7"
},
{
    "folio_field": "notes[1].note",
    "legacy_field": "STAFF_NOTE"
},
{
    "folio_field": "notes[1].staffOnly",
    "legacy_field": "",
    "value": true
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_items --base_folder ./
```

## Next Steps

1. **Post Items**: Use [InventoryBatchPoster](inventory_batch_poster) or [BatchPoster](batch_poster)
2. **Migrate Loans**: Use [LoansMigrator](loans_migrator) after posting items

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [Mapping Files for Inventory](../mapping_files_inventory) - Required mapping files
- [Statistical Code Mapping](../statistical_codes) - Mapping statistical codes
