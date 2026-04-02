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
| `boundwithFlavor` | string | No | ILS flavor for boundwith handling. Supported: `"voyager"` (default), `"aleph"` |
| `boundwithRelationshipFilePath` | string | No | TSV file for boundwith relationships (required when `boundwithFlavor` is set) |
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

The ItemsTransformer supports creating FOLIO `boundwithPart` records to link items to multiple holdings. The `boundwithFlavor` parameter determines how relationships are loaded and resolved. Supported values are `"voyager"` (default) and `"aleph"`.

```{note}
For III/Sierra/Millennium-style boundwiths — where items link to multiple bibs directly in the source data — boundwith handling is performed at the holdings level by [HoldingsCsvTransformer](holdings_csv_transformer#boundwith-handling), not here. No `boundwithFlavor` or `boundwithRelationshipFilePath` is needed in that case.
```

#### Voyager-style boundwiths

For Voyager migrations, the ItemsTransformer reads the `boundwith_relationships_map.json` file produced by [HoldingsMarcTransformer](holdings_marc_transformer) during its `wrap_up` phase. You must still specify the `boundwithRelationshipFilePath` — if it is not set, the transformer will skip loading boundwith relationships entirely. The map links holdings UUIDs to lists of instance UUIDs, and the transformer creates `boundwithPart` records for each relationship.

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "boundwithFlavor": "voyager",
    "boundwithRelationshipFilePath": "boundwith_map.tsv",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

#### Aleph-style boundwiths

For Aleph migrations, the item-level boundwith relationships are described in a separate TSV file with columns `LKR_HOL` (holdings legacy ID) and `ITEM_REC_KEY` (item legacy ID). This file is placed in `source_data/items/` and referenced via `boundwithRelationshipFilePath`.

Unlike Voyager mode (which maps holdings UUIDs to instance UUIDs), Aleph mode maps **item legacy IDs** to **holdings legacy IDs** using the `holdings_id_map` produced by the holdings transformation to resolve FOLIO UUIDs at runtime.

```text
LKR_HOL	ITEM_REC_KEY
000123456	ITEM001
000123457	ITEM001
000789012	ITEM002
```

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "boundwithFlavor": "aleph",
    "boundwithRelationshipFilePath": "item_holdings_links.tsv",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

Extract the LKR boundwith relationships via SQL from the database using a query like this:

```sql
-- Note: You will need to replace "XXX" or "xxx" in this query with the appropriate collection table prefix
-- Note: You may need to adjust enumeration and chronology matching to account for local practices
-- Credit: Aaron Bales and the team at University of Notre Dame Libraries for developing this example
WITH ITEM AS (
    SELECT item.Z30_REC_KEY AS ITEM_REC_KEY, item.z30_barcode AS BARCODE,
      LPAD(MAP.Z103_LKR_DOC_NUMBER ,9,'0') AS ITM_ADM ,
      item.Z30_SUB_LIBRARY AS sublib, item.Z30_COLLECTION AS collection ,
      item.Z30_HOL_DOC_NUMBER_X AS ITEM_HOL ,
      SUBSTR(MAP.Z103_REC_KEY_1 ,6,9) AS LKR_BIB ,
      MAP.Z103_ENUMERATION_A AS LKR_ENUM_A, ITEM.Z30_ENUMERATION_A AS ENUM_A,
      MAP.Z103_ENUMERATION_B AS LKR_ENUM_B, ITEM.Z30_ENUMERATION_B AS ENUM_B,
      MAP.Z103_ENUMERATION_C AS LKR_ENUM_C, ITEM.Z30_ENUMERATION_C AS ENUM_C
    FROM xxx01.z103 MAP INNER JOIN XXX50.Z30 item ON
        SUBSTR(MAP.Z103_REC_KEY_1 ,1,5) = 'XXX01'
        AND MAP.Z103_LKR_TYPE = 'ITM'
        AND SUBSTR(item.Z30_REC_KEY ,1,9) = LPAD(MAP.Z103_LKR_DOC_NUMBER ,9,'0')
        AND COALESCE(MAP.Z103_ENUMERATION_A,'null') = COALESCE(item.Z30_ENUMERATION_A ,'null')
        AND COALESCE(MAP.Z103_ENUMERATION_B,'null') = COALESCE(item.Z30_ENUMERATION_B ,'null')
        AND COALESCE(MAP.Z103_ENUMERATION_C,'null') = COALESCE(item.Z30_ENUMERATION_C ,'null')
), BIB AS (
    SELECT ITEM.ITEM_REC_KEY , ITEM.BARCODE , ITEM.ITM_ADM , ITEM.SUBLIB , ITEM.COLLECTION ,
        bib.Z13_REC_KEY AS ITEM_BIB, item.ITEM_HOL ,
        ITEM.ENUM_A , ITEM.ENUM_B , ITEM.ENUM_C ,
        item.LKR_BIB
    FROM ITEM LEFT JOIN xxx01.z103 
        ON ITEM.ITM_ADM = SUBSTR(z103_rec_key,6,9) 
        AND SUBSTR(z103_rec_key,1,5) = 'XXX50'
    LEFT JOIN xxx01.z13 BIB 
        ON SUBSTR(z103_rec_key_1,6.9) = z13_rec_key
), DATA AS (
    SELECT bib.*, hol.Z00R_DOC_NUMBER AS LKR_HOL, loc.Z00R_DOC_NUMBER LOC_HOL
    FROM BIB
    LEFT JOIN XXX60.Z00R hol ON (
        SUBSTR(hol.Z00R_FIELD_CODE ,1,3) = 'LKR'
        AND lpad(REPLACE(REGEXP_SUBSTR(hol.Z00R_TEXT ,'\$\$b[^$]*'),'$$b'),9,'0') = bib.LKR_BIB
    )
    LEFT JOIN XXX60.Z00R loc ON (
        SUBSTR(loc.Z00R_FIELD_CODE ,1,3) = '852'
        AND hol.Z00R_DOC_NUMBER = loc.Z00R_DOC_NUMBER
        AND bib.sublib = REPLACE(REGEXP_SUBSTR(loc.Z00R_TEXT ,'\$\$b[^$]*'),'$$b')
        AND bib.COLLECTION = REPLACE(REGEXP_SUBSTR(loc.Z00R_TEXT ,'\$\$c[^$]*'),'$$c')
    )
    ORDER BY ITEM_REC_KEY , LKR_BIB
)
SELECT ITEM_REC_KEY , ITEM_BIB , ITEM_HOL , LKR_BIB , LKR_HOL FROM DATA ;
```

Once you have the data, you can use a dataframe library to select the needed data and export an appropriate file:

```python
# Example python script (using polars dataframe library) to generate the actual boundwith_data file
import polars as pl
from pathlib import Path

relationship_file = Path("../iterations/iteration_1/source_data/items/raw_boundwith_data.tsv")

# Create the initial lazyframe for the raw data
boundwiths_df = pl.scan_csv(relationship_file, separator="\t", infer_schema=False, null_values=["", "[NULL]"])

# We need to capture all item->holdings relationships, so we will concatenate two sub-selections
prepped_df = pl.concat(
    [
        boundwiths_df.select(["ITEM_REC_KEY", "ITEM_HOL"]).rename({"ITEM_HOL": "LKR_HOL"}), boundwiths_df.select(["ITEM_REC_KEY", "LKR_HOL"])
    ]
)

# Now, we need to export to a TSV file that can be included in the items transformer task configuration
prepped_df.filter(
    pl.col("LKR_HOL").is_not_null() # We can't link an item to a holdings record that doesn't exist
).unique().sink_csv(relationship_file.parent.joinpath("item_holdings_links.tsv", separator="\t"))
```

```{important}
When using Aleph-style boundwiths, any `LKR_HOL` value that cannot be found in the `holdings_id_map` will be logged as a data issue and skipped. Ensure the holdings transformation has completed successfully before running the items transformation.
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
