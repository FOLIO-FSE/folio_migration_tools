# HoldingsCsvTransformer

Transform delimited (CSV/TSV) data into FOLIO Holdings records. Use this when your holdings data comes from item-level exports or systems without MFHD support.

## When to Use This Task

- Migrating from systems that export item-level data (Sierra, III, etc.)
- Creating holdings records from item data when no separate holdings exist
- Merging multiple items into consolidated holdings records
- Combining with previously generated MFHD-based holdings

## Configuration

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "holdingsMergeCriteria": ["instanceId", "permanentLocationId", "callNumber"],
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
| `migrationTaskType` | string | Yes | Must be `"HoldingsCsvTransformer"` |
| `holdingsMapFileName` | string | Yes | JSON mapping file for holdings fields |
| `locationMapFileName` | string | Yes | TSV file mapping legacy locations to FOLIO codes |
| `defaultCallNumberTypeName` | string | Yes | FOLIO call number type name for fallback |
| `fallbackHoldingsTypeId` | string | Yes | UUID of fallback holdings type |
| `holdingsMergeCriteria` | array | No | Fields used to group items into holdings. Default: `["instanceId", "permanentLocationId", "callNumber"]` |
| `callNumberTypeMapFileName` | string | No | TSV file mapping call number types |
| `holdingsTypeMapFileName` | string | No | TSV file mapping holdings types |
| `statisticalCodeMapFileName` | string | No | TSV file mapping statistical codes |
| `previouslyGeneratedHoldingsFiles` | array | No | List of previous holdings result files to avoid duplicates |
| `files` | array | Yes | List of source data files to process |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/items/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row
- **Prerequisite**: Run [BibsTransformer](bibs_transformer) first to create `instance_id_map`

### Holdings Mapping File

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
            "folio_field": "instanceId",
            "legacy_field": "BIB_ID",
            "description": "Links to the parent instance"
        },
        {
            "folio_field": "permanentLocationId",
            "legacy_field": "LOCATION_CODE",
            "description": "Mapped via locationMapFileName"
        },
        {
            "folio_field": "callNumber",
            "legacy_field": "CALL_NUMBER",
            "description": "Call number for the holdings"
        },
        {
            "folio_field": "callNumberTypeId",
            "legacy_field": "CN_TYPE",
            "description": "Mapped via callNumberTypeMapFileName"
        }
    ]
}
```

```{important}
The `legacyIdentifier` field is required and must map to a unique value in your source data. This value is used to generate deterministic UUIDs.
```

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `locationMapFileName` | `folio_code` | Location code |
| `callNumberTypeMapFileName` | `folio_name` | Call number type name |

## Holdings Merge Criteria

The `holdingsMergeCriteria` parameter determines how multiple rows in the source data are consolidated into single holdings records. 

**Example**: With `["instanceId", "permanentLocationId", "callNumber"]`:
- Items with the same bib ID, location, and call number → one holdings record
- Items with different locations → separate holdings records

Common configurations:

| Strategy | Merge Criteria | Result |
|----------|---------------|--------|
| One holdings per item | `["legacyIdentifier"]` | 1:1 item to holdings |
| Group by location | `["instanceId", "permanentLocationId"]` | Holdings per location |
| Group by location + call number | `["instanceId", "permanentLocationId", "callNumber"]` | Holdings per location + call number |

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_holdings_<task_name>.json` | FOLIO Holdings records |
| `holdings_id_map.json` | Legacy ID to FOLIO UUID mapping (used by ItemsTransformer) |
| `extradata_<task_name>.extradata` | Extra data including boundwith parts (when applicable) |

## Examples

### Basic Holdings from Items

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

### Combining with MFHD Holdings

When you have both MFHD-derived holdings and need additional holdings from items:

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "previouslyGeneratedHoldingsFiles": [
        "folio_holdings_transform_mfhd.json"
    ],
    "files": [
        {
            "file_name": "items_without_mfhd.tsv"
        }
    ]
}
```

### With Statistical Codes

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "statisticalCodeMapFileName": "stat_codes.tsv",
    "files": [
        {
            "file_name": "items.tsv",
            "statistical_code": "migrated"
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_csv_holdings --base_folder ./
```

## Next Steps

1. **Transform Items**: Use [ItemsTransformer](items_transformer) on the same source files
2. **Post Holdings**: Use [InventoryBatchPoster](inventory_batch_poster) or [BatchPoster](batch_poster)

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [HoldingsMarcTransformer](holdings_marc_transformer) - Alternative for MFHD records
- [ItemsTransformer](items_transformer) - Transforming items from the same data
