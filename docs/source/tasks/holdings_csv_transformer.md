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
    "hridHandling": "default",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "callNumberTypeMapFileName": "call_number_types.tsv",
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
| `hridHandling` | string | Yes | Must be present, but this task doesn't use it; HRIDs are always generated from the FOLIO HRID settings. Set it to `"default"` |
| `holdingsMapFileName` | string | Yes | JSON mapping file for holdings fields |
| `locationMapFileName` | string | Yes | TSV file mapping legacy locations to FOLIO codes |
| `callNumberTypeMapFileName` | string | Yes | TSV file mapping call number types. The file must exist in `mapping_files/`; the task stops at startup if it doesn't |
| `defaultCallNumberTypeName` | string | Yes | FOLIO call number type name for fallback |
| `fallbackHoldingsTypeId` | string | Yes | UUID of the holdings type used when `holdingsTypeId` is not mapped. Must exist in the tenant |
| `holdingsMergeCriteria` | array | No | Fields used to group items into holdings. Each must be a holdings record property. Default: `["instanceId", "permanentLocationId", "callNumber"]` |
| `statisticalCodesMapFileName` | string | No | TSV file mapping statistical codes. Loaded when the mapping file maps `statisticalCodeIds` or a file definition sets `statistical_code`; see [With Statistical Codes](#with-statistical-codes) |
| `holdingsNoteTypeMapFileName` | string | No | TSV file translating legacy note type codes to FOLIO note type names. Without it, mapped note type values must be FOLIO note type names or UUIDs |
| `holdingsTypeUuidForBoundwiths` | string | No | UUID of holdings type for boundwith holdings (enables automatic boundwith handling) |
| `previouslyGeneratedHoldingsFiles` | array | No | List of previous holdings result files to avoid duplicates |
| `updateHridSettings` | boolean | No | Update the holdings HRID counter in FOLIO at the end of the run. Default: `true` |
| `resetHridSettings` | boolean | No | Reset the holdings HRID counter at the start of the run. Only applies when `updateHridSettings` is `true`. Default: `false` |
| `files` | array | Yes | List of source data files to process. See [Configuration Files](../configuration_files.md#migrationtasks) for the file keys |

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
| `statisticalCodesMapFileName` | `folio_code` | Statistical code |
| `holdingsNoteTypeMapFileName` | `folio_name` | Holdings note type name |

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

## Holdings ID Map

`holdings_id_map.json`, in `results/`, maps each legacy holdings ID to the UUID of the FOLIO holdings record created from it. [ItemsTransformer](items_transformer) uses it to resolve `holdingsRecordId` and Aleph boundwith links. It is the only record of which legacy ID became which holdings record, so the holdings tasks add to it rather than starting over.

| Task | What it does with the map |
|------|---------------------------|
| [HoldingsMarcTransformer](holdings_marc_transformer) | Writes a new map, replacing any existing file |
| HoldingsCsvTransformer | Loads the existing map, if there is one, adds its entries, and writes the whole map back |
| [ItemsTransformer](items_transformer) | Reads the map |

### What This Task Adds

- An entry for each source row's legacy ID, pointing at the holdings record the row was merged into. For boundwith rows, it points at the first record of the set (see [Boundwith Handling](../boundwith_handling)).
- Entries for previously generated records that were merged away while loading `previouslyGeneratedHoldingsFiles` are re-pointed to the surviving record.
- Values in `formerIds` are not added. This changed in version 1.12.11 (see [ItemsTransformer](items_transformer.md#item-mapping-file)).

A source row whose legacy ID is already in the map replaces the existing entry.

### Order of Runs

Run HoldingsMarcTransformer before any HoldingsCsvTransformer tasks. It replaces the map, so running it afterwards discards the entries the CSV tasks added. You can run several HoldingsCsvTransformer tasks one after another. Each one adds to the map.

### Re-running

Re-running this task with the same configuration and source data gives the same map. Because the task updates the map in place, entries from earlier runs are kept. After you change `holdingsMergeCriteria`, the mapping file, `previouslyGeneratedHoldingsFiles`, or the source data, those kept entries can be wrong:

- Entries for rows that are no longer in the source data, or whose legacy ID changed, still point at holdings records that the new run doesn't produce.
- Entries re-pointed to a merged record are not restored if the new criteria no longer merge the records.

To start from a clean map:

1. Re-run HoldingsMarcTransformer. If you have no MFHD run, delete `holdings_id_map.json` instead.
2. Re-run each HoldingsCsvTransformer task, in the original order.
3. Re-run ItemsTransformer.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_holdings_<task_name>.json` | FOLIO Holdings records |
| `holdings_id_map.json` | Legacy ID to FOLIO UUID mapping (used by ItemsTransformer). Updated in place; see [Holdings ID Map](#holdings-id-map) |
| `extradata_<task_name>.extradata` | Extra data including boundwith parts (when applicable) |

## Examples

### Basic Holdings from Items

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "hridHandling": "default",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "callNumberTypeMapFileName": "call_number_types.tsv",
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
    "hridHandling": "default",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "callNumberTypeMapFileName": "call_number_types.tsv",
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

All files in `previouslyGeneratedHoldingsFiles` are loaded together, in the order listed. Previously generated records that share a key under `holdingsMergeCriteria`, whether from the same file or different ones, are merged into the first one loaded. See [Holdings ID Map](#holdings-id-map) for how `holdings_id_map.json` is updated.

### With Statistical Codes

A `statistical_code` on a file definition is added to every holdings record created from that file. It is resolved through `statisticalCodesMapFileName`, the same as mapped `statisticalCodeIds` values, so the map needs a `legacy_stat_code` row for it. See [Statistical Codes](../statistical_codes) for the file format.

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "hridHandling": "default",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "callNumberTypeMapFileName": "call_number_types.tsv",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "statisticalCodesMapFileName": "stat_codes.tsv",
    "files": [
        {
            "file_name": "items.tsv",
            "statistical_code": "migrated"
        }
    ]
}
```

## Boundwith Handling

The HoldingsCsvTransformer handles boundwith relationships automatically when a source data row resolves to **multiple instance IDs** — that is, the column mapped to `instanceId` contains a stringified list of bib IDs such as `['b1000001', 'b1000002']`. You may see these referred to as "Sierra-style" or "Millennium-style" boundwiths. This differs from the MFHD-based approach used by [HoldingsMarcTransformer](holdings_marc_transformer), which requires a separate boundwith relationship file.

### How It Works

When a source data row maps to multiple instances:

1. One holdings record is generated per instance, each with its `holdingsTypeId` set to `holdingsTypeUuidForBoundwiths` — including the first one.
2. A `boundwithPart` record is written to the extradata file linking the item to each of those holdings records.
3. Boundwith holdings are excluded from the normal `holdingsMergeCriteria` merge process. Instead, they are de-duplicated using their own composite key (instance ID + location + call number + the full set of bound instance IDs). If a subsequent row produces the same boundwith key, it is merged into the existing boundwith holdings record rather than creating a duplicate.

Because this task creates both the holdings records and the `boundwithPart` records, the following [ItemsTransformer](items_transformer) run needs no boundwith configuration.

### Configuration

To enable boundwith handling, set the `holdingsTypeUuidForBoundwiths` parameter to the UUID of the appropriate holdings type from your FOLIO tenant (found under Settings → Inventory → Holdings types):

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "hridHandling": "default",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "callNumberTypeMapFileName": "call_number_types.tsv",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "holdingsTypeUuidForBoundwiths": "1b6c62cf-034c-4972-ac80-fa595a9bfbde",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

```{note}
No `boundwithFlavor` or `boundwithRelationshipFilePath` is needed for the CSV transformer. Boundwith detection is automatic based on multiple instance IDs in the source data.
```

See [Boundwith Handling](../boundwith_handling) for the full source-data requirements — including per-instance call numbers and former ID handling — and for how to post the resulting `boundwithPart` records.

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_csv_holdings --base_folder ./
```

## Next Steps

1. **Transform Items**: Use [ItemsTransformer](items_transformer) on the same source files
2. **Post Holdings**: Use [InventoryBatchPoster](inventory_batch_poster) or [BatchPoster](batch_poster)

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [Boundwith Handling](../boundwith_handling) - All supported boundwith patterns and their source data
- [HoldingsMarcTransformer](holdings_marc_transformer) - Alternative for MFHD records
- [ItemsTransformer](items_transformer) - Transforming items from the same data
