# HoldingsMarcTransformer

Transform MARC Holdings (MFHD) records into FOLIO Holdings records with support for holdings statements, boundwith relationships, and optional SRS record creation.

## When to Use This Task

- Migrating holdings data from systems that export MFHD (MARC Holdings) records
- Voyager, Aleph, or other systems using MARC21 for holdings
- When you have bib-to-holdings relationships defined in MFHD 004 or a separate file
- Handling boundwith items where multiple bibs share a single holdings record

## Configuration

```json
{
    "name": "transform_mfhd",
    "migrationTaskType": "HoldingsMarcTransformer",
    "legacyIdMarcPath": "001",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "hridHandling": "default",
    "createSourceRecords": false,
    "files": [
        {
            "file_name": "holdings.mrc",
            "discovery_suppressed": false
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"HoldingsMarcTransformer"` |
| `legacyIdMarcPath` | string | Yes | MARC field (with optional subfield) containing legacy holdings ID. Examples: `"001"`, `"951$c"` |
| `locationMapFileName` | string | Yes | TSV file mapping legacy locations to FOLIO location codes |
| `defaultCallNumberTypeName` | string | Yes | FOLIO call number type name for fallback |
| `fallbackHoldingsTypeId` | string | Yes | UUID of fallback holdings type |
| `hridHandling` | string | No | `"default"` or `"preserve001"`. Default: `"default"` |
| `createSourceRecords` | boolean | No | Create SRS records for holdings. Default: `false` |
| `mfhdMappingFileName` | string | No | Custom MFHD rules file (replaces tenant rules) |
| `supplementalMfhdMappingRulesFile` | string | No | Additional mapping rules to merge with tenant rules |
| `boundwithRelationshipFilePath` | string | No | TSV file with bib-to-MFHD relationships for boundwiths |
| `holdingsTypeUuidForBoundwiths` | string | No | UUID of holdings type for boundwith holdings |
| `callNumberTypeMapFileName` | string | No | TSV file mapping call number types |
| `holdingsTypeMapFileName` | string | No | TSV file mapping holdings types |
| `statisticalCodeMapFileName` | string | No | TSV file mapping statistical codes |
| `includeMrkStatements` | boolean | No | Preserve original holdings statements as MRK in notes |
| `mrkHoldingsNoteType` | string | No | Note type name for MRK statements |
| `includeMfhdMrkAsNote` | boolean | No | Preserve entire MFHD as MRK in notes |
| `mfhdMrkNoteType` | string | No | Note type name for full MFHD MRK |
| `includeMfhdMrcAsNote` | boolean | No | Preserve entire MFHD as MARC21 in notes |
| `mfhdMrcNoteType` | string | No | Note type name for full MFHD MARC21 |
| `files` | array | Yes | List of MFHD files to process |

## Source Data Requirements

- **Location**: Place MARC21 MFHD files (`.mrc`) in `iterations/<iteration>/source_data/holdings/`
- **Format**: MARC21 Holdings Format
- **Prerequisite**: Run [BibsTransformer](bibs_transformer) first to create `instance_id_map`

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `locationMapFileName` | `folio_code` | Location code |

For MARC-based holdings, the legacy location values are extracted from the MFHD record according to the mapping rules (typically from 852$b or similar). Use the column name `legacy_code` for the legacy values when mapping from MARC data.

### Boundwith Relationship File (Optional)

For Voyager-style boundwiths, create a TSV file mapping MFHDs to multiple bibs:

```text
MFHD_ID	BIB_ID
12345	100001
12345	100002
12346	100003
```

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_holdings_<task_name>.json` | FOLIO Holdings records |
| `holdings_id_map.json` | Legacy ID to FOLIO UUID mapping (used by ItemsTransformer) |
| `folio_srs_holdings_<task_name>.json` | SRS records (if `createSourceRecords: true`) |
| `extradata_<task_name>.extradata` | Extra data including boundwith parts (when applicable) |
| `boundwith_relationships_map.json` | Boundwith relationship mappings (when processing boundwiths) |

## Examples

### Basic MFHD Transformation

```json
{
    "name": "transform_mfhd",
    "migrationTaskType": "HoldingsMarcTransformer",
    "legacyIdMarcPath": "001",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "createSourceRecords": false,
    "files": [
        {
            "file_name": "mfhd.mrc"
        }
    ]
}
```

### With Boundwith Support

```json
{
    "name": "transform_mfhd",
    "migrationTaskType": "HoldingsMarcTransformer",
    "legacyIdMarcPath": "001",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "holdingsTypeUuidForBoundwiths": "1b6c62cf-034c-4972-ac80-fa595a9bfbde",
    "boundwithRelationshipFilePath": "bib_mfhd.tsv",
    "files": [
        {
            "file_name": "mfhd.mrc"
        }
    ]
}
```

### Preserving Original MFHD Data

```json
{
    "name": "transform_mfhd",
    "migrationTaskType": "HoldingsMarcTransformer",
    "legacyIdMarcPath": "001",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "createSourceRecords": false,
    "includeMfhdMrkAsNote": true,
    "mfhdMrkNoteType": "Original MFHD Record",
    "supplementalMfhdMappingRulesFile": "custom_mfhd_rules.json",
    "files": [
        {
            "file_name": "mfhd.mrc"
        }
    ]
}
```

### With Custom Mapping Rules

```json
{
    "name": "transform_mfhd",
    "migrationTaskType": "HoldingsMarcTransformer",
    "legacyIdMarcPath": "001",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "supplementalMfhdMappingRulesFile": "supplemental_mfhd.json",
    "files": [
        {
            "file_name": "mfhd.mrc"
        }
    ]
}
```

## Holdings Statements

The transformer handles MARC holdings statements in two ways:

1. **Textual statements** (866, 867, 868) - Mapped directly to FOLIO holdings statements
2. **Enumeration/Chronology patterns** (853-855, 863-865) - Converted to textual holdings statements

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_mfhd --base_folder ./
```

## Next Steps

1. **Post Holdings**: Use [InventoryBatchPoster](inventory_batch_poster) or [BatchPoster](batch_poster)
2. **Transform Items**: Use [ItemsTransformer](items_transformer)

## See Also

- [MARC Rules Based Mapping](../marc_rule_based_mapping) - Customizing MFHD mapping rules
- [HoldingsCsvTransformer](holdings_csv_transformer) - Alternative for CSV-based holdings
- [ItemsTransformer](items_transformer) - Transforming items
