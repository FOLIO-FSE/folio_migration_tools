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
| `defaultCallNumberTypeName` | string | Yes | FOLIO call number type name used when the mapping rules don't produce a call number type |
| `fallbackHoldingsTypeId` | string | Yes | UUID of the holdings type used when the leader (LDR/06) doesn't match a FOLIO holdings type |
| `hridHandling` | string | No | `"default"` or `"preserve001"`. Default: `"default"` |
| `createSourceRecords` | boolean | No | Create SRS records for holdings. Default: `false` |
| `supplementalMfhdMappingRulesFile` | string | No | Additional mapping rules to merge with tenant rules |
| `boundwithRelationshipFilePath` | string | No | TSV file with bib-to-MFHD relationships for boundwiths. See [Boundwith Handling](../boundwith_handling) |
| `holdingsTypeUuidForBoundwiths` | string | No | UUID of holdings type for boundwith holdings. Required if `boundwithRelationshipFilePath` is set |
| `statisticalCodesMapFileName` | string | No | TSV file mapping statistical codes. See [Statistical Codes](../statistical_codes) |
| `statisticalCodeMappingFields` | array | No | MARC fields and subfields to map statistical codes from, `$`-delimited (e.g. `"907$a"`). Default: `[]` |
| `deduplicateHoldingsStatements` | boolean | No | Remove duplicate holdings statements within a record. Default: `true` |
| `deactivate035From001` | boolean | No | Don't move the existing 001 into a 035 (prefixed with 003). Default: `false` |
| `updateHridSettings` | boolean | No | Update the FOLIO HRID settings at the end of the run. Default: `true` |
| `resetHridSettings` | boolean | No | Reset the holdings HRID counter at the start of the run. Only applies when `updateHridSettings` is `true`. Default: `false` |
| `marcRecordPreprocessors` | array | No | Ordered list of MARC preprocessors. See [MARC Record Preprocessors](#marc-record-preprocessors) |
| `preprocessorsArgs` | object or string | No | Preprocessor arguments. See [MARC Record Preprocessors](#marc-record-preprocessors) |
| `includeMrkStatements` | boolean | No | Preserve original holdings statements as MRK in notes. Default: `false` |
| `mrkHoldingsNoteType` | string | No | Note type name for MRK statements. Default: `"Original MARC holdings statements"` |
| `includeMfhdMrkAsNote` | boolean | No | Preserve entire MFHD as MRK in notes. Default: `false` |
| `mfhdMrkNoteType` | string | No | Note type name for full MFHD MRK. Default: `"Original MFHD Record"` |
| `includeMfhdMrcAsNote` | boolean | No | Preserve entire MFHD as MARC21 in notes. Default: `false` |
| `mfhdMrcNoteType` | string | No | Note type name for full MFHD MARC21. Default: `"Original MFHD (MARC21)"` |
| `files` | array | Yes | List of MFHD files to process. See [Configuration Files](../configuration_files.md#migrationtasks) for the file keys |

## MARC Record Preprocessors

`HoldingsMarcTransformer` supports the same MARC preprocessor configuration as `BibsTransformer`.

- `marcRecordPreprocessors`: ordered list of preprocessor names or full module paths.
- `preprocessorsArgs`: inline JSON object or the name of a JSON file in `mapping_files/` containing per-preprocessor arguments.

The default configuration includes `folio_migration_tools.marc_rules_transformation.marc_reader_wrapper.set_leader`. If you provide your own list and omit it, leader normalization is skipped.

All preprocessors are called with the task's `migration_report` object via keyword arguments. Custom preprocessors must accept `**kwargs`.

## Source Data Requirements

- **Location**: Place MARC21 MFHD files (`.mrc`) in `iterations/<iteration>/source_data/holdings/`
- **Format**: MARC21 Holdings Format
- **Prerequisite**: Run [BibsTransformer](bibs_transformer) first to create `instance_id_map`

## Decoding Error Handling

`HoldingsMarcTransformer` uses the same MARC decoding and recovery flow as
`BibsTransformer`.

See [MARC Decoding and Recovery Behavior](../marc_rule_based_mapping.md#marc-decoding-and-recovery-behavior)
for full details of the shared decoding pipeline and heuristic order.

- MARC-8 decoding warnings are logged and processing continues.
- Recoverable decode failures are repaired with built-in heuristics.
- Unrecoverable records are logged as failed and skipped.

Review diagnostics in:

- `reports/data_issues_log_<task_name>.tsv`
- `reports/report_<task_name>.md`
- `results/failed_records_decode_<task_name>.mrc` for records that fail MARC decoding
- `results/failed_records_transformation_<task_name>.mrc` for records that fail transformation

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `locationMapFileName` | `folio_code` | Location code |
| `statisticalCodesMapFileName` | `folio_code` | Statistical code |

There are no holdings type or call number type mapping files for MARC holdings. The holdings type comes from the leader (LDR/06), falling back to `fallbackHoldingsTypeId`. The call number type comes from the mapping rules, falling back to `defaultCallNumberTypeName`.

For MARC-based holdings, the legacy location values are extracted from the MFHD record according to the mapping rules (typically from 852$b or similar). Use the column name `legacy_code` for the legacy values when mapping from MARC data.

### Boundwith Relationship File (Optional)

For Voyager-style boundwiths — where one MFHD is linked to several bibs — provide a TSV file in `source_data/holdings/` mapping MFHDs to bibs, and reference it via `boundwithRelationshipFilePath`:

```text
MFHD_ID	BIB_ID
12345	100001
12345	100002
12346	100003
```

For each MFHD listed in the file, the transformer generates one holdings record per bib, with deterministic UUIDs and `holdingsTypeId` set to `holdingsTypeUuidForBoundwiths`. The resulting relationship map is written to `results/boundwith_relationships_map.json` and consumed by the [ItemsTransformer](items_transformer), which creates the `boundwithPart` records.

```{important}
`MFHD_ID` values must match the legacy IDs extracted via `legacyIdMarcPath`, every `BIB_ID` must exist in `instance_id_map`, and the file must list **every** bib in the set — including the one named in the MFHD's `004`.
```

See [Boundwith Handling](../boundwith_handling) for the full requirements, the other supported boundwith patterns, and how to post the results.

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_holdings_<task_name>.json` | FOLIO Holdings records |
| `holdings_id_map.json` | Legacy ID to FOLIO UUID mapping (used by ItemsTransformer). Replaces any existing map; see [Holdings ID Map](holdings_csv_transformer.md#holdings-id-map) |
| `folio_srs_holdings_<task_name>.json` | SRS records (if `createSourceRecords: true`) |
| `extradata_<task_name>.extradata` | Extra data generated during mapping (when applicable) |
| `boundwith_relationships_map.json` | Boundwith relationship mappings, consumed by the ItemsTransformer (when processing boundwiths) |
| `failed_records_decode_<task_name>.mrc` | MARC records that failed to decode from MARC21 format |
| `failed_records_transformation_<task_name>.mrc` | MARC records that decoded OK but failed transformation (empty only if no failures) |

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

The following [ItemsTransformer](items_transformer) task must set `boundwithFlavor: "voyager"` and a non-empty `boundwithRelationshipFilePath` for the `boundwithPart` records to be created. See [Boundwith Handling](../boundwith_handling).

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
- [Boundwith Handling](../boundwith_handling) - All supported boundwith patterns and their source data
- [HoldingsCsvTransformer](holdings_csv_transformer) - Alternative for CSV-based holdings
- [ItemsTransformer](items_transformer) - Transforming items
