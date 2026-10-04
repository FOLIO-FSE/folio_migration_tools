# Boundwith Handling

A *boundwith* (also written "bound-with") is a single physical volume containing works that are described by more than one bibliographic record. In FOLIO, that relationship is expressed with two kinds of objects:

* One **holdings record per instance** in the boundwith set. Each holdings record describes the same physical piece, but is attached to a different instance.
* One **`boundwithPart`** record per (item, holdings) pair. These are stored in `inventory-storage/bound-with-parts` and are what FOLIO uses to surface a single item under each of the instances it is bound with.

The item itself is still attached to exactly one holdings record through `holdingsRecordId`. The `boundwithPart` records carry the rest of the relationship.

`folio_migration_tools` produces both halves of that structure, but *which task* does the work, and *what source data you must supply*, depends on how your legacy system recorded the relationship. This page documents each supported pattern ("flavor") in full.

## Which flavor do I have?

| Flavor | Legacy pattern | Typical systems | Task that creates the holdings | Task that creates the `boundwithPart` records |
|---|---|---|---|---|
| [MFHD relationship file](#flavor-1-mfhd-relationship-file-boundwiths-voyager-style) | One MFHD is linked to several bibs; the extra links live outside the MFHD record | Voyager | [HoldingsMarcTransformer](tasks/holdings_marc_transformer) | [ItemsTransformer](tasks/items_transformer) (`boundwithFlavor: "voyager"`) |
| [Item-level link file](#flavor-2-item-level-link-file-boundwiths-aleph-style) | Each bib already has its own holdings record; items are linked to several holdings records | Aleph (LKR / `Z103` links) | [HoldingsMarcTransformer](tasks/holdings_marc_transformer) (ordinary, non-boundwith run) | [ItemsTransformer](tasks/items_transformer) (`boundwithFlavor: "aleph"`) |
| [Multi-bib item rows](#flavor-3-multi-bib-item-row-boundwiths-sierra-and-millennium-style) | The item export row itself names several bib IDs | Sierra, III, Millennium | [HoldingsCsvTransformer](tasks/holdings_csv_transformer) | [HoldingsCsvTransformer](tasks/holdings_csv_transformer) |

```{note}
The flavors are named for the systems they were first built for, but nothing in the code is system-specific. If your ILS can produce an export matching one of the source-data shapes below, use that flavor regardless of vendor.
```

```{important}
The `boundwithFlavor` task setting accepts any value from the tools' `IlsFlavour` enum, but only `"voyager"` and `"aleph"` have boundwith implementations. Setting any other value *together with* `boundwithRelationshipFilePath` raises a `TransformationProcessError`. Flavor 3 needs neither setting — see below.
```

## Behavior shared by all flavors

### Deterministic identifiers

Every UUID involved is derived deterministically from your legacy identifiers and the tenant's base string, so re-running a transformation produces the same records, and posting is idempotent:

| Object | Derived from |
|---|---|
| Primary holdings record | the legacy holdings ID (`MFHD_ID`, or the `legacyIdentifier` of the source row) |
| Additional boundwith holdings records | `<primary holdings UUID>-<instance UUID>` |
| `boundwithPart` | `<item UUID>-<holdings UUID>` |

This is why the ItemsTransformer can recompute the UUIDs of boundwith holdings records it never created itself.

### How additional holdings copies are made

When the tools need a holdings record for an additional instance, they deep-copy the record they already built and then adjust it:

1. `instanceId` is set to the next instance UUID in the set.
2. `holdingsTypeId` is set to `holdingsTypeUuidForBoundwiths` — on **every** copy in the set, including the first one.
3. For copies after the first, `id` is replaced with the derived boundwith UUID and `hrid` is removed. See [HRIDs and record source on the copies](#hrids-and-record-source-on-the-copies).
4. If `callNumber` holds a stringified list, the copy takes the call number at its own position in that list. See [Per-instance call numbers](#per-instance-call-numbers).

```{important}
`holdingsTypeUuidForBoundwiths` is required by the tasks that create boundwith holdings ([HoldingsMarcTransformer](tasks/holdings_marc_transformer) and [HoldingsCsvTransformer](tasks/holdings_csv_transformer)). If a boundwith is detected and the setting is empty, the transformation fails with a `TransformationProcessError`. Create a holdings type for this purpose under **Settings → Inventory → Holdings types** and reference its UUID.

The setting is *not* part of the [ItemsTransformer](tasks/items_transformer) configuration; that task only writes `boundwithPart` records.
```

### HRIDs and record source on the copies

Only the **first** holdings record of a boundwith set is the one tied to the legacy record, so only that one carries migration-assigned metadata:

| Field | First record of the set | Additional copies |
|---|---|---|
| `hrid` | Assigned by the transformation, per `hridHandling` (MFHD flavors only) | Not set — FOLIO assigns an HRID when the record is created |
| `sourceId` | `MARC` when source records are being created, otherwise `FOLIO` | Always `FOLIO` |
| `formerIds` | The legacy holdings ID, plus the bib ID from the `004` | Identical — inherited from the first record |

Because only the first record of the set gets an SRS record, the copies must not claim `MARC` as their source; the transformation sets them to `FOLIO` regardless of `createSourceRecords`.

```{tip}
`formerIds` is how you retrieve a whole boundwith set. Every copy inherits the legacy holdings identifier, so querying holdings by former ID (for example `formerIds=="12345"` against `holdings-storage`) returns every holdings record generated from that MFHD. The HRIDs of the copies bear no relation to the first record's HRID.
```

#### HRID start values when using `preserve001`

With `"hridHandling": "preserve001"`, holdings HRIDs are taken from the MFHD `001` instead of being enumerated from the tenant's HRID counter. FOLIO still uses that counter for the boundwith copies, and HRIDs must be unique across all holdings records in the tenant.

Check whether your legacy `001` values share an alphabetical prefix with the holdings HRID prefix configured under **Settings → Inventory → HRID handling**:

* **Same prefix** (legacy `001`s look like `ho000012345` and the FOLIO prefix is `ho`) — raise the holdings start value above the highest numeric part in your legacy `001`s before loading, or FOLIO will eventually generate an HRID that collides with a preserved one.
* **No prefix, or a different prefix** (legacy `001`s are bare numbers, or look like `mfhd12345` while FOLIO uses `ho`) — no adjustment is needed; the two ranges cannot collide.

### Per-instance call numbers

Boundwith volumes often carry a different call number under each bib. To migrate them, map a source column whose value is a stringified list of call numbers, positionally aligned with the list of bib IDs:

```text
BIB_ID	CALL_NUMBER
['b1000001', 'b1000002']	['PS3566 .Y55 1974', 'PS3566 .Y56 1974']
```

* Each generated holdings record takes the call number at its own index.
* If the list is shorter than the set of instances, the remaining copies fall back to the **first** call number in the list.
* If the value cannot be parsed as a list, it is used verbatim for every copy.
* A single-element list (`['PS3566 .Y55 1974']`) is unwrapped to a plain string.

This mainly applies to [flavor 3](#flavor-3-multi-bib-item-row-boundwiths-sierra-and-millennium-style), where call numbers come from a delimited column. In the MFHD-based flavors the call number is mapped from the MARC record and copied unchanged.

## Flavor 1: MFHD relationship-file boundwiths (Voyager style)

In Voyager, a boundwith volume is a single MFHD that is linked to more than one bib. The extra links are not in the MFHD record itself (the `004` names only one bib), so they have to be supplied separately.

### Source data requirements

**MFHD records** in `iterations/<iteration>/source_data/holdings/`, as for any [HoldingsMarcTransformer](tasks/holdings_marc_transformer) run. Bib linking is read from the `004`.

**A relationship file**: a tab-separated file, also in `iterations/<iteration>/source_data/holdings/`, with a header row and the columns `MFHD_ID` and `BIB_ID`. One row per bib-to-MFHD link:

```text
MFHD_ID	BIB_ID
12345	100001
12345	100002
12346	100003
```

In Voyager this is a direct extract of the `bib_mfhd` table, restricted to the MFHDs you are migrating.

```{important}
Requirements for the relationship file:

* **`MFHD_ID` values must match the legacy holdings IDs the transformer extracts** via `legacyIdMarcPath` (usually the `001`), character for character. The MFHD's holdings UUID is derived from that value, and it is the key used to look up the relationship map.
* **`BIB_ID` values must exist in `instance_id_map`** from the [BibsTransformer](tasks/bibs_transformer) run. A `BIB_ID` that is not in the map is reported as a failed record and no boundwith holdings is created for it.
* **List every bib in the set, including the one in the `004`.** The relationship map replaces the instance link of the record it matches: the first `BIB_ID` for an `MFHD_ID` becomes the `instanceId` of the holdings record that keeps the primary UUID. If you omit the `004` bib, that instance simply will not get a holdings record.
* Rows missing (or with an empty) `MFHD_ID` or `BIB_ID` abort the run with a `TransformationProcessError`.
* MFHDs with only one row in the file are still processed through the boundwith path — they get one copy, and that copy receives `holdingsTypeUuidForBoundwiths`. Only include MFHDs that really are boundwiths.
```

### What happens during transformation

1. **HoldingsMarcTransformer** reads the relationship file and builds a map of `holdings UUID → [instance UUID, …]`.
2. Each MFHD is mapped normally. If its UUID is a key in that map, the resulting record is replaced by one copy per instance in the set, as described in [How additional holdings copies are made](#how-additional-holdings-copies-are-made).
3. On wrap-up, the map is written to `results/boundwith_relationships_map.json` as line-delimited JSON — one `[holdings UUID, [instance UUID, …]]` pair per line.
4. **ItemsTransformer** loads that file and, for every item whose `holdingsRecordId` is a key in the map, writes one `boundwithPart` per instance in the set: the first points at the primary holdings UUID, the rest at the derived boundwith UUIDs.

```{note}
The HoldingsMarcTransformer does **not** write `boundwithPart` records. For this flavor they are produced entirely by the ItemsTransformer, which is why both tasks need boundwith configuration.
```

### Configuration

Holdings task:

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

Items task, run after the holdings task:

```json
{
    "name": "transform_items",
    "migrationTaskType": "ItemsTransformer",
    "itemsMappingFileName": "item_mapping.json",
    "locationMapFileName": "locations.tsv",
    "materialTypesMapFileName": "material_types.tsv",
    "loanTypesMapFileName": "loan_types.tsv",
    "boundwithFlavor": "voyager",
    "boundwithRelationshipFilePath": "bib_mfhd.tsv",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

```{important}
For this flavor the ItemsTransformer does not actually read the file named by `boundwithRelationshipFilePath` — it reads `results/boundwith_relationships_map.json` produced by the holdings task. The setting acts as an opt-in switch: **if it is empty, no boundwith relationships are loaded and no parts are created.** Naming the same file as the holdings task keeps the configuration readable.

If the setting is populated but `results/boundwith_relationships_map.json` is missing or malformed, the task fails with a `TransformationProcessError`. Run the holdings transformation first, in the same iteration.
```

Items must link to the MFHD's own legacy ID: map `holdingsRecordId` to the source column holding the `MFHD_ID` value, so the item resolves to the primary holdings UUID that keys the relationship map.

## Flavor 2: Item-level link-file boundwiths (Aleph style)

In Aleph, each bib in a boundwith set already has its own HOL record, so no extra holdings records need to be created. What has to be migrated is the item's links to those additional HOL records (Aleph `LKR` fields, stored in `Z103`).

Because nothing happens at the holdings level, the holdings transformation is an ordinary run: no `boundwithRelationshipFilePath`, no `holdingsTypeUuidForBoundwiths`. All boundwith work happens in the ItemsTransformer.

### Source data requirements

A tab-separated file in `iterations/<iteration>/source_data/items/` with a header row and the columns `LKR_HOL` (legacy holdings ID) and `ITEM_REC_KEY` (legacy item ID). One row per item-to-holdings link:

```text
LKR_HOL	ITEM_REC_KEY
000123456	ITEM001
000123457	ITEM001
000789012	ITEM002
```

```{important}
Requirements for the link file:

* **`ITEM_REC_KEY` must match the item's `legacyIdentifier`** as mapped in the items mapping file. That value is what the tools hash into the item UUID referenced by the `boundwithPart`.
* **`LKR_HOL` must match a key in `holdings_id_map`** — that is, the legacy holdings ID as extracted by the holdings transformation. Watch out for Aleph's zero-padded nine-digit document numbers: the padding in this file must match the padding in the holdings records.
* **Include the item's own holdings record**, not just the linked ones, if you want a part for it. Unlike flavor 1, nothing is added implicitly; one `boundwithPart` is created per row. The extraction recipe below does this by unioning `ITEM_HOL` and `LKR_HOL`.
* Duplicate rows are harmless — relationships are collected into a set per item, and the derived `boundwithPart` UUIDs are deterministic.
```

### What happens during transformation

1. The ItemsTransformer loads the file into a map of `item legacy ID → {holdings legacy ID, …}`.
2. Once the mapper (and its `holdings_id_map`) is available, every relationship is validated. A `LKR_HOL` that is not in `holdings_id_map` is logged as a data issue and dropped; items left with no valid links are removed from the map. Counts land in the migration report as *Aleph boundwith relationships validated successfully* and *Aleph boundwith relationships removed (holdings not found)*.
3. For each item whose legacy ID is in the map, one `boundwithPart` is written per holdings record, using the UUID from `holdings_id_map`.

No holdings records are created or modified, so a missing `holdingsTypeUuidForBoundwiths` is irrelevant here.

### Configuration

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

### Extracting the relationships from Aleph

The `LKR` links can be pulled from the Aleph tables with a query along these lines:

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

Then reshape the result into the two-column file the task expects. This example keeps both the item's own holdings (`ITEM_HOL`) and its linked holdings (`LKR_HOL`):

```python
# Example python script (using polars dataframe library) to generate the actual boundwith_data file
import polars as pl
from pathlib import Path

relationship_file = Path("../iterations/iteration_1/source_data/items/raw_boundwith_data.tsv")

# Create the initial lazyframe for the raw data
boundwiths_df = pl.scan_csv(
    relationship_file, separator="\t", infer_schema=False, null_values=["", "[NULL]"]
)

# We need to capture all item->holdings relationships, so we will concatenate two sub-selections
prepped_df = pl.concat(
    [
        boundwiths_df.select(["ITEM_REC_KEY", "ITEM_HOL"]).rename({"ITEM_HOL": "LKR_HOL"}),
        boundwiths_df.select(["ITEM_REC_KEY", "LKR_HOL"]),
    ]
)

# Now, we need to export to a TSV file that can be included in the items transformer task configuration
prepped_df.filter(
    pl.col(
        "LKR_HOL"
    ).is_not_null()  # We can't link an item to a holdings record that doesn't exist
).unique().sink_csv(relationship_file.parent.joinpath("item_holdings_links.tsv", separator="\t"))
```

## Flavor 3: Multi-bib item-row boundwiths (Sierra and Millennium style)

Systems that export item-level data can record the boundwith relationship in the item row itself: the row names every bib the piece is attached to. Holdings records do not exist in the source data at all — [HoldingsCsvTransformer](tasks/holdings_csv_transformer) derives them from the item rows, and it handles the entire boundwith structure in that one pass.

### Source data requirements

Delimited item files in `iterations/<iteration>/source_data/items/`, as for any HoldingsCsvTransformer run. The column mapped to `instanceId` must contain a **stringified list of bib IDs** for boundwith rows:

```text
ITEM_ID	BIB_ID	LOCATION	CALL_NUMBER
i1000001	['b1000001', 'b1000002']	main	['PS3566 .Y55 1974', 'PS3566 .Y56 1974']
i1000002	b1000003	main	QA76 .B47 1985
```

```{important}
Requirements for the item rows:

* The list must be parseable as a Python literal — square brackets, comma-separated, quoted values (`['b1', 'b2']` or `["b1", "b2"]`). An unparseable value fails that record with *Instance ID could not get parsed to array of strings*.
* **A single-element list is not a boundwith.** It is unwrapped and treated as an ordinary one-instance holdings record.
* Every bib ID must resolve in `instance_id_map`. Values beginning with `b` are also tried with a leading period (`b1000001` → `.b1000001`) to accommodate Sierra/III record-number conventions; if neither form is found, the record fails with *Bib id not in instance id map*.
* Call numbers may be supplied as a positionally aligned list — see [Per-instance call numbers](#per-instance-call-numbers).
* If the value mapped to `formerIds` is itself a stringified list, it is split into individual former IDs on the holdings record.
```

### What happens during transformation

1. A row that resolves to more than one instance is treated as a boundwith, and one holdings record is generated per instance, as described in [How additional holdings copies are made](#how-additional-holdings-copies-are-made).
2. One `boundwithPart` is written per generated holdings record, linking the item (derived from the row's legacy ID) to that holdings record.
3. Boundwith holdings bypass the normal `holdingsMergeCriteria` merge. They are de-duplicated on their own composite key instead:

   ```text
   bw_<instanceId>_<permanentLocationId>_<callNumber>_<all instance IDs, sorted>
   ```

   A later row producing the same key is merged into the existing boundwith holdings record — and still gets its own `boundwithPart` — rather than creating a duplicate. Rows whose boundwith set, location, or call number differ produce separate holdings records.
4. The `holdings_id_map` entry for the source row points at the **first** record generated from it — the one that kept the UUID derived from the row's own legacy ID. A subsequent [ItemsTransformer](tasks/items_transformer) run therefore attaches the item to that record through `holdingsRecordId`, while the `boundwithPart` records tie it to every record in the set.

   Every copy still carries the same former IDs, but only the row's own legacy ID resolves to a holdings record. Other values in `formerIds`, such as legacy bib IDs, are not added to `holdings_id_map`, so an item whose `holdingsRecordId` is mapped to one of them fails with *Holdings id referenced in legacy item was not found amongst transformed Holdings records*. See [ItemsTransformer](tasks/items_transformer.md#item-mapping-file) for how this changed in version 1.12.11.

```{note}
Which copy owns the item link changed in version 1.12.11, bringing this flavor in line with the other two: the item goes on the record that kept the legacy record's own UUID. Before that, the `holdings_id_map` entry was left pointing at whichever copy of the set was written last, so the instance an item appeared under depended on the order the bib IDs came out of the source row.

For a migration already in progress, this only matters if the holdings have been loaded into FOLIO and you re-run the items task: the items move from the last copy of each set to the first. No relationship is lost — `boundwithPart` records are written for every copy either way — but post the whole item set again so the old `holdingsRecordId` values are overwritten rather than mixed with the new ones.
```

```{note}
`holdingsTypeUuidForBoundwiths` does double duty for this task. Besides marking the generated records, it is the holdings type that [`previouslyGeneratedHoldingsFiles`](tasks/holdings_csv_transformer.md#parameters) uses to exclude already-created boundwith holdings from merging when they are re-loaded in a later run. Keep the same value across all tasks in the migration.
```

### Configuration

```json
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "holdingsMapFileName": "holdings_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "holdingsTypeUuidForBoundwiths": "1b6c62cf-034c-4972-ac80-fa595a9bfbde",
    "files": [
        {
            "file_name": "items.tsv"
        }
    ]
}
```

```{important}
The following [ItemsTransformer](tasks/items_transformer) run needs **no** boundwith configuration — the parts already exist. In particular, do not set `boundwithRelationshipFilePath` on the items task: with the default `boundwithFlavor` of `"voyager"` it makes the task look for a `results/boundwith_relationships_map.json` that this flavor never produces, and the run fails.
```

## Loading the results into FOLIO

Boundwith holdings records are part of the normal holdings output (`results/folio_holdings_<task_name>.json`) and are posted with the rest of your holdings by [InventoryBatchPoster](tasks/inventory_batch_poster) or [BatchPoster](tasks/batch_poster).

`boundwithPart` records are written to the **extradata** file of the task that created them — `results/extradata_<task_name>.extradata` — and are posted separately with a [BatchPoster](tasks/batch_poster) task of `objectType: "Extradata"`, which routes them to `inventory-storage/bound-with-parts`:

```json
{
    "name": "post_boundwith_parts",
    "migrationTaskType": "BatchPoster",
    "objectType": "Extradata",
    "batchSize": 1,
    "files": [
        {
            "file_name": "extradata_transform_items.extradata"
        }
    ]
}
```

```{important}
Post the parts **after** the instances, holdings, and items they reference. A `boundwithPart` is rejected if either referenced record does not yet exist.
```

Use the extradata file from whichever task created the parts: the items task for flavors 1 and 2, the holdings task for flavor 3.

## Verifying the results

The migration report (`reports/report_<task_name>.md`) carries boundwith counters under **General statistics** and, for flavor 3, a dedicated **Bound-with mapping** section:

| Report entry | Task | Meaning |
|---|---|---|
| `Boundwith relationships loaded` | ItemsTransformer | Rows in the loaded relationship map |
| `Items matched to boundwith relationships` | ItemsTransformer | Items that matched an entry in the map |
| `Boundwith parts created` | ItemsTransformer, HoldingsCsvTransformer | Total `boundwithPart` records written |
| `Bound-with holdings created` | HoldingsMarcTransformer, HoldingsCsvTransformer | Holdings copies generated for boundwith sets |
| `Unique BW Holdings created from Items` | HoldingsCsvTransformer | Distinct boundwith holdings created |
| `BW Items found tied to previously created BW Holdings` | HoldingsCsvTransformer | Rows merged into an existing boundwith holdings record |
| `Bound-with items identified by bib id` | HoldingsCsvTransformer | Rows detected as boundwiths from a multi-bib value |
| `Bib ids referenced in bound-with items` | HoldingsCsvTransformer | Total bib IDs across all boundwith rows |
| `Bound-with items callnumber identified` | HoldingsCsvTransformer | Rows supplying a list of per-instance call numbers |
| `Aleph boundwith relationships validated successfully` | ItemsTransformer | Links resolved against `holdings_id_map` |
| `Aleph boundwith relationships removed (holdings not found)` | ItemsTransformer | Links dropped because the holdings record was missing |

Unresolvable references are listed in `reports/data_issues_log_<task_name>.tsv`.

A quick sanity check on the numbers: for a set of *n* instances sharing one physical piece, expect *n* holdings records and — in flavors 1 and 3 — *n* `boundwithPart` records per item. In flavor 2 you get one part per row you supplied for that item.

## Common pitfalls

| Symptom | Cause |
|---|---|
| `Missing task setting holdingsTypeUuidForBoundwiths` | A boundwith was detected but no boundwith holdings type is configured on the holdings task. |
| `Boundwith relationship file specified, but relationships file from holdings transformation not found` | ItemsTransformer is in `"voyager"` flavor but the holdings transformation has not run in this iteration — or the migration is really flavor 3, and `boundwithRelationshipFilePath` should not be set at all. |
| No parts created, no errors | `boundwithRelationshipFilePath` is empty on the ItemsTransformer (flavors 1 and 2 both need it as the opt-in switch). |
| `Boundwith relationship map contains a BIB_ID id not in the instance id map` | A `BIB_ID` in the relationship file was not migrated, or the ID form differs from the one in `instance_id_map`. |
| `Holdings for boundwith relationship not found in holdings id map` | An Aleph `LKR_HOL` value does not match a legacy holdings ID — frequently a zero-padding mismatch. |
| Relationship file appears to be ignored | `MFHD_ID` values do not match the legacy holdings IDs extracted by `legacyIdMarcPath`. |
| One instance in a set has no holdings record | The relationship file omits that bib — including the case of the bib named in the MFHD's `004`. |
| Duplicate boundwith holdings in FOLIO | `holdingsTypeUuidForBoundwiths` differs between runs, or between the holdings task and the `previouslyGeneratedHoldingsFiles` consumer. |

## See Also

* [HoldingsMarcTransformer](tasks/holdings_marc_transformer) — MFHD transformation, including the relationship file parameter
* [HoldingsCsvTransformer](tasks/holdings_csv_transformer) — deriving holdings from item rows
* [ItemsTransformer](tasks/items_transformer) — item transformation and `boundwithFlavor`
* [MARC Rules Based Mapping](marc_rule_based_mapping) — MFHD mapping rules
* [BatchPoster](tasks/batch_poster) — posting extradata
