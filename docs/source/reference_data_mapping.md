(reference-data-mapping)=
# Reference Data Mapping

Reference data mapping files connect values from your legacy system to FOLIO reference data (locations, material types, loan types, patron groups, etc.). This document explains the general concepts that apply across all migration tasks.

## How Reference Data Mapping Works

When migrating data to FOLIO, many fields require UUIDs that correspond to reference data configured in your FOLIO tenant. For example, an item's `materialTypeId` must be a UUID matching a material type in FOLIO, not the legacy system's material type code.

Reference data mapping files provide this translation layer:

1. **Input**: A value from your legacy data (e.g., `BOOK`)
2. **Lookup**: Find the matching row in the mapping file
3. **Output**: The corresponding FOLIO value (e.g., `book`) which is then resolved to its UUID

## File Structure

Reference data mapping files are tab-separated (TSV) files with:

- **Header row**: Column names that define the mapping structure
- **Data rows**: One row per mapping
- **Fallback row** (optional): A row with `*` in legacy columns to specify a default

### Basic Example

```text
ITYPE	folio_name
BOOK	book
DVD	dvd
CD	sound recording
*	unspecified
```

In this example:
- `ITYPE` is the legacy column (matches your source data field name)
- `folio_name` is the FOLIO column (contains the FOLIO value to map to)
- The `*` row provides a fallback when no match is found

## Column Naming

### Legacy Columns

Legacy column names are **dynamic**—they must match the field name(s) in your source data that you've mapped to the corresponding FOLIO field in your mapping file.

For example, if your item mapping file has:

```json
{
    "folio_field": "materialTypeId",
    "legacy_field": "ITEM_TYPE"
}
```

Then your material types mapping file should use `ITEM_TYPE` as the legacy column:

```text
ITEM_TYPE	folio_name
BOOK	book
```

### FOLIO Columns

The FOLIO column name depends on the type of reference data being mapped:

| Reference Data Type | FOLIO Column | Description |
|---------------------|--------------|-------------|
| Locations | `folio_code` | Location **code** (not name) |
| Material Types | `folio_name` | Material type **name** |
| Loan Types | `folio_name` | Loan type **name** |
| Call Number Types | `folio_name` | Call number type **name** |
| Patron Groups | `folio_name` | Patron group **name** (the `group` field) |
| Departments | `folio_name` | Department **name** |
| Statistical Codes | `folio_code` | Statistical code **code** |
| Acquisition Methods | `folio_value` | Acquisition method **value** |
| Organization Types | `folio_name` | Organization type **name** |
| Fee/Fine Owners | `folio_owner` | Fee/fine owner **name** (the `owner` field) |
| Service Points | `folio_name` | Service point **name** |

```{important}
The FOLIO column value must exactly match the value in your FOLIO tenant. Values are case-sensitive.
```

## Multi-Column Mapping

You can map based on multiple legacy fields by adding additional columns. All values must match for the row to be selected.

```text
LOCATION	SUBLIBRARY	folio_code
MAIN	CIRC	main-circ
MAIN	REF	main-reference
BRANCH	CIRC	branch-circ
*	*	main-stacks
```

This maps items based on both `LOCATION` and `SUBLIBRARY` fields from the source data.

## Fallback Rows

A fallback row uses `*` in all legacy columns to specify a default value when no match is found:

```text
LOCATION	folio_code
MAIN	main-stacks
REF	reference
*	unmapped-location
```

```{tip}
Use a migration-specific fallback value (like `unmapped-location` or `migration-default`) so you can easily locate records that didn't map correctly after migration.
```

### Fallback Exceptions

Some reference data types do not support fallback rows:

| Reference Data Type | Fallback Behavior |
|---------------------|-------------------|
| Item Statuses | Fallback is always `Available`; `*` rows are not allowed |
| Statistical Codes | No fallback; unmapped records simply don't get a code |

## Item Status Mapping

Item status mapping has special requirements:

1. **Fixed column names**: Must use exactly `legacy_code` and `folio_name`
2. **No fallback rows**: The `*` wildcard is not allowed
3. **Valid FOLIO statuses only**: Must be one of the predefined FOLIO item statuses

```text
legacy_code	folio_name
AVAILABLE	Available
CHECKED_OUT	Checked out
IN_TRANSIT	In transit
MISSING	Missing
```

Valid FOLIO item statuses:
- `Available`, `Awaiting pickup`, `Awaiting delivery`, `Checked out`
- `Claimed returned`, `Declared lost`, `In process`, `In process (non-requestable)`
- `In transit`, `Intellectual item`, `Long missing`, `Lost and paid`
- `Missing`, `On order`, `Paged`, `Restricted`, `Order closed`
- `Unavailable`, `Unknown`, `Withdrawn`

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Unmapped (Default value was set)" in report | Legacy value not in mapping file | Add the missing value to your mapping file |
| Wrong FOLIO value assigned | Typo in `folio_name`/`folio_code` | Verify the value matches exactly what's in FOLIO |
| All records get fallback value | Column name mismatch | Ensure legacy column name matches source data field name |
| "not a recognized field" error | Missing column in mapping file | Add the required legacy column |

### Verifying Your Mappings

1. **Check the migration report**: Look for the mapping statistics section to see which legacy values mapped to which FOLIO values
2. **Review unmapped values**: The report lists any values that fell through to the default
3. **Validate FOLIO values**: Query your FOLIO tenant to ensure the target values exist

## See Also

- [Mapping File Based Mapping](mapping_file_based_mapping) - General mapping file syntax
- [Mapping Files for Inventory](mapping_files_inventory) - Inventory-specific mapping files
- [Mapping Files for Circulation](mapping_files_circulation) - Circulation-specific mapping files
- [Statistical Code Mapping](statistical_codes) - Detailed statistical code mapping
