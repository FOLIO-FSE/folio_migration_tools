```{contents}
:depth: 1
```
# Statistical Code Mapping
## Introduction
FOLIO provides the ability to construct a controlled vocabulary of [statistical codes](https://docs.folio.org/docs/settings/settings_inventory/settings_inventory/#settings--inventory--statistical-codes) that can be assigned to three principal inventory record types (Instances, Holdings, and Items). `folio_migration_tools` allows you to map legacy values from your MARC Bibs, MARC Holdings, CSV Holdings, or CSV Items to the list of statistical codes during the migration process. The tools also allow you to map statistical codes based on the source file of the records being migrated.

## Mapping Statistical Codes for MARC-sourced Records (MARC Bib and MFHD)
Mapping of MARC-source data is controlled by a [MARC-to-FOLIO mapping rules config](./marc_rule_based_mapping.md). However, this system does not allow you to map statistical codes directly. To map statistical codes from MARC data you will need the following:

* A statistical code mapping file specified in your task configuration
    * TSV file with the following columns `legacy_stat_code` and `folio_code`
* A list of MARC fields tags (with option subfields) to pull legacy codes from
    * Note: when you specify the `legacy_stat_code` value for these codes, you must prefix them with the MARC field tag (and subfield) from when they are taken (eg. `998:arch` or `998_a:arch`, if `arch` is the code in the MARC field/subfield)
* A FileDefinition (the object in the `"files"` array of a task configuration) with `"statistical_code"` set to code value(s) you want to assign to all records created from that file (eg. `"statistical_code": "arch"`)

### Multiple codes from one field/subfield/file definition
If you want to pull multiple legacy stat codes from the same MARC field, you can either provide them as repeating (or multiple different) subfields (eg. `998$a$b$c` to pull codes from all three subfields) or use the `multi_field_delimiter` value specified in your project's `libraryConfiguration`. Here is an example of a MARC field with statistical code data and the task configuration you'll need to map them:

MARC field
```marc
=998 \\$aarch$bebooks$cebooks$cjournal
```
Statistical code map
```tsv
legacy_stat_code	folio_code
998_a:arch	arch
arch	arch
folios	oversize
998_b:ebooks	ebooks
998_c:ebooks	ebooks-sub
998_c:journal	journals-sub
```

Task configuration

```json
{
    "name": "bibs",
    "migrationTaskType": "BibsTransformer",
    "ilsFlavor": "sierra",
    "statisticalCodeMapFileName": "statistical_codes.tsv",
    "statistical_code_mapping_fields": [
        "998$a$b$c"
    ],
    "files": [
        {
            "file_name": "arch_collection.mrc",
            "discovery_suppressed": false,
            "statistical_code": "arch<delimiter>folios"
        }
    ]
}
```

Assuming the codes in the `folio_code` column exist in FOLIO, they will appear in the FOLIO record created from the record containing that MARC field.

## Mapping statistical code data from CSV/TSV data
Mapping statistical code data from delimited data files (CSV/TSV) is somewhat more straightforward. You need:

* A field mapping configuration that maps at least one legacy field to `statisticalCodeIds`
* A statistical code mapping file

Here is an example field mapping and it's mapping TSV:

```json
{
    "folio_field": "statisticalCodeIds[0]",
    "legacy_field": "icode1",
    "value": "",
    "description": "Mapping ICODE1 (normalized) to a statistical code"
}
```

```tsv
legacy_stat_code	folio_code
u	weeding
g	gift
$	facoffice
*	gencol
```

```{attention}
Note: the wildcard fallback mapping will not be used, but must be included for the mapping file to be valid.
```

## Notes and observations
* Mappings that result in the same FOLIO code being mapped more than once will be deduplicated
* You can mix repeating and non-repeating subfields and `multi_field_delimiter` when mapping from MARC fields
* Un-mapped legacy codes are dropped from the record and reported in the associated data issue log for your task