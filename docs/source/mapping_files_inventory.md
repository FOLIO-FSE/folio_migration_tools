# Mapping files for Inventory migrations
In order to successfully migrate Inventory data into FOLIO, you will need to create (or account for) the following files in the [`mapping_files`](quick_tutorial.md#step-into-the-repo-and-create-the-example-folder-structure-py-running) directory of your migration project.

## What file is needed for what objects?
File\Process | Bibs->Instances | Holdings (from MARC/MFHD) | Holdings (from item tsv/csv) | Items  | Open Loans  | Users   
------------ | ------------- | ------------- | ------------- | ------------- | ------------- | -------------   
marc-instance-mapping-rules.json  | yes | no | no | no |   no    |   no
mfhd_rules.json  | no | yes | no | no |  no   |   no
item_mapping.json  | no | no | no | yes |  no   |   no
holdings_mapping.json  | no | no | yes |  no   |   no
locations.tsv  | no | yes | yes | yes |  no   |   no
temp_locations.tsv  | no | no | no | optional |  no   |   no
material_types.tsv  | no | no | no |  yes   |   no
loan_types.tsv  | no | no | no | yes |  no   |   no
temp_loan_types.tsv  | no | no | no | optional |  no   |   no
call_number_type_mapping.tsv  | no | no | optional | optional |  no   |   no
statcodes.tsv  | optional | optional | optional | optional |  no   |   no
item_statuses.tsv | no | no | no | optional    |  no   |   no
post_loan_migration_statuses.tsv | no | no | no | no    |  optional  |   no
patron_types.tsv | no | no | no | no    |  no  |   yes
user_mapping.json | no | no | no | no    |  no  |   yes
department_mapping.tsv | no | no | no | no    |  no  |   yes

## Example Records
You will find examples of these files in the [mapping_files](https://github.com/FOLIO-FSE/migration_repo_template/tree/main/mapping_files) folder of the [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)

## MARC to FOLIO Mapping Rules
### 📄 marc-instance-mapping-rules.json
These are the mapping rules from MARC21 bib records to FOLIO instances. The rules are stored in the tenant, but it is good practice to keep them under version control so you can maintain the customizations as the mapping rules evolve. For more information on syntax etc, read the [documentation](https://github.com/folio-org/mod-source-record-manager/blob/master/RuleProcessorApi.md).

### 📄 mfhd_rules.json
This file is built out according to the [mapping rules for bibs](https://github.com/folio-org/mod-source-record-manager/blob/master/RuleProcessorApi.md). The rules are stored in the tenant. The conditions are different, and not well documented at this point. Look at the example file and refer to the mapping rules documentation.

### 📄 supplemental_mfhd_rules.json
This file contains mapping rules to augment/customize the tenant-supplied rules (see above). The field rules defined here will replace the tenant-supplied rules for that field (useful for transforming MFHD holdings to source=FOLIO holdings). For more information, see: [Supplemental MFHD Mapping Rules](marc_rule_based_mapping.md#supplemental-mfhd-mapping-rules)

## Delimited Data to FOLIO Mapping Rules
```{important}
To represent the legacy data field used to generate the deterministic UUID of the transformed record, delimited field mapping rules use a "magic" `folio_field` named `legacyIdentifier`. The value mapped to this field __*must be unique*__ in the source data, and this field must be mapped for all delimited data transformations.
```

### 📄 holdings_mapping.json
See [`item_mapping.json`](#-item_mappingjson), below.

### 📄 item_mapping.json
This is a mapping file for the items. The process assumes you have the item data in a CSV/TSV format. 
The structure of the file is dependant on the the column names in the TSV file. For example, if you have a file that looks like this:
... | Z30_BARCODE | Z30_CALL_NO | Z30_DESCRIPTION |  ... 
------------ | ------------- | ------------- | ------------- | -------------
 ... | 123456790 | Some call number | some note  | ...
 


Your map should look like this:
```
...
{
    "folio_field": "barcode",
    "legacy_field": "Z30_BARCODE",
    "value":"",
    "description": ""
},
{
    "folio_field": "itemLevelCallNumber",
    "legacy_field": "Z30_CALL_NO",
    "value":"",
    "description": ""
}, 
{
    "folio_field": "notes[0].itemNoteTypeId",
    "legacy_field": "Z30_DESCRIPTION",
    "value": "c7bc292c-a318-43d3-9b03-7a40dfba046a",
    "description": ""
},
{
    "folio_field": "notes[0].staffOnly",
    "legacy_field": "Z30_DESCRIPTION",
    "value": false,
    "description": ""
},
{
    "folio_field": "notes[0].note",
    "legacy_field": "Z30_DESCRIPTION",
    "value": false,
    "description": ""
},
...
```
The resulting FOLIO Item would look like this:
```
{
	...
	"barcode": "123456790",
	"itemLevelCallNumber": "Some call number"
	"notes":[{
			"staffOnly": false,
			"note": "some note",
			"itemNoteTypeId": "c7bc292c-a318-43d3-9b03-7a40dfba046a"			
		}],
	...
}
```

## Reference data mapping files

```{attention}
Most reference data mapping fields (locations.tsv, material_types.tsv, locations.tsv etc) allow you to add `*` to legacy fields in a row, and add the falback value from folio in the folio_code/folio_name column. If the mapping fails, the script will assign this value to the records created. Good practice is to have migration-specific value as a falback value to be able to locate the records in FOLIO. The exception to this is "item_statuses.tsv", where "Available" is always the fallback status, and `*` must not be used.
```

### 📄 locations.tsv
These mappings allow for some complexity. These are the mappings of the legacy and FOLIO locations. The file must be structured like this:
 folio_code | legacy_code | Z30_COLLECTION 
------------ | ------------- | -------------
 AFA | AFAS | AFAS   
 AFA  |  * |  *    
 
The legacy_code part is needed for both Holdings migratiom. For Item migration, the source fields can be used (Z30_COLLECTION in this case). You can add as many source fields as you like for the Items

### 📄 material_types.tsv
These mappings allow for some complexity. The first column name is fixed, since that is the target material type in FOLIO. Then you add the column names from the Item export TSV. For each column added, the values in them must match. At least one value per column must match. Se loan_types.tsv for complex examples
 folio_name | Z30_MATERIAL 
------------ | ------------- 
 Audiocassette | ACASS
 Audiocassette | *

### 📄 loan_types.tsv
These mappings allow for some complexity. The first column name is fixed, since that is the target loan type in FOLIO. Then you add the column names from the Item export TSV. For each column added, the values in them must match. At least one value per column must match

 folio_name | Z30_SUB_LIBRARY | Z30_ITEM_STATUS 
------------ | ------------- | -------------
 Non-circulating | UMDUB | 02
 Non-circulating | * | *   

### 📄 call_number_type_mapping.tsv
These mappings allow for some complexity eventhough not needed. 
 folio_name | Z30_CALL_NO_TYPE 
------------ | -------------
Dewey Decimal classification | 8
Unmapped | *   

### 📄 statcodes.tsv
In order to map one statistical code to the FOLIO UUID, you need this map, and the field mapped in the item_mappings.json. These mappings allow for some complexity even though not needed. This mapping does not allow for default values. Any record without the field will not get one assigned.
 folio_code | legacy_stat_code 
------------ | -------------
married_with_children | 8
happily_ever_after | 9

### 📄 item_statuses.tsv	
The handling of Item statuses is a bit of a project of its own, since not all statuses in legacy systems will have their equivalents in FOLIO. This mapping allows you to point one legacy status to a FOLIO status. If not status map is supplied, the status will be set to available.
legacy_code | folio_name 
------------ | -------------
checked_out | Checked out
available | Available
lost | Aged to lost

```{attention}
If the item status you are mapping is the result of a circulation transaction (i.e. "Checked out", "Paged", "Aged to lost", "Declared lost", "Claimed returned"), we recommend using an item status mapping of Available for these items, instead. You can set the `next_item_status` in your loans data migration to ensure that loan-related statuses are appropriately set after the migrated loan is created.
```
