# The migration process

In order to perform migrations according to this process, you need the following:
* Python 3.9 or later
* An Installation of [FOLIO Migration Tools](https://pypi.org/project/folio-migration-tools/). See the Installing page for information.
* A repo created from  the template repository [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)
* Access to the [Data mapping file creator](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation) web tool
* A FOLIO tenant running the latest or the second latest version of FOLIO

# FOLIO Inventory data migration process

The FSE FOLIO Migration tools requires you to run the transformation an data loading in sequence, and each step relies on previous migrations steps, like the existance of a  file with legacy system IDs and their FOLIO equivalents. 
The below picture shows the proposed migration steps for legacy objects into FOLIO:
![image](https://user-images.githubusercontent.com/1894384/139079124-b31b716f-281b-4784-b73e-a4567ee3e097.png)


## Result files
The following table outlines the result records and their use and role
 File | Content | Use for 
------------ | ------------- | ------------- 
folio_holdings.json | FOLIO Holdings records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs
folio_instances.json | FOLIO Instance records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs
folio_items.json |FOLIO Item records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs
holdings_id_map.json | A json map from legacy Holdings Id to the ID of the created FOLIO Holdings record | To be used in subsequent transformation steps 
holdings_transformation_report.md | A file containing various breakdowns of the transformation. Also contains errors to be fixed by the library | Create list of cleaning tasks, mapping refinement
instance_id_map.json | A json map from legacy Bib Id to the ID of the created FOLIO Instance record. Relies on the "ILS Flavour" parameter in the main_bibs.py scripts | To be used in subsequent transformation steps 
instance_transformation_report.md | A file containing various breakdowns of the transformation. Also contains errors to be fixed by the library | Create list of cleaning tasks, mapping refinement
item_id_map.json | A json map from legacy Item Id to the ID of the created FOLIO Item record | To be used in subsequent transformation steps 
item_transform_errors.tsv | A TSV file with errors and data issues together with the row number or id for the Item | To be used in fixing of data issues 
items_transformation_report.md | A file containing various breakdowns of the transformation. Also contains errors to be fixed by the library | Create list of cleaning tasks, mapping refinement
marc_xml_dump.xml | A MARCXML dump of the bib records, with the proper 001:s and 999 fields added | For pre-loading a Discovery system.
srs.json | FOLIO SRS records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs



## HRID handling
### Current implementation:   
Download the HRID handling settings from the tenant. 
**If there are HRID handling in the mapping rules:**
- The HRID is set on the Instance
- The 001 in the MARC21 record (bound for SRS) is replaced with this HRID.

**If the mapping-rules specify no HRID handling or the field designated for HRID contains no value:**
- The HRID is being constructed from the HRID settings
- Pad the number in the HRID Settings so length is 11
- A new 035 field is created and populated with the value from 001
- The 001 in the MARC21 record (bound for SRS) is replaced with this HRID.


## Relevant FOLIO community documentation
* [Instance Metadata Elements](https://docs.google.com/spreadsheets/d/1RCZyXUA5rK47wZqfFPbiRM0xnw8WnMCcmlttT7B3VlI/edit#gid=952741439)
* [Recommended MARC mapping to Inventory Instances](https://docs.google.com/spreadsheets/d/11lGBiPoetHuC3u-onVVLN4Mj5KtVHqJaQe4RqCxgGzo/edit#gid=1891035698)
* [Recommended MFHD to Inventory Holdings mapping ](https://docs.google.com/spreadsheets/d/1ac95azO1R41_PGkeLhc6uybAKcfpe6XLyd9-F4jqoTo/edit#gid=301923972)
* [Holdingsrecord JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/holdingsrecord.json)
* [FOLIO Instance storage JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/instance.json)
* [FOLIO Intance (BL) JSON Schema](https://github.com/folio-org/mod-inventory/blob/master/ramls/instance.json)
* [Inventory elements - Beta](https://docs.google.com/spreadsheets/d/1RCZyXUA5rK47wZqfFPbiRM0xnw8WnMCcmlttT7B3VlI/edit#gid=901484405)
* [MARC Mappings Information](https://wiki.folio.org/display/FOLIOtips/MARC+Mappings+Information)

# Perform a test migration
The mapping files and example data in this repo will enable you perform a migration against the latest FOLIO bugfest enironment. Everything is configured except for the missing FOLIO user password.
This step-by-step guide will take you through the steps involved. If there are no more steps, we are still working on these example records

## Before you begin
* Move everything under the example_data folder into the data folder.
* Setup pipenv using either the Pipfile or the requirements.txt

## Transform bibs
### Configuration
This configuration piece in the configuration file determines the behaviour
```
 {
    "name": "transform_bibs",
    "migrationTaskType": "BibsTransformer",
    "useTenantMappingRules": true,
    "ilsFlavour": "tag001",
    "tags_to_delete": [
        "841",
        "852"
    ],
    "files": [
        {
            "file_name": "bibs.mrc",
            "suppressed": false
        }
    ]
}
```

### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| useTenantMappingRules  | true  | Placeholder for option to use an external rules file  |
| ilsFlavour  | any of "aleph", "voyager", "sierra", "millennium", "koha", "tag907y", "tag001", "tagf990a"  | Used to point scripts to the correct legacy identifier and other ILS-specific things  |
| tags_to_delete  | any string  | Tags with these names will be deleted (after transformation) and not get stored in SRS  |
| files  | Objects with filename and boolean  | Filename of the MARC21 file in the data/instances folder- Suppressed tells script to mark records as suppressedFromDiscovery  |



### Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_bibs --base_folder PATH_TO_migration_repo_template/

```
## Post tranformed Instances and SRS records 
### Configuration
These configuration pieces in the configuration file determines the behaviour
```
{
    "name": "post_bibs",
    "migrationTaskType": "BatchPoster",
    "objectType": "Instances",
    "batchSize": 250,
    "file": {
        "file_name": "folio_instances_test_run_transform_bibs.json"
    }
},
{
    "name": "post_srs_bibs",
    "migrationTaskType": "BatchPoster",
    "objectType": "SRS",
    "batchSize": 250,
    "file": {
        "file_name": "folio_srs_instances_test_run_transform_bibs.json"
    }
}
```

### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| objectType  | Any of "Extradata", "Items", "Holdings", "Instances", "SRS", "Users" | Type of object to post  |
| batchSize  | integer  | The number of records per batch to post. If the API does not allow batch posting, this number will be ignored  |
| file.filename  | Any string  | Name of file to post, located in the results folder  |

### Syntax to run
``` 
 python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_bibs --base_folder PATH_TO_migration_repo_template/

  python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_srs_bibs --base_folder PATH_TO_migration_repo_template/

```

## Transform MFHD records to holdings and SRS holdings 
### Configuration
This configuration piece in the configuration file determines the behaviour
```
{
    "name": "transform_mfhd",
    "migrationTaskType": "HoldingsMarcTransformer",
    "legacyIdMarcPath": "001",
    "mfhdMappingFileName": "mfhd_rules.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "useTenantMappingRules": false,
    "hridHandling": "default",
    "createSourceRecords": true,
    "files": [
        {
            "file_name": "holding.mrc",
            "suppressed": false
        }
    ]
}
```
### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| legacyIdMarcPath  | A marc field followed by an optional subfield delimited by a $ | used to locate the legacy identifier for this record. Examles : "001", "951$c"  |
| mfhdMappingFileName  | Any string  | location of the MFHD rules in the mapping_files folder  |
| locationMapFileName  | Any string   | Location of the Location mapping file in the mapping_files folder  |
| defaultCallNumberTypeName  | Any call number name from FOLIO   | Used for fallback mapping for callnumbers  |
| fallbackHoldingsTypeId  | A uuid  | Fallback holdings type if mapping does not work  |
| useTenantMappingRules  | false | boolean (true/false) NOT YET IMPLEMENTED.  |
| hridHandling  | "default" or "preserve001"  | If default, HRIDs will be generated according to the FOLIO settings. If preserve001, the 001s will be used as hrids if possible or fallback to default settings  |
| createSourceRecords  | boolean (true/false)  |   |
| files  | Objects with filename and boolean  | Filename of the MARC21 file in the data/instances folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

### Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_mfhd --base_folder PATH_TO_migration_repo_template/
```

## Post tranformed MFHDs and Holdingsrecords to FOLIO 
### Configuration
These configuration pieces in the configuration file determines the behaviour
```
{
    "name": "post_holdingsrecords_from_mfhd",
    "migrationTaskType": "BatchPoster",
    "objectType": "Holdings",
    "batchSize": 250,
    "file": {
        "file_name": "folio_holdings_test_run_transform_mfhd.json"
    }
},
{
    "name": "post_srs_mfhds",
    "migrationTaskType": "BatchPoster",
    "objectType": "SRS",
    "batchSize": 250,
    "file": {
        "file_name": "folio_srs_holdings_test_run_transform_mfhd.json"
    }
}
```

### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| objectType  | Any of "Extradata", "Items", "Holdings", "Instances", "SRS", "Users" | Type of object to post  |
| batchSize  | integer  | The number of records per batch to post. If the API does not allow batch posting, this number will be ignored  |
| file.filename  | Any string  | Name of file to post, located in the results folder  |

### Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_holdingsrecords_from_mfhd --base_folder PATH_TO_migration_repo_template/

python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_srs_mfhds --base_folder PATH_TO_migration_repo_template/
```


## Transform CSV/TSV files into Holdingsrecords
### Configuration
These configuration pieces in the configuration file determines the behaviour
```
{
    "name": "transform_csv_holdings",
    "migrationTaskType": "HoldingsCsvTransformer",
    "holdingsMapFileName": "holdingsrecord_mapping.json",
    "locationMapFileName": "locations.tsv",
    "defaultCallNumberTypeName": "Library of Congress classification",
    "callNumberTypeMapFileName": "call_number_type_mapping.tsv",
    "previouslyGeneratedHoldingsFiles": [
        "folio_holdings_test_run_transform_mfhd"
    ],
    "holdingsMergeCriteria": [
        "instanceId",
        "permanentLocationId",
        "callNumber"
    ],
    "fallbackHoldingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "files": [
        {
            "file_name": "csv_items.tsv"
        }
    ]
}
```
### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| holdingsMapFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| locationMapFileName  | Any string   | Location of the Location mapping file in the mapping_files folder  |
| defaultCallNumberTypeName | any string | Name of callnumber in FOLIO used as a  fallback | 
| callNumberTypeMapFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| previouslyGeneratedHoldingsFiles  |   |  |
| holdingsMergeCriteria  | A list of strings with the names of [holdingsrecord](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/holdingsrecord.json) properties (on the same level) | Used to group indivitual rows into Holdings records. Proposed setting is ["instanceId", "permanentLocationId", "callNumber"] |
|  fallbackHoldingsTypeId | uuid string  | The fallback/default holdingstype UUID |
| createSourceRecords  | boolean (true/false)  |   |
| files  | Objects with filename and boolean  | Filename of the MARC21 file in the data/instances folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

### Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_mfhd --base_folder PATH_TO_migration_repo_template/
```
## Post trasformed Holdingsrecords to FOLIO
See documentation for posting above

## Transform CSV/TSV files into Items
### Configuration
These configuration pieces in the configuration file determines the behaviour
```
{
    "name": "transform_csv_items",
    "migrationTaskType": "ItemsTransformer",    
    "itemsMappingFileName": "item_mapping_for_csv_items.json",
    "locationMapFileName": "locations.tsv",
    "callNumberTypeMapFileName": "call_number_type_mapping.tsv",
    "materialTypesMapFileName": "material_types_csv.tsv",
    "loanTypesMapFileName": "loan_types_csv.tsv",
    "itemStatusesMapFileName": "item_statuses.tsv",
    "files": [
        {
            "file_name": "csv_items.tsv"
        }
    ]
}
```
### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| itemsMappingFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| locationMapFileName  | Any string   | Location of the Location mapping file in the mapping_files folder  |
| callNumberTypeMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| materialTypesMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| loanTypesMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| itemStatusesMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| files  | Objects with filename and boolean  | Filename of the MARC21 file in the data/instances folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

### Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_csv_items --base_folder PATH_TO_migration_repo_template/
```

## Post transformed Items to FOLIO
See documentation for posting above

## Transform CSV/TSV files into FOLIO users
### Configuration
These configuration pieces in the configuration file determines the behaviour
```
{
    "name": "user_transform",
    "migrationTaskType": "UserTransformer",
    "groupMapPath": "user_groups.tsv",
    "userMappingFileName": "user_mapping.json",
    "useGroupMap": true,
    "userFile": {
        "file_name": "staff.tsv"
    }
}
```
### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| userMappingFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| groupMapPath  | Any string   | Location of the user group mapping file in the mapping_files folder  |
| useGroupMap  | boolean   | Use the above group map file or use code-to-code direct mapping  |
| userFile.file_name  | Any string  | name of csv/tsv file of legacy users in the data/users folder |


### Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json user_transform --base_folder PATH_TO_migration_repo_template/
```

## Post transformed users to FOLIO
See documentation for posting above
¨
