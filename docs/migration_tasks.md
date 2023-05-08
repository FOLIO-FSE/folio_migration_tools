```{contents} 
:depth: 1
```
# The migration tasks
The folio_migration_tools are all build on the concept of a set type of migration tasks, that either 
*transforms*, *posts* (loads), or *migrates* data or transactions from a legacy system into FOLIO
# Transform bibs
## Configuration
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

## Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| useTenantMappingRules  | true  | Placeholder for option to use an external rules file  |
| ilsFlavour  | any of "aleph", "voyager", "sierra", "millennium", "koha", "tag907y", "tag001", "tagf990a"  | Used to point scripts to the correct legacy identifier and other ILS-specific things  |
| tags_to_delete  | any string  | Tags with these names will be deleted (after transformation) and not get stored in SRS  |
| files  | Objects with filename and boolean  | Filename of the MARC21 file in the data/instances folder- Suppressed tells script to mark records as suppressedFromDiscovery  |



## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_bibs --base_folder PATH_TO_migration_repo_template/

```
# Post tranformed Instances and SRS records 
## Configuration
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

## Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| objectType  | Any of "Extradata", "Items", "Holdings", "Instances", "SRS", "Users" | Type of object to post  |
| batchSize  | integer  | The number of records per batch to post. If the API does not allow batch posting, this number will be ignored  |
| file.filename  | Any string  | Name of file to post, located in the results folder  |

## Syntax to run
``` 
 python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_bibs --base_folder PATH_TO_migration_repo_template/

  python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_srs_bibs --base_folder PATH_TO_migration_repo_template/

```

# Transform MFHD records to holdings and SRS holdings 
## Configuration
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
## Explanation of parameters
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
| files  | Objects with filename and boolean  | Filename of the MARC21 file in the data/holdings folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_mfhd --base_folder PATH_TO_migration_repo_template/
```

# Post tranformed MFHDs and Holdingsrecords to FOLIO 
## Configuration
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

## Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| objectType  | Any of "Extradata", "Items", "Holdings", "Instances", "SRS", "Users" | Type of object to post  |
| batchSize  | integer  | The number of records per batch to post. If the API does not allow batch posting, this number will be ignored  |
| file.filename  | Any string  | Name of file to post, located in the results folder  |

## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_holdingsrecords_from_mfhd --base_folder PATH_TO_migration_repo_template/

python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_srs_mfhds --base_folder PATH_TO_migration_repo_template/
```


# Transform CSV/TSV files into Holdingsrecords
## Configuration
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
## Explanation of parameters
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
| files  | Objects with filename and boolean  | Filename of the tab-delimited source file in the source_data/items folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_csv_holdings --base_folder PATH_TO_migration_repo_template/
```
# Post trasformed Holdingsrecords to FOLIO
See documentation for posting above

# Transform CSV/TSV files into Items
## Configuration
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
## Explanation of parameters
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
| files  | Objects with filename and boolean  | Filename tab-delimited source file in the source_data/items folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_csv_items --base_folder PATH_TO_migration_repo_template/
```

# Post transformed Items to FOLIO
See documentation for posting above

# Transform CSV/TSV files into FOLIO users
## Configuration
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
## Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| userMappingFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| groupMapPath  | Any string   | Location of the user group mapping file in the mapping_files folder  |
| useGroupMap  | boolean   | Use the above group map file or use code-to-code direct mapping  |
| userFile.file_name  | Any string  | name of csv/tsv file of legacy users in the data/users folder |


## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json user_transform --base_folder PATH_TO_migration_repo_template/
```

# Post transformed users to FOLIO
See documentation for posting above
¨

# Transform CSV/TSV files into FOLIO Organizations
## Configuration
These configuration pieces in the configuration file determines the behaviour
```
{
    "name": "transform_organizations",
    "migrationTaskType": "OrganizationTransformer",
    "organizationMapPath": "organizations_map.json",
    "organizationTypesMapPath": "organizations_types_mapping.tsv",
    "addressCategoriesMapPath": "address_categories_map.tsv",
    "emailCategoriesMapPath": "email_categories_map.tsv",
    "phoneCategoriesMapPath": "phone_categories_map.tsv",
    "files": [
        {
            "file_name": "organizations_export.tsv"
        }
    ]
}

```
## Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| organizationMapPath  | Any string  | location of the Organizations mapping file in the mapping_files folder  |
| organizationTypesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| addressCategoriesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| emailCategoriesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| phoneCategoriesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| files  | Objects with filename and boolean  | List of filenames containing the organization source data  |

## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_organizations --base_folder PATH_TO_migration_repo_template/
```

# Post transformed Organizations to FOLIO
See documentation for posting above. Note that any linked contacts, interfaces, credentials and notes will be in the "organizations.extradata" file. The "extradata" file should be posted after the "folio_organisations" file.
¨

# Transform CSV/TSV files into FOLIO Manual fees/fines
## General considerations
### Manual (static) fees/fines vs Automatic (incrementing) fees/fnes
This migration task allows you to create static, so-called "manual" fees/fines in FOLIO. These are different from "automatic" fees/fines, which are generated and incremented automatically for open loans by FOLIO's BL pocesses. To avoid "duplicating" fees/fines during migration, we recmmend only creating manual fees/fines for charges that are not related to open loans.
## Mapping best practices
### Account and feefineAction
Behind the scenes, a manual fee/fine in FOLIO is made up of one "account" and one or more "feeFineActions". In its current implementation, this migration task creates one accoount and one feeFineAction for each row in the source data file. Check out the migration_example repo for a tried and tested example of how you can map your source data to this structure: [manual_feefines_map.json](https://github.com/FOLIO-FSE/migration_example/blob/main/mapping_files/manual_feefines_map.json)

### Status and Payment status
This migration task allows you to map your fees/fines to any of the allowed Payment statuses. The overall Fee/Fine/Status will however be set to Open if the remaining amount > 0, else to Closed.
### Reference data mapping
This task allows you to specify up to three reference data mapping files: Fee fien owners, Fee fine types, and Service points. All of the reference data files are optional, so if you prefer you can set them to "" in the task configuration and instead add the UUID of the prefered owner/type/service point as a value in the mapping file.

> Be aware that:
> - The Fee/fine type assigned to the fee/fine must be associated with the Fee/fine owner assigned to the fee/fine. The migration task does not validate this, so your mapping must take this into account.
> - FOLIO allows you to create multiple Fee/fine types with identical names. The reference data mapping requires the names to be unique (#616).

## Configuration
These configuration pieces in the configuration file determines the behaviour
```
{
    "name": "transform_manual_feefines",
    "migrationTaskType": "ManualFeeFinesTransformer",
    "feefinesMap": "manual_feefines_map.json",
    "feefinesOwnerMap": "feefine_owners.tsv",
    "feefinesTypeMap": "feefine_types.tsv",
    "servicePointMap": "feefines_service_points.tsv",
    "files": [
        {
            "file_name": "test_feefines.tsv"
        }
    ]
}

```
## Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks]()  | The type of migration task you want to run  |
| feefinesMap  | Any string  | location of the fee/fine mapping file in the mapping_files folder  |
| feefinesOwnerMap  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| feefinesTypeMap  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| servicePointMap  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| files  | Objects with filename and boolean  | List of filenames containing the fee/fine source data  |

## Syntax to run
``` 
python -m folio_migration_tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_organizations --base_folder PATH_TO_migration_repo_template/
```
# Post transformed Manual fees/fines to FOLIO
See documentation for posting above. Note that all of the transformed fee/fine information is stored in the fees_fines.extradata file. 
```
{
    "name": "post_feefines",
    "migrationTaskType": "BatchPoster",
    "objectType": "Extradata",
    "batchSize": 1,
    "files": [
        {
            "file_name": "extradata_transform_manual_feefines.extradata"
        }
    ]
}
```
