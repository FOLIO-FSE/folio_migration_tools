# The migration tasks
The folio_migration_tools are all build on the concept of a set type of migration tasks, that either 
*transforms*, *posts* (loads), or *migrates* data or transactions from a legacy system into FOLIO

## Transform bibs
### Configuration
This configuration piece in the configuration file determines the behaviour
```json
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
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| useTenantMappingRules  | true  | Placeholder for option to use an external rules file  |
| ilsFlavour  | any of "aleph", "voyager", "sierra", "millennium", "koha", "tag907y", "tag001", "tagf990a", "custom"  | Used to point scripts to the correct legacy identifier and other ILS-specific things  |
| custom_bib_id_field  | any MARC field + subfield (eg. 991$a)  | The MARC record field (with optional $-delimited subfield) containing the legacy system ID of the record. Only used when `ilsFlavour` is "custom".  |
| tags_to_delete  | any string  | Tags with these names will be deleted (after transformation) and not get stored in SRS  |
| create_source_records  | `true` or `false`  | Indicates whether the task should create a file of SRS record objects. If `false`, created instance records will be `"source": "FOLIO"`. |
| data_import_marc  | `true` or `false`  | Use alternative MARC transformation process (new in v1.9.0+) to create `"source": "FOLIO"` instance records using a minimal map and a `.mrc` file that can be loaded via Data Import using the default Single Record Import update profile. Implicitly sets `create_source_records` to `false`  |
| files  | Objects with `file_name`, `discoverySuppressed`, and `staffSuppressed` attributes  | `file_name` is the name of the MARC21 file in the `source_data/instances` folder. `discoverySuppressed` is a boolean (`true` or `false`) tells the tools if they should mark records as `"discoverySuppress": true`. `staffSuppressed` (`true` or `false`) tells the tools if they should mark the records as `"staffSuppress": true` |



### Syntax to run
```shell 
folio-migration-tools ./config/exampleConfiguration.json transform_bibs --base_folder ./

```
## Post tranformed Instances
### Configuration
These configuration pieces in the configuration file determines the behaviour

```json
{
    "name": "post_bibs",
    "migrationTaskType": "BatchPoster",
    "objectType": "Instances",
    "batchSize": 250,
    "file": {
        "file_name": "folio_instances_test_run_transform_bibs.json"
    }
}
```

### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| objectType  | Any of "Extradata", "Items", "Holdings", "Instances", "SRS", "Users" | Type of object to post  |
| batchSize  | integer  | The number of records per batch to post. If the API does not allow batch posting, this number will be ignored  |
| file.filename  | Any string  | Name of file to post, located in the results folder  |

### Syntax to run
```shell 
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_bibs --base_folder PATH_TO_migration_repo_template/
```
```{attention}
To load MARC records to SRS for your transformed instances, see: [Posting BibTransformer MARC records](marc_rule_based_mapping.md#posting-bibtransformer-marc-records)
```

## Transform MFHD records to holdings and SRS holdings 
### Configuration
This configuration piece in the configuration file determines the behaviour
```json
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
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| legacyIdMarcPath  | A marc field followed by an optional subfield delimited by a $ | used to locate the legacy identifier for this record. Examles : "001", "951$c"  |
| mfhdMappingFileName  | Any string  | location of the MFHD rules in the mapping_files folder  |
| locationMapFileName  | Any string   | Location of the Location mapping file in the mapping_files folder  |
| defaultCallNumberTypeName  | Any call number name from FOLIO   | Used for fallback mapping for callnumbers  |
| fallbackHoldingsTypeId  | A uuid  | Fallback holdings type if mapping does not work  |
| useTenantMappingRules  | false | boolean (true/false) NOT YET IMPLEMENTED.  |
| hridHandling  | "default" or "preserve001"  | If default, HRIDs will be generated according to the FOLIO settings. If preserve001, the 001s will be used as hrids if possible or fallback to default settings  |
| createSourceRecords  | boolean (true/false)  | Whether or not to create a file of SRS record objects. If `false`, created holdings records will be `"source": "FOLIO"`  |
| supplemental_mfhd_mapping_rules_file  | Any string  | Location of a `.json` file containing MARC mapping rules for MFHD records in the `mapping_files` directory. Contents will be used to "update" the system-provided rules. Only use if `createSourceRecords` is `false`  |
| files  | Objects with filename and boolean  | Filename of the MARC21 file in the data/holdings folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

### Syntax to run
```shell 
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_mfhd --base_folder PATH_TO_migration_repo_template/
```

## Post tranformed MFHDs and Holdingsrecords to FOLIO 
### Configuration
These configuration pieces in the configuration file determines the behaviour
```json
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
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| objectType  | Any of "Extradata", "Items", "Holdings", "Instances", "SRS", "Users" | Type of object to post  |
| batchSize  | integer  | The number of records per batch to post. If the API does not allow batch posting, this number will be ignored  |
| file.filename  | Any string  | Name of file to post, located in the results folder  |

### Syntax to run
```shell 
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_holdingsrecords_from_mfhd --base_folder PATH_TO_migration_repo_template/

folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json post_srs_mfhds --base_folder PATH_TO_migration_repo_template/
```


## Transform CSV/TSV files into Holdingsrecords
### Configuration
These configuration pieces in the configuration file determines the behaviour
```json
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
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| holdingsMapFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| locationMapFileName  | Any string   | Location of the Location mapping file in the mapping_files folder  |
| defaultCallNumberTypeName | any string | Name of callnumber in FOLIO used as a  fallback | 
| callNumberTypeMapFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| previouslyGeneratedHoldingsFiles  |   |  |
| holdingsMergeCriteria  | A list of strings with the names of [holdingsrecord](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/holdingsrecord.json) properties (on the same level) | Used to group indivitual rows into Holdings records. Proposed setting is ["instanceId", "permanentLocationId", "callNumber"] |
|  fallbackHoldingsTypeId | uuid string  | The fallback/default holdingstype UUID |
| createSourceRecords  | boolean (true/false)  |   |
| files  | Objects with filename and boolean  | Filename of the tab-delimited source file in the source_data/items folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

### Syntax to run
```shell 
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_csv_holdings --base_folder PATH_TO_migration_repo_template/
```
## Post trasformed Holdingsrecords to FOLIO
See documentation for posting above

## Transform CSV/TSV files into Items
### Configuration
These configuration pieces in the configuration file determines the behaviour
```json
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
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| itemsMappingFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| locationMapFileName  | Any string   | Location of the location mapping file in the mapping_files folder  |
| prevent_permanent_location_map_default  | `true` or `false`  | If `true`, item permanent location mapping will not use the fallback (`*`) mapping  |
| tempLocationMapFileName  | Any string   | Location of the temporary location mapping file in the mapping_files folder  |
| callNumberTypeMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| materialTypesMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| loanTypesMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| itemStatusesMapFileName  | Any string   | location of the mapping file in the mapping_files folder  |
| files  | Objects with filename and boolean  | Filename tab-delimited source file in the source_data/items folder- Suppressed tells script to mark records as suppressedFromDiscovery  |

### Syntax to run
```shell 
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_csv_items --base_folder PATH_TO_migration_repo_template/
```

## Post transformed Items to FOLIO
See documentation for posting above

## Transform CSV/TSV files into FOLIO users
### Configuration
These configuration pieces in the configuration file determines the behaviour
```json
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
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| userMappingFileName  | Any string  | location of the mapping file in the mapping_files folder  |
| departmentsMapPath  | Any string   | Location of the user departments mapping file in the mapping_files folder  |
| groupMapPath  | Any string   | Location of the user group mapping file in the mapping_files folder  |
| useGroupMap  | boolean   | Use the above group map file or use code-to-code direct mapping  |
| userFile.file_name  | Any string  | name of csv/tsv file of legacy users in the data/users folder |

```{note}
To map multiple departments for a user, ensure that all legacy values are in the same column of your delimited data file, sub-delimited with the `multi_field_delimiter` value from your `libraryConfiguration`
```

### Syntax to run
```shell
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json user_transform --base_folder PATH_TO_migration_repo_template/
```

## Post transformed users to FOLIO
See documentation for posting above

## Transform CSV/TSV files into FOLIO Organizations
### Configuration
These configuration pieces in the configuration file determines the behaviour
```json
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

### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| organizationMapPath  | Any string  | location of the Organizations mapping file in the mapping_files folder  |
| organizationTypesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| addressCategoriesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| emailCategoriesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| phoneCategoriesMapPath  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| files  | Objects with filename and boolean  | List of filenames containing the organization source data  |

### Syntax to run
```shell 
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_organizations --base_folder PATH_TO_migration_repo_template/
```

## Post transformed Organizations to FOLIO
See documentation for posting above. Note that any linked contacts, interfaces, credentials and notes will be in the "organizations.extradata" file. The "extradata" file should be posted after the "folio_organisations" file.

## Transform CSV/TSV files into FOLIO Manual fees/fines
### General considerations
#### Manual (static) fees/fines vs Automatic (incrementing) fees/fnes
This migration task allows you to create static, so-called "manual" fees/fines in FOLIO. These are different from "automatic" fees/fines, which are generated and incremented automatically for open loans by FOLIO's BL pocesses. To avoid "duplicating" fees/fines during migration, we recmmend only creating manual fees/fines for charges that are not related to open loans.

### Mapping best practices
#### Account and feefineAction
Behind the scenes, a manual fee/fine in FOLIO is made up of one "account" and one or more "feeFineActions". In its current implementation, this migration task creates one accoount and one feeFineAction for each row in the source data file. Check out the migration_example repo for a tried and tested example of how you can map your source data to this structure: [manual_feefines_map.json](https://github.com/FOLIO-FSE/migration_example/blob/main/mapping_files/manual_feefines_map.json)

#### Status and Payment status
This migration task allows you to map your fees/fines to any of the allowed Payment statuses. The overall Fee/Fine/Status will however be set to Open if the remaining amount > 0, else to Closed.
#### Reference data mapping
This task allows you to specify up to three reference data mapping files: Fee fien owners, Fee fine types, and Service points. All of the reference data files are optional, so if you prefer you can set them to "" in the task configuration and instead add the UUID of the prefered owner/type/service point as a value in the mapping file.

```{attention}
- The Fee/fine type assigned to the fee/fine must be associated with the Fee/fine owner assigned to the fee/fine. The migration task does not validate this, so your mapping must take this into account.
- FOLIO allows you to create multiple Fee/fine types with identical names. The reference data mapping requires the names to be unique (#616).
```

### Configuration
These configuration pieces in the configuration file determines the behaviour
```json
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
### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| feefinesMap  | Any string  | location of the fee/fine mapping file in the mapping_files folder  |
| feefinesOwnerMap  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| feefinesTypeMap  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| servicePointMap  | Any string   | Location of the reference data mapping file in the mapping_files folder  |
| files  | Objects with filename and boolean  | List of filenames containing the fee/fine source data  |

### Syntax to run
```shell
folio-migration-tools PATH_TO_migration_repo_template/mapping_files/exampleConfiguration.json transform_organizations --base_folder PATH_TO_migration_repo_template/
```
## Post transformed Manual fees/fines to FOLIO
See documentation for posting above. Note that all of the transformed fee/fine information is stored in the fees_fines.extradata file. 
```json
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

## Migration Open Loans
Unlike the preceding data types, migrating open loan data does not require separate transformation and loading tasks or mapping files. It is run as a single task that reads loan data from a CSV/TSV file with a prescribed column set. Optionally, this data can be validated against transformed item and user data to pre-determine if transactiosn will fail due to missing item or patron data in FOLIO

### Loans `source_data` file format
Unlike other data types that allow you to map columns in a legacy source data file to fields in a FOLIO object, the `LoansMigrator` task requires a CSV/TSV file with specific column headings:

* Required headings
  * `item_barcode`
  * `patron_barcode`
  * `due_date`
  * `out_date`
* Optional headings
  * `proxy_patron_barcode`
  * `renewal_count`
  * `next_item_status`
  * `service_point_id`

### Example task configuration
```json
{
  "name": "migrate_open_loans",
  "migrationTaskType": "LoansMigrator",
  "fallbackServicePointId": "a77b55e7-f9f3-40a1-83e0-241bc606a826",
  "openLoansFiles": [
    {
        "file_name": "loans.tsv",
        "service_point_id": "a77b55e7-f9f3-40a1-83e0-241bc606a826"
    }
  ],
  "startingRow": 1,
  "item_files": [
    {
        "file_name": "folio_items_transform_csv_items.json"
    },
    {
        "file_name": "folio_items_transform_bw_items.json"
    },
    {
        "file_name": "folio_items_transform_mfhd_items.json"
    }
  ],
  "patron_files": [
    {
        "file_name": "folio_users_user_transform.json"
    }
  ]
}
```

### Explanation of parameters
| Parameter  | Possible values  | Explanation  | 
| ------------- | ------------- | ------------- |
| Name  | Any string  | The name of this task. Created files will have this as part of their names.  |
| migrationTaskType  | Any of the [avialable migration tasks](https://github.com/FOLIO-FSE/folio_migration_tools/tree/main/src/folio_migration_tools/migration_tasks)  | The type of migration task you want to run  |
| openLoansFiles  | Objects with filename and optional `service_point_id`   | location of the open loan source data in the `source_data/loans` folder and optional fallback `service_point_id` for the file |
| startingRow  | Integer   | Row of the loans file(s) to start on  |
| itemFiles  | Objects with filename    | Location of the transformed item records for the iteration in the `results` folder  |
| patronFiles  | Objects with filename    | Location of the transformed patron records for the iteration in the `results` folder  |

### How it Works
As mentioned, this task attempts to create new `/circulation/loans` transactions via the standard `/circulation/check-out-by-barcode` API, rather than writing `loan-storage` objects to FOLIO. This is done for two reasons:

* To avoid the need to replicate the circulation rule system in these tools (to set appropriate policy values)
* To generate the appropriate schedule notices in FOLIO

```{attention}
This process can generate _thousands_ of notices, depending on how many loans are migrated and patron notice policies configured. We *strongly recommend* disabling SMTP while ading open loans. The task will check to see if SMTP is disabled before it begins and give you ten seconds to stop the task before it proceeds. How to disable outgoing SMTP is left as an exercise for the reader.
```

If patron and/or item files are specified in the task configuration, the task will validate the loans data files against them, setting aside any rows that contain item or patron barcodes not found in the transformed users or items files. Rows with `due_date` values that precede `out_date` will also be set aside. Both sets of records will be saved to the `failed_records_failed_<task_name>_<timestamp>.txt` in `results`.

Once the source data has made it through validation, the task will attempt to post post the loans via `check-out-by-barcode`, with all overrides specified. If the loan is created successfully, the task will then update the `loanDate` and `dueDate` values of the resulting loan to match the original values. This will re-generate any scheduled notices.

If the loan's item has a status that cannot normally be checked out ("Aged to lost", "Declared lost", "Claimed returned", "Checked out"), the task will report the initial failure and then attempt to change the status to "Available" and try to create the loan again. Once that is complete, the status will be reset, as needed.

```{tip}
While the task will, in most cases, be able to load a loan where the item is not in a loanable status, this will significantly slow things down. It's highly recommended to migrate any loaned items as "Available" or another loanable status.
```

If a patron is inactive in FOLIO, the first attempt to create the loan will fail. The task will then attempt to activate the patron, create the loan again, and then deactivate the patron before proceeding.
