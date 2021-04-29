# MARC21-To-FOLIO
AA set of Python3 script transforming MARC21 and Items in delimited files to FOLIO inventory objects.

The scripts requires a FOLIO tenant with reference data set. The script will throw messages telling what reference data is missing. 

When the files have been created, post them to FOLIO using the [service_tools](https://github.com/FOLIO-FSE/service_tools) set of programs.

## Relevant FOLIO community documentation
* [Instance Metadata Elements](https://docs.google.com/spreadsheets/d/1RCZyXUA5rK47wZqfFPbiRM0xnw8WnMCcmlttT7B3VlI/edit#gid=952741439)
* [Recommended MARC mapping to Inventory Instances](https://docs.google.com/spreadsheets/d/11lGBiPoetHuC3u-onVVLN4Mj5KtVHqJaQe4RqCxgGzo/edit#gid=1891035698)
* [Recommended MFHD to Inventory Holdings mapping ](https://docs.google.com/spreadsheets/d/1ac95azO1R41_PGkeLhc6uybAKcfpe6XLyd9-F4jqoTo/edit#gid=301923972)
* [Holdingsrecord JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/holdingsrecord.json)
* [FOLIO Instance storage JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/instance.json)
* [FOLIO Intance (BL) JSON Schema](https://github.com/folio-org/mod-inventory/blob/master/ramls/instance.json)
* [Inventory elements - Beta](https://docs.google.com/spreadsheets/d/1RCZyXUA5rK47wZqfFPbiRM0xnw8WnMCcmlttT7B3VlI/edit#gid=901484405)
* [MARC Mappings Information](https://wiki.folio.org/display/FOLIOtips/MARC+Mappings+Information)

# FOLIO Inventory data migration process
This template plays a vital part in a process together with other repos allowing you to perform bibliographic data migration from a legacy ILS into FOLIO. For more information on the process, head over to the linked repos below.
In order to perform migrations according to this process, you need to clone the following repositories:   
* [MARC21-to-FOLIO](https://github.com/FOLIO-FSE/MARC21-To-FOLIO)
* [service_tools](https://github.com/FOLIO-FSE/service_tools)
* [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)

# Part of a process
The scripts rely on previous migrations steps, like the existance of a map file with legacy system IDs and their FOLIO equivalents. 

# Mapping files
The scripts also relies on a folder with a set of mapping files. There is a [template repository](https://github.com/FOLIO-FSE/migration_repo_template) with examples of the files needed and some documentation around it in the [Readme](https://github.com/FOLIO-FSE/migration_repo_template/blob/main/README.md). There is also a [web tool](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation) for creating mapping files from delimited source files

## Bib records to Invcentory and SRS records
MARC mapping for Bib level records is based on the mapping-rules residing in a FOLIO tenant.
Read more on this in the Readme in the [Source record manager Module repo](https://github.com/folio-org/mod-source-record-manager/blob/25283ebabf402b5870ae4b3846285230e785c17d/RuleProcessorApi.md).

### MFHD-to-Inventory
#### Mapping rules
This processing does not store the MARC records anywhere since this is not available in FOLIO yet. Only FOLIO Holdings records are created.
MFHD-to-Inventory mapping also relies on mapping based on a similar JSON structure. This is not stored in the tenant and must be maintained by you. A template/example is available in [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)

#### Location mapping
For holdings mapping, you also need to map legacy locations to FOLIO locations. An example map file is available at [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) 

## Items-to-Inventory
Items-to-Inventory mapping is based on a json structure where the CSV headers are matched against the target fields in the FOLIO items. To create a mapping file, use the [web tool](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation).

# Tests
There is a test suite for Bibs-to-Instance mapping.
## Running the tests for the Rules mapper

* Install the packages in the Pipfile
* Make a copy of the test_config.json.template and add the necessary credentials.
* Run ```pipenv run python3 -m unittest test_rules_mapper.TestRulesMapper```

Since you need to point your test towards a FOLIO tenant the Test suit is somwehat unstable. But it is still very useful for ironing out complex mapping issues.

# Running the scripts
For information on what files are needed and produced by the tools, refer to the documentation and example files in the [template repository](https://github.com/FOLIO-FSE/migration_repo_template).

## Bibs Migration
```pipenv run python3 main_bibs.py PATH_TO_FOLDER_WITH_MARC_FILES RESULTS_FOLDER OKAPI_URL TENANT_ID USERNAME PASSWORD ILS_FLAVOUR```

The above will fetch the mapping-rules from the FOLIO tenant specified and transform the supplied MARC21 record files into FOLIO Instance and SRS records.

## MFHD Migration
```pipenv run python main_holdings.py PATH_TO_MFHD_RECORDS RESULTS_FOLDER OKAPI_URL TENANT_ID USERNAME PASSWORD -m MAPPING_FILES_FOLDER ILS_FLAVOUR```

## Item Migration
```pipenv run python main_items.py PATH_TO_FOLDER_WITH_ITEM_RECORDS RESULTS_FOLDER MAPPING_FILES_FOLDER OKAPI_URL TENANT_ID USERNAME PASSWORD```


Example:
```
pipenv run python ~/code/MARC21-To-FOLIO/main_bibs.py ~/code/migration_repo_template/example_files/data/bibs ~/code/migration_repo_template/example_files/results/ https://okapi-bugfest-honeysuckle.folio.ebsco.com fs090000
00 folio folio voyager
```
### Explanation
**time** will write out some statistics about time elapsed and memory usage. This is optional but useful. 

**pipenv run python** instructs to run the script in a virtual environment using python. 

#### Arguments used within the script

**~/code/MARC21-To-FOLIO/main_bibs.py** is the script we want to run. 

**~/client_data/hogwartslibrary/goldenrod/data/bibs** is the folder where we’ll fetch the file with MARC records to transform. 

**~/client_data/hogwartslibrary/goldenrod/results** is the folder where we’ll place the result files (created instances and transformation report). 

**https://okapi-hogwartslibrary.folio.ebsco.com fs00000000 admin 'password' iii** is a string containing the okapi URL, tenant ID, username and password for the tenant, as well as a code indicating the legacy ILS. 

**| tee ~/client_data/hogwartslibrary/goldenrod/results/instance_transformation.log** prints the log that is printed in the terminal during the running and also prints it to a file. This is optional but useful. 
## main_holdings.py
For actual examples of the output, go to the [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)
## main_bibs.py (Bib transformation)
```
 pipenv run python3 /codez/MARC21-To-FOLIO/main_holdings.py ~/code/migration_repo_template/example_files/data/holdings ~/code/migration_repo_template/example_files/results https://okapi-bugfest-honeysuckle.folio.ebsco.com fs09000000 folio folio voyager -m ~/code/migration_repo_template/mapping_files
 ```
 
 ## main_items.py
 ```
 pipenv run python3 /codez/MARC21-To-FOLIO/main_items.py ~/code/migration_repo_template/example_files/data/items ~/code/migration_repo_template/example_files/results https://okapi-bugfest-honeysuckle.folio.ebsco.com fs09000000 folio folio -m ~/code/migration_repo_template/mapping_files
```
# Bib records mapping
## SRS record Loading
In order for SRS record loading to run, you need a snapshot object in the FOLIO database. The snapshot ID (jobExecutionId) is hard coded into the SRS records by the transformation scripts. To do this, do the following:    
Make a POST request to your FOLIO tenant to this endpoint:   
```
{{baseUrl}}/source-storage/snapshots
```
with the following payload:   
```
{ 

    "jobExecutionId": "67dfac11-1caf-4470-9ad1-d533f6360bdd", 
    "status": "PARSING_IN_PROGRESS", 
    "processingStartedDate": "2020-12-30T14:33:50.478+0000", 
    "metadata": { 
        "createdDate": "2020-12-30T14:33:50.478+0000", 
        "createdByUserId": "0280835d-b08d-4187-969d-9b4ecc247eae", 
        "updatedDate": "2020-12-30T14:33:50.478+0000", 
        "updatedByUserId": "0280835d-b08d-4187-969d-9b4ecc247eae" 
    } 
} 
```

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
