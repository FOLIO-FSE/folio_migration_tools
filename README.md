# MARC21-To-FOLIO
A set of Python3 script parsing MARC21 to FOLIO inventory format.

# Relevant FOLIO community documentation
* [Instance Metadata Elements](https://docs.google.com/spreadsheets/d/1RCZyXUA5rK47wZqfFPbiRM0xnw8WnMCcmlttT7B3VlI/edit#gid=952741439)
* [Recommended MARC mapping to Inventory Instances](https://docs.google.com/spreadsheets/d/11lGBiPoetHuC3u-onVVLN4Mj5KtVHqJaQe4RqCxgGzo/edit#gid=1891035698)
* [Recommended MFHD to Inventory Holdings mapping ](https://docs.google.com/spreadsheets/d/1ac95azO1R41_PGkeLhc6uybAKcfpe6XLyd9-F4jqoTo/edit#gid=301923972)
* [Holdingsrecord JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/holdingsrecord.json)
* [FOLIO Instance storage JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/instance.json)
* [FOLIO Intance (BL) JSON Schema](https://github.com/folio-org/mod-inventory/blob/master/ramls/instance.json)

# FOLIO Inventory data migration process
This template plays a vital part in a process together with other repos allowing you to perform bibliographic data migration from a legacy ILS into FOLIO. For more information on the process, head over to the linked repos below.
In order to perform migrations according to this process, you need to clone the following repositories:   
* [MARC21-to-FOLIO](https://github.com/FOLIO-FSE/MARC21-To-FOLIO)
* [service_tools](https://github.com/FOLIO-FSE/service_tools)
* [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)

## Setup reference data
The scripts requires a FOLIO tenant with reference data setup properly. The script will throw messages telling what reference data is missing. 
One way to set up reference data is to use [service_tools](https://github.com/FOLIO-FSE/service_tools) to download the reference data from one FOLIO tenant and then upload it to the target tenant.

## Create mapping files
The Scripts also relies on a Folder with a set of mapping files. Look in the [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) repo to understand what is needed.

### Bib record mapping
MARC mapping for Bib level records is based on the mapping-rules residing in a FOLIO tenant.
Read more on this in the Readme in the [Source record manager Module repo](https://github.com/folio-org/mod-source-record-manager/blob/25283ebabf402b5870ae4b3846285230e785c17d/RuleProcessorApi.md).

### MFHD-to-Inventory
#### Mapping rules
This processing does not store the MARC records anywhere since this is not available in FOLIO yet. Only FOLIO Holdings records are created.
MFHD-to-Inventory mapping also relies on mapping based on a similar JSON structure. This is not stored in the tenant and must be maintained by you. A template/example is available in [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)

#### Location mapping
For holdings mapping, you also need to map legacy locations to FOLIO locations. An example map file is available at [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) 

## Items-to-Inventory
#### Mapping rules
Items-to-Inventory mapping is based on a json structure where the CSV headers are matched against the target fields in the FOLIO items. The mapping is limited currently, but will be built out as work progresses. For an example go to the [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) repository

#### Location mapping
For Item mapping, you also need to map legacy locations to FOLIO locations. An example map file is available at [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) 

#### Material type mapping
For Item mapping, you also need to map legacy item types to FOLIO equivalents. An example map file is available at [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) 
In order to set this up, you need to have a concept of how the FOLI circulation rules will look like.

#### Loan type mapping
For Item mapping, you also need to map legacy loan types to FOLIO equivalents. An example map file is available at [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) 
In order to set this up, you need to have a concept of how the FOLI circulation rules will look like.

# Tests
There is a test suite for Bibs-to-Instance mapping.
### Running the tests for the Rules mapper

* Install the packages in the Pipfile
* pipenv run python3 -m unittest test_rules_mapper.TestRulesMapper

# Running the scripts
For actual examples of the output, go to the [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)
## main_bibs.py (Bib transformation)
pipenv run python3 main_bibs.py PATH_TO_FOLDER_WITH_MARC_FILES RESULTS_FOLDER OKAPI_URL TENANT_ID USERNAME PASSWORD RECORD_SOURCE_NAME

The above will fetch the mapping-rules from the FOLIO tenant specified and transform the supplied MARC21 record files into FOLIO Instance

Example:
```
pipenv run python ~/code/MARC21-To-FOLIO/main_bibs.py ~/code/migration_repo_template/example_files/data/bibs ~/code/migration_repo_template/example_files/results/ https://okapi-bugfest-honeysuckle.folio.ebsco.com fs090000
00 folio folio voyager
```

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
- The 001 in the MARC21 record (bound for SRS) is replaced with this HRID.
