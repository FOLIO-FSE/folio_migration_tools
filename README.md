# MARC21-To-FOLIO
A Python3 script parsing MARC21 toFOLIO inventory format. 
The script requires a FOLIO tenant with reference data set. The script will throw messages telling what reference data is missing. 

MARC mapping can either be based on a custom mapper in Code, or it can rely on the mapping-rules residing in a FOLIO tenant.
Read more on this in the Readme in the [Source record manager Module repo](https://github.com/folio-org/mod-source-record-manager/blob/25283ebabf402b5870ae4b3846285230e785c17d/RuleProcessorApi.md).

The mapping-rules mapper path is the way forward, and this repo will defer from the previous path.

## Running the tests for the default_mapper and the Rules mapper

* Install the packages in the Pipfile
* pipenv run python3 -m unittest test_rules_mapper.TestRulesMapper

## Running the script
pipenv run python3 main_bibs.py PATH_TO_FOLDER_WITH_MARC_FILES RESULTS_FOLDER OKAPI_URL TENANT_ID USERNAME PASSWORD RECORD_SOURCE_NAME 


