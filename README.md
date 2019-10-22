# MARC21-To-FOLIO
A Python3 script parsing MARC21 toFOLIO inventory format. 
The script requires a FOLIO tenant with reference data set. The script will throw messages telling what reference data is missing. 


## Running the tests for the default_mapper

* Install the packages in the Pipfile
* run pipenv run python3 -m unittest test_default_mapper

## Running the script
pipenv run python3 main_bibs.py PATH_TO_FOLDER_WITH_MARC_FILES RESULTS_FOLDER OKAPI_URL TENANT_ID USERNAME PASSWORD RECORD_SOURCE_NAME -p 

## Extending the default mapper
The chalmers_mapper.py is a good template to use in order to see how to extend/customize the mappers. 
