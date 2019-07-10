# MARC21-To-FOLIO
A Python3 script parsing MARC21 toFOLIO inventory format. 

## Running the tests for the default_mapper

* Install the packages in the Pipfile
* Make a copy of/rename the test_config.json.template in the test folder to tests/test_config.json. Fill out the values.
* run pipenv run python3 -m unittest test_default_mapper

## Extending the default mapper
The chalmers_mapper.py is a good template to use in order to see how to extend/customize the mappers. 
