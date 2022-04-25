# TOC

# Writing Issues.

# Git(hub) workflow
We use [Github Flow](https://docs.github.com/en/get-started/quickstart/github-flow) 
* Create a branch off of main and name it accoring to the feature you are working on
* Do you thing
* Format your code. Sort your imports (TBD)
* Run the tests
* Make sure the code can run
* 
* More things tbd
* Create a pull request
* 

# Python Coding standards
## Formatting.
Use black. Make sure Black runs every time you save your code.
TODO: Add instructions on how to automatically run black in VS code.

### Sorting imports

# Testing
## Running tests
Pytest. Run the test suite against the latest bugfest release. Example call:

```
pipenv run pytest -v --cov=./ --cov-report=xml --log-level=DEBUG --password PASSWORD --tenant_id fs09000000 --okapi_url https://okapi-bugfest-lotus.int.aws.folio.org --username USERNAME
```

## Writing tests
TBD

## Code coverage
TBD
