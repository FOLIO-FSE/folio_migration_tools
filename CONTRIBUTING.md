# About this document
This document is intended to take effect on August 1st 2022, and before that, it is WIP.

When you make edits to this document, make sure you update the Table of contents. There is a nice VS Code plugin for it called [markdown-all-in-one](https://marketplace.visualstudio.com/items?itemName=yzhang.markdown-all-in-one#table-of-contents).
# Table of contents
- [About this document](#about-this-document)
- [Table of contents](#table-of-contents)
- [Writing Issues.](#writing-issues)
- [Git(hub) workflow](#github-workflow)
  - [1. Create a branch off of main and name it according to the feature you are working on](#1-create-a-branch-off-of-main-and-name-it-according-to-the-feature-you-are-working-on)
  - [2. :sparkles: Do you thing :sparkles:](#2-sparkles-do-you-thing-sparkles)
  - [3. Prepare for merging](#3-prepare-for-merging)
    - [3.1. :adhesive_bandage: Check for vulnerabilities](#31-adhesive_bandage-check-for-vulnerabilities)
    - [3.2. :monocle_face: Check and format your code](#32-monocle_face-check-and-format-your-code)
    - [3.3. :test_tube: Run the entire tests suite.](#33-test_tube-run-the-entire-tests-suite)
  - [3.4. Make sure the code can run](#34-make-sure-the-code-can-run)
  - [3.5. Create a pull request in GitHub](#35-create-a-pull-request-in-github)
  - [3.6. :people_holding_hands: Code review.](#36-people_holding_hands-code-review)
- [Create release](#create-release)
  - [Create the release on Github](#create-the-release-on-github)
  - [Create release notes and change log using gren](#create-release-notes-and-change-log-using-gren)
  - [Publish package to pypi](#publish-package-to-pypi)
    - [1. Up the release in setup.cfg](#1-up-the-release-in-setupcfg)
    - [2. Build the package](#2-build-the-package)
    - [3. Push the file to pypi test and make a test installation](#3-push-the-file-to-pypi-test-and-make-a-test-installation)
    - [4. Push the release to pypi](#4-push-the-release-to-pypi)
    - [5. Finalize the release](#5-finalize-the-release)
- [Python Coding standards and practices](#python-coding-standards-and-practices)
  - [What to install](#what-to-install)
  - [Important settings](#important-settings)
  - [Setting up Visual studio](#setting-up-visual-studio)
- [Testing](#testing)
  - [Running tests](#running-tests)
  - [Writing tests](#writing-tests)
  - [Code coverage](#code-coverage)
  - [Running an end-to-end transformation](#running-an-end-to-end-transformation)


# Writing issues
Writing good issues is key.
Both for sharing to the larger group of users what is needed or not working, but also for helping the developer working on the issue to reach the Definition Of Done (DoD) and beyond.

For the developer writing the issue, it is good practice to share a screenshot or some data examples or a drawing on what changed. Since Issues are linked into the [CHANGELOG.MD](https://github.com/FOLIO-FSE/folio_migration_tools/blob/main/CHANGELOG.md), this habit will propagate well-written issues over to Pypi and more.

Formulating a DoD is good practice. Take a moment to do this properly.   


# Code contribution workflow
We use [Github Flow](https://docs.github.com/en/get-started/quickstart/github-flow)
In the ideal situation, this is what you do:
## 1. Create a branch off of main and name it according to the feature you are working on
```
> git checkout main    <-- Everything starts with main
> git pull    <-- Make sure you have the latest
> git branch my_new_feature    <-- Name your new branch
> git checkout my_new_feature    <-- Check it out
> git push --set-upstream origin my_new_feature    <-- Publish it
```

## 2. :sparkles: Do you thing :sparkles:
* :test_tube: Write your tests 
* :keyboard: Write your code
* :ledger: Add documentation to the readme if necessary 
* :vertical_traffic_light: It's good practice to add test data to the migration_repo_template in order to maintain a set of examples for new users and also maintain integration test coverage   



## 3. Prepare for merging

### 3.1. :adhesive_bandage: Check for vulnerabilities
Run
```
pipenv run safety check
```
and update any packages with a vulnerability.

### 3.2. :monocle_face: Check and format your code
The following command runs Flake8 with plugins on your code. It:
* Uses black to format the code. The Line-lenght is to be 99 characters
* Uses isort to sort your imports. This makes merging much easier.

```pre-commit run --all-files```

### 3.3. :test_tube: Run the entire tests suite.
This is cruical for making sure nothing else has broken during your work
```
pipenv run pytest -v --cov=./ --cov-report=xml --log-level=DEBUG --password PASSWORD --tenant_id fs09000000 --okapi_url https://okapi-LATEST_BUGFEST_URI --username USERNAME
```

## 3.4. Make sure the code can run
```
> cd src
> pipenv run python3 -m folio_migration_tools -h

```
should output
```
usage: __main__.py [-h] [--okapi_password OKAPI_PASSWORD] [--base_folder_path BASE_FOLDER_PATH] configuration_path task_name

positional arguments:
  configuration_path    Path to configuration file
  task_name             Task name. Use one of: BatchPoster, BibsTransformer, HoldingsCsvTransformer, HoldingsMarcTransformer, ItemsTransformer, LoansMigrator,
                        RequestsMigrator, UserTransformer

optional arguments:
  -h, --help            show this help message and exit
  --okapi_password OKAPI_PASSWORD
                        pasword for the tenant in the configuration file
  --base_folder_path BASE_FOLDER_PATH
                        path to the base folder for this library. Built on migration_repo_template
```

## 3.5. Create a pull request in GitHub
## 3.6. :people_holding_hands: Code review

# Create release
## Create the release on Github
Choose your version, and tag the release
## Create release notes and change log using gren
Once released, create release notes using gren:
```
gren release --override 
```
and create the change log, also using gren:
```
gren changelog --override 
```

## Publish package to pypi
### 1. Up the release in setup.cfg
Open setup.cfg and apply the new version number
```
version = 1.2.1
```
### 2. Build the package
```
python3 -m build
```
Make sure one of the builds aligns with the version number you choosed above   
Installing build: sudo apt install build-essential cmake python3-dev   
```pipenv install build```   
### 3. Push the file to pypi test and make a test installation
Upload:
```
twine upload --repository testpypi dist/*
```
Test install 
```
pipenv install folio_migration_tools -i https://test.pypi.org/simple/
```
### 4. Push the release to pypi
Run
```
twine upload dist/*  
```
and follow the instructions

### 5. Finalize the release
Save the file and commit (and push) the file back to main.
```
(main) > git add setup.cfg
(main) > git commit -m "version 1.2.1
(main) > git push
```

# Python Coding standards and practices
## What to install
```
> pipx install pre-commit  (and run pre-commit install)
> pipx install isort
> pipx install nox
> pipx install poetry
> pipx install twine 
> poetry shell
> poetry install
> npm install github-release-notes -g
```
## Important settings
* Set black max-line-length to 99
* Use black in conjunction with Isort. Make sure to set the _--force-single-line-imports_ parameter

## Setting up Visual studio
Here is one example of the python settings part to use in VS code:
```
"[python]": {
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    },
    "editor.wordBasedSuggestions": false
},
"editor.rulers": [99],
"python.formatting.blackArgs": ["--line-length=99"],
"python.formatting.provider": "black",
"python.languageServer": "Pylance",
"python.linting.flake8Args": [
    "--max-line-length=99",
    "--ignore=E203,W503 ",
    "--select=B,B9,BLK,C,E,F,I,S,W"
],
"python.linting.flake8Enabled": true,
"python.linting.mypyEnabled": true,
"python.linting.pylintEnabled": true,
"python.sortImports.args": [
    "--profile",
    "black",
    "--force-single-line-imports"
    ],
```
https://cereblanco.medium.com/setup-black-and-isort-in-vscode-514804590bf9


# Testing
## Running tests 
### Running tests against a FOLIO environment
Pytest. Run the test suite against the latest bugfest release. Example call:

```
pipenv run pytest -v --cov=./ --cov-report=xml --log-level=DEBUG --password PASSWORD --tenant_id fs09000000 --okapi_url https://okapi-bugfest-lotus.int.aws.folio.org --username USERNAME
```
### Running unit tests
If you configure VS code properly (for example by using the vs code settings in this repository), you will be able to either run or debug your tests from the IDE itself. Just right-click the green triangle next to the test method and either choose Run test or Debug test
![image](https://user-images.githubusercontent.com/1894384/190123117-4f98dbbd-7954-44a9-ae22-18f336a83f48.png)

Running will just run the test for you, but debugging the test will allow you to step through the code and look at the values in the varous objects. Make sure you add a breakpoint at the right place. The following screenshot shows how the value of the schema variable is visible in the Variables pane in VS Code
![image](https://user-images.githubusercontent.com/1894384/190123875-b4cd6d67-45e4-41d0-bfcc-fe4450680847.png)




## Writing tests
### Naming
Tests are written and maintained in the tests folder in the repository. Test files should be named after the class/file they are testing, and then the tests are named according to the methods being tested. 
So, if you are to test a method named *condition_trim_period* in the *conditions.py* file, your test file should be named *test_conditions.py* and the test method should be named *test_condition_trim_period*
![image](https://user-images.githubusercontent.com/1894384/190117341-55d78ca0-853d-4e2b-b55a-48c04a111df3.png)

### Unit tests or integration-like tests?
The test suite contains both tests that needs a connection to a FOLIO tenant to run, as well as a growing number of unit tests that can be run without any actual FOLIO tennant. The latter is preferable, so try to write unit tests, mocking the behaviour of a FOLIO tenant. 

The exception to this is the test suite in *test_rules_mapper_bibs.py* that needs to be rewritten long-term, but that will remain in the current form as is. So if you want to test the tools agains real-world data and a tenant, then this is the place to do it.

### Test libraries used
We rely on Pytest in conjunction with unittest.mock. There are numerous introductions to both libraries:   
* [Intro to test framework Pytest](https://medium.com/testcult/intro-to-test-framework-pytest-5b1ce4d011ae)   
* [Understanding the Python Mock Object Library](https://realpython.com/python-mock-library/)   

### Test data
In the past we have used OAI-PMH-formatted MARC records. This is for historical reasons no longer needed, and going MARC records should be as close to the original form as possible. One could argue that having all MARC records in JSON or .mrk for readability and for searching, but this would risk loosing important nuances.

Test records should be placed in the tests/test_data folder.

### Testing infrastructure
There is a folder in the *src/* folder named *test_infrastructure*. This folder contains classes and mocks that are and could be shared in a wider set of tests.  This way the behaviour of ```FolioClient.folio_get_all()``` method could be standardized for example, and more complexity could be added to these mocks as we introduce new tests.

## Code coverage
Your ambition should be to increase code coverage with every new commit. Coverage does not have to mean that you cover every single outcome or side-effect of a method, but start by testing and verifying that the "happy path" works as expected.

By ensuring we have at least "happy path" test coverage, when a bug is discovered, the threshold for writing a test to make sure the bug is handled gets significantly lowered..

## Running an end-to-end transformation
(migration_repo_template)[] contains a bash script called bash run_test_data_suite.sh allowing you to run the transformers against the latest bugfest environment:
```
> bash run_test_data_suite.sh -pwd
```
When doing larger changes to the code base, it is a good idea to see that all of this works.
