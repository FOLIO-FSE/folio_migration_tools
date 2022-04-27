# TOC
TBD

# About this document
This document is intended to take effect on August 1st 2022, and before that, it is WIP.

# Writing Issues.
Writing good issues is key.
Both for sharing to the larger group of users what is needed or not working, but also for helping the developer working on the issue to reach the DoD and beyond.

For the developer writing the issue, it is good practice to share a screenshot or some data examples or a drawing on what changed. Since Issues are linked into the [CHANGELOG.MD](https://github.com/FOLIO-FSE/folio_migration_tools/blob/main/CHANGELOG.md), this habit will propagate well-written issues over to Pypi and more.


# Git(hub) workflow
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
Write your tests, write your code. More on this part in sections below.


## 3. Prepare for merging

### 3.1. :adhesive_bandage: Check for vulnerabilities
Run
```
pipenv run safety check
```
and update any packages with a vulnerability.
### 3.2. Check and format your code
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
## 3.6. Code review.

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

### 3. Push the file to pypi test and make a test installation
TBD
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
> pipx install pre-commit 
> pipx install twine 
> pipenv install flake8
> pipenv install black
> pipenv install flake8-bugbear
> pipenv install flake8-bandit
> pipenv install safety
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
Pytest. Run the test suite against the latest bugfest release. Example call:

```
pipenv run pytest -v --cov=./ --cov-report=xml --log-level=DEBUG --password PASSWORD --tenant_id fs09000000 --okapi_url https://okapi-bugfest-lotus.int.aws.folio.org --username USERNAME
```

## Writing tests
TBD

## Code coverage
TBD

## Running an end-to-end transformation
(migration_repo_template)[] contains a bash script called bash run_test_data_suite.sh allowing you to run the transformers against the latest bugfest environment:
```
> bash run_test_data_suite.sh -pwd
```
When doing larger changes to the code base, it is a good idea to see that all of this works.
