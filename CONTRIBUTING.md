# About this document
When you make edits to this document, make sure you update the Table of contents. There is a nice VS Code plugin for it called [markdown-all-in-one](https://marketplace.visualstudio.com/items?itemName=yzhang.markdown-all-in-one#table-of-contents).
# Table of contents
- [About this document](#about-this-document)
- [Table of contents](#table-of-contents)
- [Writing issues](#writing-issues)
- [Code contribution workflow](#code-contribution-workflow)
  - [1. Create a branch off of main and name it according to the feature you are working on](#1-create-a-branch-off-of-main-and-name-it-according-to-the-feature-you-are-working-on)
  - [2. Do your thing](#2-sparkles-do-your-thing-sparkles)
  - [3. Prepare for merging](#3-prepare-for-merging)
    - [3.1. Check for vulnerabilities](#31-adhesive_bandage-check-for-vulnerabilities)
    - [3.2. Check and format your code](#32-monocle_face-check-and-format-your-code)
    - [3.3. Run the entire test suite](#33-test_tube-run-the-entire-test-suite)
    - [3.4. Make sure the code can run](#34-make-sure-the-code-can-run)
    - [3.5. Create a pull request in GitHub](#35-create-a-pull-request-in-github)
    - [3.6. Code review](#36-people_holding_hands-code-review)
    - [3.7. After a successful code review, merge the branch into main](#37-after-a-successful-code-review-merge-the-branch-into-main)
- [Create release](#create-release)
- [Python Coding standards and practices](#python-coding-standards-and-practices)
  - [What to install](#what-to-install)
  - [Important settings](#important-settings)
  - [Setting up Visual studio](#setting-up-visual-studio)
- [Testing](#testing)
  - [Running tests](#running-tests)
  - [Writing tests](#writing-tests)
  - [Code coverage](#code-coverage)
  - [Running an end-to-end transformation](#running-an-end-to-end-transformation)
- [Contributing to the documentation](#contributing-to-the-documentation)


# Writing issues
Writing good issues is key.
Both for sharing to the larger group of users what is needed or not working, but also for helping the developer working on the issue to reach the Definition Of Done (DoD) and beyond.

For the developer writing the issue, it is good practice to share a screenshot or some data examples or a drawing on what changed. Since Issues are linked into the [CHANGELOG.MD](https://github.com/FOLIO-FSE/folio_migration_tools/blob/main/CHANGELOG.md), this habit will propagate well-written issues over to Pypi and more.

Formulating a DoD is good practice. Take a moment to do this properly.


# Code contribution workflow
We use [Github Flow](https://docs.github.com/en/get-started/quickstart/github-flow)
In the ideal situation, this is what you do:
## 1. Create a branch off of main and name it according to the feature you are working on
If you are working based on a GitHub issue (which you should be), it is good practice to [create your branch from the issue in GitHub](https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-a-branch-for-an-issue). This will automatically give the branch a descriptive name, and link it to the issue.

```
> git checkout main    <-- Everything starts with main
> git pull    <-- Make sure you have the latest
> git branch my_new_feature    <-- Name your new branch
> git checkout my_new_feature    <-- Check it out
> git push --set-upstream origin my_new_feature    <-- Publish it
```

## 2. :sparkles: Do your thing :sparkles:
* :test_tube: Write your tests
* :keyboard: Write your code
* :ledger: Add or update documentation in `docs/source` if necessary
* :vertical_traffic_light: It's good practice to add test data to the migration_repo_template in order to maintain a set of examples for new users and also maintain integration test coverage



## 3. Prepare for merging

### 3.1. :adhesive_bandage: Check for vulnerabilities
Run
```
nox -rs safety
```
and update any packages with a vulnerability.

### 3.2. :monocle_face: Check and format your code
The following command runs the pre-commit hooks on your code. It:
* Uses ruff to lint and format the code. The line length is 99 characters.
* Sorts imports, which makes merging much easier.
* Type checks `src` with pyrefly. Existing errors are recorded in `pyrefly-baseline.json`, so only new ones fail.

```pre-commit run --all-files```

### 3.3. :test_tube: Run the entire test suite
This is crucial for making sure nothing else has broken during your work. The tests need `GITHUB_TOKEN` to be set (put it in a `.env` file and source it).
```
> source .env && uv run pytest
```

### 3.4. Make sure the code can run
```
> uv run folio-migration-tools -h
```
should output
```
usage: folio-migration-tools [-h] [--folio_password FOLIO_PASSWORD]
                             [--base_folder_path BASE_FOLDER_PATH]
                             [--report_language REPORT_LANGUAGE] [--version]
                             configuration_path task_name

positional arguments:
  configuration_path    Path to configuration file
  task_name             Task name. Use one of: BatchPoster, BibsTransformer,
                        CoursesMigrator, HoldingsCsvTransformer,
                        HoldingsMarcTransformer, InventoryBatchPoster,
                        ItemsTransformer, LoansMigrator, MARCImportTask,
                        ManualFeeFinesTransformer, OrdersTransformer,
                        OrganizationTransformer, RequestsMigrator,
                        ReservesMigrator, UserImportTask, UserTransformer

options:
  -h, --help            show this help message and exit
  --folio_password, --okapi_password FOLIO_PASSWORD
                        password for the tenant in the configuration file
  --base_folder_path BASE_FOLDER_PATH
                        path to the base folder for this library. Built on
                        migration_repo_template
  --report_language REPORT_LANGUAGE
                        Language to write the reports. Defaults english for
                        untranslated languages/strings.
  --version, -V         Show the version of the FOLIO Migration Tools
```

### 3.5. Create a pull request in GitHub
Fill in the [pull request template](.github/PULL_REQUEST_TEMPLATE.md) and tick the checklist.

### 3.6. :people_holding_hands: Code review
### 3.7. After a successful code review, merge the branch into main
Use the closes tag in your merge commit message to automatically close any issue(s) that should be closed by your merged changes.

```
closes #123 #456 #789
```

# Create release
Publishing is automated. Creating a release on GitHub triggers the `Publish Python Package` workflow, which lints, runs the tests on Python 3.10-3.14, builds the package with `uv build` and publishes it to PyPI with `uv publish`.

## 1. Prepare the release pull request
Open a pull request against main that contains two changes:

1. Up the version in pyproject.toml
```
version = "1.12.1"
```
2. Add the release notes for the new version to the top of `CHANGELOG.md`. GitHub can generate them before the release exists, using the categories and label rules in `.github/release.yml`:
```
> gh api repos/FOLIO-FSE/folio_migration_tools/releases/generate-notes \
    -f tag_name=v1.12.1 -f previous_tag_name=v1.12.0 --jq .body
```
Paste the output under a `## v1.12.1 (DD/MM/YYYY)` heading, drop the `## What's Changed` heading and the generated HTML comment, and use `####` for the category headings so they match the existing entries. Finish the entry with a `---` separator.

The notes are built from merged pull requests, so good PR titles and labels (`breaking-change`, `enhancement`, `ignore-for-release`) make for a good change log.

## 2. Create the release on GitHub
Once the pull request is merged, create the release from main. Use the same tag name as above, and let GitHub generate the release notes:
```
> gh release create v1.12.1 --generate-notes
```
Publishing the release triggers the workflow described above.

## 3. Check the result
Verify that the workflow run succeeded and that the new version is available on [PyPI](https://pypi.org/project/folio-migration-tools/).

# Python Coding standards and practices
## What to install
```
> uv tool install pre-commit  (and run pre-commit install)
> uv tool install nox
> uv sync --all-groups --all-extras
```
## Important settings
* Ruff is configured in `pyproject.toml` (line length 99). Do not configure black, isort or flake8 separately.

## Setting up Visual studio
Install the Ruff and Pyrefly extensions, then use settings along these lines:
```
"[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.fixAll.ruff": "explicit",
        "source.organizeImports.ruff": "explicit"
    }
},
"editor.rulers": [99],
```


# Testing
## Running tests
### Running tests against a FOLIO environment
Pytest. Make sure `GITHUB_TOKEN` is set (see 3.3), then run:

```
> source .env && uv run pytest
```
or, to also write a coverage report:
```
> nox -rs tests
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
There is a folder in the *tests/* folder named *test_infrastructure*. This folder contains classes and mocks that are and could be shared in a wider set of tests.  This way the behaviour of ```FolioClient.folio_get_all()``` method could be standardized for example, and more complexity could be added to these mocks as we introduce new tests.

## Code coverage
Your ambition should be to increase code coverage with every new commit. Coverage does not have to mean that you cover every single outcome or side-effect of a method, but start by testing and verifying that the "happy path" works as expected.

By ensuring we have at least "happy path" test coverage, when a bug is discovered, the threshold for writing a test to make sure the bug is handled gets significantly lowered..

## Running an end-to-end transformation
[migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) contains a bash script called bash run_test_data_suite.sh allowing you to run the transformers against the latest bugfest environment:
```
> bash run_test_data_suite.sh -pwd
```
When doing larger changes to the code base, it is a good idea to see that all of this works.


# Contributing to the documentation
Documentation is hosted on [Read the docs](https://folio-migration-tools.readthedocs.io/)

## Writing
The documentation is build in Sphinx, using the MyST parser to enable Markdown.
Some documentation can be found here:
* https://www.sphinx-doc.org/en/master/usage/index.html
* https://myst-parser.readthedocs.io/en/latest/


## Publishing
There is a Github Hook that automatically builds the documentation when pushing to main.

## Building
In order to build the documentation locally, run
```
nox -rs docs
```
The documentation should now have been built in the docs/_build folder. Open up the *index.html* file in a browser to see how it looks. Note that the formatting on Read the docs will make it look different, but that the resulting HTML should be the same
