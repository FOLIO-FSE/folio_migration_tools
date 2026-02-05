```{toctree}
:maxdepth: 3
:caption: Getting Started
:hidden:
installing.md
quick_tutorial.md
```

```{toctree}
:maxdepth: 3
:hidden:
:caption: Using the Tools
migration_process.md
tasks/index.md
migration_reports.md
mapping_file_based_mapping.md
marc_rule_based_mapping.md
reference_data_mapping.md
Transforming inventory <mapping_files_inventory.md>
statistical_codes.md
q_and_a.md
```


# About the tools
FOLIO Migration tools enable you to migrate libraries with the most common ILS:s over to FOLIO without data losses or any major data transformation tasks. The tools transform and load the data providing you and the library with good actionable logs and data-cleaning task lists together with the migrated data.

## What data does it cover?
FOLIO Migration Tools currently covers the following data sets:
* Catalog (Inventory and SRS in FOLIO terminology)
* Circulation transactions (Open loans and requests)
* Users/Patrons (In FOLIO, these share the same app/database)
* Courses and Reserves (Course reserves)
* Organizations (Used in ERM and Aquisitions)


## Can I use the tools for ongoing imports and integrations?
The tools are primarily maintained for performing initial data migrations. We recommend that you use native FOLIO functionality for ongoing loads where possible. 
In theory, these tools can be used for ongoing patron loads from systems like Banner, Workday, or PeopleSoft. But we recommend you weigh your options carefully before going down this path. 

# Contributing
Want to contribute? Read the [CONTRIBUTING.MD](https://github.com/FOLIO-FSE/folio_migration_tools/blob/main/CONTRIBUTING.md)

# Found an issue?
If you have come across an issue, reach out on our #fse_folio_migration_tools on the [FOLIO Slack](https://folio-project.slack.com), or create an issue in the [GitHub Issue tracker](https://github.com/FOLIO-FSE/folio_migration_tools/issues)


# Tests
There is a test suite for Bibs-to-Instance mapping. You need to add arguments in order to run it against a FOLIO environment. The tests are run against the latest [FOLIO Bugfest environment](https://wiki.folio.org/dosearchsite.action?cql=siteSearch%20~%20%22bugfest%22%20AND%20type%20in%20(%22space%22%2C%22user%22%2C%22com.atlassian.confluence.extra.team-calendars%3Acalendar-content-type%22%2C%22attachment%22%2C%22page%22%2C%22com.atlassian.confluence.extra.team-calendars%3Aspace-calendars-view-content-type%22%2C%22blogpost%22)&includeArchivedSpaces=false) as part of the commit process in this repo.

IMPORTANT!
the tests and the tools rely on many calls to GitHub, and you need to create a [GitHub personal Access token](https://github.com/settings/tokens) and add a .env file in the root of the folder with the following contents:   
```GITHUB_TOKEN=ghp_.....```   
Then, either restart your shell or run   
```source .env```    
from the command line 

## Running the tests for the Rules mapper
### Using uv
* Install the packages from the pyproject.toml:
```shell
uv sync --all-groups --all-extras
```
* Run the tests:
```shell
uv run pytest -v --log-level=DEBUG --password PASSWORD --tenant_id TENANT_ID --okapi_url OKAPI_URL --username USERNAME
```
* With coverage:
```shell
uv run pytest --cov=./ --cov-report=xml
```

 {sub-ref}`today` | {sub-ref}`wordcount-words` words | {sub-ref}`wordcount-minutes` min read
