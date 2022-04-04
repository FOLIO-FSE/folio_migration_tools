# FOLIO Migration Tools
![example workflow](https://github.com/FOLIO-FSE/MARC21-To-FOLIO/actions/workflows/python-app.yml/badge.svg)    
A toolkit that enables you to migrate data over from a legacy ILS system into [FOLIO LSP](https://www.folio.org/)


The scripts requires a FOLIO tenant with reference data properly set up. The script will throw messages telling what reference data is missing. 
# Installing
## 1. Using pip and venv
1. Create and activate a [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment)
2. Install using pip: `pip install folio-migration-tools`
3. Test the installation by running `python3 -m folio-migration-tools -h`
## 2. Using pipenv
1. Run `pipenv install folio-migration-tools`
2. Test the installation by calling `pipenv run python3 -m folio-migration-tools -h`


# FOLIO migration process
This repo plays the main part in a process using a collection of tools. The process itself is documented in more detail, including example configuration files, at [this template repository](https://github.com/FOLIO-FSE/migration_repo_template)
In order to perform migrations according to this process, you need the following:   
* An Installation of [FOLIO Migration Tools](https://pypi.org/project/folio-migration-tools/). Installation instructions below.
* A clone, or a separate repo created from [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)
* Access to the [Data mapping file creator](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation) web tool
* A FOLIO tenant...

# Mapping files
The tool is run against a folder with a set of mapping files and data files. There is a [template repository](https://github.com/FOLIO-FSE/migration_repo_template) with examples of the files needed and documentation around it in the [Readme](https://github.com/FOLIO-FSE/migration_repo_template/blob/main/README.md). The template has everything needed to run the tools agains a FOLIO test environment. 

## Bib records to Inventory and SRS records
MARC mapping for Bib level records is based on the mapping-rules residing in a FOLIO tenant.
Read more on this in the Readme in the [Source record manager Module repo](https://github.com/folio-org/mod-source-record-manager/blob/25283ebabf402b5870ae4b3846285230e785c17d/RuleProcessorApi.md).

![image](https://user-images.githubusercontent.com/1894384/137994473-10fea92f-1966-41d5-bd41-d6be00594b58.png)   
In the picture above, you can se the files needed and the files created as part of the proces.

### MFHD-to-Inventory
#### Mapping rules
This process creates FOLIO Holdings records . A template/example is available in [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template). You have the option of either just create Holdings records, or creating a controlling SRS MFHD record together with the FOLIO Holdingsrecord.

If you do not have MFHD records available, you can build a mapping file [this web tool](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation) from the Item data. This will generate Holdings records to connect to the items. 

#### Location mapping
For holdings mapping, you also need to map legacy locations to FOLIO locations. An example map file is available at [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) 

## Items-to-Inventory
Items-to-Inventory mapping is based on a json structure where the CSV headers are matched against the target fields in the FOLIO items. To create a mapping file, use the [web tool](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation).

![image](https://user-images.githubusercontent.com/1894384/137995011-dd6a78a7-61d7-46d8-a35c-363f65c33ce0.png)


## Open loans
The tools allows you to migrate open loand into FOLIO. This uses the Business logic APIs for this, which mean that the actual circulation rules are being excercised. The toolkit handles various exceptions as well, as expired users and other things that would normally block the user from checking things out. Be sure to turn off the SMTP settings before checking out anything or you will have a lot of patrons wondering...

# Tests
There is a test suite for Bibs-to-Instance mapping. You need to add arguments in order to run it against a FOLIO environment. The tests are run against the latest [FOLIO Bugfest environment](https://wiki.folio.org/dosearchsite.action?cql=siteSearch%20~%20%22bugfest%22%20AND%20type%20in%20(%22space%22%2C%22user%22%2C%22com.atlassian.confluence.extra.team-calendars%3Acalendar-content-type%22%2C%22attachment%22%2C%22page%22%2C%22com.atlassian.confluence.extra.team-calendars%3Aspace-calendars-view-content-type%22%2C%22blogpost%22)&includeArchivedSpaces=false) as part of the commit process in this repo.

## Running the tests for the Rules mapper

* Install the packages in the Pipfile
* Run ```clear; pipenv run pytest -v --log-level=DEBUG --password PASSWORD --tenant_id TENANT_ID --okapi_url OKAPI URL --username USERNAME```


# Running the scripts
For information on syntax, what files are needed and produced by the toolkit, refer to the documentation and example files in the [template repository](https://github.com/FOLIO-FSE/migration_repo_template).
¨
# Building / Packaging
* Update setup.cfg with the latest version
* Build the new version: `python3 -m build`
* Upload to pypi: `twine upload dist/*`
