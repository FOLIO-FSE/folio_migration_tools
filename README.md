# FOLIO Migration Tools
![example workflow](https://github.com/FOLIO-FSE/MARC21-To-FOLIO/actions/workflows/python-app.yml/badge.svg)    
A toolkit that enables you to migrate data over from a legacy ILS system into [FOLIO LSP](https://www.folio.org/)


The scripts requires a FOLIO tenant with reference data properly set up. The script will throw messages telling what reference data is missing. 

When the files have been created, post them to FOLIO using the [service_tools](https://github.com/FOLIO-FSE/service_tools) set of programs. Preferably BatchPoster



# FOLIO migration process
This repo plays the main part in a process using many tools. The process itself is documented in more detail, including example configuration files, at [This template repository](https://github.com/FOLIO-FSE/migration_repo_template)
In order to perform migrations according to this process, you need to clone or make  the following repositories:   
* [MARC21-to-FOLIO](https://github.com/FOLIO-FSE/MARC21-To-FOLIO)
* [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)


# Mapping files
The scripts also relies on a folder with a set of mapping files. There is a [template repository](https://github.com/FOLIO-FSE/migration_repo_template) with examples of the files needed and some documentation around it in the [Readme](https://github.com/FOLIO-FSE/migration_repo_template/blob/main/README.md). There is also a [web tool](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation) for creating mapping files from delimited source files

## Bib records to Invcentory and SRS records
MARC mapping for Bib level records is based on the mapping-rules residing in a FOLIO tenant.
Read more on this in the Readme in the [Source record manager Module repo](https://github.com/folio-org/mod-source-record-manager/blob/25283ebabf402b5870ae4b3846285230e785c17d/RuleProcessorApi.md).

The trigger for this process it the main_bibs.py. In order to see what parameters are needed, just do pipenv run python main_bibs.py -h

![image](https://user-images.githubusercontent.com/1894384/137994473-10fea92f-1966-41d5-bd41-d6be00594b58.png)   
In the picture above, you can se the files needed and the files created as part of the proces.

### MFHD-to-Inventory
#### Mapping rules
This processing does not store the MARC records anywhere since this is not available in FOLIO yet (Planned for the Kiwi release). Only FOLIO Holdings records are created.
MFHD-to-Inventory mapping also relies on mapping based on a similar JSON structure. This is not stored in the tenant and must be maintained by you. A template/example is available in [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)

If you do not have MFHD records available, you can build a mapping file [this web tool](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation) from the Item data. This will generate Holdings records to connect to the items. 
There are two scripts, depending on what source data you have: main_holdings_csv.py and main_holdings_marc.py

![image](https://user-images.githubusercontent.com/1894384/137994847-f27f5e09-329e-4f75-a9fd-a83423d73068.png)


#### Location mapping
For holdings mapping, you also need to map legacy locations to FOLIO locations. An example map file is available at [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template) 

## Items-to-Inventory
Items-to-Inventory mapping is based on a json structure where the CSV headers are matched against the target fields in the FOLIO items. To create a mapping file, use the [web tool](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation).

![image](https://user-images.githubusercontent.com/1894384/137995011-dd6a78a7-61d7-46d8-a35c-363f65c33ce0.png)


# Tests
There is a test suite for Bibs-to-Instance mapping. You need to add arguments in order to run it against a FOLIO environment. The tests are run against the latest [FOLIO Bugfest environment](https://wiki.folio.org/dosearchsite.action?cql=siteSearch%20~%20%22bugfest%22%20AND%20type%20in%20(%22space%22%2C%22user%22%2C%22com.atlassian.confluence.extra.team-calendars%3Acalendar-content-type%22%2C%22attachment%22%2C%22page%22%2C%22com.atlassian.confluence.extra.team-calendars%3Aspace-calendars-view-content-type%22%2C%22blogpost%22)&includeArchivedSpaces=false) as part of the commit process in this repo.

## Running the tests for the Rules mapper

* Install the packages in the Pipfile
* Run ```clear; pipenv run pytest -v --log-level=DEBUG --password PASSWORD --tenant_id TENANT_ID --okapi_url OKAPI URL --username USERNAME```


# Running the scripts
For information on what files are needed and produced by the toolkit, refer to the documentation and example files in the [template repository](https://github.com/FOLIO-FSE/migration_repo_template).

## All Migration tasks
```pipenv run python main.py CONFIGURATION_FILE_PATH MIGRATION_TASK_NAME```

The above will fetch the mapping-rules from the FOLIO tenant specified and transform the supplied MARC21 record files into FOLIO Instance and SRS records.

Example:
```
pipenv run python main.py ~/code/migration_repo_template/mapping_files/exampleConfiguration.json transform_bibs
```
### Explanation
**--okapi_password** The password to the FOLIO tenant configured in your configuration file

**--base_folder** The base folder for the library you are migrating from. This should ideally be a git repository created from the migration_repo_template


