# Quick tutorial

This is a quick guide on how to transform and load bib-level MARC records into a FOLIO tenant. 

## Prerequisites
* Make sure you have **Python 3.10** or higher installed. uv can manage Python versions for you automatically.
* Install [uv](https://docs.astral.sh/uv/) for package management (recommended)
* You need to have git installed
* You need access to a FOLIO tenant, with the following properties:
   * The **API gateway url** to the tenant (aka "OKAPI url")
   * The **Tenant id** for the tenant
   * A **username** and **password** for a user with the right permission sets/roles
* A file of MARC21 Bib records in .mrc (binary) format

## 1. Create a git repository for your settings and data
### The Migration Repo Template
The tools are build to be run within a certain folder structure and with a set of mapping- and configuration files. 

For this purpose, we maintain a [repo template](https://github.com/folio-fse/migration_repo_template) in order to get folks started quickly with the tools.

###  Create a repo or clone this one
Either use the Github button below to create your own repo based on this template    
![image](https://user-images.githubusercontent.com/1894384/215045112-6964ecfb-a446-4036-99d0-323104f262c5.png)   

Or clone the repo by running 
```shell
git clone git@github.com:FOLIO-FSE/migration_repo_template.git
```

(step-into-the-repo-and-create-the-example-folder-structure-py-running)=
### Step into the repo and create the example folder structure py running
```shell
cd migration_repo_template
python create_folder_structure.py
```
Your repository should look like this:    
![image](https://user-images.githubusercontent.com/1894384/215044991-5b648501-aa10-46e2-873f-0b0996180a16.png)


## 2. Add your data, and configure the settings
1. Locate the MARC file you want to use, and move it into the ```iterations/test_iteration/source_data/instances``` folder.
2. Open up the mapping_files/exampleConfiguration.json file in a text editor and replace the outlined values in the picture below with your values:   
![image](https://user-images.githubusercontent.com/1894384/215045374-fa84f983-fbee-4a54-8383-78934af77484.png)


3. Save the file   

## 3. Install the tools and make sure they can run
1. Install dependencies using uv
The migration repo template uses [uv](https://docs.astral.sh/uv/) for dependency management. First, go to the root of the repository. Then run the following:
```shell
uv sync                    # Install project dependencies (creates virtual environment automatically)
```
2. Test the installation by showing the help pages
```shell
uv run folio-migration-tools -h
```

## Transform the bibs into FOLIO Instances and SRS records
The following command will invoke the first of the three tasks in the configuration task, the **transform_bibs** one. Have your password ready.
```shell
uv run folio-migration-tools mapping_files/exampleConfiguration.json --base_folder_path ./ transform_bibs
```
Congratulations! You have now completed the first step. Take a look around in the results and reports folders in iterations/test_iteration to get an understanding for what has happened.

## Post the Instances into FOLIO Instances and SRS records
```shell
uv run folio-migration-tools mapping_files/exampleConfiguration.json --base_folder_path ./ post_instances
```

## Post the SRS (MARC) Records into FOLIO
```shell
uv run folio-migration-tools mapping_files/exampleConfiguration.json --base_folder_path ./ post_srs_bibs
```
Now, you can go off and explore the records in the FOLIO tenant! Make sure to check all the reports and lists of failed records in the reports and results folders!


For complete documentation on how to run the process, refer to [Read the Docs](https://folio-migration-tools.readthedocs.io/) (under construction)
