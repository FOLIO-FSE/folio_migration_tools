# Quick tutorial

This is a quick guide on how to transform and load bib-level MARC records into a FOLIO tenant. 

## Prerequisites
* Make sure you have **Python 3.9** or higher installed. We recommend using [pyenv](https://github.com/pyenv/pyenv) for managing various Python versions
* You need to have git installed
* You need access to a FOLIO tenant, with the following properties:
   * The **OKAPI url** to the tenant
   * The **Tenant id** for the tenant
   * A **username** and **password** for a user with the right permission sets
* A file of MARC21 Bib records in .mrc (binary) format
## 1. Create a git repository for your settings and data
### The Migration Repo Template
The tools are build to be run within a certain folder structure and with a set of mapping- and configuration files. 

For this purpose, we maintain a repo template in order to get folks started quickly with the tools. 

###  Create a repo or clone this one
Either use the Github button below to create your own repo based on this template    
![image](https://user-images.githubusercontent.com/1894384/215045112-6964ecfb-a446-4036-99d0-323104f262c5.png)   

Or clone the repo by running 
```
git clone git@github.com:FOLIO-FSE/migration_repo_template.git
```

### Step into the repo and create the example folder structure py running
```
cd migration_repo_template
sh create_folder_structure.sh
```
Your repository should look like this:    
![image](https://user-images.githubusercontent.com/1894384/215044991-5b648501-aa10-46e2-873f-0b0996180a16.png)

(If you are on Windows, create a similar folder structure in the way you are familiar with)

## 2. Add your data, and configure the settings
1. Locate the MARC file you want to use, and move it into the ```iterations/test_iteration/source_data/instances``` folder.
2. Open up the mapping_files/exampleConfiguration.json file in a text editor and replace the outlined values in the picture below with your values:   
![image](https://user-images.githubusercontent.com/1894384/215045374-fa84f983-fbee-4a54-8383-78934af77484.png)


3. Save the file   

## 3. Install the tools and make sure they can run
1. Create and activate a virtual environment
First, go to the root of the repository. Then run the following
```
python -m venv ./.venv     # Creates a virtual env in the current folder
source .venv/bin/activate  # Activates the venv
```
2. Install using pip:
```
python -m pip install folio_migration_tools
```
3. Test the installation by showing the help pages
```
python -m folio_migration_tools -h
```
## Transform the bibs into FOLI Instances and SRS records
The following command will invoke the first of the three tasks in the configuration task, the **transform_bibs** one. Have your password ready.
```
python -m folio_migration_tools mapping_files/exampleConfiguration.json --base_folder_path ./ transform_bibs
```
Congratulations! You have now completed the first step. Take a lookaround in the results and reports folders in iterations/test_iteration to get an understanding for what has happened.
## Post the Instances into FOLI Instances and SRS records
```
python -m folio_migration_tools mapping_files/exampleConfiguration.json --base_folder_path ./ post_instances
```
## Post the SRS (MARC) Records into FOLIO
```
python -m folio_migration_tools mapping_files/exampleConfiguration.json --base_folder_path ./ post_srs_bibs
```
Now, you can go off and explore the records in the FOLIO tenant! Make sure to check all the reports and lists of failed records in the reports and results folders!


For complete documentation on how to run the process, refer to [Read the Docs](https://folio-migration-tools.readthedocs.io/) (under construction)
