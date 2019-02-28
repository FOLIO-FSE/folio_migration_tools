NOTE! This is still work in progress. Please reach out for any questions.

# MARC21-To-FOLIO
A Python3 library/script parsing MARC21 toFOLIO inventory format. 

# Prerequisits
## Python 3
Install python 3 in a newer version
## Pipenv
Pipenv will make your life easier. install it.

# Usage (in a bash command prompt)
pipenv run python3 main.py PATH_TO_FOLDER_CONTAINING_MARC_FILES PATH_TO_RESULTS_FILE FOLIO_OKAPI_URL FOLIO_TENANT_ID X-OKAPI-TOKEN SOURCE_SYSTEM_ID PATH_TO_NEW_HOLDINGS (-i PATH TO OLD_ID-NEW_ID_MAP | -p |-x | -c)

##Parameter explanation
###PATH_TO_FOLDER_CONTAINING_MARC_FILES 
The path of a Folder containing marc records. The script will try to go through every file in the folder. 
###PATH_TO_RESULTS_FILE 
This is where the instances will be saved to.
###FOLIO_OKAPI_URL 
The Okapi URL. Not the url of the folio instance.
###FOLIO_TENANT_ID 
Folio tenant ID
###X-OKAPI-TOKEN 
The token for authenticating OKAPI requests.
###SOURCE_SYSTEM_ID
The name of this data dump. Is used inside FOLIO to keep track of the origins of a record.
###PATH_TO_NEW_HOLDINGS
If holdings are to be deduced from the MARC21 dump, they will be saved to this path.
###-i PATH TO OLD_ID-NEW_ID_MAP
if -c is specified, a file will be saved keeping track on old and new ids for the instances. This is important for the future migrations so the scrits know which Instance a Holdings record belongs to.
###-p
save results-file in a POSTGRESQL-friendly format so that the files can be used directly into the psql vcopy command
###-c
Use Chalmers-specific functionality
###-x
MARC Files are in MARCXML format
