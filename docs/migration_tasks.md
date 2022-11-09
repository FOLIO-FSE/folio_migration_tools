# Migration tasks
```{contents} 
```
## Batch Poster   
Posts transformed FOLIO objects to a FOLIO tenant   
## Bibs Transformer   
Transform MARC21 Bib records to FOLIO Instances and SRS records   
## Courses Migrator   
### Instructor enrichment from the Users app
If you have turned this feature on in the configuration like this, 
```
"lookUpInstructor": true,
```
and also mapped the value that is mapped to the User record's *externalSystemId* to the Instructor *userId* field, like this,
```
{
    "folio_field": "instructors[0].userId",
    "legacy_field": "FACULTY_ID",
    "value": "",
    "description": ""
},
```
The tool will perform a lookup in the Users app, and if a match is found, the Instructor record will be populated with data from the user
## Holdings CSV Transformer
Creates FOLIO holdingsrecords from a TSV or CSV File   
## Holdings MARC Transformer   
Transforms MARC21 MFHD records into FOLIO Holdings and SRS records   
## Items Transformer   
Creates FOLIO Items from a TSV or CSV File   

## Loans Migrator
Migrates open loans into FOLIO

## Organization Transformer

## Requests Migrator
Migrates open requests, including page, recall and hold requests.

## Reserves Migrator
Adds Course reserve items into Courses
## User Transformer   
Creates FOLIO Users from a TSV or CSV File. 
The data created is based on the format needed for mod-user-import, the User Import module for FOLIO. 

### External resources
This module and format are described on [The Mod user import GitHub repository](https://github.com/folio-org/mod-user-import)
