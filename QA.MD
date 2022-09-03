## Q: How are UUIDs constructed in the tools?   
The tools make use of a separate library, https://github.com/FOLIO-FSE/folio_uuid, that creates deterministic UUIDs based on the FOLIO Tenant URL, the legacy records id, and the type of object getting created.

## Q: As I think the Holdings records should have (identifiers) for the (Bibs) records, and items records should have (identifiers) for the (Bibs & Holdings) records so that every Holding belongs to its (Bib) record and every item belongs to its belongs to its (Bib & Holding) record, am I correct? If this thought is correct, so we need a field in the holding csv file, to be a reference for the (Bib) record, and two fields in the items csv file (one for Bib and one for the holding [004 field in holdings should be created, How to create it?
As part of the files generated from the bib transformation, there is an instance_id_map_dress_XXX.json file. This one is being used by the holdings process to link bibs to holdings. And as you state, this identifier must be in the *sv file, and mapped to the instanceId field in the map. For the Item ID (that will be used to generate both Holdings and Item UUIDs in a deterministic manner), you map this to the LegacyIdentifier field in the map. 

## Q: This tool (https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation)is for mapping only, Could we upload a file with the header only? or we have to upload a sample record?
The tool just reads the first row of the file, so just attach the data file that you have. It will never really be uploaded anywhere. Everything is happening on the client-side in the JS.

## Q: Regarding the Location field, What if the library here contains one location, and the marc records don't have a location sub-field in the holding or items fields, could the library be the location?
There must be a location, or FOLIO and the circulation rules will not work. I would map the locations to one default location. Map locations against any field, create a location mapping file and make the wildcard matching pattern match any contents in the field against this default location.

## Q: What are the MigrationTasks  we should use when transforming (csv/tsv) Holdings & Items files to json, and also the posting tasks?
First use HoldingsCsvTransformer followed by ItemsTransformer to build the objects. Then use two separate BatchPoster tasks to post them to FOLIO.
