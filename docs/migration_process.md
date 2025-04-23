```{contents} 
:depth: 2
```

# The migration process

In order to perform migrations according to this process, you need the following:
* Python 3.9 or later
* An Installation of [FOLIO Migration Tools](https://pypi.org/project/folio-migration-tools/). See the Installing page for information.
* A repo created from  the template repository [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template)
* A FOLIO tenant running the latest or the second latest version of FOLIO

# FOLIO Inventory data migration process

The FSE FOLIO Migration tools requires you to run the transformation an data loading in sequence, and each step relies on previous migrations steps, like the existance of a  file with legacy system IDs and their FOLIO equivalents. 
The below picture shows the proposed migration steps for legacy objects into FOLIO:
![image](https://user-images.githubusercontent.com/1894384/139079124-b31b716f-281b-4784-b73e-a4567ee3e097.png)


## Result files
The following table outlines the result records and their use and role
 File | Content | Use for 
------------ | ------------- | ------------- 
folio_holdings.json | FOLIO Holdings records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs
folio_instances.json | FOLIO Instance records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs
folio_items.json |FOLIO Item records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs
holdings_id_map.json | A json map from legacy Holdings Id to the ID of the created FOLIO Holdings record | To be used in subsequent transformation steps 
holdings_transformation_report.md | A file containing various breakdowns of the transformation. Also contains errors to be fixed by the library | Create list of cleaning tasks, mapping refinement
instance_id_map.json | A json map from legacy Bib Id to the ID of the created FOLIO Instance record. Relies on the "ILS Flavour" parameter in the main_bibs.py scripts | To be used in subsequent transformation steps 
instance_transformation_report.md | A file containing various breakdowns of the transformation. Also contains errors to be fixed by the library | Create list of cleaning tasks, mapping refinement
item_id_map.json | A json map from legacy Item Id to the ID of the created FOLIO Item record | To be used in subsequent transformation steps 
item_transform_errors.tsv | A TSV file with errors and data issues together with the row number or id for the Item | To be used in fixing of data issues 
items_transformation_report.md | A file containing various breakdowns of the transformation. Also contains errors to be fixed by the library | Create list of cleaning tasks, mapping refinement
marc_xml_dump.xml | A MARCXML dump of the bib records, with the proper 001:s and 999 fields added | For pre-loading a Discovery system.
srs.json | FOLIO SRS records in json format. One per row in the file | To be loaded into FOLIO using the batch APIs



## HRID handling
### Current implementation:   
Download the HRID handling settings from the tenant. 
**If there are HRID handling in the mapping rules:**
- The HRID is set on the Instance
- The 001 in the MARC21 record (bound for SRS) is replaced with this HRID.

**If the mapping-rules specify no HRID handling or the field designated for HRID contains no value:**
- The HRID is being constructed from the HRID settings
- Pad the number in the HRID Settings so length is 11
- A new 035 field is created and populated with the value from 001
- The 001 in the MARC21 record (bound for SRS) is replaced with this HRID.


## Relevant FOLIO community documentation
* [Instance Metadata Elements](https://docs.google.com/spreadsheets/d/1RCZyXUA5rK47wZqfFPbiRM0xnw8WnMCcmlttT7B3VlI/edit#gid=952741439)
* [Recommended MARC mapping to Inventory Instances](https://docs.google.com/spreadsheets/d/11lGBiPoetHuC3u-onVVLN4Mj5KtVHqJaQe4RqCxgGzo/edit#gid=1891035698)
* [Recommended MFHD to Inventory Holdings mapping ](https://docs.google.com/spreadsheets/d/1ac95azO1R41_PGkeLhc6uybAKcfpe6XLyd9-F4jqoTo/edit#gid=301923972)
* [Holdingsrecord JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/holdingsrecord.json)
* [FOLIO Instance storage JSON Schema](https://github.com/folio-org/mod-inventory-storage/blob/master/ramls/instance.json)
* [FOLIO Intance (BL) JSON Schema](https://github.com/folio-org/mod-inventory/blob/master/ramls/instance.json)
* [Inventory elements - Beta](https://docs.google.com/spreadsheets/d/1RCZyXUA5rK47wZqfFPbiRM0xnw8WnMCcmlttT7B3VlI/edit#gid=901484405)
* [MARC Mappings Information](https://wiki.folio.org/display/FOLIOtips/MARC+Mappings+Information)
