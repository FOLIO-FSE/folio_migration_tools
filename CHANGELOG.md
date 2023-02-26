# Changelog

## v_1_7_5 (26/02/2023)

#### Organizations

- [**Organizations**] Make mapper map array > object > object > string [#502](https://github.com/FOLIO-FSE/folio_migration_tools/issues/502)
- [**Organizations**] Refine handling of identical Contacts in Organizations [#468](https://github.com/FOLIO-FSE/folio_migration_tools/issues/468)

#### Tool enhancements

- [**Tool enhancements**][**Orders**] Add Instance Matching to Orders Mapper [#394](https://github.com/FOLIO-FSE/folio_migration_tools/issues/394)
- [**Tool enhancements**][**Organizations**] Make Organization schema in Mapping file creator Lotus-compliant [#298](https://github.com/FOLIO-FSE/folio_migration_tools/issues/298)
- [**Tool enhancements**][**Organizations**] When creating Organizations, create Interfaces as extradata [#295](https://github.com/FOLIO-FSE/folio_migration_tools/issues/295)
- [**Tool enhancements**][**Orders**] Create an initial implementation of a migration task for compositePurchaseOrders (Orders and PO Lines) [#202](https://github.com/FOLIO-FSE/folio_migration_tools/issues/202)

#### bug

- [**bug**] MFHD Transformer crashes when MFHD records contain more than one 852$b [#532](https://github.com/FOLIO-FSE/folio_migration_tools/issues/532)
- [**bug**] Mapper incorrectly fails record where a non-required enum is empty [#509](https://github.com/FOLIO-FSE/folio_migration_tools/issues/509)

#### wontfix

- [**wontfix**][**Organizations**] Create organizations legacy id map  [#511](https://github.com/FOLIO-FSE/folio_migration_tools/issues/511)

---

## 1.7.4 (17/02/2023)

---

## v_1_7_3 (15/02/2023)

#### Inventory

- [**Inventory**] Add ILS flavour for Koha 999c [#493](https://github.com/FOLIO-FSE/folio_migration_tools/issues/493)

#### bug

- [**bug**][**organizations**] Mapper is mapping array_object_array_string as array_object_string [#485](https://github.com/FOLIO-FSE/folio_migration_tools/issues/485)

#### closed

- [**closed**] Make batchposter use the "-unsafe" endpoints [#478](https://github.com/FOLIO-FSE/folio_migration_tools/issues/478)

#### enhancement/new feature

- [**enhancement/new feature**][**simplify_migration_process**] Treat map file values as regex  [#199](https://github.com/FOLIO-FSE/folio_migration_tools/issues/199)

#### organizations

- [**organizations**] The mapping process should validate enums-type properties according to schemas [#486](https://github.com/FOLIO-FSE/folio_migration_tools/issues/486)

---

## v_1_7_2 (31/01/2023)

#### bug

- [**bug**] Instance loading fails in Nolana due to empty authorityId:s [#487](https://github.com/FOLIO-FSE/folio_migration_tools/issues/487)

#### closed

- [**closed**] Handle new error messages for Aged to lost loans  [#480](https://github.com/FOLIO-FSE/folio_migration_tools/issues/480)

---

## v_1_7_1 (18/01/2023)

#### Authorities

- [**Authorities**] Correct spelling of type enum in FOLIO UUIDs for authorities [#438](https://github.com/FOLIO-FSE/folio_migration_tools/issues/438)

#### bug

- [**bug**] Mapper overwrites existing object properties when adding new object properties [#455](https://github.com/FOLIO-FSE/folio_migration_tools/issues/455)

#### closed

- [**closed**] Do not create Organization Contacts without required property name -- quick fix [#474](https://github.com/FOLIO-FSE/folio_migration_tools/issues/474)
- [**closed**] Typo in mapping file confusingly reported as error parsing configuration file [#470](https://github.com/FOLIO-FSE/folio_migration_tools/issues/470)
- [**closed**] Remove extraneous fields from User objects created by UserMapper [#469](https://github.com/FOLIO-FSE/folio_migration_tools/issues/469)
- [**closed**] Missing hrid_settings attribute causing Errors in BibsRulesMapper [#462](https://github.com/FOLIO-FSE/folio_migration_tools/issues/462)
- [**closed**] Update BatchPoster to generalize handling of record types without batch APIs [#454](https://github.com/FOLIO-FSE/folio_migration_tools/issues/454)

#### enhancement/new feature

- [**enhancement/new feature**][**organizations**] Add Batchposter support for organizations [#312](https://github.com/FOLIO-FSE/folio_migration_tools/issues/312)
- [**enhancement/new feature**][**organizations**] When creating Organizations, create Contacts as extradata [#294](https://github.com/FOLIO-FSE/folio_migration_tools/issues/294)
- [**enhancement/new feature**][**reporting**] Keep track of minted UUID:s within the same run and warn for duplicates [#235](https://github.com/FOLIO-FSE/folio_migration_tools/issues/235)

#### orders

- [**orders**] Create basic tests for Composite Orders migration task [#442](https://github.com/FOLIO-FSE/folio_migration_tools/issues/442)

#### organizations

- [**organizations**] Add Organizations and Contacts to BatchPoster [#457](https://github.com/FOLIO-FSE/folio_migration_tools/issues/457)
- [**organizations**] Add mapping depth tests for organization contacts [#446](https://github.com/FOLIO-FSE/folio_migration_tools/issues/446)

#### reporting

- [**reporting**] Improve reporting on legacy loans migration [#263](https://github.com/FOLIO-FSE/folio_migration_tools/issues/263)

---

## v_1_7_0 (13/12/2022)

#### closed

- [**closed**] Map 86[6-8] $x to staff notes [#448](https://github.com/FOLIO-FSE/folio_migration_tools/issues/448)
- [**closed**] Support token representing iteration identifier within config file parameters and filenames [#441](https://github.com/FOLIO-FSE/folio_migration_tools/issues/441)
- [**closed**] Move documentation from migration_repo_template to this repo and improve it! [#248](https://github.com/FOLIO-FSE/folio_migration_tools/issues/248)
- [**closed**] Reduce memory footprint for transformations scripts from the legacy id maps [#46](https://github.com/FOLIO-FSE/folio_migration_tools/issues/46)

#### enhancement/new feature

- [**enhancement/new feature**] Add same logic for mapping locations  for MARC Holdings mappings as for mapping-file-based ref-data-mappings [#319](https://github.com/FOLIO-FSE/folio_migration_tools/issues/319)
- [**enhancement/new feature**] Check if HoldingsTypes are set to the expected values in FOLIO and fail the parsing if not [#318](https://github.com/FOLIO-FSE/folio_migration_tools/issues/318)
- [**enhancement/new feature**] Create migration task for Courses [#200](https://github.com/FOLIO-FSE/folio_migration_tools/issues/200)

#### new_folio_functionality

- [**new_folio_functionality**][**Authorities**] Add support for Authority File configuration and mappings [#437](https://github.com/FOLIO-FSE/folio_migration_tools/issues/437)
- [**new_folio_functionality**][**Authorities**] Create migration task for Authorities [#389](https://github.com/FOLIO-FSE/folio_migration_tools/issues/389)
- [**new_folio_functionality**] Implement set_holdings_type_id for MFHD rules mapping [#376](https://github.com/FOLIO-FSE/folio_migration_tools/issues/376)
- [**new_folio_functionality**] Implement set_holdings_note_type_id for MFHD rules mapping [#375](https://github.com/FOLIO-FSE/folio_migration_tools/issues/375)
- [**new_folio_functionality**] Implement set_authority_note_type_id for Auth rules mapping [#374](https://github.com/FOLIO-FSE/folio_migration_tools/issues/374)
- [**new_folio_functionality**] Implement set_call_number_type_id  for MFHD rules mapping [#373](https://github.com/FOLIO-FSE/folio_migration_tools/issues/373)
- [**new_folio_functionality**] Use the Tenant-stored MFHD rules for MFHD transformations [#124](https://github.com/FOLIO-FSE/folio_migration_tools/issues/124)

#### question/decision

- [**question/decision**] Map callnumber type id on MFHDs [#56](https://github.com/FOLIO-FSE/folio_migration_tools/issues/56)

#### simplify_migration_process

- [**simplify_migration_process**] Report and discard bib records with same legacy ID as previously transformed records [#186](https://github.com/FOLIO-FSE/folio_migration_tools/issues/186)

---

## 1.6.4 (06/12/2022)

---

## 1_6_3 (23/11/2022)

#### bug

- [**bug**] Implement fieldReplacementBy3Digits  [#426](https://github.com/FOLIO-FSE/folio_migration_tools/issues/426)

#### closed

- [**closed**] Make sure schema properties are generated with snakeCase [#429](https://github.com/FOLIO-FSE/folio_migration_tools/issues/429)

#### enhancement/new feature

- [**enhancement/new feature**][**organizations**][**morning-glory**] Add reference data mapping for Organizations: Types (Morning Glory) [#358](https://github.com/FOLIO-FSE/folio_migration_tools/issues/358)

#### organizations

- [**organizations**][**morning-glory**] Add support for organizationType [#382](https://github.com/FOLIO-FSE/folio_migration_tools/issues/382)

#### reporting

- [**reporting**] Move suppression status in bib report to its own section [#333](https://github.com/FOLIO-FSE/folio_migration_tools/issues/333)
- [**reporting**] Move Total number of tags to a "trivia" section (or similar) [#332](https://github.com/FOLIO-FSE/folio_migration_tools/issues/332)

#### simplify_migration_process

- [**simplify_migration_process**] Rewrite the extra data process to not rely on logging [#343](https://github.com/FOLIO-FSE/folio_migration_tools/issues/343)

---

## 1_6_2 (16/11/2022)

#### bug

- [**bug**] MARC Holdings transformer crashes silently when hridhandling is set to preserve001 [#425](https://github.com/FOLIO-FSE/folio_migration_tools/issues/425)
- [**bug**] MappingFileMapperBase value mappings does not work unless the legacy field is populated [#423](https://github.com/FOLIO-FSE/folio_migration_tools/issues/423)

#### closed

- [**closed**] Implement preserve001 for MFHD transformations [#427](https://github.com/FOLIO-FSE/folio_migration_tools/issues/427)
- [**closed**] Remove Kiwi references from the code base [#421](https://github.com/FOLIO-FSE/folio_migration_tools/issues/421)
- [**closed**] Add documentation from migration_repo_template. In a course manner. [#416](https://github.com/FOLIO-FSE/folio_migration_tools/issues/416)
- [**closed**] Create proper technical structure for documentation and publish it on Read The Docs [#410](https://github.com/FOLIO-FSE/folio_migration_tools/issues/410)

#### enhancement/new feature

- [**enhancement/new feature**][**organizations**] Add reference data mapping for Organizations: Categories [#359](https://github.com/FOLIO-FSE/folio_migration_tools/issues/359)

#### morning-glory

- [**morning-glory**]  Add FOLIO Release Enum for Morning Glory in configuration [#414](https://github.com/FOLIO-FSE/folio_migration_tools/issues/414)

#### organizations

- [**organizations**] Clean out organizationType if configured FOLIO version is "lotus" [#413](https://github.com/FOLIO-FSE/folio_migration_tools/issues/413)

---

## 1_6_1 (21/10/2022)

#### bug

- [**bug**][**Course Reserves**] Courses: Remove user id field if there is no match in the call to the users app [#407](https://github.com/FOLIO-FSE/folio_migration_tools/issues/407)

---

## 1_6_0 (21/10/2022)

#### closed

- [**closed**] Add "Fallback value" in addition to the "Fallback legacy field" mapping entry [#405](https://github.com/FOLIO-FSE/folio_migration_tools/issues/405)
- [**closed**] Rewrite UserMapper and UserTransformer to use MappingFileMapperBase [#402](https://github.com/FOLIO-FSE/folio_migration_tools/issues/402)
- [**closed**] Pick first Match on regex [#400](https://github.com/FOLIO-FSE/folio_migration_tools/issues/400)
- [**closed**] look for missing required properties in notes [#399](https://github.com/FOLIO-FSE/folio_migration_tools/issues/399)
- [**closed**] up the dependencies on pymarc and folio_uuid [#398](https://github.com/FOLIO-FSE/folio_migration_tools/issues/398)
- [**closed**] allow multiple legacyIdentifier fields to be concatenated into one [#396](https://github.com/FOLIO-FSE/folio_migration_tools/issues/396)
- [**closed**] Explore format for string replacements in mapping files [#393](https://github.com/FOLIO-FSE/folio_migration_tools/issues/393)
- [**closed**] Remove "Subfield b not in 336" warning if mapping to resource type by $a is successful and report the mapping in the report. [#392](https://github.com/FOLIO-FSE/folio_migration_tools/issues/392)
- [**closed**] Implement array mapping on level 2 in MappingFileMapperBase [#379](https://github.com/FOLIO-FSE/folio_migration_tools/issues/379)
- [**closed**] Pull Courses Instructor information from externalSystemId [#378](https://github.com/FOLIO-FSE/folio_migration_tools/issues/378)
- [**closed**] Concatenated fields in mapping file transformed data comes out in different order [#370](https://github.com/FOLIO-FSE/folio_migration_tools/issues/370)
- [**closed**] When creating Users never set more than one address as "primary" [#301](https://github.com/FOLIO-FSE/folio_migration_tools/issues/301)

#### enhancement/new feature

- [**enhancement/new feature**] Move to Poetry from Pipenv, Build etc. [#305](https://github.com/FOLIO-FSE/folio_migration_tools/issues/305)
- [**enhancement/new feature**][**users**] Make Id field and requestPreference optional when processing users [#205](https://github.com/FOLIO-FSE/folio_migration_tools/issues/205)
- [**enhancement/new feature**][**wontfix**] Create migration task for PO lines [#203](https://github.com/FOLIO-FSE/folio_migration_tools/issues/203)

#### question/decision

- [**question/decision**][**new_folio_functionality**] Create summary of missing conditions in MARC mapping rules for Morning glory [#377](https://github.com/FOLIO-FSE/folio_migration_tools/issues/377)

#### wontfix

- [**wontfix**] Map system identifier to barcode where barcode is null [#387](https://github.com/FOLIO-FSE/folio_migration_tools/issues/387)
- [**wontfix**] Trouble shoot false negatives in RDA field mappings [#386](https://github.com/FOLIO-FSE/folio_migration_tools/issues/386)
- [**wontfix**] Loans migrator needs to add timestamps [#340](https://github.com/FOLIO-FSE/folio_migration_tools/issues/340)

---

## 1_5_0 (05/10/2022)

#### bug

- [**bug**] The bib transformer task should not update the holdings HRID start number in Inventory HRID Settings [#259](https://github.com/FOLIO-FSE/folio_migration_tools/issues/259)

#### closed

- [**closed**] QuickMARC only accepts correct leader 05 values (a,c,d,n,p).  Fail records with other leaders? [#367](https://github.com/FOLIO-FSE/folio_migration_tools/issues/367)
- [**closed**] instance_id_map and holdings_id_map still retrain iteration_identifier in filename [#364](https://github.com/FOLIO-FSE/folio_migration_tools/issues/364)
- [**closed**] Always add an Administrative note on the Legacy system identifier in Instance records [#356](https://github.com/FOLIO-FSE/folio_migration_tools/issues/356)
- [**closed**] Count number of rows in CSV/TSV file before feeding them to the CSV Dictreader  [#339](https://github.com/FOLIO-FSE/folio_migration_tools/issues/339)
- [**closed**] Clarify handling of true/false values for User > addresses > primaryAddress [#265](https://github.com/FOLIO-FSE/folio_migration_tools/issues/265)
- [**closed**] Change heading from "Git(hub) workflow" to "Code contribution workflow" to avoid confusion with git workflows [#218](https://github.com/FOLIO-FSE/folio_migration_tools/issues/218)

#### enhancement/new feature

- [**enhancement/new feature**][**wontfix**] Store failed records in BIB and MFHD transformer for other failures than encoding issues [#317](https://github.com/FOLIO-FSE/folio_migration_tools/issues/317)
- [**enhancement/new feature**] When creating Courses, assign Course Department based on a Course Departments reference data map [#308](https://github.com/FOLIO-FSE/folio_migration_tools/issues/308)

#### improve_test_coverage

- [**improve_test_coverage**] Increase test coverage in src/folio_migration_tools/marc_rules_transformation/holdings_processor.py [#348](https://github.com/FOLIO-FSE/folio_migration_tools/issues/348)
- [**improve_test_coverage**] Increase test coverage in src/folio_migration_tools/marc_rules_transformation/rules_mapper_holdings.py [#347](https://github.com/FOLIO-FSE/folio_migration_tools/issues/347)

#### reporting

- [**reporting**] Add header to Item migration report [#336](https://github.com/FOLIO-FSE/folio_migration_tools/issues/336)
- [**reporting**] Placeholder issue for Records in File before parsing [#331](https://github.com/FOLIO-FSE/folio_migration_tools/issues/331)
- [**reporting**] Remove "Time Started" from reports [#330](https://github.com/FOLIO-FSE/folio_migration_tools/issues/330)

#### simplify_migration_process

- [**simplify_migration_process**] Proposal: Create and save to an [iterationIdentifier] folder in migration_repo/reports  [#110](https://github.com/FOLIO-FSE/folio_migration_tools/issues/110)

#### wontfix

- [**wontfix**][**reporting**] if no --base_folder_path CLI argument is supplied, assume it is the same as the root of the configuration file [#335](https://github.com/FOLIO-FSE/folio_migration_tools/issues/335)

---

## 1_4_8 (30/08/2022)

#### bug

- [**bug**] Fix bugs and minor issues with loans migrations [#323](https://github.com/FOLIO-FSE/folio_migration_tools/issues/323)

#### closed

- [**closed**] Add toggle for updating hrid or not. [#338](https://github.com/FOLIO-FSE/folio_migration_tools/issues/338)
- [**closed**] Honor commonRetainLeadingZeroes setting when generating instance HRIDs

 [#231](https://github.com/FOLIO-FSE/folio_migration_tools/issues/231)

#### simplify_migration_process

- [**simplify_migration_process**] Add configuration option to reset HRID settings [#105](https://github.com/FOLIO-FSE/folio_migration_tools/issues/105)

---

## 1_4_7 (25/06/2022)

#### bug

- [**bug**] JSON parsing fails when 409:s are returned from batch apis [#320](https://github.com/FOLIO-FSE/folio_migration_tools/issues/320)

---

## 1_4_6 (24/06/2022)

#### closed

- [**closed**] Unclear error message when you have mapped temporary locations but no location map [#315](https://github.com/FOLIO-FSE/folio_migration_tools/issues/315)

#### good first issue

- [**good first issue**] Make Loans migration script clearly  WARN if SMTP is not disabled when posting new loans [#244](https://github.com/FOLIO-FSE/folio_migration_tools/issues/244)

---

## 1_4_5 (22/06/2022)

#### bug

- [**bug**][**organizations**] When creating Organizations, clean out address objects where all properties, or all properties except "isPrimary", are empty [#293](https://github.com/FOLIO-FSE/folio_migration_tools/issues/293)
- [**bug**][**wontfix**] HoldingsStatements generated from multiple items does not get extended, but instead overwritten [#287](https://github.com/FOLIO-FSE/folio_migration_tools/issues/287)
- [**bug**] Organization transformation lacks report writing parameter [#286](https://github.com/FOLIO-FSE/folio_migration_tools/issues/286)
- [**bug**] Don't strip leading and trailing brackets from callNumbers as these are likely to be part of the callNumber value [#269](https://github.com/FOLIO-FSE/folio_migration_tools/issues/269)

#### closed

- [**closed**] Request migration always fails in Kiwi (request not allowed for patron/item combination) [#302](https://github.com/FOLIO-FSE/folio_migration_tools/issues/302)
- [**closed**] FOLIO handles preferred first names weird.  [#291](https://github.com/FOLIO-FSE/folio_migration_tools/issues/291)
- [**closed**] Make Requests Migrator UTC offset handling DST-aware [#288](https://github.com/FOLIO-FSE/folio_migration_tools/issues/288)

#### enhancement/new feature

- [**enhancement/new feature**] Tie a servicepoint per loans file [#303](https://github.com/FOLIO-FSE/folio_migration_tools/issues/303)
- [**enhancement/new feature**] Make LoansPoster UTC offset handling DST-aware [#279](https://github.com/FOLIO-FSE/folio_migration_tools/issues/279)
- [**enhancement/new feature**] Add support for servicePointIdAtCheckout from source data of open loans [#251](https://github.com/FOLIO-FSE/folio_migration_tools/issues/251)

#### improve_test_coverage

- [**improve_test_coverage**] Add test files for all migration tasks [#311](https://github.com/FOLIO-FSE/folio_migration_tools/issues/311)

---

## 1_4_4 (13/06/2022)

#### bug

- [**bug**] Courses transformation task halts if property utc_difference not in task config -- not reflected the migration repo template [#282](https://github.com/FOLIO-FSE/folio_migration_tools/issues/282)
- [**bug**] UTF Handling of MFHD records are not handled the same way as bibs, leading to more encoding errors [#278](https://github.com/FOLIO-FSE/folio_migration_tools/issues/278)

#### closed

- [**closed**] Fetch organization schemas linked to latest mod-organizations-storage release [#234](https://github.com/FOLIO-FSE/folio_migration_tools/issues/234)

#### enhancement/new feature

- [**enhancement/new feature**] Allow month codes in season fields and vice versa when parsing 853/863 combos [#283](https://github.com/FOLIO-FSE/folio_migration_tools/issues/283)
- [**enhancement/new feature**][**good first issue**] Include task name in INFO output [#245](https://github.com/FOLIO-FSE/folio_migration_tools/issues/245)
- [**enhancement/new feature**] Create transformation task for simple organizations [#146](https://github.com/FOLIO-FSE/folio_migration_tools/issues/146)

#### reporting

- [**reporting**] Add time duration for migration task to migration reports [#264](https://github.com/FOLIO-FSE/folio_migration_tools/issues/264)
- [**reporting**] Create migration report for BatchPoster [#60](https://github.com/FOLIO-FSE/folio_migration_tools/issues/60)

---

## 1_4_2 (05/06/2022)

#### bug

- [**bug**] Holdings statements for indexes and supplementes are leaking into the regular holdings statements [#276](https://github.com/FOLIO-FSE/folio_migration_tools/issues/276)
- [**bug**] UTF Handling of MFHD records are not handled the same way as bibs, leading to more encoding errors [#278]

---

## 1_4_0 (04/06/2022)

#### bug

- [**bug**] Holdings statements generated from marc records do get deduplicated despite it should be turned off. [#274](https://github.com/FOLIO-FSE/folio_migration_tools/issues/274)
- [**bug**] holdings statements in holdings generated from csv items are not getting deduplicated properly [#271](https://github.com/FOLIO-FSE/folio_migration_tools/issues/271)
- [**bug**] FOLIO Client causes 403:s due to missing authentication tokens in GITHUB API requests [#266](https://github.com/FOLIO-FSE/folio_migration_tools/issues/266)
- [**bug**] Failed bib records file overwritten for each file in bib transformation task [#252](https://github.com/FOLIO-FSE/folio_migration_tools/issues/252)

#### closed

- [**closed**] Add data and examples to migration_repo_template [#261](https://github.com/FOLIO-FSE/folio_migration_tools/issues/261)
- [**closed**] Add Batchposter support for courses objects [#260](https://github.com/FOLIO-FSE/folio_migration_tools/issues/260)
- [**closed**] Make Notes mapping more generic [#258](https://github.com/FOLIO-FSE/folio_migration_tools/issues/258)
- [**closed**] Create migration task for reserves [#257](https://github.com/FOLIO-FSE/folio_migration_tools/issues/257)
- [**closed**] Create Migration Task for courses [#256](https://github.com/FOLIO-FSE/folio_migration_tools/issues/256)
- [**closed**] Add refDataMapping for Terms [#255](https://github.com/FOLIO-FSE/folio_migration_tools/issues/255)
- [**closed**] Generate schema for "Composite Courses" [#254](https://github.com/FOLIO-FSE/folio_migration_tools/issues/254)
- [**closed**] Update FOLIO Namespaces with course-related objects [#253](https://github.com/FOLIO-FSE/folio_migration_tools/issues/253)

#### enhancement/new feature

- [**enhancement/new feature**][**question/decision**] Handle loans with incomplete date/time information [#226](https://github.com/FOLIO-FSE/folio_migration_tools/issues/226)

---

## 1_3_10 (18/05/2022)

#### closed

- [**closed**] Multiple empty holdings statements are being generated. [#249](https://github.com/FOLIO-FSE/folio_migration_tools/issues/249)

---

## 1_3_9 (16/05/2022)

#### bug

- [**bug**] Loans migration fails if you only supply the script only one of items or patron files [#247](https://github.com/FOLIO-FSE/folio_migration_tools/issues/247)
- [**bug**] MFHD Parsing: Nonexistant holdings statements on holdingsRecords are getting created as empty lists, making them render in the UI [#243](https://github.com/FOLIO-FSE/folio_migration_tools/issues/243)
- [**bug**] UserMapper: Concatenated fields from multiple xSV fields are not stripped from spaces before getting joined [#242](https://github.com/FOLIO-FSE/folio_migration_tools/issues/242)

---

## 1_3_0 (11/05/2022)

#### bug

- [**bug**][**reporting**] Configuration error reporting reports wrong search location for MARC bibs in BibTransformer [#238](https://github.com/FOLIO-FSE/folio_migration_tools/issues/238)
- [**bug**] Migrate open loans task skips first loan (row) when creating loams [#236](https://github.com/FOLIO-FSE/folio_migration_tools/issues/236)
- [**bug**][**good first issue**] Improve 853/863 mapping according to feedback - Part 2 [#112](https://github.com/FOLIO-FSE/folio_migration_tools/issues/112)
- [**bug**] Make sure that holdings callNumbers are correctly formatted for III non-boundwiths when there are boundwiths in the file [#98](https://github.com/FOLIO-FSE/folio_migration_tools/issues/98)

#### closed

- [**closed**] Exception TypeError being raised in rules_mapper_bibs [#232](https://github.com/FOLIO-FSE/folio_migration_tools/issues/232)
- [**closed**] Report and halt when mapped legacy field in reference data mapping is not found in the data [#229](https://github.com/FOLIO-FSE/folio_migration_tools/issues/229)

#### enhancement/new feature

- [**enhancement/new feature**] Introduce Lotus support [#201](https://github.com/FOLIO-FSE/folio_migration_tools/issues/201)
- [**enhancement/new feature**] Split string according to delimiter and create multiple fields for the elements [#183](https://github.com/FOLIO-FSE/folio_migration_tools/issues/183)

---

## y1_2_3 (02/05/2022)

#### bug

- [**bug**] UTC correction increases utc difference [#223](https://github.com/FOLIO-FSE/folio_migration_tools/issues/223)
- [**bug**] HoldingsCSVTransformer does not halt if previously_generated_holdings_files is not found [#216](https://github.com/FOLIO-FSE/folio_migration_tools/issues/216)

#### closed

- [**closed**] Handle issues with positions in leader20-23 [#219](https://github.com/FOLIO-FSE/folio_migration_tools/issues/219)
- [**closed**] Implement staffSuppress for bib migrations [#206](https://github.com/FOLIO-FSE/folio_migration_tools/issues/206)

#### reporting

- [**reporting**] Create proper report on suppression for all objects [#145](https://github.com/FOLIO-FSE/folio_migration_tools/issues/145)

---

## 1_2_2 (27/04/2022)
### breaking changes
Batchposter now allows running multiple files of the same object type in one run, so configuration has changed to facilitate that.
#### bug

- [**bug**][**wontfix**] Merged BW Holdings are losing their BW part links [#215](https://github.com/FOLIO-FSE/folio_migration_tools/issues/215)

#### simplify_migration_process

- [**simplify_migration_process**] Make BatchPoster more responsive to failing batches  [#108](https://github.com/FOLIO-FSE/folio_migration_tools/issues/108)

---

## 1_2_0 (22/04/2022)
- [**closed**] Halt when instance id maps are empty [#214](https://github.com/FOLIO-FSE/folio_migration_tools/issues/214)
- [**closed**] Add reporting on legacy fields that get concatenated from being mapped to the same folio field [#212](https://github.com/FOLIO-FSE/folio_migration_tools/issues/212)
- [**closed**] Report on date parsing results for User expiration and enrollment dates [#209](https://github.com/FOLIO-FSE/folio_migration_tools/issues/209)
- [**closed**] Add numeric values to sys.exit() calls [#208](https://github.com/FOLIO-FSE/folio_migration_tools/issues/208)
- [**closed**] test monday status updates [#204](https://github.com/FOLIO-FSE/folio_migration_tools/issues/204)
- [**enhancement/new feature**] Use the mechanism for logging data issues for open loans and open requests [#194](https://github.com/FOLIO-FSE/folio_migration_tools/issues/194)
- [**enhancement/new feature**] Include open requests migration to migration tools [#162](https://github.com/FOLIO-FSE/folio_migration_tools/issues/162)
- [**enhancement/new feature**] Include Open Loans migrations to migration tools [#161](https://github.com/FOLIO-FSE/folio_migration_tools/issues/161)
- [**simplify_migration_process**] Make the Batchposter handle the failed  batches, running the failed batches one-by-one and only store the "true failures" to disk. [#129](https://github.com/FOLIO-FSE/folio_migration_tools/issues/129)
- [**simplify_migration_process**] Speed up loans migration [#126](https://github.com/FOLIO-FSE/folio_migration_tools/issues/126)
- [**simplify_migration_process**] Make it possible to add multiple files to one batchposter run. [#109](https://github.com/FOLIO-FSE/folio_migration_tools/issues/109)
- [**enhancement/new feature**][**simplify_migration_process**] Validate that all mapped legacy fields correspond to a header in the data [#107](https://github.com/FOLIO-FSE/folio_migration_tools/issues/107)
- [**wontfix**][**new_folio_functionality**] Add support for migrating MARC Authority records [#90](https://github.com/FOLIO-FSE/folio_migration_tools/issues/90)

---

## 1_1_0 (13/04/2022)
- [**closed**] Consistently handle due time for III day loans to avoid duedate earlier outdate error [#188](https://github.com/FOLIO-FSE/folio_migration_tools/issues/188)
- [**enhancement/new feature**][**simplify_migration_process**] Publish MARC21-to-FOLIO as Package to PyPi [#77](https://github.com/FOLIO-FSE/folio_migration_tools/issues/77)
-  [**closed**] Speed up loans migration [#126](https://github.com/FOLIO-FSE/folio_migration_tools/issues/126)
-  [**closed**] Include Open Loans migrations to migration tools [#161](https://github.com/FOLIO-FSE/folio_migration_tools/issues/161)
-  [**closed**] Include open requests migration to migration tools [#162](https://github.com/FOLIO-FSE/folio_migration_tools/issues/162)
-  [**won't fix**] Add support for migrating MARC Authority records [#90](https://github.com/FOLIO-FSE/folio_migration_tools/issues/90)
-  [**closed**] Use the mechanism for logging data issues for open loans and open requests [#194](https://github.com/FOLIO-FSE/folio_migration_tools/issues/194)
-  [**closed**] Loans migrations are not handling the fact that the source data is not in UTC [#179](https://github.com/FOLIO-FSE/folio_migration_tools/issues/179)
-  [**closed**] Add setting in config for the timezone for dates transactional data [#176](https://github.com/FOLIO-FSE/folio_migration_tools/issues/176)

---

## 1.0.4 Pypi release (04/04/2022)
<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/kiwi_1_1...1.0.4
---

## Kiwi 1.1 (31/03/2022)
## What's Changed
* Adds use_logging parameter to BatchPoster by @jermnelson in https://github.com/FOLIO-FSE/folio_migration_tools/pull/173
* Fixes to MFHD and User notes. by @fontanka16 in https://github.com/FOLIO-FSE/folio_migration_tools/pull/185

## Fixes the following issues and bugs:
* #156 Add notes to users through extradata
* #165 Bug: Item transformation: Don't add a "notes" object when there is no a "note" property
* #167 Bug: mapping_file_mapper_base.py: object is excluded from array if non-required sub-property is ""
* #174 Make scripts create separate extradata files per task
* #175 Reset extradata file when running a task 
* #178 Bug: Fix issue with srs mfhd ids
* #180 Bug: Bound-with-holdings does not come over from previously run holdings
* #181 Handle MFHD with illegal lengths in 008:s
* #182 Bug: Holdings migrating scripts updates/resets the HRID counters when HRIDs are not controlled by the script
* #184 Bug: User notes are created even thought they have no content

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/kiwi_1_0...kiwi_1_1
---

## Kiwi 1.0 (16/03/2022)

---

## juniper_lts (31/01/2022)
