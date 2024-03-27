# Changelog

## v_1_8_8 (25/03/2024)

#### bug

- [**bug**] MARC Holdings Transformer Fails During wrap-up [#742](https://github.com/FOLIO-FSE/folio_migration_tools/issues/742)

---

## v_1_8_7 (25/03/2024)

#### bug

- [**bug**][**Inventory**] dedupe_list_of_dict method on HoldingsStatementsParser does not preserve item order [#733](https://github.com/FOLIO-FSE/folio_migration_tools/issues/733)

#### closed

- [**closed**] Prepare 1.8.7 Release [#739](https://github.com/FOLIO-FSE/folio_migration_tools/issues/739)
- [**closed**] Change OrdersTransformer TaskConfiguration to inherit from AbstractTaskConfiguration [#735](https://github.com/FOLIO-FSE/folio_migration_tools/issues/735)

---

## v_1_8_6 (26/02/2024)

#### Inventory

- [**Inventory**] Make the BibsTransformer to create Source=FOLIO records without SRS records [#449](https://github.com/FOLIO-FSE/folio_migration_tools/issues/449)

#### bug

- [**bug**][**Inventory**] Presence of mismatched 85x/86x patterns (subfields "missing") Causes MFHD transformer to fail [#729](https://github.com/FOLIO-FSE/folio_migration_tools/issues/729)

#### closed

- [**closed**] Prepare 1.8.6 release [#731](https://github.com/FOLIO-FSE/folio_migration_tools/issues/731)

---

## v_1_8_5 (16/02/2024)

#### bug

- [**bug**] Batch posting jobs against Poppy system fail after running for 10 minutes [#724](https://github.com/FOLIO-FSE/folio_migration_tools/issues/724)

#### closed

- [**closed**] Create 1.8.5 release [#726](https://github.com/FOLIO-FSE/folio_migration_tools/issues/726)
- [**closed**] Add an option on user transforms to remove request preferences [#716](https://github.com/FOLIO-FSE/folio_migration_tools/issues/716)
- [**closed**] Support proxy borrowers in loans migrator [#709](https://github.com/FOLIO-FSE/folio_migration_tools/issues/709)
- [**closed**] Allow specifying an ECS member tenant ID at the library_configuration level [#701](https://github.com/FOLIO-FSE/folio_migration_tools/issues/701)

#### wontfix

- [**wontfix**][**Inventory**] Separate holdings records generate the same UUID [#397](https://github.com/FOLIO-FSE/folio_migration_tools/issues/397)

---

## v_1_8_4 (07/11/2023)

#### Questions & Decisions

- [**Questions & Decisions**][**Inventory**] Make trimming of trailing spaces that are part of the OCLC number consistent between bib 001 and mfhd 004 [#557](https://github.com/FOLIO-FSE/folio_migration_tools/issues/557)

#### Support for changes in FOLIO

- [**Support for changes in FOLIO**][**Authorities**] Handle Address FOLIO Authorities Refactor rename `mod-entities-links` => `mod-authorities-manager` [#695](https://github.com/FOLIO-FSE/folio_migration_tools/issues/695)
- [**Support for changes in FOLIO**][**Authorities**] Authority JSON spec refactored in version 27 of `mod-inventory-storage`  [#693](https://github.com/FOLIO-FSE/folio_migration_tools/issues/693)

#### Tool enhancements

- [**Tool enhancements**][**Support for changes in FOLIO**][**Inventory**][**marc**] Implement trim_punctuation condition for marc rules mapper [#691](https://github.com/FOLIO-FSE/folio_migration_tools/issues/691)
- [**Tool enhancements**][**Good first issue**] Allow Reading Command Line Parameters from Enviornment Variables [#683](https://github.com/FOLIO-FSE/folio_migration_tools/issues/683)
- [**Tool enhancements**] Migration Configuration File Inheritance [#682](https://github.com/FOLIO-FSE/folio_migration_tools/issues/682)
- [**Tool enhancements**][**Migration Reports**] Add Localization Support to Reports [#669](https://github.com/FOLIO-FSE/folio_migration_tools/issues/669)

#### closed

- [**closed**] i18n changes require files not included in the package distribution [#703](https://github.com/FOLIO-FSE/folio_migration_tools/issues/703)
- [**closed**] Bump version to 1.8.4 [#700](https://github.com/FOLIO-FSE/folio_migration_tools/issues/700)
- [**closed**] Do not include 'metadata' objects in generated FOLIO records [#697](https://github.com/FOLIO-FSE/folio_migration_tools/issues/697)
- [**closed**] Prevent creation of duplicate 035 entries when performing 001 -> 035 transformation [#680](https://github.com/FOLIO-FSE/folio_migration_tools/issues/680)
- [**closed**] Remove 003 when converting 001 to 035 during instance transformation [#679](https://github.com/FOLIO-FSE/folio_migration_tools/issues/679)
- [**closed**] Handle existing $9 for controllable MARC Bib fields when transforming legacy bibs [#673](https://github.com/FOLIO-FSE/folio_migration_tools/issues/673)

---

## v_1_8_3 (05/09/2023)

#### closed

- [**closed**] Prepare 1.8.3 release [#676](https://github.com/FOLIO-FSE/folio_migration_tools/issues/676)
- [**closed**] Switch batch poster from using data=json.dumps(object) to json=object [#674](https://github.com/FOLIO-FSE/folio_migration_tools/issues/674)

---

## v_1_8_2 (23/08/2023)

#### Support for changes in FOLIO

- [**Support for changes in FOLIO**][**Authorities**] Implement mapping of naturalId for MARC authority records [#662](https://github.com/FOLIO-FSE/folio_migration_tools/issues/662)
- [**Support for changes in FOLIO**] Implement Condition concat_subfields_by_name (including subfieldsToConcat and subfieldsToStopConcat)  [#326](https://github.com/FOLIO-FSE/folio_migration_tools/issues/326)

#### Tool enhancements

- [**Tool enhancements**][**Authorities**] Fix invalid LDR 17 values in MARC authority records [#663](https://github.com/FOLIO-FSE/folio_migration_tools/issues/663)

#### closed

- [**closed**] Bump version to 1.8.2 [#671](https://github.com/FOLIO-FSE/folio_migration_tools/issues/671)
- [**closed**] Fix syntax error in language code mapping [#667](https://github.com/FOLIO-FSE/folio_migration_tools/issues/667)
- [**closed**] Preserve language code order when mapping languages from 041 with multiple codes in MARC Bib transformer [#661](https://github.com/FOLIO-FSE/folio_migration_tools/issues/661)
- [**closed**] Subject subfields concatenated with spaces rather than dashes as per mapping rules in MARC to Instance mapping [#655](https://github.com/FOLIO-FSE/folio_migration_tools/issues/655)

---

## v_1_8_1 (29/06/2023)

#### Orders

- [**Orders**] Implement Vendor mapping for Orders - Step 1 [#516](https://github.com/FOLIO-FSE/folio_migration_tools/issues/516)

#### Support for changes in FOLIO

- [**Support for changes in FOLIO**] Update Loans Migrator task to support Nolana SMTP configuration changes [#500](https://github.com/FOLIO-FSE/folio_migration_tools/issues/500)

#### closed

- [**closed**] Bump version to 1.8.1 [#653](https://github.com/FOLIO-FSE/folio_migration_tools/issues/653)
- [**closed**] HridHandling.preserve001 not working when not creating source records [#652](https://github.com/FOLIO-FSE/folio_migration_tools/issues/652)
- [**closed**] Object build routine require Instance, Holdings, Item prefix [#648](https://github.com/FOLIO-FSE/folio_migration_tools/issues/648)
- [**closed**] Contributor data not mapped to Instances when multiple relator terms are present [#647](https://github.com/FOLIO-FSE/folio_migration_tools/issues/647)
- [**closed**] Implement discoverySuppress from file definition for delimited holdings and items [#639](https://github.com/FOLIO-FSE/folio_migration_tools/issues/639)
- [**closed**] Orders process hangs (~30 min) before build start [#631](https://github.com/FOLIO-FSE/folio_migration_tools/issues/631)

---

## v_1_8_0 (16/05/2023)

#### Good first issue

- [**Good first issue**][**Documentation**] Update annotations for Bib and MFHD transformer tasks to change wording of files object description [#598](https://github.com/FOLIO-FSE/folio_migration_tools/issues/598)

#### Orders

- [**Orders**] Orders, alternative implementation: fetch and cache vendors only when needed [#634](https://github.com/FOLIO-FSE/folio_migration_tools/issues/634)
- [**Orders**] Orders report missing Mapped FOLIO fields + total number created is one too few [#627](https://github.com/FOLIO-FSE/folio_migration_tools/issues/627)
- [**Orders**] acquisitionMethod reference data wildcard mapping not working [#626](https://github.com/FOLIO-FSE/folio_migration_tools/issues/626)
- [**Orders**] Implement Location mapping for Orders [#515](https://github.com/FOLIO-FSE/folio_migration_tools/issues/515)

#### Organizations

- [**Organizations**] Organizations transformer should create organizaitons_id_map [#635](https://github.com/FOLIO-FSE/folio_migration_tools/issues/635)

#### Simplify migration process

- [**Simplify migration process**] Make the *SV-based mappers add default values from the schemas [#501](https://github.com/FOLIO-FSE/folio_migration_tools/issues/501)

#### Tool enhancements

- [**Tool enhancements**] Replace the current use of requests with something that is faster and more modern... [#553](https://github.com/FOLIO-FSE/folio_migration_tools/issues/553)
- [**Tool enhancements**][**Organizations**] Make mapping_file_mapper_base split value by subfield delimiter before applying replaceValues rule [#542](https://github.com/FOLIO-FSE/folio_migration_tools/issues/542)
- [**Tool enhancements**][**Orders**] Add Composite Purchase Orders to BatchPoster [#391](https://github.com/FOLIO-FSE/folio_migration_tools/issues/391)
- [**Tool enhancements**] Create Composite Purchase Order Mapper Class [#390](https://github.com/FOLIO-FSE/folio_migration_tools/issues/390)
- [**Tool enhancements**] Include open fee-fines migration into migration_tools [#163](https://github.com/FOLIO-FSE/folio_migration_tools/issues/163)

#### bug

- [**bug**] Read The Docs build is failing: "Could not import extension sphinx.builders.linkcheck" [#625](https://github.com/FOLIO-FSE/folio_migration_tools/issues/625)
- [**bug**][**Users**] Error when transforming users with addresses [#620](https://github.com/FOLIO-FSE/folio_migration_tools/issues/620)
- [**bug**][**Orders**] Location map not being loaded properly in migration_task_base [#612](https://github.com/FOLIO-FSE/folio_migration_tools/issues/612)
- [**bug**] Verify that mapping of boolean values works across *SV-based mappers [#504](https://github.com/FOLIO-FSE/folio_migration_tools/issues/504)

#### closed

- [**closed**] Orders: log that setup process is loading instance map and fetching organizations [#632](https://github.com/FOLIO-FSE/folio_migration_tools/issues/632)
- [**closed**] Add documentation for Fee/fine transformation [#623](https://github.com/FOLIO-FSE/folio_migration_tools/issues/623)
- [**closed**] Fees/fines: adjust actionDate to reflect local tenant timezone [#619](https://github.com/FOLIO-FSE/folio_migration_tools/issues/619)
- [**closed**] Fail fees/fines without a Status (UI-required) [#618](https://github.com/FOLIO-FSE/folio_migration_tools/issues/618)
- [**closed**] Unmapped fields with a fixed value do not undergo the reference data mapping [#614](https://github.com/FOLIO-FSE/folio_migration_tools/issues/614)

---

## v_1_7_11 (16/04/2023)

#### Orders

- [**Orders**] Added location mapping for PoL locations [#515](https://github.com/FOLIO-FSE/folio_migration_tools/issues/515)

---

## v_1_7_10 (14/04/2023)
#### Orders

- [**Orders**] Added orders support to BatchPoster task [#391](https://github.com/FOLIO-FSE/folio_migration_tools/issues/391)
- [**Orders**] Fixed issued with mapping numbers and integers in purchasOrderLines objects on composite purchase orders [#599](https://github.com/FOLIO-FSE/folio_migration_tools/issues/599)

#### Inventory

- [**Inventory**] Remove HRIDs from FOLIO Holdings records when not creating MFHD SRS [#596](https://github.com/FOLIO-FSE/folio_migration_tools/issues/596)

#### Bugs

- [**bug**] Nolana and Orchid are not recognized as valid FOLIO releases [#601](https://github.com/FOLIO-FSE/folio_migration_tools/issues/601)
---

## v_1_7_9_post1 (30/03/2023)

#### Inventory

- [**Inventory**] Implement condition set_contributor_type_text [#555](https://github.com/FOLIO-FSE/folio_migration_tools/issues/555)
- [**Inventory**] When matching of Contributor type string fails, add the string to the freetext field of the contributor type. i [#523](https://github.com/FOLIO-FSE/folio_migration_tools/issues/523)
- [**Inventory**] Make sure cataloged dates mapped are properly formatted. [#385](https://github.com/FOLIO-FSE/folio_migration_tools/issues/385)
- [**Inventory**] Implement Bound-with mapping for Voyager [#380](https://github.com/FOLIO-FSE/folio_migration_tools/issues/380)

#### Migration Reports

- [**Migration Reports**][**Organizations**][**Inventory**] Include legacy values mapped to array subproperties in Mapped legacy fields [#543](https://github.com/FOLIO-FSE/folio_migration_tools/issues/543)

#### Orders

- [**Orders**] Implement Notes handling for Composite Orders [#530](https://github.com/FOLIO-FSE/folio_migration_tools/issues/530)

#### Simplify migration process

- [**Simplify migration process**][**performance**] Improve performance for ItemsTransformer by calling super().get_prop() only when needed. [#569](https://github.com/FOLIO-FSE/folio_migration_tools/issues/569)

#### Support for changes in FOLIO

- [**Support for changes in FOLIO**] implement new bib rule feature: AlternativeMapping [#498](https://github.com/FOLIO-FSE/folio_migration_tools/issues/498)
- [**Support for changes in FOLIO**] Implement condition set_contributor_type_id_by_code_or_name for bibs [#497](https://github.com/FOLIO-FSE/folio_migration_tools/issues/497)

#### Tool enhancements

- [**Tool enhancements**][**performance**] Introduce setting in Batchposter for toggling reposting of records [#558](https://github.com/FOLIO-FSE/folio_migration_tools/issues/558)
- [**Tool enhancements**] Update "ilsFlavour" handling for legacy Bib ID to support merged records for MOBIUS [#546](https://github.com/FOLIO-FSE/folio_migration_tools/issues/546)

#### Users

- [**Users**] Add requestPreference object schema to user schema [#549](https://github.com/FOLIO-FSE/folio_migration_tools/issues/549)

#### bug

- [**bug**][**Users**] Empty user dates are returned as today's date  [#575](https://github.com/FOLIO-FSE/folio_migration_tools/issues/575)
- [**bug**][**Inventory**] HRID settings fail to update at the end of transformation [#550](https://github.com/FOLIO-FSE/folio_migration_tools/issues/550)
- [**bug**] Make validation of required properties work for arrays containing objects/arrays [#531](https://github.com/FOLIO-FSE/folio_migration_tools/issues/531)
- [**bug**] Re-posting Inventory records to FOLIO over the Batch API:s renders in HTTP 409:s [#250](https://github.com/FOLIO-FSE/folio_migration_tools/issues/250)
- [**bug**] Some legacy fields on items does not get reported into the legacy mapping report even though they are mapped [#84](https://github.com/FOLIO-FSE/folio_migration_tools/issues/84)
- [**bug**][**Migration Reports**] main_items.py does not seem to count all available legacy fields [#79](https://github.com/FOLIO-FSE/folio_migration_tools/issues/79)

#### closed

- [**closed**] Create release tag [#570](https://github.com/FOLIO-FSE/folio_migration_tools/issues/570)
- [**closed**] Fix unclosed StringIO objects in mapping_file_mapper_base tests [#563](https://github.com/FOLIO-FSE/folio_migration_tools/issues/563)
- [**closed**] Add requests and yaml to folio_migration_tools requirements [#552](https://github.com/FOLIO-FSE/folio_migration_tools/issues/552)
- [**closed**] Make sure all FileMappers uses MappingFileMapperBase.get_legacy_value [#513](https://github.com/FOLIO-FSE/folio_migration_tools/issues/513)

#### duplicate

- [**duplicate**][**Orders**] Make Batchposter post Composite POs/POLs [#526](https://github.com/FOLIO-FSE/folio_migration_tools/issues/526)
- [**duplicate**][**Support for changes in FOLIO**] implement Condition concat_subfields_by_name [#499](https://github.com/FOLIO-FSE/folio_migration_tools/issues/499)

#### wontfix

- [**wontfix**][**async-support**] Repost of records in failed batches should be multithreaded [#540](https://github.com/FOLIO-FSE/folio_migration_tools/issues/540)
- [**wontfix**][**Support for changes in FOLIO**] Adapt tools to Morning Glory [#329](https://github.com/FOLIO-FSE/folio_migration_tools/issues/329)

---

## v_1_7_8 (05/03/2023)
*No changelog for this release.*

---

## v_1_7_6 (04/03/2023)

#### Organizations

- [**Organizations**] When creating Organizations with Interfaces, create Credentials as extradata [#465](https://github.com/FOLIO-FSE/folio_migration_tools/issues/465)
- [**Organizations**] Handle posting of extradata when some types need to be posted before the main object, some after [#451](https://github.com/FOLIO-FSE/folio_migration_tools/issues/451)

#### Tool enhancements

- [**Tool enhancements**][**Organizations**] When creating Organizations, create Notes as extradata [#296](https://github.com/FOLIO-FSE/folio_migration_tools/issues/296)

#### bug

- [**bug**][**Inventory**] Ensure that properties required in the schema are honoured on all levels - Inventory [#536](https://github.com/FOLIO-FSE/folio_migration_tools/issues/536)
- [**bug**][**wontfix**][**Organizations**][**Orders**] Ensure that properties required in the schema are honoured on all levels [#464](https://github.com/FOLIO-FSE/folio_migration_tools/issues/464)

#### closed

- [**closed**] Implement replaceValues mapping feature for Organizations [#541](https://github.com/FOLIO-FSE/folio_migration_tools/issues/541)
- [**closed**] Record POST fails if electronicAccess[]relationshipId provided but uri is null [#539](https://github.com/FOLIO-FSE/folio_migration_tools/issues/539)
- [**closed**] Record POST fails if classificationTypeId provided but classificationNumber is null [#538](https://github.com/FOLIO-FSE/folio_migration_tools/issues/538)
- [**closed**] POST fails for any Instance batch containing a record lacking classifications [#534](https://github.com/FOLIO-FSE/folio_migration_tools/issues/534)

---

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

## 1_4_6 (23/06/2022)

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
