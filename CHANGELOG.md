# Changelog

## v1.12.10 (11/09/2026)

#### Other Changes
* replaceValues cannot assign an empty string to a value by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1042
* Add documentation for configuration files and inheritance by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1045
* Fix patronGroup/departments mapping to fall back to hybrid wildcard rows by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1044
* Bump version and fix typing error by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1046

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.9...v1.12.10

---

## v1.12.9 (25/08/2026)

#### Other Changes
* Typing fixes by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1039
* Custom legacy id field for bibs doesn't work if the field is a control field (< 010) by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1041

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.8...v1.12.9

---

## v1.12.8 (31/07/2026)

#### Other Changes
* Feature/allow code based service point references in loans and request transaction data map service points by code 1034 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1035

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.7...v1.12.8

---

## v1.12.7 (28/07/2026)

#### Other Changes
* Add name-based order bill-to and ship-to mapping by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1031
* Maps to ref data UUIDs should be based on whitespace normalized forms by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1032

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.6...v1.12.7

---

## v1.12.6 (22/07/2026)

#### Other Changes
* Feature/reference data mapping for note types by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1030

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.5...v1.12.6

---

## v1.12.5 (21/07/2026)

#### Other Changes
* Fix/orders custom fields mapping by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1027

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.4...v1.12.5

---

## v1.12.4 (17/07/2026)

#### Other Changes
* update requirements.txt for docs to fix build by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1022
* Add address type validation and dedupe in UserTransformer by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1023
* Docs/add docs details for user notes transformation by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1025
* Fix order line path mapping for nested objects and JSON-safe field mapping errors by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1026

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.3...v1.12.4

---

## v1.12.3 (13/07/2026)

#### Other Changes
* Feature/support lists in legacy fallback field mapping option by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1021

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.2...v1.12.3

---

## v1.12.2 (09/07/2026)

#### Other Changes
* Update MARC transformer docs for decoding updates by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1018
* Update docs with new tutorial by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1019
* Feature/save record failed marc bibs for all errors by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1020

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.1...v1.12.2

---

## v1.12.1 (07/07/2026)

#### Other Changes
* Add support for MARC preprocessors to MARC Transformer tasks and Loan Poster fixes by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1013
* Fix fund map reference by @banerjek in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1015
* Enhance MARC decoding handling by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1017

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.12.0...v1.12.1

---

## v1.12.0 (23/06/2026)

#### Other Changes
* Feature/add alternate barcode lookup support to loans poster by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1007
* Update deepdiff version, fix some type check errors, and use logging.exception where appropriate instead of logging.error by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1008
* Fix/array object validation silent failures by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1009
* Adopt folio_uuid deterministic UUIDs for boundWithParts by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1011

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.11.3...v1.12.0

---

## v1.11.3 (24/04/2026)

#### Other Changes
* Fix/acq schema fetching wrong module version by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1006

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.11.2...v1.11.3

---

## v1.11.2 (16/04/2026)

#### Other Changes
* Bump version to 1.11.2 and update logging to use file_def.file_name directly in items transformer by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1004

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.11.1...v1.11.2

---

## v1.11.1 (16/04/2026)

#### Other Changes
* Add Aleph boundwith example query and processing instructions to documentation by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1000
* Update statistical code mapping documentation to explicitly require task configuration reference to statistical code map by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1001
* Fix logging of legacy items count to include file name in migration report instead of the entire file object by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/1003

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.11.0...v1.11.1

---

## v1.11.0 (02/04/2026)

#### Other Changes
* Feature/aleph lkr boundwiths by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/998
* Fix/file descriptor leak in batch poster async loops by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/999

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.10.6...v1.11.0

---

## v1.10.6 (27/03/2026)

#### Other Changes
* Fix/concurrency batch poster fix2 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/997

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.10.5...v1.10.6

---

## v1.10.5 (27/03/2026)

#### Other Changes
* Refactor BatchPoster to lazily initialize semaphore for concurrent requests by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/996

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.10.4...v1.10.5

---

## v1.10.4 (16/02/2026)

#### Other Changes
* Feature/use-call-number-type-fallback by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/991

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.10.3...v1.10.4

---

## v1.10.3 (04/02/2026)

#### Other Changes
* Feature/replace-batchposter-folio-data-import by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/986

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.10.2...v1.10.3

---

## v1.10.2 (27/01/2026)

#### Other Changes
* Fix/add-semaphore-to-batch-poster by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/985

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.10.1...v1.10.2

---

## v1.10.1 (24/01/2026)

#### Other Changes
* Feature/output-raw-report-data by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/983

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.10.0...v1.10.1

---

## v1.10.0 (23/01/2026)

#### Other Changes
* Exit when list_source_files fails by @banerjek in https://github.com/FOLIO-FSE/folio_migration_tools/pull/959
* Update to support FolioClient 1.0+ and Python 3.14 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/963
* Move circulation_helper tests to tests directory by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/967
* Fix tenant_id assignment to avoid setting it to an empty string in non-ECS migration context by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/969
* Merge Feature/allow-exclude-marc-bibs-by-file-def by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/971
* Refactor API request handling to use FolioClient methods instead of httpx.Client/AsyncClien in BatchPoster by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/973
* Implement refactor/974-remove-authorities-transformer-task by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/975
* Fix posting error in BatchPoster and file handling errors on Windows by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/976
* add parameter to build user objects without username by @marnold-ebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/977
* Feature/save-raw-report-data by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/980
* Fix: Update release event type to 'published' and add Python version 3.14 to the matrix by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/981
* Update version to 1.10.0 and add folio-data-import dependency back by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/982

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.10...v1.10.0

---

## v1.9.10 (08/11/2025)

#### Other Changes
* 821 improve fund mapping for composite orders by @banerjek in https://github.com/FOLIO-FSE/folio_migration_tools/pull/958

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.9...v1.9.10

---

## v1.9.9 (29/09/2025)

#### Other Changes
* Bump folio-data-import dependency to >=0.4.1 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/955

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.8...v1.9.9

---

## v1.9.8 (14/09/2025)

#### Other Changes
* Add patching functionality to BatchPoster for upsert process by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/954

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.7...v1.9.8

---

## v1.9.7 (12/09/2025)

#### Other Changes
* 952 stat code mapping doesnt work for csv holdings transformation by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/953

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.6...v1.9.7

---

## v1.9.6 (04/09/2025)

#### Other Changes
* Bump version to 1.9.6 and update Python dependencies by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/950

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.5...v1.9.6

---

## v1.9.5 (15/08/2025)

#### Other Changes
* Update loan handling and version bump to 1.9.5 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/948

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.4...v1.9.5

---

## v1.9.4 (15/08/2025)

#### Other Changes
* Fixes for Loans, Orgs, MFHD, BatchPoster Upsert, and Docs Improvements by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/947

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.3...v1.9.4

---

## v1.9.3 (13/08/2025)

#### Other Changes
* 872 increase test coverage for items transformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/929
* Fixes #928 and #933 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/934
* 873 Increase test coverage for user_transformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/932
* 870 Increase test coverage for holdings_marc_transformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/935
* Implement mapping conditions for policies and acquisition methods by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/940
* 874 increase test coverage for batch_poster and improve code formatting by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/937
* Fix crash on empty legacy_id values by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/941
* 888 increase test coverage for user_mapper by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/939
* Implement fix for Support mapping multiple departments using multi_field_delimiter by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/946

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.2...v1.9.3

---

## v1.9.2 (05/06/2025)

#### Other Changes
* Fix Holdings CSV Transformer for Ramsons and Log invalid invalid authorityId mapping as a record-level data issue and modify the entity mapping to allow transformation ton continue and produce valid records by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/927

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.1...v1.9.2

---

## v1.9.1 (27/05/2025)

#### Other Changes
* Add support for splitting MRC/MRK holdings notes to fit in 32K character limit and support field mapped enum in multi-field mappings by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/924

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.9.0...v1.9.1

---

## v1.8.25 (22/05/2025)

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v1.8.24...v1.8.25

---

## v1.9.0 (14/05/2025)

#### Other Changes
* Create PULL_REQUEST_TEMPLATE.md by @jjensen-ebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/818
* 771 composite order mapper needs to remove invalid characters from po numbers by @ealexch in https://github.com/FOLIO-FSE/folio_migration_tools/pull/825
* Merge 1.9.x branch into main by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/833
* Update pyproject.toml to poetry 2.1 syntax by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/836
* Fix optional item permanent location map implementation by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/838
* Fix file path references in failed records clean-up by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/839
* get_call_number fix to handle valueerror by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/841
* 578 Add annotation to the Task configuration entries for BatchPoster by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/844
* 346 Two test cases for condition_remove_prefix_by_indicator by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/842
* 580 Add annotation to the Task configuration entries for CoursesMigrator by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/845
* 589 Add annotations to the Task configuration entries for UserTransformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/855
* 588 Add annotations to the Task configuration entries for ReservesMigrator by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/854
* 587 Add annotations to the Task configuration entries for RequestsMigrator by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/853
* 586 Add annotations to the Task configuration entries for OrganizationTransformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/852
* Poetry export plugin has been added to pyproject.toml by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/851
* 585 Add annotations to the Task configuration entries for OrdersTransformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/850
* 584 Add annotations to the Task configuration entries for LoansMigrator by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/849
* 583 Add annotations to the Task configuration entries for ItemsTransformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/848
* 581 Add annotations to the Task configuration entries for HoldingsCsvTransformer by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/846
* 713 loans migrator empty renewal count issue by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/829
* 843 support generating instance relationships parentchild during marc bib transformation by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/858
* 349 Increase test coverage in legacy_reserve.py (rebased) by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/860
* Refactoring validate_po_number method in order_mapper by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/859
* Add ShadowInstances handling to BatchPoster by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/862
* Update MigrationTaskBase to support structural verification of reference data files (#826) by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/861
* Adding set_version methods to BatchPoster for to support inventory upserts, tweaks to bound with handling for Voyager-style boundwiths, and enhancements to MARC mapping by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/867
* Test coverage in legacy_loan has been increased (#350) by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/863
* Remove custom ASCII art and replace with art module by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/881
* 882-add-python-313-to-pre-publish-tests by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/883
* Create fixtures, compile test datasets, and mock mapper classes for bibs_transformer.py (#869) by @mtrineyev in https://github.com/FOLIO-FSE/folio_migration_tools/pull/875
* Fix temporary loan type mapping file path reference in ItemsTransformer to use specified instead of fixed path. by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/892
* Change get_mapped_name arguments for departments_mapping to prevent_default=True. by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/893
* 894-add-option-to-use-deterministic-uuids-based-on-tenantid-instead-of-api-base-url by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/895
* Remove pandas from dev dependencies by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/896
* bump lxml dev dependency by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/897
* Fix-unbound-local-error-in-1.9.0rc8 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/898
* 899-when-data_import_marctrue-for-a-bibstransformer-task-create_source_records-should-be-forced-to-false by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/900
* Bump Version to 1.9.0rc10 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/902
* Bump folioclient to 0.70.1, update references to okapi_url to gateway_url and related changes, add option to print version when calling as a script by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/906
* 884-update-docs-for-190 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/885
* 904-add-support-for-mapping-all-85x86x-incl-866-867-868-to-staff-only-holdings-notes-as-marcmaker-strings by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/907
* Refactor LegacyLoan to use instance variable for legacy_loan_dict and enhance error reporting for missing date information. Update tests. by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/909
* 912-simple_bib_map-fails-when-no-1xx-or-7xx-is-present-in-the-bib-record by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/915
* Refactor statistical code mapping for inventory records to support mapping from arbitrary MARC fields/subfields and multiple legacy fields in CSV-based mappings by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/918
* preserve hrid when performing upsert by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/920

#### New Contributors
* @jjensen-ebsco made their first contribution in https://github.com/FOLIO-FSE/folio_migration_tools/pull/818
* @ealexch made their first contribution in https://github.com/FOLIO-FSE/folio_migration_tools/pull/825

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_18...v1.9.0

---

## v1.8.24 (06/05/2025)

#### Other Changes
* Update holdings_helper.py by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/917

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_23...v1.8.24

---

## v1.8.23 (22/04/2025)

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_22...v_1_8_23

---

## v1.8.22 (22/04/2025)

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_21...v_1_8_22

---

## v1.8.21 (17/04/2025)

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_20...v_1_8_21

---

## v1.8.20 (19/02/2025)

#### Other Changes
* Fix get call number square brackets by @bltravis in https://github.com/FOLIO-FSE/folio_migration_tools/pull/840

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_19...v_1_8_20

---

## v1.8.19 (12/02/2025)

#### Other Changes
* Create PULL_REQUEST_TEMPLATE.md by @jjensen-ebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/818
* 771 composite order mapper needs to remove invalid characters from po numbers by @ealexch in https://github.com/FOLIO-FSE/folio_migration_tools/pull/825

#### New Contributors
* @jjensen-ebsco made their first contribution in https://github.com/FOLIO-FSE/folio_migration_tools/pull/818
* @ealexch made their first contribution in https://github.com/FOLIO-FSE/folio_migration_tools/pull/825

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_18...v_1_8_19

---

## v1.8.18 (21/11/2024)

#### Other Changes
* Add folio_client to BatchPoster __init__ and super().__init__ by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/809

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_17_post1...v_1_8_18

---

## v1.8.17.post1 (15/11/2024)

#### Other Changes
* Fix-task-inits-1817 by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/807

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_17...v_1_8_17_post1

---

## v1.8.17 (12/11/2024)

#### Other Changes
* Update changelog by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/803
* Set httpx logger level to WARNING to avoid unneeded log messages and add upsert support to inventory batchposters by @btravisebsco in https://github.com/FOLIO-FSE/folio_migration_tools/pull/804

**Full Changelog**: https://github.com/FOLIO-FSE/folio_migration_tools/compare/v_1_8_16...v_1_8_17

---

## v_1_8_16 (13/10/2024)

#### closed

- [**closed**] Bump FolioClient version requirement to 0.61.0 [#801](https://github.com/FOLIO-FSE/folio_migration_tools/issues/801)

---

## v_1_8_15 (11/10/2024)

#### closed

- [**closed**] BatchPoster for ExtraData broken in 1.8.14 [#799](https://github.com/FOLIO-FSE/folio_migration_tools/issues/799)

---

## v_1_8_14 (08/10/2024)

#### closed

- [**closed**] Prevent MFHD transformer from creating invalid electronicAccess objects (eg. no URL) [#794](https://github.com/FOLIO-FSE/folio_migration_tools/issues/794)

---

## v_1_8_13 (07/08/2024)
#### closed

- [**closed**] BibTransformer task fails with TypeError in 1.8.12 [#791](https://github.com/FOLIO-FSE/folio_migration_tools/issues/791)
- [**closed**] Add support for banking information as extradata type for BatchPoster [#789](https://github.com/FOLIO-FSE/folio_migration_tools/issues/789)

---

## v_1_8_12 (31/07/2024)
#### closed

- [**closed**] Pymarc 5.2.0 breaks MARC transformer tasks [#783](https://github.com/FOLIO-FSE/folio_migration_tools/issues/783)

---

## v_1_8_11 (10/07/2024)
#### Inventory

- [**Inventory**] Handle bib call numbers (eg. for III items data) formatted as list string representation with only one value [#721](https://github.com/FOLIO-FSE/folio_migration_tools/issues/721)

#### bug

- [**bug**][**Inventory**] Holdings CSV transformer does not apply file-indicated discovery suppression [#762](https://github.com/FOLIO-FSE/folio_migration_tools/issues/762)
- [**bug**][**Inventory**] III-style bound with fixes for multiple holdings with the same linked bibIds [#755](https://github.com/FOLIO-FSE/folio_migration_tools/issues/755)
- [**bug**][**Inventory**] Handle call numbers containing square brackets "[]" [#754](https://github.com/FOLIO-FSE/folio_migration_tools/issues/754)

#### closed

- [**closed**] Bump version to 1.8.11 [#781](https://github.com/FOLIO-FSE/folio_migration_tools/issues/781)
- [**closed**] Unable to map user type with user_transformer/user_mapper [#779](https://github.com/FOLIO-FSE/folio_migration_tools/issues/779)
- [**closed**] Tools have hardcoded limit of only 50 errors [#773](https://github.com/FOLIO-FSE/folio_migration_tools/issues/773)
- [**closed**] UserTransformer creates invalid address objects [#769](https://github.com/FOLIO-FSE/folio_migration_tools/issues/769)
- [**closed**] Handle single-item list of call numbers for III-style bound with items [#768](https://github.com/FOLIO-FSE/folio_migration_tools/issues/768)
- [**closed**] MARC Bib transformer creates invalid preceding-succeeding-title records [#764](https://github.com/FOLIO-FSE/folio_migration_tools/issues/764)
- [**closed**] Staff-only note mapping rules in MARC bib 5xx fields not honored by migration tools [#751](https://github.com/FOLIO-FSE/folio_migration_tools/issues/751)
- [**closed**] Use normalized version of barcode for duplicate checks [#738](https://github.com/FOLIO-FSE/folio_migration_tools/issues/738)

---

## v_1_8_9 (27/03/2024)

#### closed

- [**closed**] Prepare 1.8.9 release [#746](https://github.com/FOLIO-FSE/folio_migration_tools/issues/746)
- [**closed**] holdings_statementparser key errors [#744](https://github.com/FOLIO-FSE/folio_migration_tools/issues/744)

---

## v_1_8_10 (27/03/2024)

#### bug

- [**bug**] MARC Holdings Transformer Fails During wrap-up [#742](https://github.com/FOLIO-FSE/folio_migration_tools/issues/742)

#### closed

- [**closed**] prepare 1.8.10 release [#749](https://github.com/FOLIO-FSE/folio_migration_tools/issues/749)

---

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
