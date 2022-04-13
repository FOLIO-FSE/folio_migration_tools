# Changelog

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

---

## Juniper 1.2 (09/12/2021)
## What's Changed
#69 ##Handle duplicate barcodes
#85 Remove suppression CLI argument from main_holdings_csv since this is not used. 
#82 ## Add options for getting Cataloged date from 008 
#51 ## 001s as HRIDs: Log Instances that had 001 collisions and where assigned FOLIO-style HRIDs
## Handling of duplicate Instance ids at main_holdings_marc.py. 
## Log failing location mapping to Data issues log
## Report on duplicate instance ids from multible main_bibs.py runs when running any of the main_holdings scripts

**Full Changelog**: https://github.com/FOLIO-FSE/MARC21-To-FOLIO/compare/juniper_1_1...juniper_1_2
---

## Juniper 1.1 (15/11/2021)
# Breaking and major changes
## property legacyIdentifier required for mapping file-based migration scripts
As part of the work with deterministic UUIDs, we now need a legacyIdentifer property in the mapping files. data-mapping-file-creator has been updated with this new property:

![image](https://user-images.githubusercontent.com/1894384/141301030-b3785435-6dd8-43a2-8202-d35f0ef950f4.png)

main_items.py and main_holdings_csv.py will halt if the property is missing:

![image](https://user-images.githubusercontent.com/1894384/141300583-e39c460f-efa1-4a7e-977c-7a665a33b812.png)

---

## Juniper 1.0.1 (27/10/2021)
# Juniper 1.0.1
## What is new?
### Introducing the Data Issues Log file
![image](https://user-images.githubusercontent.com/1894384/136540471-a471fb6f-d195-4eeb-996e-37be5797212b.png)
As part of every transformation, there is now a logfile named according to 

> folio_OBJECTTYPE_data_issues_TIMESTAMP.tsv

There are four columns: 	
* Severity of issue	
* Legacy Identifier or filename+index in file
* Description of the issue
* The data causing the issue

This report should - in conjunction with the transformation reports be sent to the libraries allowing them to sort and filter on the report and then fix the issues surfaced.

### Parsing for 853/863, 854/864, 855/865 pairs and other Holdings statements-related fields for MFHS (main_holdings_marc.py)
When running MFHD files, Holdings statements will now be parsed into human-readable strings out-of-the-box. For some time, the transformation logs will contain the following output in order to help identify issues with the parsed strings.

```
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)$k(day)	=863  \\$81.1$a253$b2$i2006$j01$k09	STATEMENT: v.253:no.2 (Jan 2006 09)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)	=863  \\$81.1$a34$b48$i2005$j11	STATEMENT: v.34:no.48 (Nov 2005)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)	=863  \\$81.2$a35$b2$i2006$j01	STATEMENT: v.35:no.2 (Jan 2006)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$b $i(year)$j(month)	=863  \\$81.1$a110-111$b3-3$i2003-2004$j05/06$wn	STATEMENT: v.110: 3 (05/06 2003)-v.111: 3 (2004)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$b $i(year)$j(month)	=863  \\$81.2$a111-111$b5-6$i2004$j09/10-11/12	STATEMENT: v.111: 5 (09/10 2004)-v.111: 6 (11/12)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)	=863  \\$81.1$a16-29$b1-7$i1990-2003	STATEMENT: v.16:no.1 (1990)-v.29:no.7 (2003)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)	=863  \\$81.1$a1-60$b1-2$i1955-2014$j01-04$zPrint copy canceled in 2014.	STATEMENT: v.1:no.1 (Jan 1955)-v.60:no.2 (Apr 2014)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)	=863  \\$81.1$a3-58$b1-4$i1959-2014$zPrint copy canceled in 2014.	STATEMENT: v.3:no.1 (1959)-v.58:no.4 (2014)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)	=863  \\$81.1$a25-42$b1-4$i1997-2014$zPrint copy canceled in 2014.	STATEMENT: v.25:no.1 (1997)-v.42:no.4 (2014)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$i(year)	=863  \\$81.1$a1-43$i1970-2012$zCanceled in 2013.	STATEMENT: v.1 (1970)-v.43 (2012)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)	=863  \\$81.1$a1-26$b1-10$i1973-1999	STATEMENT: v.1:no.1 (1973)-v.26:no.10 (1999)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(season)	=863  \\$81.1$a22-41$b1-4$i1992-2011$j21-23$zPrint copy cancelled in 2011.	STATEMENT: v.22:no.1 (1992 Spring)-v.41:no.4 (2011 23)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)	=863  \\$81.1$a1-48$b1-4$i1966-2014$j11$zPrint copy canceled in 2014.	STATEMENT: v.1:no.1 (Nov 1966)-v.48:no.4 (2014)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(season)	=863  \\$81.1$a1-24$b1-3$i1985-2009$j24-24	STATEMENT: v.1:no.1 (1985 Winter)-v.24:no.3 (2009 24)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$i(year)	=863  \\$81.1$a1-$i1980-	STATEMENT: v.1 (1980)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)	=863  \\$81.1$a1-$b1-$i1972-$j09-	STATEMENT: v.1:no.1 (Sep 1972)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)	=863  \\$81.1$a11-27$b23-50$i1994-2010	STATEMENT: v.11:no.23 (1994)-v.27:no.50 (2010)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(season)	=863  \\$81.1$a1-25$b1-3$i1979-2003	STATEMENT: v.1:no.1 (1979)-v.25:no.3 (2003)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)	=863  \\$81.1$a1-68$b1-1$i1948-2015$j03$zPrint copy canceled in 2014.	STATEMENT: v.1:no.1 (Mar 1948)-v.68:no.1 (2015)
HOLDINGS STATEMENT PATTERN	=853  \\$81$av.$bno.$i(year)$j(month)	=863  \\$81.1$a67-89$b1-12$i1985-2007$j01-12	STATEMENT: v.67:no.1 (Jan 1985)-v.89:no.12 (Dec 2007)

```
### Allowing for wildcard matching in individual cells in Reference data mapping-files
![image](https://user-images.githubusercontent.com/1894384/136540274-c89b827d-1079-47f4-b38b-2242f6f53fba.png)

### Enable mapping of temporary loan types and temporary locations from separate mapping files.
In the migration_repo_template there are now two mapping files with the file ending .optional. Remove that file ending if you want these mappings enabled for FOLIO. Syntax and mapping are the same as their siblings (permanent location and loan type). 

### Ability to specify FOLIO Release
There is a small breaking change between Juniper and Iris. In order for the script to handle both cases, there is a new parameter in main_bibs.py allowing you to specify FOLIO release name

## Breaking changes
* main_holdings.py is renamed to main_holdings_marc.py
*
## Bugfixes and maintenance
* Alignment with new Juniper srs record structure
* Improve HRID mapping reporting
* Rename main_holdings.py to main_holdings_marc.py
* Refinement in reporting
* Removed bib migration report "Unspecified Mode of Issuance code" since it contained redundant information
* Restructure blurbs in order to get fewer warnings and have an easier way of referencing them
* Removed duplicated code and aligned code that did the same things in various places
* Field mapping reports (the last to sections in the transformation reports) all use the same algorithm
* Introduced thousand separators in the reports.
* Log failed reference data mappings along with file index of record ID
* Improved performance after some profiling.
* Fix crashes caused in Aleph Legacy ID handling
* Removed the annoying required extra column in the locations.tsv ref data mapping file when running main_holdings_marc.py. If there is one (legacy_code), it will be used, if not, the preset column not named folio_code will be used.


**Full Changelog**: https://github.com/FOLIO-FSE/MARC21-To-FOLIO/compare/iris_1_2...juniper_1_0
---

## Iris 1.3 (15/10/2021)
Stepping stone release before Juniper support
---

## Iris 1.2 (30/07/2021)
#Bugfixes
* Missing stat code mapping file make main_items.py crash
* Clear exceptions get buried in nested exception output

---

## Iris 1.1 (28/07/2021)
# Bug fixes
* Fixes  a bug where the former 001 of the MARC Bib record did not get added as an Identifier on the attached Instance. This is only relevant to libraries that uses the default HRID handling of the script.
---

## Iris 1.0 (27/07/2021)
Release compliant with FOLIO Iris release. 

It has not been tested with Hotfix #3 and #4 in Iris, but it should be compliant.
---

## Honeysuckle v1 (16/03/2021)
