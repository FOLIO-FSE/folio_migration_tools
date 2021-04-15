blurbs = {
        "Introduction": "<br/>Data errors preventing records from being migrated are marked **FIX BEFORE MIGRATION**. The library is advised to clean up these errors in the source data.<br/><br/> The sections related to field counts and mapping results are marked **REVIEW**. These do not indicate errors preventing records from being migrated, but may point to data anomalies or in the mappings. The library should review these to make sure that the numbers are what one would expect, knowing the source data. Is this the expected number of serials? Is this the expected number of cartographic materials?",
        "Mapped Legacy fields": "**REVIEW** This table lists all the MARC fields in the source data, and whether it has been mapped to a FOLIO instance record field. The library should examine the MARC tags with a high 'Unmapped' figure and determine if these MARC tags contain data that you would like mapped to the FOLIO instance record.",
        "Mapped FOLIO fields": "**REVIEW** This table shows how many of the FOLIO instance records created contain data in the different FOLIO fields. The library should review the mapped totals against what they would expect to see mapped.",
        "__Section 1: instances": "This entries below seem to be related to instances",
        "Record status (leader pos 5)":  "**REVIEW** An ovrview of the Record statuses (Leader position 5) present in your source data.",
        "Bib records that failed to parse": "**FIX BEFORE MIGRATION** This section outputs the contents of records that could not be parsed by the transformation script (e.g. due to encoding issues). These should be reviewed by the library. The records cannot be migrated until they parse correctly.",
        "Records failed to migrate due to Value errors found in Transformation": "**FIX BEFORE MIGRATION** This section identifies records that have unexpected or missing values that prevent the transformation. The type of error will be specified. The library must resolve the issue for the record to be migrated.",
        "Records without titles": "**FIX IN SOURCE DATA** These records are missing a 245 field. FOLIO requires an instance title. The library must enter this information for the record to be migrated.",
        "Records without Instance Type Ids": "**IC ACTION REQUIRED** These reords should get an instance type ID mapped from 336, or a default of Undefined, or they will not be transformed.",
        "Mapped instance formats": "**REVIEW** The created FOLIO instances contain the following Instance format values. The library should review the total number for each value against what they would expect to see mapped.",
        "Mapped identifier types": "**REVIEW** The created FOLIO instances contain the following Identifier type values. The library should review the total number for each value against what they would expect to see mapped.",
        "Mapped note types": "**REVIEW** The created FOLIO instances contain the following Note type values. The library should review the total number for each value against what they would expect to see mapped.",
        "Mapped contributor name types": "**REVIEW** The created FOLIO instances contain the following Name type values. The library should review the total number for each value against what they would expect to see mapped.",
        "Unmapped contributor name types": "**REVIEW/IC ACTION REQUIRED** Contributor bame types present in the source data, but not mapped to a FOLIO value. The library and IC should review values and mapping.",
        "Contributor type mapping": "**REVIEW** The created FOLIO instances contain the following Contributor type values. The library should review the total number for each value against what they would expect to see mapped.",
        "Mapped electronic access relationships types": "**REVIEW** The created FOLIO instances contain the following Electronic access relationship type values. The library should review the total number for each value against what they would expect to see mapped.",
        "Incomplete entity mapping (a code issue) adding entity": "This is a coding anomaly that FSE will look into. The library does not have to do anything about it.",
        "Resource Type Mapping (336, 008)": "**REVIEW** The created FOLIO instances contain the following Instance type values. The library should review the total number for each value against what they would expect to see mapped.",
        "Mapped Alternative title types": "**REVIEW** The created FOLIO instances contain the following Alternative title type values. The library should review the total number for each value against what they would expect to see mapped.",
        "880 mappings": "This table shows how the 880 (Alternate Graphic Representation) has been mapped.",
        "880 mappings: mapped field not in mapping-rules": "**REVIEW** Fields that are referenced in the 880 mapping, but not configured in the mapping-rules.",
        "Instance level callNumber": "**REVIEW** True if the source data contains bib level call numbers in MARC field 099.",
        "Non-numeric tags in records": "**REVIEW** Non-numeric tags may indicate locally defined fields.",
        "Instance format ids handling (337 + 338)": "**REVIEW** This is how data in source MARC fields 337 and 338 have been mapped to FOLIO instance format ID.",
        "Unspecified Modes of issuance code": "**REVIEW** Number of created FOLIO instances with Mode of issueance set to *Unspecified*.",
        "Matched Modes of issuance code": "**REVIEW** The created FOLIO instances contain the following Mode of issuace values. The library should review the total number for each value against what they would expect to see mapped.",
        "Unmatched Modes of issuance code": "**REVIEW** Mode of issuance values present in the source data, but not mapped to a FOLIO value. The library and IC should review values and mapping.",
        "Language coude sources in 041":  "Section description to be added.",
        "Unrecognized language codes in records":  "**REVIEW** Language code values in the source data that do not match standard language codes. If not fixed before migration, these will display as Undetermined in the instance record.",
        "__Section 2: holdings": "The entries below seem to be related to holdings",
        "Callnumber types": "Section description to be added.",
        "Holdings type mapping": "Section description to be added.",
        "Legacy location codes": "Section description to be added.",
        "Locations - Unmapped legacy codes": "Section description to be added.",
        "Mapped Locations": "Section description to be added.",
        "Leader 06 (Holdings type)":  "Section description to be added.",
        "__Section 3: items": "The entries below seem to be related to items",
        "ValueErrors": "Section description to be added.",
        "Exceptions": "Section description to be added.",
        "Top missing holdings ids": "Section description to be added.",
        "Top duplicate item ids": "Section description to be added.",
        "Missing location codes": "Section description to be added.",
        "Circulation notes": "Section description to be added.",
        "Call number legacy typesName - Not yet mapped": "Section description to be added.",
        "Legacy item status - Not mapped": "Section description to be added.",
        "Mapped Material Types": "Section description to be added.",
        "Unapped Material Types": "Section description to be added.",
        "Mapped loan types": "Section description to be added.",
        "Unmapped loan types": "Section description to be added.",
        "HRID Handling": "Section description to be added.",
        "Preceeding and Succeeding titles": "Section description to be added.",
        "Holdings generation from bibs": "Section description to be added.",
        "'Instance format ids handling (337 + 338))": "Section description to be added."
    }
