from enum import Enum
from typing import NamedTuple


class ReportSection(Enum):
    GENERAL = "General statistics"
    LIBRARY_ACTION = "Library action needed"
    ADDITIONAL_INFORMATION = "Additional information"
    TECHNICAL = "Technical information"


class Blurbs(NamedTuple):
    MarcValidation = ("MARC21 validation issues found in records", "")
    Introduction = (
        "Introduction",
        "<br/>Data errors preventing records from being migrated are marked **FIX BEFORE MIGRATION**. The library is advised to clean up these errors in the source data.<br/><br/> The sections related to field counts and mapping results are marked **REVIEW**. These do not indicate errors preventing records from being migrated, but may point to data anomalies or in the mappings. The library should review these to make sure that the numbers are what one would expect, knowing the source data. Is this the expected number of serials? Is this the expected number of cartographic materials?",
    )
    HoldingsMerging = ("Holdings Merging", "")
    DepartmentsMapping = ("Departments mappings", "")
    MissingRequiredProperties = ("Missing or empty required properties", "")
    MappedLegacyFields = (
        "Mapped Legacy Fields",
        "Library action: **REVIEW** <br/>This table lists all the MARC fields in the source data, and whether it has been mapped to a FOLIO instance record field. The library should examine the MARC tags with a high 'Unmapped' figure and determine if these MARC tags contain data that you would like mapped to the FOLIO instance record.",
    )
    MappedFolioFields = (
        "Mapped FOLIO fields",
        "Library action: **REVIEW** <br/>This table shows how many of the FOLIO instance records created contain data in the different FOLIO fields. The library should review the mapped totals against what they would expect to see mapped.",
    )
    Section1 = (
        "__Section 1: instances",
        "This entries below seem to be related to instances",
    )
    RecordStatus = (
        "Record status (leader pos 5)",
        "Library action: **Consider fixing d-values before migration**<br/>An overview of the Record statuses (Leader position 5) present in your source data.    Pay attention to the number of occurrences of the value 'd'. These d's are expressing that they are deleted, and the records might not work as expected in FOLIO. Consider marking them as suppressed in your current system and export them as a separate batch in order to have them suppressed in FOLIO. Allowed values according to the MARC standard are a,c,d,n,p",
    )
    ValueErrors = (
        "Records failed to migrate due to Value errors found in Transformation",
        "**FIX BEFORE MIGRATION** This section identifies records that have unexpected or missing values that prevent the transformation. The type of error will be specified. The library must resolve the issue for the record to be migrated.",
    )
    MissingTitles = (
        "Records without titles",
        "**FIX IN SOURCE DATA** These records are missing a 245 field. FOLIO requires an instance title. The library must enter this information for the record to be migrated.",
    )
    MissingInstanceTypeIds = (
        "Records without Instance Type Ids",
        "**IC ACTION REQUIRED** These reords should get an instance type ID mapped from 336, or a default of Undefined, or they will not be transformed.",
    )
    MappedInstanceFormats = (
        "Mapped instance formats",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Instance format values. The library should review the total number for each value against what they would expect to see mapped.",
    )
    MappedIdentifierTypes = (
        "Mapped identifier types",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Identifier type values. The library should review the total number for each value against what they would expect to see mapped.",
    )
    MappedNoteTypes = (
        "Mapped note types",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Note type values.  <br/>The library should review the total number for each value against what they would expect to see mapped.",
    )
    MappedContributorNameTypes = (
        "Mapped contributor name types",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Name type values. The library should review the total number for each value against what they would expect to see mapped.",
    )
    UnmappedContributorNameTypes = (
        "Unmapped contributor name types",
        "**REVIEW/IC ACTION REQUIRED** <br/>Contributor name types present in the source data, but not mapped to a FOLIO value. The library and IC should review values and mapping.",
    )
    ContributorTypeMapping = (
        "Contributor type mapping",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Contributor type values. The library should review the total number for each value against what they would expect to see mapped.",
    )
    MappedElectronicRelationshipTypes = (
        "Mapped electronic access relationships types",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Electronic access relationship type values. The library should review the total number for each value against what they would expect to see mapped.",
    )
    IncompleteEntityMapping = (
        "Incomplete entity mapping adding entity",
        "**NO ACTION REQUIRED** <br/>This is a coding anomaly that FSE will look into.  <br/>Usually, the library does not have to do anything about it.<br/> One thing to look at is if there are many repeated subfields or unexpected patterns of subfields in the table.",
    )
    RecourceTypeMapping = (
        "Resource Type Mapping (336)",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Instance type values. The library should review the total number for each value against what they would expect to see mapped.",
    )
    MappedAlternativeTitleTypes = (
        "Mapped Alternative title types",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Alternative title type values. The library should review the total number for each value against what they would expect to see mapped. The FOLIO community recommends a coarse-grained mapping.",
    )
    Field880Mappings = (
        "880 mappings",
        "This table shows how the 880 (Alternate Graphic Representation) has been mapped.",
    )
    Field880MappingsMissingFromRules = (
        "880 mappings: mapped field not in mapping-rules",
        "Library action: **REVIEW** <br/>Fields that are referenced in the 880 mapping, but not configured in the mapping-rules.",
    )
    InstanceLevelCallnumber = (
        "Instance level callNumber",
        "Library action: **REVIEW** <br/>True if the source data contains bib level call numbers in MARC field 099.",
    )
    NonNumericTagsInRecord = (
        "Non-numeric tags in records",
        "Library action: **REVIEW** <br/>Non-numeric tags may indicate locally defined fields.",
    )
    MatchedModesOfIssuanceCode = (
        "Matched Modes of issuance code",
        "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Mode of issuace values. The library should review the total number for each value against what they would expect to see mapped.",
    )
    UnrecognizedLanguageCodes = (
        "Unrecognized language codes in records",
        "Library action: **REVIEW** <br/>Language code values in the source data that do not match standard language codes. If not fixed before migration, these will display as Undetermined in the instance record and Filtering by language in Inventory will not be conclusive.",
    )
    Section2 = (
        "__Section 2: holdings",
        "The entries below seem to be related to holdings",
    )
    HoldingsTypeMapping = ("Holdings type mapping", "")
    LegacyLocationCodes = ("Legacy location codes", "")
    Locations = (
        "Locations - Unmapped legacy codes",
        "",
    )
    UserGroupMapping = ("User group mapping", "")
    DefaultValuesAdded = (
        "Default values added",
        "The values below was added to all records from the value field in the mapping file instead of coming from the source records",
    )
    UsersPerPatronType = ("Users per patron type", "")
    MappedLocations = ("Mapped Locations", "")
    Leader06 = ("Leader 06 (Holdings type)", "")
    Section3 = ("__Section 3: items", "The entries below seem to be related to items")
    Exceptions = ("Exceptions", "")
    HridHandling = (
        "HRID Handling",
        "There are two ways of handling HRIDs. The default behaviour is to take the current 001 and move that to a new 035. This will also emerge as an Identifier on the Inventory Instances. The 001 and Instance HRID will be generated from the HRID settings in FOLIO. The second option is to maintain the 001s in the records, and also add this as the Instance HRID",
    )
    PrecedingSuccedingTitles = (
        "Preceding and Succeeding titles",
        "",
    )
    HoldingsGenerationFromBibs = (
        "Holdings generation from bibs",
        "Some libraries have Holdings/MFHD information baked into their bib records. The following breakdown gives an idea on the occurrence of 852/866 combinations",
    )
    InstanceFormat = (
        "Instance format ids handling (337 + 338))",
        "",
    )
    MappedClassificationTypes = (
        "Mapped classification types",
        "",
    )
    LocationMapping = (
        "Location mapping",
        "These are the results for the mapping between legacy locations and your new FOLIO location structure",
    )
    ValueSetInMappingFile = (
        "Value set in mapping file",
        "The value for these fields are set in the mapping file instead of coming from the legacy system data.",
    )
    ValuesMappedFromLegacyFields = (
        "Values mapped from legacy fields",
        "A list fo the values and what they were mapped to",
    )
    GeneralStatistics = (
        "General statistics",
        "A list of general counterts to outline the transformation as a whole.",
    )
    MappedPublisherRoleFromIndicator2 = (
        "Mapped publisher role from Indicator2",
        "Publication Role, taken from the code in Ind2",
    )
    CallNumberTypeMapping = (
        "Callnumber type mapping",
        "Call number types in MFHDs are mapped from 852, Indicator 1 according to a certain scheme. (LOC documentation)[https://www.loc.gov/marc/holdings/hd852.html]",
    )
    LanguagesInRecords = (
        "Language codes in records",
        "A breakdown of language codes occuring in the records. Purely informational.",
    )
    ReceiptStatusMapping = (
        "Reciept status mapping",
        "Mapped reciept status from 008[06]. (LoC documentation)[https://www.loc.gov/marc/holdings/hd008.html]",
    )
    LanguageCodeSources = (
        "Language code sources in 041",
        'Most language codes in MARC records come from this list: https://www.loc.gov/marc/languages/, But other codes are allowed. The controlled vocabulary needs to be stated in $2 and this is where the information in this list comes from. The mapping tools can not currently handle other language codes, and so cannot FOLIO. <br/>One solution is to migrate the codes into a string or/and replace the language code with the parent language (these languages are usually subgroups of other languages). Many times, these records already have the "super-language" recorded as well. The data will remain in the MARC records.',
    )
    FailedFiles = ("Failed files", "")
    BoundWithMappings = ("Bound-with mapping", "")
    TemporaryLocationMapping = ("Temporary location mapping", "")
    TemporaryLoanTypeMapping = ("Temporary Loan type mapping", "")
    PermanentLoanTypeMapping = ("Permanent Loan type mapping", "")
    StatisticalCodeMapping = ("Statistical code mapping", "")
    HoldingsRecordIdMapping = ("Holdings IDs mapped", "")
    UnmappedProperties = ("Unmapped properties", "")
    StatusMapping = ("Status mapping", "")
    ReferenceDataMapping = ("Reference Data Mapping", "")
    FieldMappingErrors = ("Field mapping errors", "")
    AddedValueFromParameter = ("Added value from parameter since value is empty", "")
    InstanceTypeMapping = ("Instance Type Mapping", "")
    StaffOnlyViaIndicator = ("Set note to staff only via indicator", "")
    PossibleCleaningTasks = ("Possible cleaning tasks", "")
    HoldingsStatementsParsing = (
        "Parsed Holdings statements from 85X/86X combinations",
        "",
    )
    MaterialTypeMapping = ("Mapped Material Types", "")
