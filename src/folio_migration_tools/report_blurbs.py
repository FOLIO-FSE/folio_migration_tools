from enum import Enum
import i18n


class ReportSection(Enum):
    GENERAL = i18n.t("General statistics")
    LIBRARY_ACTION = i18n.t("Library action needed")
    ADDITIONAL_INFORMATION = i18n.t("Additional information")
    TECHNICAL = i18n.t("Technical information")


class Blurbs:
    DiffsBetweenOrders = (
        i18n.t("Differences between generated orders with same Legacy Identifier"),
        i18n.t(
            "This is a technical report that helps you to identify differences in the mapped order fields. "
        ),
    )
    AuthoritySources = (i18n.t("Authorization sources and related information"), "")
    IncompleteSubPropertyRemoved = (
        i18n.t("Sub-property removed due to missing required fields"),
        i18n.t(
            "Add the missing required information to the record in your current ILS to ensure that it can be migrated over."
        ),
    )
    POLCreation = (
        i18n.t("PO-Line creation"),
        i18n.t("Report on how PO-Line creation went, what merge criterias were used, etc"),
    )
    AcquisitionMethodMapping = (i18n.t("POL Acquisition Method Mapping"), "")
    MalformedInterfaceUri = (
        i18n.t("Malformed interface URIs"),
        i18n.t(
            "Interfaces with malformed URIs will not be migrated. See data issues log.\nFOLIO Interface URIs must start with one of the following: 'ftp://', 'sftp://', 'http://', 'https://'."
        ),
    )
    FieldMappingDetails = (i18n.t("Mapping details"), "")
    CatalogingAgency = (i18n.t("Cataloging sources"), "")
    Trivia = (i18n.t("Trivia"), "")
    LeaderManipulation = (i18n.t("Leader manipulation"), "")
    TermsMapping = (i18n.t("Terms Mapping"), i18n.t("Reference data mapping for Terms."))
    FeeFineOnwerMapping = (
        i18n.t("Fee/Fine Owner mapping"),
        i18n.t("Reference data mapping for Fee/Fine owners."),
    )
    FeeFineTypesMapping = (
        i18n.t("Fee/Fine Type mapping"),
        i18n.t("Reference data mapping for Fee/Fine types."),
    )
    FeeFineServicePointTypesMapping = (
        i18n.t("Fee/Fine Service Point mapping"),
        i18n.t("Reference data mapping for Fee/Fine service points."),
    )
    CategoriesMapping = (
        i18n.t("Organization contact categories"),
        i18n.t("Reference data mapping for contacts, addresses, emails, and phones numbers."),
    )
    OrganizationTypeMapping = (
        i18n.t("Organization types"),
        i18n.t("Reference data mapping for Organization types."),
    )

    DateTimeConversions = (
        i18n.t("DateTime conversions"),
        i18n.t(
            "Some date and date time strings are converted to UTC DateTime objects and then printed accoding to ISO standard."
        ),
    )
    Details = (i18n.t("Details"), "")
    MarcValidation = (i18n.t("MARC21 validation issues found in records"), "")
    Introduction = (
        i18n.t("Introduction"),
        i18n.t(
            "<br/>Data errors preventing records from being migrated are marked **FIX BEFORE MIGRATION**. The library is advised to clean up these errors in the source data.<br/><br/> The sections related to field counts and mapping results are marked **REVIEW**. These do not indicate errors preventing records from being migrated, but may point to data anomalies or in the mappings. The library should review these to make sure that the numbers are what one would expect, knowing the source data. Is this the expected number of serials? Is this the expected number of cartographic materials?"
        ),
    )
    DiscardedLoans = (
        i18n.t("Discarded loans"),
        i18n.t("List of loans discarded for various resons"),
    )
    DiscardedReserves = (
        i18n.t("Discarded reserves"),
        i18n.t("List of reserves discarded for various resons"),
    )
    DiscardedRequests = (
        i18n.t("Discarded Requests"),
        i18n.t("List of requests discarded for various resons"),
    )
    HoldingsMerging = (i18n.t("Holdings Merging"), "")
    DepartmentsMapping = (i18n.t("Departments mappings"), "")
    MissingRequiredProperties = (i18n.t("Missing or empty required properties"), "")
    MappedLegacyFields = (
        i18n.t("Mapped Legacy Fields"),
        i18n.t(
            "Library action: **REVIEW** <br/>This table lists all the MARC fields in the source data, and whether it has been mapped to a FOLIO instance record field. The library should examine the MARC tags with a high 'Unmapped' figure and determine if these MARC tags contain data that you would like mapped to the FOLIO instance record."
        ),
    )
    MappedFolioFields = (
        i18n.t("Mapped FOLIO fields"),
        i18n.t(
            "Library action: **REVIEW** <br/>This table shows how many of the FOLIO instance records created contain data in the different FOLIO fields. The library should review the mapped totals against what they would expect to see mapped."
        ),
    )
    Section1 = (
        i18n.t("__Section 1: instances"),
        i18n.t("This entries below seem to be related to instances"),
    )
    RecordStatus = (
        i18n.t("Record status (leader pos 5)"),
        i18n.t(
            "Library action: **All values that are not a, c, d, n or p will be set to c. If this is not what you want, you need to correct these values in your system. **<br/>An overview of the Record statuses (Leader position 5) present in your source data.    Pay attention to the number of occurrences of the value 'd'. These d's are expressing that they are deleted, and the records might not work as expected in FOLIO. Consider marking them as suppressed in your current system and export them as a separate batch in order to have them suppressed in FOLIO. Allowed values according to the MARC standard are a,c,d,n,p"
        ),
    )
    Suppression = (
        i18n.t("Suppression"),
        i18n.t("What records got assigned what suppression setting in the records."),
    )
    ValueErrors = (
        i18n.t("Records failed to migrate due to Value errors found in Transformation"),
        i18n.t(
            "**FIX BEFORE MIGRATION** This section identifies records that have unexpected or missing values that prevent the transformation. The type of error will be specified. The library must resolve the issue for the record to be migrated."
        ),
    )
    MissingTitles = (
        i18n.t("Records without titles"),
        i18n.t(
            "**FIX IN SOURCE DATA** These records are missing a 245 field. FOLIO requires an instance title. The library must enter this information for the record to be migrated."
        ),
    )
    MissingInstanceTypeIds = (
        i18n.t("Records without Instance Type Ids"),
        i18n.t(
            "**IC ACTION REQUIRED** These reords should get an instance type ID mapped from 336, or a default of Undefined, or they will not be transformed."
        ),
    )
    MappedInstanceFormats = (
        i18n.t("Mapped instance formats"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Instance format values. The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    MappedIdentifierTypes = (
        i18n.t("Mapped identifier types"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Identifier type values. The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    MappedNoteTypes = (
        i18n.t("Mapped note types"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO records contain the following Note type values.  <br/>The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    MappedContributorNameTypes = (
        i18n.t("Mapped contributor name types"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Name type values. The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    UnmappedContributorNameTypes = (
        i18n.t("Unmapped contributor name types"),
        i18n.t(
            "**REVIEW/IC ACTION REQUIRED** <br/>Contributor name types present in the source data, but not mapped to a FOLIO value. The library and IC should review values and mapping."
        ),
    )
    ContributorTypeMapping = (
        i18n.t("Contributor type mapping"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Contributor type values. The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    MappedElectronicRelationshipTypes = (
        i18n.t("Mapped electronic access relationships types"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Electronic access relationship type values. The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    IncompleteEntityMapping = (
        i18n.t("Incomplete entity mapping adding entity"),
        i18n.t(
            "**NO ACTION REQUIRED** <br/>This is a coding anomaly that FSE will look into.  <br/>Usually, the library does not have to do anything about it.<br/> One thing to look at is if there are many repeated subfields or unexpected patterns of subfields in the table."
        ),
    )
    RecourceTypeMapping = (
        i18n.t("Resource Type Mapping (336)"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Instance type values. The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    MappedAlternativeTitleTypes = (
        i18n.t("Mapped Alternative title types"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Alternative title type values. The library should review the total number for each value against what they would expect to see mapped. The FOLIO community recommends a coarse-grained mapping."
        ),
    )
    Field880Mappings = (
        i18n.t("880 mappings"),
        i18n.t("This table shows how the 880 (Alternate Graphic Representation) has been mapped."),
    )
    Field880MappingsMissingFromRules = (
        i18n.t("880 mappings: mapped field not in mapping-rules"),
        i18n.t(
            "Library action: **REVIEW** <br/>Fields that are referenced in the 880 mapping, but not configured in the mapping-rules."
        ),
    )
    InstanceLevelCallnumber = (
        i18n.t("Instance level callNumber"),
        i18n.t(
            "Library action: **REVIEW** <br/>True if the source data contains bib level call numbers in MARC field 099."
        ),
    )
    NonNumericTagsInRecord = (
        i18n.t("Non-numeric tags in records"),
        i18n.t(
            "Library action: **REVIEW** <br/>Non-numeric tags may indicate locally defined fields."
        ),
    )
    MatchedModesOfIssuanceCode = (
        i18n.t("Matched Modes of issuance code"),
        i18n.t(
            "Library action: **REVIEW** <br/>The created FOLIO instances contain the following Mode of issuace values. The library should review the total number for each value against what they would expect to see mapped."
        ),
    )
    UnrecognizedLanguageCodes = (
        i18n.t("Unrecognized language codes in records"),
        i18n.t(
            "Library action: **REVIEW** <br/>Language code values in the source data that do not match standard language codes. If not fixed before migration, these will display as Undetermined in the instance record and Filtering by language in Inventory will not be conclusive."
        ),
    )
    Section2 = (
        i18n.t("__Section 2: holdings"),
        i18n.t("The entries below seem to be related to holdings"),
    )
    HoldingsTypeMapping = (i18n.t("Holdings type mapping"), "")
    LegacyLocationCodes = (i18n.t("Legacy location codes"), "")
    Locations = (
        i18n.t("Locations - Unmapped legacy codes"),
        "",
    )
    UserGroupMapping = (i18n.t("User group mapping"), "")
    DefaultValuesAdded = (
        i18n.t("Default values from mapping added"),
        i18n.t(
            "The values below were added to all records from the 'value' field in the mapping file, overriding any mapped values from the source data."
        ),
    )
    FolioDefaultValuesAdded = (
        i18n.t("FOLIO default values added"),
        i18n.t(
            "The below FOLIO default values were added to records that had no mapped value in the source data."
        ),
    )
    UsersPerPatronType = (i18n.t("Users per patron type"), "")
    MappedLocations = (i18n.t("Mapped Locations"), "")
    Leader06 = (i18n.t("Leader 06 (Holdings type)"), "")
    Section3 = (
        i18n.t("__Section 3: items"),
        i18n.t("The entries below seem to be related to items"),
    )
    Exceptions = (i18n.t("Exceptions"), "")
    HridHandling = (
        i18n.t("HRID and 001/035 handling"),
        i18n.t(
            "There are two ways of handling HRIDs. The default behaviour is to take the current 001 and move that to a new 035. This will also emerge as an Identifier on the Inventory Instances. The 001 and Instance HRID will be generated from the HRID settings in FOLIO. The second option is to maintain the 001s in the records, and also add this as the Instance HRID"
        ),
    )
    PrecedingSuccedingTitles = (
        i18n.t("Preceding and Succeeding titles"),
        "",
    )
    HoldingsGenerationFromBibs = (
        i18n.t("Holdings generation from bibs"),
        i18n.t(
            "Some libraries have Holdings/MFHD information baked into their bib records. The following breakdown gives an idea on the occurrence of 852/866 combinations"
        ),
    )
    InstanceFormat = (
        i18n.t("Instance format ids handling (337 + 338))"),
        "",
    )
    MappedClassificationTypes = (
        i18n.t("Mapped classification types"),
        "",
    )
    LocationMapping = (
        i18n.t("Location mapping"),
        i18n.t(
            "These are the results for the mapping between legacy locations and your new FOLIO location structure"
        ),
    )
    ValueSetInMappingFile = (
        i18n.t("Value set in mapping file"),
        i18n.t(
            "The value for these fields are set in the mapping file instead of coming from the legacy system data."
        ),
    )
    ValuesMappedFromLegacyFields = (
        i18n.t("Values mapped from legacy fields"),
        i18n.t("A list fo the values and what they were mapped to"),
    )
    GeneralStatistics = (
        i18n.t("General statistics"),
        i18n.t("A list of general counters to outline the transformation as a whole."),
    )
    MappedPublisherRoleFromIndicator2 = (
        i18n.t("Mapped publisher role from Indicator2"),
        i18n.t("Publication Role, taken from the code in Ind2"),
    )
    CallNumberTypeMapping = (
        i18n.t("Callnumber type mapping"),
        i18n.t(
            "Call number types in MFHDs are mapped from 852, Indicator 1 according to a certain scheme. (LOC documentation)[https://www.loc.gov/marc/holdings/hd852.html]"
        ),
    )
    LanguagesInRecords = (
        i18n.t("Language codes in records"),
        i18n.t("A breakdown of language codes occuring in the records. Purely informational."),
    )
    ReceiptStatusMapping = (
        i18n.t("Reciept status mapping"),
        i18n.t(
            "Mapped reciept status from 008[06]. (LoC documentation)[https://www.loc.gov/marc/holdings/hd008.html]"
        ),
    )
    LanguageCodeSources = (
        i18n.t("Language code sources in 041"),
        'Most language codes in MARC records come from this list: https://www.loc.gov/marc/languages/, But other codes are allowed. The controlled vocabulary needs to be stated in $2 and this is where the information in this list comes from. The mapping tools can not currently handle other language codes, and so cannot FOLIO. <br/>One solution is to migrate the codes into a string or/and replace the language code with the parent language (these languages are usually subgroups of other languages). Many times, these records already have the i18n.t("super-language") recorded as well. The data will remain in the MARC records.',
    )
    FailedFiles = (i18n.t("Failed files"), "")
    BoundWithMappings = (i18n.t("Bound-with mapping"), "")
    TemporaryLocationMapping = (i18n.t("Temporary location mapping"), "")
    TemporaryLoanTypeMapping = (i18n.t("Temporary Loan type mapping"), "")
    PermanentLoanTypeMapping = (i18n.t("Permanent Loan type mapping"), "")
    OrderLineLocationMapping = (
        i18n.t("POL location mapping"),
        i18n.t("This is the location for for the purchase order line."),
    )
    PurchaseOrderVendorLinking = (
        i18n.t("Linked Organizations"),
        i18n.t("All purchase orders MUST be linked to an organization."),
    )
    PurchaseOrderInstanceLinking = (
        i18n.t("Linked Instances"),
        i18n.t("Purchase Order Lines can but do not have to be linked to instances"),
    )
    StatisticalCodeMapping = (i18n.t("Statistical code mapping"), "")
    HoldingsRecordIdMapping = (i18n.t("Holdings IDs mapped"), "")
    UnmappedProperties = (i18n.t("Unmapped properties"), "")
    StatusMapping = (i18n.t("Status mapping"), "")
    ReferenceDataMapping = (i18n.t("Reference Data Mapping"), "")
    FieldMappingErrors = (i18n.t("Field mapping errors"), "")
    AddedValueFromParameter = (i18n.t("Added value from parameter since value is empty"), "")
    InstanceTypeMapping = (i18n.t("Instance Type Mapping"), "")
    StaffOnlyViaIndicator = (i18n.t("Set note to staff only via indicator"), "")
    PossibleCleaningTasks = (i18n.t("Possible cleaning tasks"), "")
    HoldingsStatementsParsing = (
        i18n.t("Parsed Holdings statements from 85X/86X combinations"),
        "",
    )
    MaterialTypeMapping = (i18n.t("Mapped Material Types"), "")
    AuthoritySourceFileMapping = (
        i18n.t("Authority Source File Mapping Results"),
        i18n.t("Mappings based on FOLIO authority `naturalId` alpha prefix"),
    )
    AuthorityEncodingLevel = (
        i18n.t("Encoding level (leader pos 17)"),
        i18n.t(
            "Library action: **All values that are not n or o will be set to n. If this is not what you want, you need to correct these values in your system. **<br/>An overview of the Encoding levels (Leader position 17) present in your source data.  Allowed values according to the MARC standard are n or o"
        ),
    )
