from enum import Enum
import i18n
from pathlib import Path

i18n.load_config(Path(__file__).parents[2] / "i18n_config.py")


class ReportSection(Enum):
    GENERAL = i18n.t("General statistics")
    LIBRARY_ACTION = i18n.t("Library action needed")
    ADDITIONAL_INFORMATION = i18n.t("Additional information")
    TECHNICAL = i18n.t("Technical information")


class Blurbs:
    DiffsBetweenOrders = (
        i18n.t("blurbs.DiffsBetweenOrders.title"),
        i18n.t("blurbs.DiffsBetweenOrders.description"),
    )
    AuthoritySources = (
        i18n.t("blurbs.AuthoritySources.title"),
        i18n.t("blurbs.AuthoritySources.description"),
    )
    IncompleteSubPropertyRemoved = (
        i18n.t("blurbs.IncompleteSubPropertyRemoved.title"),
        i18n.t("blurbs.IncompleteSubPropertyRemoved.description"),
    )
    POLCreation = (i18n.t("blurbs.POLCreation.title"), i18n.t("blurbs.POLCreation.description"))
    AcquisitionMethodMapping = (
        i18n.t("blurbs.AcquisitionMethodMapping.title"),
        i18n.t("blurbs.AcquisitionMethodMapping.description"),
    )
    MalformedInterfaceUri = (
        i18n.t("blurbs.MalformedInterfaceUri.title"),
        i18n.t("blurbs.MalformedInterfaceUri.description"),
    )
    FieldMappingDetails = (
        i18n.t("blurbs.FieldMappingDetails.title"),
        i18n.t("blurbs.FieldMappingDetails.description"),
    )
    CatalogingAgency = (
        i18n.t("blurbs.CatalogingAgency.title"),
        i18n.t("blurbs.CatalogingAgency.description"),
    )
    Trivia = (i18n.t("blurbs.Trivia.title"), i18n.t("blurbs.Trivia.description"))
    LeaderManipulation = (
        i18n.t("blurbs.LeaderManipulation.title"),
        i18n.t("blurbs.LeaderManipulation.description"),
    )
    TermsMapping = (i18n.t("blurbs.TermsMapping.title"), i18n.t("blurbs.TermsMapping.description"))
    FeeFineOnwerMapping = (
        i18n.t("blurbs.FeeFineOnwerMapping.title"),
        i18n.t("blurbs.FeeFineOnwerMapping.description"),
    )
    FeeFineTypesMapping = (
        i18n.t("blurbs.FeeFineTypesMapping.title"),
        i18n.t("blurbs.FeeFineTypesMapping.description"),
    )
    FeeFineServicePointTypesMapping = (
        i18n.t("blurbs.FeeFineServicePointTypesMapping.title"),
        i18n.t("blurbs.FeeFineServicePointTypesMapping.description"),
    )
    CategoriesMapping = (
        i18n.t("blurbs.CategoriesMapping.title"),
        i18n.t("blurbs.CategoriesMapping.description"),
    )
    OrganizationTypeMapping = (
        i18n.t("blurbs.OrganizationTypeMapping.title"),
        i18n.t("blurbs.OrganizationTypeMapping.description"),
    )
    DateTimeConversions = (
        i18n.t("blurbs.DateTimeConversions.title"),
        i18n.t("blurbs.DateTimeConversions.description"),
    )
    Details = (i18n.t("blurbs.Details.title"), i18n.t("blurbs.Details.description"))
    MarcValidation = (
        i18n.t("blurbs.MarcValidation.title"),
        i18n.t("blurbs.MarcValidation.description"),
    )
    Introduction = (i18n.t("blurbs.Introduction.title"), i18n.t("blurbs.Introduction.description"))
    DiscardedLoans = (
        i18n.t("blurbs.DiscardedLoans.title"),
        i18n.t("blurbs.DiscardedLoans.description"),
    )
    DiscardedReserves = (
        i18n.t("blurbs.DiscardedReserves.title"),
        i18n.t("blurbs.DiscardedReserves.description"),
    )
    DiscardedRequests = (
        i18n.t("blurbs.DiscardedRequests.title"),
        i18n.t("blurbs.DiscardedRequests.description"),
    )
    HoldingsMerging = (
        i18n.t("blurbs.HoldingsMerging.title"),
        i18n.t("blurbs.HoldingsMerging.description"),
    )
    DepartmentsMapping = (
        i18n.t("blurbs.DepartmentsMapping.title"),
        i18n.t("blurbs.DepartmentsMapping.description"),
    )
    MissingRequiredProperties = (
        i18n.t("blurbs.MissingRequiredProperties.title"),
        i18n.t("blurbs.MissingRequiredProperties.description"),
    )
    MappedLegacyFields = (
        i18n.t("blurbs.MappedLegacyFields.title"),
        i18n.t("blurbs.MappedLegacyFields.description"),
    )
    MappedFolioFields = (
        i18n.t("blurbs.MappedFolioFields.title"),
        i18n.t("blurbs.MappedFolioFields.description"),
    )
    Section1 = (i18n.t("blurbs.Section1.title"), i18n.t("blurbs.Section1.description"))
    RecordStatus = (i18n.t("blurbs.RecordStatus.title"), i18n.t("blurbs.RecordStatus.description"))
    Suppression = (i18n.t("blurbs.Suppression.title"), i18n.t("blurbs.Suppression.description"))
    ValueErrors = (i18n.t("blurbs.ValueErrors.title"), i18n.t("blurbs.ValueErrors.description"))
    MissingTitles = (
        i18n.t("blurbs.MissingTitles.title"),
        i18n.t("blurbs.MissingTitles.description"),
    )
    MissingInstanceTypeIds = (
        i18n.t("blurbs.MissingInstanceTypeIds.title"),
        i18n.t("blurbs.MissingInstanceTypeIds.description"),
    )
    MappedInstanceFormats = (
        i18n.t("blurbs.MappedInstanceFormats.title"),
        i18n.t("blurbs.MappedInstanceFormats.description"),
    )
    MappedIdentifierTypes = (
        i18n.t("blurbs.MappedIdentifierTypes.title"),
        i18n.t("blurbs.MappedIdentifierTypes.description"),
    )
    MappedNoteTypes = (
        i18n.t("blurbs.MappedNoteTypes.title"),
        i18n.t("blurbs.MappedNoteTypes.description"),
    )
    MappedContributorNameTypes = (
        i18n.t("blurbs.MappedContributorNameTypes.title"),
        i18n.t("blurbs.MappedContributorNameTypes.description"),
    )
    UnmappedContributorNameTypes = (
        i18n.t("blurbs.UnmappedContributorNameTypes.title"),
        i18n.t("blurbs.UnmappedContributorNameTypes.description"),
    )
    ContributorTypeMapping = (
        i18n.t("blurbs.ContributorTypeMapping.title"),
        i18n.t("blurbs.ContributorTypeMapping.description"),
    )
    MappedElectronicRelationshipTypes = (
        i18n.t("blurbs.MappedElectronicRelationshipTypes.title"),
        i18n.t("blurbs.MappedElectronicRelationshipTypes.description"),
    )
    IncompleteEntityMapping = (
        i18n.t("blurbs.IncompleteEntityMapping.title"),
        i18n.t("blurbs.IncompleteEntityMapping.description"),
    )
    RecourceTypeMapping = (
        i18n.t("blurbs.RecourceTypeMapping.title"),
        i18n.t("blurbs.RecourceTypeMapping.description"),
    )
    MappedAlternativeTitleTypes = (
        i18n.t("blurbs.MappedAlternativeTitleTypes.title"),
        i18n.t("blurbs.MappedAlternativeTitleTypes.description"),
    )
    Field880Mappings = (
        i18n.t("blurbs.Field880Mappings.title"),
        i18n.t("blurbs.Field880Mappings.description"),
    )
    Field880MappingsMissingFromRules = (
        i18n.t("blurbs.Field880MappingsMissingFromRules.title"),
        i18n.t("blurbs.Field880MappingsMissingFromRules.description"),
    )
    InstanceLevelCallnumber = (
        i18n.t("blurbs.InstanceLevelCallnumber.title"),
        i18n.t("blurbs.InstanceLevelCallnumber.description"),
    )
    NonNumericTagsInRecord = (
        i18n.t("blurbs.NonNumericTagsInRecord.title"),
        i18n.t("blurbs.NonNumericTagsInRecord.description"),
    )
    MatchedModesOfIssuanceCode = (
        i18n.t("blurbs.MatchedModesOfIssuanceCode.title"),
        i18n.t("blurbs.MatchedModesOfIssuanceCode.description"),
    )
    UnrecognizedLanguageCodes = (
        i18n.t("blurbs.UnrecognizedLanguageCodes.title"),
        i18n.t("blurbs.UnrecognizedLanguageCodes.description"),
    )
    Section2 = (i18n.t("blurbs.Section2.title"), i18n.t("blurbs.Section2.description"))
    HoldingsTypeMapping = (
        i18n.t("blurbs.HoldingsTypeMapping.title"),
        i18n.t("blurbs.HoldingsTypeMapping.description"),
    )
    LegacyLocationCodes = (
        i18n.t("blurbs.LegacyLocationCodes.title"),
        i18n.t("blurbs.LegacyLocationCodes.description"),
    )
    Locations = (i18n.t("blurbs.Locations.title"), i18n.t("blurbs.Locations.description"))
    UserGroupMapping = (
        i18n.t("blurbs.UserGroupMapping.title"),
        i18n.t("blurbs.UserGroupMapping.description"),
    )
    DefaultValuesAdded = (
        i18n.t("blurbs.DefaultValuesAdded.title"),
        i18n.t("blurbs.DefaultValuesAdded.description"),
    )
    FolioDefaultValuesAdded = (
        i18n.t("blurbs.FolioDefaultValuesAdded.title"),
        i18n.t("blurbs.FolioDefaultValuesAdded.description"),
    )
    UsersPerPatronType = (
        i18n.t("blurbs.UsersPerPatronType.title"),
        i18n.t("blurbs.UsersPerPatronType.description"),
    )
    MappedLocations = (
        i18n.t("blurbs.MappedLocations.title"),
        i18n.t("blurbs.MappedLocations.description"),
    )
    Leader06 = (i18n.t("blurbs.Leader06.title"), i18n.t("blurbs.Leader06.description"))
    Section3 = (i18n.t("blurbs.Section3.title"), i18n.t("blurbs.Section3.description"))
    Exceptions = (i18n.t("blurbs.Exceptions.title"), i18n.t("blurbs.Exceptions.description"))
    HridHandling = (i18n.t("blurbs.HridHandling.title"), i18n.t("blurbs.HridHandling.description"))
    PrecedingSuccedingTitles = (
        i18n.t("blurbs.PrecedingSuccedingTitles.title"),
        i18n.t("blurbs.PrecedingSuccedingTitles.description"),
    )
    HoldingsGenerationFromBibs = (
        i18n.t("blurbs.HoldingsGenerationFromBibs.title"),
        i18n.t("blurbs.HoldingsGenerationFromBibs.description"),
    )
    InstanceFormat = (
        i18n.t("blurbs.InstanceFormat.title"),
        i18n.t("blurbs.InstanceFormat.description"),
    )
    MappedClassificationTypes = (
        i18n.t("blurbs.MappedClassificationTypes.title"),
        i18n.t("blurbs.MappedClassificationTypes.description"),
    )
    LocationMapping = (
        i18n.t("blurbs.LocationMapping.title"),
        i18n.t("blurbs.LocationMapping.description"),
    )
    ValueSetInMappingFile = (
        i18n.t("blurbs.ValueSetInMappingFile.title"),
        i18n.t("blurbs.ValueSetInMappingFile.description"),
    )
    ValuesMappedFromLegacyFields = (
        i18n.t("blurbs.ValuesMappedFromLegacyFields.title"),
        i18n.t("blurbs.ValuesMappedFromLegacyFields.description"),
    )
    GeneralStatistics = (
        i18n.t("blurbs.GeneralStatistics.title"),
        i18n.t("blurbs.GeneralStatistics.description"),
    )
    MappedPublisherRoleFromIndicator2 = (
        i18n.t("blurbs.MappedPublisherRoleFromIndicator2.title"),
        i18n.t("blurbs.MappedPublisherRoleFromIndicator2.description"),
    )
    CallNumberTypeMapping = (
        i18n.t("blurbs.CallNumberTypeMapping.title"),
        i18n.t("blurbs.CallNumberTypeMapping.description"),
    )
    LanguagesInRecords = (
        i18n.t("blurbs.LanguagesInRecords.title"),
        i18n.t("blurbs.LanguagesInRecords.description"),
    )
    ReceiptStatusMapping = (
        i18n.t("blurbs.ReceiptStatusMapping.title"),
        i18n.t("blurbs.ReceiptStatusMapping.description"),
    )
    LanguageCodeSources = (
        i18n.t("blurbs.LanguageCodeSources.title"),
        i18n.t("blurbs.LanguageCodeSources.description"),
    )
    FailedFiles = (i18n.t("blurbs.FailedFiles.title"), i18n.t("blurbs.FailedFiles.description"))
    BoundWithMappings = (
        i18n.t("blurbs.BoundWithMappings.title"),
        i18n.t("blurbs.BoundWithMappings.description"),
    )
    TemporaryLocationMapping = (
        i18n.t("blurbs.TemporaryLocationMapping.title"),
        i18n.t("blurbs.TemporaryLocationMapping.description"),
    )
    TemporaryLoanTypeMapping = (
        i18n.t("blurbs.TemporaryLoanTypeMapping.title"),
        i18n.t("blurbs.TemporaryLoanTypeMapping.description"),
    )
    PermanentLoanTypeMapping = (
        i18n.t("blurbs.PermanentLoanTypeMapping.title"),
        i18n.t("blurbs.PermanentLoanTypeMapping.description"),
    )
    OrderLineLocationMapping = (
        i18n.t("blurbs.OrderLineLocationMapping.title"),
        i18n.t("blurbs.OrderLineLocationMapping.description"),
    )
    PurchaseOrderVendorLinking = (
        i18n.t("blurbs.PurchaseOrderVendorLinking.title"),
        i18n.t("blurbs.PurchaseOrderVendorLinking.description"),
    )
    PurchaseOrderInstanceLinking = (
        i18n.t("blurbs.PurchaseOrderInstanceLinking.title"),
        i18n.t("blurbs.PurchaseOrderInstanceLinking.description"),
    )
    StatisticalCodeMapping = (
        i18n.t("blurbs.StatisticalCodeMapping.title"),
        i18n.t("blurbs.StatisticalCodeMapping.description"),
    )
    HoldingsRecordIdMapping = (
        i18n.t("blurbs.HoldingsRecordIdMapping.title"),
        i18n.t("blurbs.HoldingsRecordIdMapping.description"),
    )
    UnmappedProperties = (
        i18n.t("blurbs.UnmappedProperties.title"),
        i18n.t("blurbs.UnmappedProperties.description"),
    )
    StatusMapping = (
        i18n.t("blurbs.StatusMapping.title"),
        i18n.t("blurbs.StatusMapping.description"),
    )
    ReferenceDataMapping = (
        i18n.t("blurbs.ReferenceDataMapping.title"),
        i18n.t("blurbs.ReferenceDataMapping.description"),
    )
    FieldMappingErrors = (
        i18n.t("blurbs.FieldMappingErrors.title"),
        i18n.t("blurbs.FieldMappingErrors.description"),
    )
    AddedValueFromParameter = (
        i18n.t("blurbs.AddedValueFromParameter.title"),
        i18n.t("blurbs.AddedValueFromParameter.description"),
    )
    InstanceTypeMapping = (
        i18n.t("blurbs.InstanceTypeMapping.title"),
        i18n.t("blurbs.InstanceTypeMapping.description"),
    )
    StaffOnlyViaIndicator = (
        i18n.t("blurbs.StaffOnlyViaIndicator.title"),
        i18n.t("blurbs.StaffOnlyViaIndicator.description"),
    )
    PossibleCleaningTasks = (
        i18n.t("blurbs.PossibleCleaningTasks.title"),
        i18n.t("blurbs.PossibleCleaningTasks.description"),
    )
    HoldingsStatementsParsing = (
        i18n.t("blurbs.HoldingsStatementsParsing.title"),
        i18n.t("blurbs.HoldingsStatementsParsing.description"),
    )
    MaterialTypeMapping = (
        i18n.t("blurbs.MaterialTypeMapping.title"),
        i18n.t("blurbs.MaterialTypeMapping.description"),
    )
    AuthoritySourceFileMapping = (
        i18n.t("blurbs.AuthoritySourceFileMapping.title"),
        i18n.t("blurbs.AuthoritySourceFileMapping.description"),
    )
    AuthorityEncodingLevel = (
        i18n.t("blurbs.AuthorityEncodingLevel.title"),
        i18n.t("blurbs.AuthorityEncodingLevel.description"),
    )
