import json
import logging

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.organization_mapper import (
    OrganizationMapper,
)
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


# Test inheritance and schema


def test_subclass_inheritance():
    assert issubclass(OrganizationMapper, MappingFileMapperBase)


def test_fetch_acq_schemas_from_github_happy_path():
    organization_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-organizations-storage", "mod-orgs", "organization"
    )

    assert organization_schema["$schema"]


# Mock mapper object


@pytest.fixture(scope="module")
def mapper(pytestconfig) -> OrganizationMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    print("init")
    mock_folio_client = mocked_classes.mocked_folio_client()

    lib_config = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.lotus,
        library_name="Organization tester Library",
        log_level_debug=False,
        iteration_identifier="Test!",
        base_folder="/",
        multi_field_delimiter="^-^",
    )

    address_categories_map = [
        {"address_categories": "rt", "folio_value": "Returns"},
        {"address_categories": "*", "folio_value": "General"},
    ]

    email_categories_map = [
        {
            "email1_categories": "tspt",
            "email2_categories": "*",
            "folio_value": "Technical Support",
        },
        {"email1_categories": "sls", "email2_categories": "*", "folio_value": "Sales"},
        {
            "email1_categories": "*",
            "email2_categories": "tspt",
            "folio_value": "Technical Support",
        },
        {"email1_categories": "*", "email2_categories": "sls", "folio_value": "Sales"},
        {"email1_categories": "*", "email_2categories": "*", "folio_value": "General"},
    ]

    phone_categories_map = [
        {"phone_categories": "mspt", "folio_value": "Moral Support"},
        {"phone_categories": "*", "folio_value": "General"},
    ]

    organization_types_map = [
        {"organization_types": "cst", "folio_name": "Consortium"},
        {"organization_types": "*", "folio_name": "Unspecified"},
    ]

    return OrganizationMapper(
        mock_folio_client,
        lib_config,
        organization_map,
        organization_types_map,
        address_categories_map,
        email_categories_map,
        phone_categories_map,
    )


def test_parse_record_mapping_file(mapper):
    folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(organization_map)

    assert folio_keys


def test_organization_mapping(mapper):

    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    # Test string values mapping
    assert organization["name"] == "Abe Books"
    assert organization["code"] == "AbeBooks"
    assert organization["description"] == "Good stuff!"
    assert organization["status"] == "Active"
    assert organization["accounts"][0]["accountNo"] == "aha112233"


def test_single_org_type_refdata_mapping(mapper):

    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    # Test reference data mapping
    assert organization["organizationTypes"] == ["837d04b6-d81c-4c49-9efd-2f62515999b3"]


def test_single_category_refdata_mapping(mapper):

    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    # Test arrays of contact information
    assert organization["addresses"][0]["categories"] == ["c78640d5-a1ec-4721-9a1f-c6f876d4c179"]

    assert organization["phoneNumbers"][0]["categories"] == [
        "e193b0d1-4674-4a9e-818b-375f013d963f"
    ]


@pytest.mark.skip(
    reason="We would need a way of using the same ref data file for multiple values. See #411"
)
def test_multiple_emails_array_objects(mapper):
    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    correct_email_objects = 0

    for email in organization["emails"]:
        # One of the email addresses
        if (
            email["value"] == "email1@abebooks.com"
            and email["categories"][0] == "604c2c9d-ed3a-46cd-bec4-69926c303b22"
            and email["isPrimary"]
        ):
            correct_email_objects += 1

        # One of the email addresses
        if (
            email["value"] == "email2@abebooks.com"
            and email["categories"][0] == "97dcb23df-1aba-444e-b88d-804d17c715a5"
            and not email["isPrimary"]
        ):
            correct_email_objects += 1

    assert correct_email_objects == 2


@pytest.mark.skip(reason="Extra data has not been implemented in the mapper yet.")
def test_extra_data(mapper, caplog):
    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    # TODO Add tests for extradata. It should


# Shared data and maps
data = {
    "vendor_code": "AbeBooks",
    "ACCTNUM": "aha112233",
    "VENNAME": "Abe Books",
    "EMAIL": "email1@abebooks.com",
    "email1_categories": "sls",
    "EMAIL2": "email2@abebooks.com",
    "email2_categories": "tspt",
    "PHONE NUM": "123-456",
    "phone_categories": "mspt",
    "Alt name type": "Nickname",
    "Alternative Names": "Abby",
    "status": "Active",
    "address_line_1": "Suite 500 - 655 Typee Rd",
    "address_city": "Victoria",
    "address_categories": "rt",
    "tp": "Consortium",
    "tgs": "A, B, C",
    "organization_types": "cst",
    "org_note": "Good stuff!",
}


# A mocked mapping file
organization_map = {
    "data": [
        {
            "folio_field": "accounts[0].accountNo",
            "legacy_field": "ACCTNUM",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "addresses[0].addressLine1",
            "legacy_field": "address_line_1",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "addresses[0].categories[0]",
            "legacy_field": "address_categories",
            "value": "",
            "description": "Use ref data mapping",
        },
        {
            "folio_field": "addresses[0].city",
            "legacy_field": "address_city",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "addresses[0].isPrimary",
            "legacy_field": "",
            "value": True,
            "description": "",
        },
        {
            "folio_field": "addresses[0].stateRegion",
            "legacy_field": "address_state",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "addresses[0].zipCode",
            "legacy_field": "address_zip",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "aliases[0].description",
            "legacy_field": "Alt name type",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "aliases[0].value",
            "legacy_field": "Alternative Names",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "description",
            "legacy_field": "org_note",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "emails[0].categories[0]",
            "legacy_field": "email1_categories",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "emails[0].isPrimary",
            "legacy_field": "Not mapped",
            "value": True,
            "description": "",
        },
        {
            "folio_field": "emails[0].value",
            "legacy_field": "EMAIL",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "emails[1].isPrimary",
            "legacy_field": "Not mapped",
            "value": False,
            "description": "",
        },
        {
            "folio_field": "emails[1].value",
            "legacy_field": "EMAIL2",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "emails[1].categories[0]",
            "legacy_field": "email2_categories",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "isVendor",
            "legacy_field": "Not mapped",
            "value": True,
            "description": "",
        },
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "vendor_code",
            "value": "",
            "description": "",
        },
        {"folio_field": "name", "legacy_field": "VENNAME", "value": "", "description": ""},
        {"folio_field": "code", "legacy_field": "vendor_code", "value": "", "description": ""},
        {
            "folio_field": "phoneNumbers[0].categories[0]",
            "legacy_field": "phone_categories_map",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "phoneNumbers[0].isPrimary",
            "legacy_field": "Not mapped",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "phoneNumbers[0].phoneNumber",
            "legacy_field": "PHONE NUM",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "phoneNumbers[0].type",
            "legacy_field": "Not mapped",
            "value": "Mobile",
            "description": "",
        },
        {"folio_field": "status", "legacy_field": "status", "value": "", "description": ""},
        {"folio_field": "tags.tagList[0]", "legacy_field": "tgs", "value": "", "description": ""},
        {
            "folio_field": "organizationTypes",
            "legacy_field": "organization_types",
            "value": "",
            "description": "",
        },
    ]
}
