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
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
        multi_field_delimiter="^-^",
    )

    address_categories_map = [
        {"address_categories": "rt", "folio_value": "Returns"},
        {"address_categories": "*", "folio_value": "General"},
    ]

    email_categories_map = [
        {"email_categories": "tspt", "folio_value": "Technical Support"},
        {"email_categories": "*", "folio_value": "General"},
    ]

    phone_categories_map = [
        {"phone_categories": "mspt", "folio_value": "Moral Support"},
        {"phone_categories": "*", "folio_value": "General"},
    ]

    organization_types_map = [
        {"organization_types": "cst", "folio_name": "Consortium"},
        {"organization_types": "*", "folio_name": "General"},
    ]

    return OrganizationMapper(
        mock_folio_client,
        lib_config,
        basic_organization_map,
        organization_types_map,
        address_categories_map,
        email_categories_map,
        phone_categories_map,
    )


def test_basic_mapping(mapper, caplog):
    # caplog.set_level(25)
    data = {
        "vendor_code": "AbeBooks",
        "ACCTNUM": "aha112233",
        "VENNAME": "Abe Books",
        "EMAIL": "email1@abebooks.com",
        "email_categories": "aspt",
        "EMAIL2": "email2@abebooks.com",
        "PHONE NUM": "123-456",
        "phone_categories": "mspt",
        "Alt name type": "Nickname",
        "Alternative Names": "Abby",
        "status": "Active",
        "org_note": "Good stuff!",
        "address_line_1": "Suite 500 - 655 Typee Rd",
        "address_city": "Victoria",
        "address_categories": "rt^-^ad",
        "tp": "Consortium",
        "tgs": "A, B, C",
        "organization_types": "cst^-^aah",
    }

    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    # TODO Add tests for extradata
    # mapper.perform_additional_mappings(res)
    # mapper.store_objects(res)
    # assert "Level 25" in caplog.text
    # assert "organization\t" in caplog.text
    # generated_objects = {}
    # for m in caplog.messages:
    #     s = m.split("\t")
    #     generated_objects[s[0]] = json.loads(s[1])
    # organization = generated_objects["organization"]

    # Test basic values
    assert organization["name"] == "Abe Books"

    # Test reference data mappings
    assert organization["organizationTypes"] == ["76b06c55-d95a-4ae0-a47d-5708f4e5e166"]

    # Test arrays of contact information
    assert organization["addresses"][0]["categories"] == ["76b06c55-d95a-4ae0-a47d-5708f4e5e166"]

    assert organization["phoneNumbers"][0]["categories"] == [
        "87042758-5266-472a-a3e9-ea1ca0ccf056"
    ]

    # TODO Sort out the below index-specific tests which are currently failing because items are
    # added to the emails array in random order.
    # assert organization["emails"][0]["value"] == "email1@abebooks.com"
    # assert organization["emails"][0]["isPrimary"]
    # assert organization["emails"][0]["categories"] == ["93042758-5266-472a-a3e9-ea1ca0ccf056"]

    # assert organization["emails"][1]["value"] == "email2@abebooks.com"
    # assert not organization["emails"][1]["isPrimary"]
    # assert organization["emails"][1]["categories"] == [""]


def test_parse_record_mapping_file(mapper):
    folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
        basic_organization_map
    )
    
    assert folio_keys

# A mocked mapping file
basic_organization_map = {
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
            "folio_field": "emails[0].categories[0]",
            "legacy_field": "email_categories",
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
            "legacy_field": "Not mapped",
            "value": "",
            "description": "If we have multiple email addresses, how can that be mapped?",
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
