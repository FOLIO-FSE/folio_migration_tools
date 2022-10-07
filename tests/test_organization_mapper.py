import json
import logging

import pytest

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.organization_mapper import (
    OrganizationMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
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
    mock_folio = mocked_classes.mocked_folio_client()

    lib = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.lotus,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
        multi_field_delimiter="^-^"
    )
    
    categories_map = [
        {"category": "spt", "folio_name": "Support"},
        {"category": "*", "folio_name": "General"},
    ]
    return OrganizationMapper(mock_folio, basic_organization_map, categories_map, lib)


def test_basic_mapping(mapper, caplog):
    # caplog.set_level(25)
    data = {
        "vendor_code": "AbeBooks",
        "ACCTNUM": "aha112233",
        "VENNAME": "Abe Books",
        "EMAIL": "buyertech@abebooks.com",
        "catagory": "spt",
        "PHONE NUM": "123-456",
        "Alt name type": "Nickname",
        "Alternative Names": "Abby",
        "status": "Active",
        "org_note": "Good stuff!",
        "address_line_1": "Suite 500 - 655 Typee Rd",
        "address_city": "Victoria"
    }

    res = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    organization = res[0]

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

    assert organization["name"] == "Abe Books"
    assert organization["emails"][0]["value"] == "buyertech@abebooks.com"
    assert organization["emails"][0]["isPrimary"] == True
    assert organization["addresses"][0]["categories"] == ["returns"]
    assert organization["emails"][0]["categories"] == ["spt"]






basic_organization_map = {
    "data": [
    {
        "folio_field": "accounts[0].accountNo",
        "legacy_field": "ACCTNUM",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "addresses[0].addressLine1",
        "legacy_field": "address_line_1",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "addresses[0].categories[0]",
        "legacy_field": "",
        "value": "returns",
        "description": ""
    },
    {
        "folio_field": "addresses[0].city",
        "legacy_field": "address_city",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "addresses[0].isPrimary",
        "legacy_field": "",
        "value": True,
        "description": ""
    },
    {
        "folio_field": "addresses[0].stateRegion",
        "legacy_field": "address_state",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "addresses[0].zipCode",
        "legacy_field": "address_zip",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "aliases[0].description",
        "legacy_field": "Alt name type",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "aliases[0].value",
        "legacy_field": "Alternative Names",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "emails[0].categories[0]",
        "legacy_field": "category",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "emails[0].isPrimary",
        "legacy_field": "Not mapped",
        "value": True,
        "description": ""
    },
    {
        "folio_field": "emails[0].value",
        "legacy_field": "EMAIL",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "isVendor",
        "legacy_field": "Not mapped",
        "value": True,
        "description": ""
    },
    {
        "folio_field": "legacyIdentifier",
        "legacy_field": "vendor_code",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "name",
        "legacy_field": "VENNAME",
        "value": "",
        "description": ""
    },
        {
        "folio_field": "code",
        "legacy_field": "vendor_code",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "phoneNumbers[0].categories[0]",
        "legacy_field": "Not mapped",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "phoneNumbers[0].isPrimary",
        "legacy_field": "Not mapped",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "phoneNumbers[0].phoneNumber",
        "legacy_field": "PHONE NUM",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "phoneNumbers[0].type",
        "legacy_field": "Not mapped",
        "value": "Mobile",
        "description": ""
    },
    {
        "folio_field": "status",
        "legacy_field": "status",
        "value": "",
        "description": ""
    },
    {
        "folio_field": "tags.tagList[0]",
        "legacy_field": "Not mapped",
        "value": "",
        "description": ""
    }
    ]
}
