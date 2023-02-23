import logging

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
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


def test_fetch_org_schemas_from_github_happy_path():
    organization_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-organizations-storage", "mod-orgs", "organization"
    )

    assert organization_schema["$schema"]

# Build contact schema from github
def test_fetch_contact_schemas_from_github_happy_path():
    contact_schema = OrganizationMapper.fetch_additional_schema("contact")
    assert contact_schema["$schema"]


# Build interface schema from github
def test_fetch_interfaces_schemas_from_github_happy_path():
    contact_schema = OrganizationMapper.fetch_additional_schema("interface")
    assert contact_schema["$schema"]


# Mock mapper object
@pytest.fixture(scope="session", autouse=True)
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
        folio_release=FolioRelease.morning_glory,
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
    data["code"] = "o1"

    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

    # Test string values mapping
    assert organization["name"] == "Abe Books"
    assert organization["code"] == "o1"
    assert organization["description"] == "Good stuff!"
    assert organization["status"] == "Active"


def test_single_org_type_refdata_mapping(mapper):
    data["vendor_code"] = "vr2"
    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    # Test reference data mapping
    assert organization["organizationTypes"] == ["837d04b6-d81c-4c49-9efd-2f62515999b3"]


def test_single_category_refdata_mapping(mapper):
    data["vendor_code"] = "vr3"
    organization, idx = mapper.do_map(data, data["vendor_code"], FOLIONamespaces.organizations)

    # Test arrays of contact information
    assert organization["addresses"][0]["categories"] == ["c78640d5-a1ec-4721-9a1f-c6f876d4c179"]

    assert organization["phoneNumbers"][0]["categories"] == [
        "e193b0d1-4674-4a9e-818b-375f013d963f"
    ]


def test_tags_object_array(mapper):
    data["code"] = "o4"
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

    assert organization["tags"] == {"tagList": ["A", "B", "C"]}

    assert organization["contacts"][0]["addresses"][0]["addressLine1"] == "My Street"


def test_enforce_schema_required_properties_in_organization(mapper):
    data["EMAIL2"] = ""
    data["PHONE NUM"] = ""
    data["code"] = "o4b"

    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

    # There should only be one email, as the other one is empty
    assert len(organization["emails"]) == 1

    # There should be no phone numbers, as the data is empty
    assert not organization.get("phoneNumbers")

@pytest.mark.skip(
    reason="We would need a way of using the same ref data file for multiple values. See #411"
)
def test_multiple_emails_array_objects(mapper):
    data["code"] = "o5"
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

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


# Test "contacts" array


def test_contacts_basic_mapping(mapper):
    data["code"] = "co6"
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

    assert organization["contacts"][0]["firstName"] == "Jane"
    assert organization["contacts"][0]["lastName"] == "Deer"


def test_contacts_address_mapping(mapper):
    data["code"] = "co7"
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)
    assert organization["contacts"][0]["firstName"] == "Jane"


def test_contacts_required_properties(mapper):
    data["code"] = "co7"
    data["contact_person_f"] = ""

    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

    assert "contacts" not in organization


# Test "interfaces" array
def test_interfaces_basic_mapping(mapper):
    data["code"] = "o8"
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

    assert organization["interfaces"][0]["name"] == "FOLIO"
    assert organization["interfaces"][0]["uri"] == "https://www.folio.org"


def test_interfaces_type_enum_invalid(mapper):
    data["code"] = "io2"
    data["interface_1_type"] = "Whaaaat?"
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)
    assert len(organization["interfaces"][0]["type"]) == 1

def test_interfaces_type_enum_empty(mapper):
    data["code"] = "io3"
    data["interface_1_type"] = ""
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)
    assert len(organization["interfaces"][0]["type"]) == 0


def test_invalid_non_required_enum_in_sub_object_mapping(mapper):
    records = [
        data
        | {
            "name": "Vendor With Account 1",  # String, required
            "code": "eo1",  # String, required
            "status": "Active",  # Enum, required
            "account_number": "ac1",  # String, required for Account
            "account_name": "MyAccount",  # String, required for Account
            "account_status": "Active",  # String, required for Account
            "account_paymentMethod": "Cash",  # Enum
        },
        data
        | {
            "name": "Vendor With Account 2",  # String, required
            "code": "eo2",  # String, required
            "status": "Active",  # Enum, required
            "account_number": "ac2",  # String, required for Account
            "account_name": "MyAccount",  # String, required for Account
            "account_status": "Active",  # String, required for Account
            "account_paymentMethod": "Invalid Value",  # Enum,
        },
    ]

    organization, idx = mapper.do_map(
        records[0], records[0]["code"], FOLIONamespaces.organizations
    )
    assert organization["accounts"][0]["name"]

    with pytest.raises(TransformationRecordFailedError):
        organization, idx = mapper.do_map(
            records[1], records[1]["code"], FOLIONamespaces.organizations
        )


def test_empty_non_required_enum_in_sub_object_mapping(mapper):
    records = [
        data
        | {
            "name": "Vendor With Account 3",  # String, required
            "code": "eo3",  # String, required
            "status": "Active",  # Enum, required
            "account_number": "ac3",  # String, required for Account
            "account_name": "MyAccount",  # String, required for Account
            "account_status": "Active",  # String, required for Account
            "account_paymentMethod": "",  # Enum
        }
    ]

    organization, idx = mapper.do_map(
        records[0], records[0]["code"], FOLIONamespaces.organizations
    )
    assert "name" in organization["accounts"][0]
    assert "paymentMethod" not in organization["accounts"][0]

@pytest.mark.skip(
    reason="Under implementation."
)
def test_interface_credentials(mapper):
    data["code"] = "ic1"
    data["interface_1_username"] = "myUsername"
    data["interface_1_password"] = "myPassword"  # noqa: S105
    organization, idx = mapper.do_map(data, data["code"], FOLIONamespaces.organizations)

    assert organization["interfaces"][0]["interfaceCredential"]["username"] == "myUsername"


# Shared data and maps
data = {
    "name": "Abe Books",  # String
    "code": "AbeBooks",  # String
    "status": "Active",  # Enum, required,
    "account_number": "mac",  # String, required for Account
    "account_name": "MyAccount",  # String, required for Account
    "account_status": "Active",  # String, required for Account
    "account_paymentMethod": "Cash",  # Enum
    "EMAIL": "EMAIL",  # String
    "email1_categories": "sls",  # -> UUID of ref data
    "EMAIL2": "email2@abebooks.com",  # String
    "email2_categories": "tspt",  # -> UUID of ref data
    "PHONE NUM": "123-456",  # String
    "phone_categories": "mspt",  # -> UUID of ref data
    "Alt name type": "Nickname",  # String
    "Alternative Names": "Abby",  # String
    "address_line_1": "Suite 500 - 655 Typee Rd",  # String
    "address_city": "Victoria",  # String
    "address_categories": "rt",
    "tp": "Consortium",
    "tgs": "A^-^B^-^C",  # String (must match tags in tenant))
    "organization_types": "cst",  # -> UUID of ref data
    "org_note": "Good stuff!",  # String
    "contact_person_f": "Jane",  # String
    "contact_person_l": "Deer",  # String
    "contact_address_line1": "My Street",  # String
    "contact_address_town": "Gothenburg",  # String
    "interface_1_uri": "https://www.folio.org",  # String
    "interface_1_name": "FOLIO",  # String
    "interface_1_notes": "A good starting point for FOLIO info/links.",  # String
    "interface_1_delivery": "Online",  # Enum
    "interface_1_statFormat": "Interpretative dance",  # String
    "interface_1_statNotes": "May be performed anytime, anywhere.",  # String
    "interface_1_localLocation": "The shelf behind the houseplant",  # String
    "interface_1_onlineLocation": "How does this differ from URI?",  # String
    "interface_2_uri": "https://www.wiki.folio.org",
    "interface_2_type": "End user",
}

# A mocked mapping file
organization_map = {
    "data": [
        {"folio_field": "name", "legacy_field": "name", "value": "", "description": ""},
        {"folio_field": "code", "legacy_field": "code", "value": "", "description": ""},
        {"folio_field": "status", "legacy_field": "status", "value": "", "description": ""},
        {
            "folio_field": "accounts[0].accountNo",
            "legacy_field": "account_number",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "accounts[0].name",
            "legacy_field": "account_name",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "accounts[0].accountStatus",
            "legacy_field": "account_status",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "accounts[0].paymentMethod",
            "legacy_field": "account_paymentMethod",
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
            "folio_field": "contacts[0].firstName",
            "legacy_field": "contact_person_f",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "contacts[0].lastName",
            "legacy_field": "contact_person_l",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "contacts[0].addresses[0].addressLine1",
            "legacy_field": "contact_address_line1",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "contacts[0].addresses[0].city",
            "legacy_field": "contact_address_town",
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
            "folio_field": "emails[2].isPrimary",
            "legacy_field": "Not mapped",
            "value": False,
            "description": "",
        },
        {
            "folio_field": "emails[2].value",
            "legacy_field": "EMAIL3",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].name",
            "legacy_field": "interface_1_name",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].uri",
            "legacy_field": "interface_1_uri",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].type[0]",
            "legacy_field": "interface_1_type",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].notes",
            "legacy_field": "interface_1_notes",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].available",
            "legacy_field": "",
            "value": True,
            "description": "",
        },
        {
            "folio_field": "interfaces[0].interfaceCredential.username",
            "legacy_field": "interface_1_username",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].interfaceCredential.password",
            "legacy_field": "interface_1_password",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].deliveryMethod",
            "legacy_field": "interface_1_delivery",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].statisticsFormat",
            "legacy_field": "interface_1_statFormat",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].statisticsNotes",
            "legacy_field": "interface_1_statNotes",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].locallyStored",
            "legacy_field": "interface_1_localLocation",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[0].onlineLocation",
            "legacy_field": "interface_1_onlineLocation",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[1].uri",
            "legacy_field": "interface_2_uri",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "interfaces[1].type",
            "legacy_field": "interface_2_type",
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
            "legacy_field": "code",
            "value": "",
            "description": "",
        },
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
        {"folio_field": "tags.tagList[0]", "legacy_field": "tgs", "value": "", "description": ""},
        {
            "folio_field": "organizationTypes",
            "legacy_field": "organization_types",
            "value": "",
            "description": "",
        },
    ]
}
