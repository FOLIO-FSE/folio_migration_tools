import uuid
from pathlib import Path
from unittest.mock import Mock

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.migration_tasks.organization_transformer import (
    OrganizationMapper,
)
from folio_migration_tools.migration_tasks.organization_transformer import (
    OrganizationTransformer,
)


def test_get_object_type():
    assert OrganizationTransformer.get_object_type() == FOLIONamespaces.organizations


def test_subclass_inheritance():
    assert issubclass(OrganizationTransformer, MigrationTaskBase)

# Organizations -- Post-transformation cleanup
def test_remove_organization_types_pre_morning_glory():
    rec = {
        "id": "c15aabf7-8a4a-5a6c-8c44-2a51f17db6a9",
        "name": "Academic International Press",
        "organizationTypes": ["fc54327d-fd60-4f6a-ba37-a4375511b91b"],
    }

    clean_org_lotus = OrganizationTransformer.clean_org_type_pre_morning_glory(
        OrganizationTransformer, rec, "lotus"
    )
    assert clean_org_lotus == {
        "id": "c15aabf7-8a4a-5a6c-8c44-2a51f17db6a9",
        "name": "Academic International Press",
    }

    rec = {
        "id": "c15aabf7-8a4a-5a6c-8c44-2a51f17db6a9",
        "name": "Academic International Press",
        "organizationTypes": ["fc54327d-fd60-4f6a-ba37-a4375511b91b"],
    }

    clean_org_morning_glory = OrganizationTransformer.clean_org_type_pre_morning_glory(
        OrganizationTransformer, rec, "morning_glory"
    )
    assert clean_org_morning_glory == {
        "id": "c15aabf7-8a4a-5a6c-8c44-2a51f17db6a9",
        "name": "Academic International Press",
        "organizationTypes": ["fc54327d-fd60-4f6a-ba37-a4375511b91b"],
    }


def test_clean_up_one_address():
    rec = {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "",
                "zipCode": "PO Box 1111",
                "isPrimary": True,
            }
        ]
    }

    clean_address = OrganizationTransformer.clean_addresses(OrganizationTransformer, rec)

    assert clean_address == {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "",
                "zipCode": "PO Box 1111",
                "isPrimary": True,
            }
        ]
    }


def test_clean_up_two_addresses_no_primary():
    rec = {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "BC",
                "zipCode": "",
                "isPrimary": False,
            },
            {
                "addressLine1": "Vita Villan",
                "city": "Horred",
                "stateRegion": "BC",
                "zipCode": "20",
                "isPrimary": False,
            },
        ]
    }

    clean_address = OrganizationTransformer.clean_addresses(OrganizationTransformer, rec)

    assert clean_address == {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "BC",
                "zipCode": "",
                "isPrimary": True,
            },
            {
                "addressLine1": "Vita Villan",
                "city": "Horred",
                "stateRegion": "BC",
                "zipCode": "20",
                "isPrimary": False,
            },
        ]
    }


def test_clean_up_two_addresses_both_primary():
    """
    Having two primary addresses will be weird in FOLIO. We should be able to
    avoid it by only every mapping one address type as Primary.
    """
    rec = {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "BC",
                "zipCode": "",
                "isPrimary": True,
            },
            {
                "addressLine1": "Vita Villan",
                "city": "Horred",
                "stateRegion": "BC",
                "zipCode": "20",
                "isPrimary": True,
            },
        ]
    }

    clean_address = OrganizationTransformer.clean_addresses(OrganizationTransformer, rec)

    assert clean_address == {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "BC",
                "zipCode": "",
                "isPrimary": True,
            },
            {
                "addressLine1": "Vita Villan",
                "city": "Horred",
                "stateRegion": "BC",
                "zipCode": "20",
                "isPrimary": True,
            },
        ]
    }


def test_clean_up_two_addresses_one_empty():
    rec = {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "BC",
                "zipCode": "",
                "isPrimary": True,
            },
            {"addressLine1": "", "city": "", "stateRegion": "", "zipCode": "", "isPrimary": False},
        ]
    }

    clean_address = OrganizationTransformer.clean_addresses(OrganizationTransformer, rec)

    assert clean_address == {
        "addresses": [
            {
                "addressLine1": "Suite 500 - 655 Typee Rd",
                "city": "Victoria",
                "stateRegion": "BC",
                "zipCode": "",
                "isPrimary": True,
            }
        ]
    }


def test_clean_up_two_addresses_both_empty():
    rec = {
        "addresses": [
            {"addressLine1": "", "city": "", "stateRegion": "", "zipCode": "", "isPrimary": True},
            {"addressLine1": "", "city": "", "stateRegion": "", "zipCode": "", "isPrimary": ""},
        ]
    }

    clean_address = OrganizationTransformer.clean_addresses(OrganizationTransformer, rec)

    assert clean_address == {"addresses": []}

    
# Check that embedded objects are removed
def test_handle_embedded_extradata_objects():
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.embedded_extradata_object_cache = set()
    mocked_organization_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_organization_transformer.extradata_writer.cache = []
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)

    organization = {
        "name": "FOLIO",
        "interfaces": [
            {"name": "FOLIO", "uri": "https://www.folio.org"},
            {"name": "Community wiki", "uri": "https://www.wiki.folio.org"},
        ],
        "contacts": [
            {
                "firstName": "Jane",
                "lastName": "Deer",
                "emailAddresses": [{"value": "me(at)me.com"}],
            },
            {
                "firstName": "John",
                "lastName": "Doe",
                "addresses": [{"addressLine1": "MyStreet"}, {"city": "Bogotá"}],
                "emailAddresses": [{"value": "andme(at)me.com"}],
            },
            {
                "firstName": "Jane",
                "lastName": "Deer",
                "emailAddresses": [{"value": "me(at)me.com"}],
            },
        ],
    }

    OrganizationTransformer.handle_embedded_extradata_objects(
        mocked_organization_transformer, organization
    )

    assert organization["interfaces"] == []
    assert organization["contacts"] == []

# Test extradata creation
def test_create_linked_extradata_objects():
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.embedded_extradata_object_cache = set()
    mocked_organization_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_organization_transformer.extradata_writer.cache = []
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)

    organization = {"name": "FOLIO", "interfaces": [], "contacts": []}

    interfaces = [
        {"name": "FOLIO", "uri": "https://www.folio.org"},
        {"name": "Community wiki", "uri": "https://www.wiki.folio.org"},
    ]

    contacts = [
        {
            "firstName": "Jane",
            "lastName": "Deer",
            "emailAddresses": [{"value": "me(at)me.com"}],
        },
        {
            "firstName": "John",
            "lastName": "Doe",
            "addresses": [{"addressLine1": "MyStreet"}, {"city": "Bogotá"}],
            "emailAddresses": [{"value": "andme(at)me.com"}],
        },
        {
            "firstName": "Jane",
            "lastName": "Deer",
            "emailAddresses": [{"value": "me(at)me.com"}],
        },
    ]

    for interface in interfaces:
        OrganizationTransformer.create_linked_extradata_objects(
            mocked_organization_transformer, organization, interface, "interfaces"
        )

    for contact in contacts:
        OrganizationTransformer.create_linked_extradata_objects(
            mocked_organization_transformer, organization, contact, "contacts"
        )

    # Check that UUIDs have been added to the organization record
    assert all(uuid.UUID(str(value), version=4) for value in organization["interfaces"])
    assert all(uuid.UUID(str(value), version=4) for value in organization["contacts"])

    # Check that all the assigned UUIDs are in the extradata writer cache
    assert all(
        str(id) in str(mocked_organization_transformer.extradata_writer.cache)
        for id in organization["interfaces"]
    )
    assert all(
        str(id) in str(mocked_organization_transformer.extradata_writer.cache)
        for id in organization["contacts"]
    )

    # Check that there are contacts in the extradata writer
    assert "contacts" in str(mocked_organization_transformer.extradata_writer.cache)
    # Check that there are contacts in the extradata writer
    assert "interfaces" in str(mocked_organization_transformer.extradata_writer.cache)

    # Check that reoccuring contacts are NOT deduplicated
    assert str(mocked_organization_transformer.extradata_writer.cache).count("Jane") == 2
    assert str(mocked_organization_transformer.extradata_writer.cache).count("www") == 2


def test_contact_formatting_and_content():
    # Check that contacts in the extradata writer contain the right information
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.embedded_extradata_object_cache = set()
    mocked_organization_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_organization_transformer.extradata_writer.cache = []
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    organization = {
        "name": "YourCompany",
        "contacts": [
            {
                "firstName": "June",
                "lastName": "Day",
                "addresses": [{"addressLine1": "MyStreet"}, {"city": "Stockholm"}],
                "phoneNumbers": [{"phoneNumber": "123"}],
                "emailAddresses": [{"value": "andme(at)me.com"}],
            }
        ],
    }

    OrganizationTransformer.create_linked_extradata_objects(
        mocked_organization_transformer, organization, organization["contacts"][0], "contacts"
    )

    assert (
        'contacts\\t{"firstName": "June", "lastName": "Day", '
        '"addresses": [{"addressLine1": "MyStreet"}, {"city": "Stockholm"}], '
        '"phoneNumbers": [{"phoneNumber": "123"}], '
        '"emailAddresses": [{"value": "andme(at)me.com"}]'
        in str(mocked_organization_transformer.extradata_writer.cache)
    )
