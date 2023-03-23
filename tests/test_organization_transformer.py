import io
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

    assert len(organization["interfaces"]) == 2
    assert "FOLIO" not in organization["interfaces"]
    assert len(organization["contacts"]) == 3
    assert "Jane" not in organization["contacts"]
    assert "interfaceCredentials" not in organization["interfaces"]


def test_create_linked_extradata_object_contacts():
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.embedded_extradata_object_cache = set()
    mocked_organization_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_organization_transformer.extradata_writer.cache = []
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    mocked_organization_transformer.legacy_id = "etxra_org1"

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

    linked_contacts = []
    for contact in contacts:
        linked_contact = OrganizationTransformer.create_referenced_extradata_object(
            mocked_organization_transformer, contact, "contacts"
        )
        linked_contacts.append(linked_contact)

    # Check that UUIDs have been added to the organization record
    assert all(uuid.UUID(str(value), version=4) for value in linked_contacts)

    # Check that all the assigned UUIDs are in the extradata writer cache
    assert all(
        str(id) in str(mocked_organization_transformer.extradata_writer.cache)
        for id in linked_contacts
    )

    # Check that there are contacts in the extradata writer
    assert "contacts" in str(mocked_organization_transformer.extradata_writer.cache)


def test_create_linked_extradata_object_interfaces():
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.embedded_extradata_object_cache = set()
    mocked_organization_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_organization_transformer.extradata_writer.cache = []
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    mocked_organization_transformer.legacy_id = "etxra_org1"

    interfaces = [
        {
            "name": "FOLIO",
            "uri": "https://www.folio.org",
            "interfaceCredential": {
                "username": "folioUsername",
                "password": "folioPassword",  # noqa: S105
                "interfaceId": "replace_with_interface_id",
            },
        },
        {
            "name": "Community wiki",
            "uri": "https://www.wiki.folio.org",
            "interfaceCredential": {
                "username": "wikiUsername",
                "password": "wikiPassword",  # noqa: S105
                "interfaceId": "replace_with_interface_id",
            },
        },
    ]

    linked_interfaces = []
    for interface in interfaces:
        linked_interface = OrganizationTransformer.create_referenced_extradata_object(
            mocked_organization_transformer, interface, "interfaces"
        )
        linked_interfaces.append(linked_interface)

    # Check that UUIDs have been added to the organization record
    assert all(uuid.UUID(str(value), version=4) for value in linked_interfaces)

    # Check that all the assigned UUIDs are in the extradata writer cache
    assert all(
        str(id) in str(mocked_organization_transformer.extradata_writer.cache)
        for id in linked_interfaces
    )

    # Check that there are contacts in the extradata writer
    assert "interfaces" in str(mocked_organization_transformer.extradata_writer.cache)

    # Check that reoccuring contacts are NOT deduplicated
    assert str(mocked_organization_transformer.extradata_writer.cache).count("www") == 2


def test_create_linked_extradata_object_credentials():
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.embedded_extradata_object_cache = set()
    mocked_organization_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_organization_transformer.extradata_writer.cache = []
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    mocked_organization_transformer.legacy_id = "etxra_org1"

    organization = {
        "name": "OLF",
        "interfaces": [
            {
                "name": "Community wiki",
                "uri": "https://www.wiki.folio.org",
                "interfaceCredential": {
                    "username": "wikiUsername",
                    "password": "wikiPassword",  # noqa: S105
                    "interfaceId": "replace_with_interface_id",
                },
            },
            {
                "name": "FOLIO",
                "uri": "https://www.folio.org",
                "interfaceCredential": {
                    "username": "folioUsername",
                    "password": "folioPassword",  # noqa: S105
                    "interfaceId": "replace_with_interface_id",
                },
            },
            {
                "name": "google",
                "uri": "https://www.google.com",
            },
        ],
    }

    credential_interface_ids = []
    credentials = []
    interfaces = []

    for interface in organization["interfaces"]:
        interface_credential = interface.pop("interfaceCredential", None)
        interface_id = OrganizationTransformer.create_referenced_extradata_object(
            mocked_organization_transformer, interface, "interfaces"
        )

        if interface_credential and "username" in interface_credential:
            credential_interface_ids.append(interface_id)
            interface_credential["interfaceId"] = interface_id

            credential = OrganizationTransformer.create_referenced_extradata_object(
                mocked_organization_transformer,
                interface_credential,
                "interfaceCredential",
            )

            credentials.append(credential)
        interfaces.append(interface)

    assert all(
        f'"interfaceId": "{interface_id}"'
        in str(mocked_organization_transformer.extradata_writer.cache)
        for interface_id in credential_interface_ids
    )

    assert len(credentials) == 2

    assert "interfaceCredentials" not in (interface for interface in interfaces)


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

    OrganizationTransformer.create_referenced_extradata_object(
        mocked_organization_transformer, organization["contacts"][0], "contacts"
    )

    assert (
        'contacts\\t{"firstName": "June", "lastName": "Day", '
        '"addresses": [{"addressLine1": "MyStreet"}, {"city": "Stockholm"}], '
        '"phoneNumbers": [{"phoneNumber": "123"}], '
        '"emailAddresses": [{"value": "andme(at)me.com"}]'
        in str(mocked_organization_transformer.extradata_writer.cache)
    )


def test_validate_uri():
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)

    record = {
        "name": "FOLIO",
        "code": "FOLIO",
        "status": "Active",
        "interfaces": [
            {"name": "FOLIO", "uri": "https://www.folio.org"},
            {"name": "Community wiki", "uri": "ww.wiki.folio.org"},
        ],
    }
    record = OrganizationTransformer.validate_uri(mocked_organization_transformer, record)

    assert len(record["interfaces"]) == 1


def test_contact_remove_incomplete_object():
    mocked_organization_transformer = Mock(spec=OrganizationTransformer)
    mocked_organization_transformer.embedded_extradata_object_cache = set()
    mocked_organization_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_organization_transformer.extradata_writer.cache = []
    mocked_organization_transformer.mapper = Mock(spec=OrganizationMapper)
    mocked_organization_transformer.mapper.migration_report = Mock(spec=MigrationReport)

    organization = {
        "name": "YourCompany",
        "code": "YC",
        "status": "Active",
        "contacts": [
            {
                "firstName": "June",
                "lastName": "Day",
                "addresses": [{"addressLine1": "MyStreet"}, {"city": "Stockholm"}],
                "phoneNumbers": [{"phoneNumber": "123"}],
                "emailAddresses": [{"value": "andme(at)me.com"}],
            },
            {
                "firstName": "Joe",
                "addresses": [{"addressLine1": "MyStreet"}, {"city": "Stockholm"}],
                "phoneNumbers": [{"phoneNumber": "123"}],
                "emailAddresses": [{"value": "andme(at)me.com"}],
            },
        ],
        "interfaces": [],
    }

    organization = OrganizationTransformer.handle_embedded_extradata_objects(
        mocked_organization_transformer, organization
    )

    assert len(organization["contacts"]) == 1
