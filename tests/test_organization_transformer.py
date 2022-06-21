from folio_migration_tools.migration_tasks.organization_transformer import OrganizationTransformer
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase


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

    clean_address = OrganizationTransformer.clean_org(rec)

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

    clean_address = OrganizationTransformer.clean_org(rec)

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

    clean_address = OrganizationTransformer.clean_org(rec)

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

    clean_address = OrganizationTransformer.clean_org(rec)

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

    clean_address = OrganizationTransformer.clean_org(rec)

    assert clean_address == {"addresses": []}
