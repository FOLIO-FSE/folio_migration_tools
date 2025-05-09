import json
import logging
import uuid
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import Mock

from folioclient import FolioClient

from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.library_configuration import (
    LibraryConfiguration,
    FolioRelease,
)


def mocked_holdings_mapper() -> Mock:
    mock_mapper = Mock(spec=HoldingsMapper)
    mock_mapper.migration_report = MigrationReport()
    mock_mapper.extradata_writer = ExtradataWriter(Path(""))

    return mock_mapper


def mocked_folio_client() -> FolioClient:
    try:
        FolioClient.login = MagicMock(name="login", return_value=None)
        FolioClient.okapi_token = "token"  # noqa:S105
        mocked_folio = FolioClient("okapi_url", "tenant_id", "username", "password")
        mocked_folio.folio_get_single_object = folio_get_single_object_mocked
        mocked_folio.folio_get_all = folio_get_all_mocked
        mocked_folio.get_from_github = folio_get_from_github
        mocked_folio.current_user = str(uuid.uuid4())
        return mocked_folio
    except Exception as ee:
        logging.error(ee)
        raise ee


def folio_get_all_mocked(ref_data_path, array_name, query="", limit=10):
    with open("./static/reference_data.json", "r") as super_schema_file:
        super_schema = json.load(super_schema_file)
    if ref_data_path == "/coursereserves/terms":
        yield from [
            {"name": "Fall 2022", "id": "42093be3-d1e7-4bb6-b2b9-18e153d109b2"},
            {"name": "Summer 2022", "id": "415b14a8-c94c-4aa1-a0a8-d397efae343e"},
        ]
    elif ref_data_path == "/coursereserves/departments":
        yield from [
            {
                "id": "7532e5ab-9812-496c-ab77-4fbb6a7e5dbf",
                "name": "Department_t",
                "description": "Art & Art History",
            },
            {
                "id": "af7ae6be-c0b2-444d-b76f-4061098d17cd",
                "name": "Department_FALLBACK",
                "description": "FALLBACK",
            },
        ]
    elif ref_data_path == "/organizations-storage/categories":
        yield from [
            {"id": "c78640d5-a1ec-4721-9a1f-c6f876d4c179", "value": "Returns"},
            {"id": "604c2c9d-ed3a-46cd-bec4-69926c303b22", "value": "Sales"},
            {"id": "c5b175bd-34a0-4a4d-9bd9-8eddae8e67f8", "value": "General"},
            {"id": "97dcb23df-1aba-444e-b88d-804d17c715a5", "value": "Technical Support"},
            {"id": "e193b0d1-4674-4a9e-818b-375f013d963f", "value": "Moral Support"},
        ]

    elif ref_data_path == "/organizations-storage/organization-types":
        yield from [
            {"id": "837d04b6-d81c-4c49-9efd-2f62515999b3", "name": "Consortium"},
            {"id": "fc54327d-fd60-4f6a-ba37-a4375511b91b", "name": "Unspecified"},
        ]
    elif (
        ref_data_path == "/organizations-storage/organizations"
        and query == '?query=(code=="EBSCO")'
    ):
        yield from [{"id": "some id", "code": "some code", "name": "EBSCO Information Services"}]

    elif (
        ref_data_path == "/organizations-storage/organizations"
        and query == '?query=(code=="LisasAwesomeStartup")'
    ):
        yield from []

    elif ref_data_path == "/organizations-storage/organizations":
        yield from [
            {"id": "837d04b6-d81c-4c49-9efd-2f62515999b3", "code": "GOBI"},
            {"id": "fc54327d-fd60-4f6a-ba37-a4375511b91b", "code": "EBSCO"},
        ]

    elif ref_data_path == "/orders/acquisition-methods":
        yield from [
            {"id": "837d04b6-d81c-4c49-9efd-2f62515999b3", "value": "Purchase"},
            {"id": "fc54327d-fd60-4f6a-ba37-a4375511b91b", "value": "Theft"},
            {"id": "fc54327d-fd60-4f6a-ba37-a437551sarfs91b", "value": "Other"},
        ]

    elif ref_data_path == "/groups":
        yield from [
            {
                "group": "FOLIO fallback group name",
                "desc": "Mocked response",
                "id": "27ab99d3-0e17-41f0-a20a-99e05acc0e6f",
            },
            {
                "group": "FOLIO group name",
                "desc": "Mocked response",
                "id": "5fc96cbd-a860-42a7-8d2b-72af30206712",
            },
        ]
    elif ref_data_path == "/departments":
        yield from [
            {
                "id": "12a2ad12-951d-4124-9fb2-58c70f0b7f71",
                "name": "FOLIO user department name",
                "code": "fdp",
            },
            {
                "id": "2f452d21-507d-4b32-a89d-8ea9753cc946",
                "name": "FOLIO fallback user department name",
                "code": "fb",
            },
        ]
    elif ref_data_path == "/owners":
        yield from [
            {
                "owner": "The Best Fee Fine Owner",
                "desc": "She really is!",
                "servicePointOwner": [
                    {"value": "a77b55e7-f9f3-40a1-83e0-241bc606a826", "label": "lisatest"}
                ],
                "id": "5abfff3f-50eb-432a-9a43-21f8f7a70194",
            },
            {
                "owner": "The Other Fee Fine Owner",
                "desc": "heeey",
                "servicePointOwner": [
                    {"value": "1543c345-dcaf-4367-84a8-853d95837a3b", "label": "lisatest2 :) <3 "}
                ],
                "id": "62a0eb54-de96-46ee-b184-5be6c8114a19",
            },
        ]
    elif ref_data_path == "/feefines":
        yield from [
            {
                "automatic": False,
                "feeFineType": "Coffee spill",
                "ownerId": "5abfff3f-50eb-432a-9a43-21f8f7a70194",
                "id": "6e8dc178-f667-45cd-90b5-338c78c3a85c",
            },
            {
                "automatic": False,
                "feeFineType": "Coffee spill",
                "ownerId": "62a0eb54-de96-46ee-b184-5be6c8114a19",
                "id": "031836ec-521a-4493-9f76-0e02c2e7d241",
            },
            {
                "automatic": False,
                "feeFineType": "Replacement library card",
                "ownerId": "5abfff3f-50eb-432a-9a43-21f8f7a70194",
                "id": "8936606d-223b-428e-9a70-4b8105f60cdb",
            },
            {
                "automatic": True,
                "feeFineType": "Replacement processing fee",
                "id": "d20df2fb-45fd-4184-b238-0d25747ffdd9",
            },
        ]

    elif ref_data_path == "/service-points":
        yield from [
            {
                "id": "finance_office_uuid",
                "name": "Finance Office",
                "code": "fo",
            },
            {
                "id": "library_main_desk_uuid",
                "name": "Library Main Desk",
                "code": "lmd",
            },
        ]

    elif ref_data_path == "/users" and query == '?query=(externalSystemId=="Some external id")':
        yield from [{"id": "some id", "barcode": "some barcode", "patronGroup": "some group"}]
    elif ref_data_path == "/users" and query == '?query=(barcode=="u123")':
        yield from [{"id": "user123", "barcode": "u123", "patronGroup": "some group"}]
    elif ref_data_path == "/users" and query == '?query=(barcode=="u456")':
        yield from [{"id": "user456", "barcode": "u456", "patronGroup": "some group"}]

    elif ref_data_path == "/inventory/items" and query == '?query=(barcode=="some barcode")':
        yield from [
            {
                "id": "a FOLIO item uuid",
                "title": "Döda fallen i Avesta.",
                "barcode": "some barcode",
                "callNumber": "QB611 .C44",
                "materialType": {
                    "id": "4eea3f27-8910-46fc-9666-e2b44326c2b8",
                    "name": "sound recording",
                },
                "effectiveLocation": {
                    "id": "2e48e713-17f3-4c13-a9f8-23845bb210a4",
                    "name": "Reading room",
                },
            }
        ]

    elif ref_data_path == "/holdings-note-types":
        yield from [
            {
                "id": "88914775-f677-4759-b57b-1a33b90b24e0",
                "name": "Electronic bookplate",
                "source": "folio",
                "metadata": {
                    "createdDate": "2024-09-04T01:54:20.719+00:00",
                    "updatedDate": "2024-09-04T01:54:20.719+00:00"
                }
            },
            {
                "id": "c4407cc7-d79f-4609-95bd-1cefb2e2b5c5",
                "name": "Copy note",
                "source": "folio",
                "metadata": {
                    "createdDate": "2024-09-04T01:54:20.722+00:00",
                    "updatedDate": "2024-09-04T01:54:20.722+00:00"
                }
            },
            {
                "id": "d6510242-5ec3-42ed-b593-3585d2e48fd6",
                "name": "Action note",
                "source": "folio",
                "metadata": {
                    "createdDate": "2024-09-04T01:54:20.723+00:00",
                    "updatedDate": "2024-09-04T01:54:20.723+00:00"
                }
            },
            {
                "id": "e19eabab-a85c-4aef-a7b2-33bd9acef24e",
                "name": "Binding",
                "source": "folio",
                "metadata": {
                    "createdDate": "2024-09-04T01:54:20.724+00:00",
                    "updatedDate": "2024-09-04T01:54:20.724+00:00"
                }
            },
            {
                "id": "db9b4787-95f0-4e78-becf-26748ce6bdeb",
                "name": "Provenance",
                "source": "folio",
                "metadata": {
                    "createdDate": "2024-09-04T01:54:20.725+00:00",
                    "updatedDate": "2024-09-04T01:54:20.725+00:00"
                }
            },
            {
                "id": "6a41b714-8574-4084-8d64-a9373c3fbb59",
                "name": "Reproduction",
                "source": "folio",
                "metadata": {
                    "createdDate": "2024-09-04T01:54:20.728+00:00",
                    "updatedDate": "2024-09-04T01:54:20.728+00:00"
                }
            },
            {
                "id": "b160f13a-ddba-4053-b9c4-60ec5ea45d56",
                "name": "Note",
                "source": "folio",
                "metadata": {
                    "createdDate": "2024-09-04T01:54:20.728+00:00",
                    "updatedDate": "2024-09-04T01:54:20.728+00:00"
                }
            },
            {
                "id": "841d1873-015b-4bfb-a69f-6cbb41d925ba",
                "name": "Original MARC holdings statements",
                "source": "local",
                "metadata": {
                    "createdDate": "2025-05-02T01:54:20.728+00:00",
                    "updatedDate": "2025-05-02T01:54:20.728+00:00"
                }
            },
            {
                "id": "09c1e5c9-6f11-432e-bcbe-b9e733ccce57",
                "name": "Original MFHD Record",
                "source": "local",
                "metadata": {
                    "createdDate": "2025-05-02T01:54:20.728+00:00",
                    "updatedDate": "2025-05-02T01:54:20.728+00:00"
                }
            },
            {
                "id": "474120b0-d64e-4a6f-9c9c-e7d3e76f3cf5",
                "name": "Original MFHD (MARC21)",
                "source": "local",
                "metadata": {
                    "createdDate": "2025-05-02T01:54:20.728+00:00",
                    "updatedDate": "2025-05-02T01:54:20.728+00:00"
                }
            }
        ]

    elif ref_data_path in super_schema:
        yield from super_schema.get(ref_data_path)
    else:
        yield {}


def folio_get_single_object_mocked(*args, **kwargs):
    with open("./static/reference_data.json", "r") as super_schema_file:
        super_schema = json.load(super_schema_file)
    if args[0] == "/hrid-settings-storage/hrid-settings":
        return {
            "instances": {"prefix": "pref", "startNumber": 1},
            "holdings": {"prefix": "pref", "startNumber": 1},
            "items": {"prefix": "pref", "startNumber": 1},
            "commonRetainLeadingZeroes": True,
        }

    elif (
        args[0] == "/configurations/entries?query=(module==ORG%20and%20configName==localeSettings)"
    ):
        return {
            "configs": [
                {
                    "value": '{"timezone":"America/New_York"}',
                }
            ]
        }

    elif args[0] in super_schema:
        return super_schema.get(args[0])


def folio_get_from_github(owner, repo, file_path):
    return FolioClient.get_latest_from_github(owner, repo, file_path, "")

OKAPI_URL = "http://localhost:9130"
LIBRARY_NAME = "Test Library"

def get_mocked_library_config():
    return LibraryConfiguration(
        okapi_url=OKAPI_URL,
        tenant_id="test_tenant",
        okapi_username="test_user",
        okapi_password="test_password",
        base_folder=Path("."),
        library_name=LIBRARY_NAME,
        log_level_debug=False,
        folio_release=FolioRelease.sunflower,
        iteration_identifier="test_iteration"
    )

def get_mocked_ecs_central_libarary_config():
    return LibraryConfiguration(
        okapi_url=OKAPI_URL,
        tenant_id="test_tenant",
        okapi_username="test_user",
        okapi_password="test_password",
        base_folder=Path("."),
        library_name=LIBRARY_NAME,
        log_level_debug=False,
        folio_release=FolioRelease.sunflower,
        iteration_identifier="central_iteration",
        is_ecs=True,
    )

def get_mocked_ecs_member_libarary_config():
    return LibraryConfiguration(
        okapi_url=OKAPI_URL,
        tenant_id="test_tenant",
        ecs_tenant_id="test_ecs_tenant",
        okapi_username="test_user",
        okapi_password="test_password",
        base_folder=Path("."),
        library_name=LIBRARY_NAME,
        log_level_debug=False,
        folio_release=FolioRelease.sunflower,
        iteration_identifier="member_iteration",
        ecs_central_iteration_identifier="central_iteration",
        is_ecs=True,
    )

def get_mocked_folder_structure():
    mock_fs = MagicMock()
    mock_fs.mapping_files = Path("mapping_files")
    mock_fs.results_folder = Path("results")
    mock_fs.legacy_records_folder = Path("source_files")
    mock_fs.logs_folder = Path("logs")
    mock_fs.migration_reports_file = Path("/dev/null")
    mock_fs.transformation_extra_data_path = Path("transformation_extra_data")
    mock_fs.transformation_log_path = Path("/dev/null")
    mock_fs.data_issue_file_path = Path("/dev/null")
    mock_fs.failed_marc_recs_file = Path("failed_marc_recs.txt")
    return mock_fs
