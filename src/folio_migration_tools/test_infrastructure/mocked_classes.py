import json
import logging
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import Mock

from folioclient import FolioClient

from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport


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
    elif ref_data_path == "/users" and query == '?query=(externalSystemId=="Some external id")':
        yield from [{"id": "some id", "barcode": "some barcode", "patronGroup": "some group"}]
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
    elif args[0] in super_schema:
        return super_schema.get(args[0])


def get_latest_from_github(owner, repo, file_path):
    return FolioClient.get_latest_from_github(owner, repo, file_path, "")
