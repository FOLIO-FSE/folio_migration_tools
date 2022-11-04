import json
import logging
import os
from unittest.mock import Mock

import requests
from folioclient import FolioClient


# @pytest.fixture(scope="session", autouse=True)
def mocked_folio_client() -> FolioClient:
    try:
        mocked_folio = Mock(spec=FolioClient)
        mocked_folio.okapi_url = "okapi_url"
        mocked_folio.tenant_id = "tenant_id"
        mocked_folio.username = "username"
        mocked_folio.password = "password"  # noqa: S105
        # setup_ref_data_from_github(mocked_folio)
        mocked_folio.instance_formats = [
            {
                "code": "test_code_99",
                "id": "605e9527-4008-45e2-a78a-f6bfb027c43a",
                "name": "test -- name",
            },
            {
                "code": "ab",
                "id": "605e9527-4008-45e2-a78a-f6bfb027c43a",
                "name": "test -- name 2",
            },
        ]

        mocked_folio.folio_get_single_object = folio_get_single_object_mocked
        mocked_folio.folio_get_all = folio_get_all
        return mocked_folio
    except Exception as ee:
        logging.error(ee)
        raise ee


def setup_ref_data_from_github(mocked_folio):
    mocked_folio.alt_title_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/alternative-title-types"
    )

    mocked_folio.authority_note_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/authority-note-types"
    )
    mocked_folio.call_number_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/call-number-types"
    )
    mocked_folio.class_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/classification-types"
    )
    mocked_folio.contrib_name_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/contributor-name-types"
    )
    mocked_folio.contributor_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/contributor-types"
    )
    mocked_folio.electronic_access_relationships = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/electronic-access-relationships"
    )
    mocked_folio.holding_note_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/holdings-note-types"
    )
    mocked_folio.holdings_sources = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/holdings-sources"
    )
    mocked_folio.holdings_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/holdings-types"
    )
    mocked_folio.identifier_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/identifier-types"
    )
    mocked_folio = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/ill-policies"
    )
    mocked_folio.instance_formats = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/instance-formats"
    )
    mocked_folio.instance_note_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/instance-note-types"
    )
    mocked_folio.instance_relationship_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/instance-relationship-types"
    )
    mocked_folio.instance_statuses = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/instance-statuses"
    )
    mocked_folio.instance_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/instance-types"
    )
    mocked_folio.item_damaged_statuses = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/item-damaged-statuses"
    )
    mocked_folio.item_note_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/item-note-types"
    )
    mocked_folio.loan_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/loan-types"
    )
    mocked_folio.location_units = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/location-units"
    )
    mocked_folio.locations = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/locations"
    )
    mocked_folio = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/material-types"
    )
    mocked_folio.modes_of_issuance = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/modes-of-issuance"
    )
    mocked_folio.nature_of_content_terms = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/nature-of-content-terms"
    )
    mocked_folio.service_points = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/service-points"
    )
    mocked_folio.statistical_code_types = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/statistical-code-types"
    )
    mocked_folio.statistical_codes = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/statistical-codes"
    )
    mocked_folio.templates = get_ref_data_from_github_folder(
        "folio-org", "mod-inventory-storage", "/reference-data/templates"
    )
    mocked_folio.instance_formats = [
        {
            "code": "test_code_99",
            "id": "605e9527-4008-45e2-a78a-f6bfb027c43a",
            "name": "test -- name",
        },
        {
            "code": "ab",
            "id": "605e9527-4008-45e2-a78a-f6bfb027c43a",
            "name": "test -- name 2",
        },
    ]
    return mocked_folio


def folio_get_all(ref_data_path, array_name, query, limit=10):
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
    else:
        yield {}


def folio_get_single_object_mocked(*args, **kwargs):
    return {
        "instances": {"prefix": "pref", "startNumber": "1"},
        "holdings": {"prefix": "pref", "startNumber": "1"},
        "items": {"prefix": "pref", "startNumber": "1"},
        "commonRetainLeadingZeroes": True,
    }


def get_latest_from_github(owner, repo, file_path):
    return FolioClient.get_latest_from_github(owner, repo, file_path, "")


def get_ref_data_from_github_folder(owner, repo, folder_path: str):
    ret_arr = []
    logging.info("Using GITHB_TOKEN environment variable for Gihub API Access")
    github_headers = {
        "content-type": "application/json",
        "User-Agent": "Folio Client (https://github.com/FOLIO-FSE/FolioClient)",
        "authorization": f"token {os.environ.get('GITHUB_TOKEN')}",
    }

    latest_path = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    req = requests.get(latest_path, headers=github_headers)
    req.raise_for_status()
    latest = json.loads(req.text)
    # print(json.dumps(latest, indent=4))
    latest_tag = latest["tag_name"]
    path = f"https://api.github.com/repos/{owner}/{repo}/contents{folder_path}?ref={latest_tag}"
    # print(latest_path)
    req = requests.get(path, headers=github_headers)
    req.raise_for_status()
    file_list = [x["name"] for x in json.loads(req.text) if x["type"] == "file"]
    github_path = "https://raw.githubusercontent.com"
    for file_name in file_list:
        file_path = f"{github_path}/{owner}/{repo}/{latest_tag}{folder_path}/{file_name}"
        resp = requests.get(file_path, headers=github_headers)
        resp.raise_for_status()
        ret_arr.append(json.loads(resp.text))
    logging.debug(folder_path)
    return ret_arr
