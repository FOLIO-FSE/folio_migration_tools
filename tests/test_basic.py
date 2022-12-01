# content of test_sample.py
import ast
import json
import os

import pymarc
from folio_uuid import FOLIONamespaces
from folio_uuid import FolioUUID
from pymarc import Field
from pymarc.reader import MARCReader

from folio_migration_tools import mapper_base
from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.report_blurbs import Blurbs


# flake8: noqa: E501
def func(x):
    return x + 1


def test_answer2():
    assert func(4) == 5


def test_deterministic_uuid_generation_holdings():
    deterministic_uuid = FolioUUID(
        "https://okapi-bugfest-juniper.folio.ebsco.com",
        FOLIONamespaces.holdings,
        "000000167",
    )
    assert "a0b4c8a2-01fd-50fd-8158-81bd551412a0" == str(deterministic_uuid)


def test_dedupe():
    rec = {
        "identifiers": [
            {"id": "001", "value": "val1"},
            {"id": "001", "value": "val1"},
            {"id": "001", "value": "val2"},
            {"id": "002", "value": "val1"},
        ]
    }
    RulesMapperBase.dedupe_rec(rec)
    assert len(rec["identifiers"]) == 3


def test_blurbs():
    b = Blurbs.Introduction
    assert b[0] == "Introduction"


def test_get_marc_record():
    file_path = "./tests/test_data/default/test_get_record.xml"
    record = pymarc.parse_xml_to_array(file_path)[0]
    assert record["001"].value() == "21964516"


def test_eval():
    cnr = "['973 B967a', '']"
    a = ast.literal_eval(cnr)
    assert isinstance(a, list)


def test_flatten():
    instance_str = '{"id": "11af72aa-5921-46c4-8fa3-55b481849948", "metadata": {"createdDate": "2021-10-14T13:40:48.848", "createdByUserId": "f446ed29-2dac-436a-b1bc-8ebe5ac7ea76", "updatedDate": "2021-10-14T13:40:48.848", "updatedByUserId": "f446ed29-2dac-436a-b1bc-8ebe5ac7ea76"}, "hrid": "2471459", "identifiers": [{"identifierTypeId": "c858e4f2-2b6b-4385-842b-60732ee14abb", "value": "2018027930"}, {"identifierTypeId": "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef", "value": "(OCoLC)on1040079128"}, {"identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422", "value": "1541541839"}, {"identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422", "value": "9781541546745 (electronic bk.)"}, {"identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422", "value": "1541546741 (electronic bk.)"}, {"identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422", "value": "9781541541832 (electronic bk.)"}, {"identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422", "value": "9781541546752 (e-book)"}, {"identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422", "value": "154154675X"}], "classifications": [{"classificationTypeId": "ce176ace-a53e-4b4d-aa89-725ed7b2edac", "classificationNumber": "PZ7.1.H433"}], "contributors": [{"contributorNameTypeId": "2b94c631-fca9-4892-a730-03ee529ffe2a", "contributorTypeId": "6e09d47d-95e2-4d8a-831b-f777b8ef6d81", "contributorTypeText": "Contributor", "primary": true, "name": "Heathfield, Lisa"}, {"contributorNameTypeId": "2e48e713-17f3-4c13-a9f8-23845bb210aa", "contributorTypeId": "9f0a2cf0-7a9b-45a2-a403-f68d2850d07c", "contributorTypeText": "Contributor", "primary": false, "name": "PALCI EBSCO books"}], "title": "Flight of a starling / Lisa Heathfield.", "indexTitle": "Flight of a starling", "publication": [{"place": "Minneapolis", "publisher": "Carolrhoda Lab", "dateOfPublication": "[2019]", "role": "Publication"}], "physicalDescriptions": ["1 online resource"], "instanceFormatIds": ["8d511d33-5e85-4c5d-9bce-6e3c9cd0c324"], "notes": [{"instanceNoteTypeId": "6a2533a7-4de2-4e64-8466-074c2fa9308c", "note": "Originally published: London : Electric Monkey, 2017", "staffOnly": false}, {"instanceNoteTypeId": "e814a32e-02da-4773-8f3a-6629cdb7ecdf", "note": "Electronic access restricted to Villanova University patrons", "staffOnly": false}, {"instanceNoteTypeId": "10e2e11b-450f-45c8-b09b-0f819999966e", "note": "Told from two viewpoints, sisters Lo and Rita spend their lives flying high on the trapeze, but real danger comes as secrets begin to unravel the tightknit circus community and Lo finds love with a ", "staffOnly": false}, {"instanceNoteTypeId": "66ea8f28-d5da-426a-a7c9-739a5d676347", "note": "Description based on print version record and CIP data provided by publisher", "staffOnly": false}], "subjects": ["Sisters Fiction", "Circus Fiction", "Family life Fiction", "Dating (Social customs) Fiction", "Aerialists Fiction", "Families Fiction", "Aerialists", "Circus", "Dating (Social customs)", "Families", "Sisters", "Genre: Electronic books", "Genre: Fiction"], "electronicAccess": [{"relationshipId": "f5d0068e-6272-458e-8a81-b85e7b9a14aa", "uri": "http://ezproxy.villanova.edu/login?URL=http://search.ebscohost.com/login.aspx?direct=true&scope=site&db=nlebk&AN=1947377", "linkText": "", "materialsSpecification": "", "publicNote": "Online version"}], "source": "MARC", "instanceTypeId": "6312d172-f0cf-40f6-b27d-9fa8feaf332f", "modeOfIssuanceId": "9d18a02f-5897-4c31-9106-c9abb5c7ae8b", "languages": ["eng"], "discoverySuppress": false, "staffSuppress": false}'
    instance = json.loads(instance_str)

    flat = set(mapper_base.flatten(instance))
    assert "id" in flat
    assert "metadata.createdDate" in flat
    assert "contributors.name" in flat


def test_flatten2():
    my_dict = {
        "a": {
            "b": {"c": "95"},
            "d": "apa",
            "e": ["papa", "lapa"],
            "f": [{"g": "another string"}, {"g": "a string"}],
        },
        "h": [{"g": "another string"}, {"g": "a string"}],
        "i": ["papa", "lapa"],
    }
    flat = list(mapper_base.flatten(my_dict))

    assert "a" in flat
    assert "a.b" in flat
    assert "a.d" in flat
    assert "a.e" in flat
    assert "a.f" in flat
    assert "a.b.c" in flat
    assert "a.f.g" in flat
    assert len([a for a in flat if a == "a.f.g"]) == 2
    assert len([a for a in flat if a == "a.e"]) == 2
    assert len([a for a in flat if a == "h"]) == 1


def test_udec():
    path = "./tests/test_data/msplit00000005.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = None
        for record in reader:
            if record["001"].value() == "udec000207828":
                record1 = record
        assert record1["911"]["a"] == "Biblioteca de Chillán. Hemeroteca"
        my_tuple_json = record1.as_json()
        assert '"Biblioteca de Chilla\\u0301n. Hemeroteca"' in my_tuple_json


def test_udec2():
    path = "./tests/test_data/msplit00000005.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = False
        record1 = None
        for record in reader:
            if record["001"].value() == "udec000207828":
                record1 = record
        assert record1["911"]["a"] == "Biblioteca de Chilla n. Hemeroteca"
        my_tuple_json = record1.as_json()
        assert '"Biblioteca de Chilla n. Hemeroteca"' in my_tuple_json


def test_ude2c():
    path = "./tests/test_data/crashes.mrc"
    assert os.path.isfile(path)
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True

        for record in reader:
            assert record is None
            chunk = reader.current_chunk
            print(type(reader.current_exception).__name__)
            print(reader.current_exception)
            reader2 = MARCReader(chunk)
            rec2 = next(reader2)
            # print(rec2)
            # assert rec2.title


# def test_identifier_rules_mapping_single_subfield():
#    assert False
