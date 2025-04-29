import uuid
from unittest.mock import MagicMock, Mock

import pytest
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.mapper_base import MapperBase
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.test_infrastructure import mocked_classes
from folio_uuid.folio_namespaces import FOLIONamespaces

@pytest.fixture
def mocked_mapper():
    return MapperBase(
        mocked_classes.get_mocked_library_config(),
        mocked_classes.mocked_folio_client(),
    )

def test_validate_required_properties():
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "",
        "type": "object",
        "required": ["d", "h", "i"],
        "properties": {},
    }
    record = {
        "a": None,
        "b": [],
        "c": "",
        "d": {"e": None, "f": "aaa"},
        "g": {},
        "h": "actual value",
        "type": "object",
    }
    with pytest.raises(TransformationRecordFailedError):
        MapperBase.validate_required_properties("", record, schema, FOLIONamespaces.other)


def test_validate_required_properties_remove_object():
    schema = {"required": ["d", "h"]}
    record = {
        "a": None,
        "b": [],
        "c": "",
        "d": {"e": None, "f": "aaa"},
        "g": {},
        "h": "actual value",
        "type": "object",
    }
    clean_record = MapperBase.validate_required_properties(
        "", record, schema, FOLIONamespaces.other
    )
    assert "type" not in clean_record


def test_generate_bound_with_holding_default_no_callnumbers(mocked_mapper):
    mocked_mapper.migration_report = MigrationReport()
    hold_1_id = "66db04ef-fbfb-5c45-9ed7-65a1f2495eaf"
    inst_1_id = "ae0c833c-e76f-53aa-975a-7ac4c2be7972"
    inst_2_id = "fae73ef8-b546-5310-b4ee-c2d68fed48c5"
    bw_rel_map = {hold_1_id: [inst_1_id, inst_2_id]}
    holding = {"id": hold_1_id, "instanceId": inst_1_id}
    res = list(
        mocked_mapper.create_bound_with_holdings(
            holding, bw_rel_map[hold_1_id], str(uuid.uuid4())
        )
    )
    assert len(res) == 2
    assert res[0]["id"] == hold_1_id
    assert res[1]["id"] != hold_1_id


def test_generate_bound_with_holding_default_single_callnumber(mocked_mapper):
    # mocked_mapper = Mock(spec=MapperBase)
    # mocked_mapper.folio_client = mocked_classes.mocked_folio_client()
    mocked_mapper.migration_report = MigrationReport()
    hold_1_id = "66db04ef-fbfb-5c45-9ed7-65a1f2495eaf"
    inst_1_id = "ae0c833c-e76f-53aa-975a-7ac4c2be7972"
    inst_2_id = "fae73ef8-b546-5310-b4ee-c2d68fed48c5"
    bw_rel_map = {hold_1_id: [inst_1_id, inst_2_id]}
    holding = {"id": hold_1_id, "instanceId": inst_1_id, "callNumber": "Vna [vol. 1]"}
    res = list(
        mocked_mapper.create_bound_with_holdings(
            holding,
            bw_rel_map[hold_1_id],
            str(uuid.uuid4()),
        )
    )
    assert len(res) == 2
    assert all(h["callNumber"] == "Vna [vol. 1]" for h in res)


def test_generate_bound_with_holding_default_mulitple_callnumber(mocked_mapper):
    mocked_mapper.migration_report = MigrationReport()
    hold_1_id = "66db04ef-fbfb-5c45-9ed7-65a1f2495eaf"
    inst_1_id = "ae0c833c-e76f-53aa-975a-7ac4c2be7972"
    inst_2_id = "fae73ef8-b546-5310-b4ee-c2d68fed48c5"
    bw_rel_map = {hold_1_id: [inst_1_id, inst_2_id]}
    holding = {"id": hold_1_id, "instanceId": inst_1_id, "callNumber": '["Vna [vol. 1]", "Vnd"]'}
    res = list(
        mocked_mapper.create_bound_with_holdings(
            holding, bw_rel_map[hold_1_id], str(uuid.uuid4())
        )
    )
    assert res[0]["callNumber"] == "Vna [vol. 1]"
    assert res[1]["callNumber"] == "Vnd"


def test_generate_bound_with_holding_default_mulitple_callnumber_count_mismatch(mocked_mapper):
    mocked_mapper.migration_report = MigrationReport()
    hold_1_id = "66db04ef-fbfb-5c45-9ed7-65a1f2495eaf"
    inst_1_id = "ae0c833c-e76f-53aa-975a-7ac4c2be7972"
    inst_2_id = "fae73ef8-b546-5310-b4ee-c2d68fed48c5"
    inst_3_id = str(uuid.uuid4())
    bw_rel_map = {hold_1_id: [inst_1_id, inst_2_id, inst_3_id]}
    holding = {"id": hold_1_id, "instanceId": inst_1_id, "callNumber": '["Vna [vol. 1]", "Vnd"]'}
    res = list(
        mocked_mapper.create_bound_with_holdings(
            holding, bw_rel_map[hold_1_id], str(uuid.uuid4())
        )
    )
    assert res[0]["callNumber"] == "Vna [vol. 1]"
    assert res[1]["callNumber"] == "Vnd"
    assert res[2]["callNumber"] == "Vna [vol. 1]"


def test_generate_and_write_bound_with_part(mocked_mapper):
    mocked_writer = Mock(spec=ExtradataWriter)
    mocked_mapper.extradata_writer = mocked_writer
    mocked_mapper.extradata_writer.write = MagicMock(name="write")
    mocked_mapper.migration_report = MigrationReport()
    item_id = "1"
    hold_1_id = "66db04ef-fbfb-5c45-9ed7-65a1f2495eaf"
    mocked_mapper.create_and_write_boundwith_part(item_id, hold_1_id)
    mocked_mapper.extradata_writer.write.assert_called
    args = mocked_mapper.extradata_writer.write.call_args[0]
    assert args[0] == "boundwithPart"
    assert args[1]["holdingsRecordId"] == hold_1_id
    assert args[1]["itemId"] == "d33c5266-df65-5187-9358-b115afe55f2d"


def test_clean_none_props():
    record = {
        "a": None,
        "b": [],
        "c": "",
        "d": {"e": None, "f": "aaa"},
        "g": {},
        "h": "actual value",
        "i": ["", "j"],
        "k": {"l": "", "m": "aaa"},
    }
    cleaned = MapperBase.clean_none_props(record)
    for ck in ["a"]:
        assert ck not in cleaned.keys()
    assert "e" not in cleaned["d"]
    assert len(cleaned["i"]) == 1
    assert next(iter(cleaned["i"])) == "j"
    assert "l" in cleaned["k"]


def test_add_legacy_identifier_to_admin_note(mocked_mapper):
    folio_record = {}
    legacy_id = "legacy_ID"
    mocked_mapper.add_legacy_id_to_admin_note(folio_record, legacy_id)
    assert f"{MapperBase.legacy_id_template} {legacy_id}" in folio_record["administrativeNotes"]


def test_add_legacy_identifier_to_admin_note_additional_id(mocked_mapper):
    folio_record = {"administrativeNotes": [f"{MapperBase.legacy_id_template} legacy_ID"]}
    legacy_id = "legacy_ID_2"
    mocked_mapper.add_legacy_id_to_admin_note(folio_record, legacy_id)
    assert len(folio_record["administrativeNotes"]) == 1
    assert (
        f"{MapperBase.legacy_id_template} legacy_ID, legacy_ID_2"
        in folio_record["administrativeNotes"]
    )


def test_add_legacy_identifier_to_admin_note_dupe(mocked_mapper):
    folio_record = {"administrativeNotes": [f"{MapperBase.legacy_id_template} legacy_ID"]}
    legacy_id = "legacy_ID"
    mocked_mapper.add_legacy_id_to_admin_note(folio_record, legacy_id)
    assert f"{MapperBase.legacy_id_template} legacy_ID" in folio_record["administrativeNotes"]
