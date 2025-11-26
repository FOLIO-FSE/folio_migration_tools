import json
import logging
from unittest.mock import Mock

import pymarc
import pytest
from pymarc import Field, MARCReader, Record, Subfield

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)
from .test_infrastructure import mocked_classes


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> RulesMapperHoldings:
    folio = mocked_classes.mocked_folio_client()
    lib = LibraryConfiguration(
        okapi_url=folio.gateway_url,
        tenant_id=folio.tenant_id,
        okapi_username=folio.username,
        okapi_password=folio.password,
        folio_release=FolioRelease.ramsons,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
        multi_field_delimiter="^-^",
    )
    conf = HoldingsMarcTransformer.TaskConfiguration(
        name="test",
        migration_task_type="HoldingsTransformer",
        hrid_handling=HridHandling.default,
        files=[],
        ils_flavour=IlsFlavour.voyager,
        legacy_id_marc_path="001",
        location_map_file_name="",
        default_call_number_type_name="Dewey Decimal classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
        create_source_records=True
    )
    parent_id_map: dict[str, tuple] = {}
    location_map = [
        {"legacy_code": "jnlDesk", "folio_code": "KU/CC/DI/2"},
        {"legacy_code": "*", "folio_code": "KU/CC/DI/2"},
    ]
    statistical_codes_map = [
        {"folio_code": "compfiles", "legacy_stat_code": "998_c:compfiles"},
        {"folio_code": "ebooks", "legacy_stat_code": "998_b:ebooks"},
        {"folio_code": "vidstream", "legacy_stat_code": "003:FoD"},
        {"folio_code": "arch", "legacy_stat_code": "998_a:arch"},
        {"folio_code": "audstream", "legacy_stat_code": "998_a:audstream"},
        {"folio_code": "XOCLC", "legacy_stat_code": "990:xoclc"},
        {"folio_code": "audstream", "legacy_stat_code": "*"},
    ]

    mapper = RulesMapperHoldings(folio, location_map, conf, lib, parent_id_map, [], statistical_codes_map)
    mapper.folio_client = folio
    mapper.migration_report = MigrationReport()
    return mapper


def test_basic(mapper: RulesMapperHoldings, caplog):
    path = "./tests/test_data/mfhd/holding.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        mapper.parent_id_map = {
            "7611780": (
                "7611780",
                "9d1673a3-a546-5afa-b0fb-5ab971f73eca",
                "in00000000005",
            )
        }
        mapper.integrate_supplemental_mfhd_mappings()
        record = next(reader)
        ids = RulesMapperHoldings.get_legacy_ids(mapper, record, 1)
        res = mapper.parse_record(
            record,
            FileDefinition(file_name="", discovery_suppressed=False, staff_suppressed=False),
            ids,
        )[0]
        assert res
        assert res["permanentLocationId"] == "f34d27c6-a8eb-461b-acd6-5dea81771e70"
        assert res["hrid"] == "pref00000000001"
        assert len(res["administrativeNotes"]) > 0
        assert res["callNumber"] == "QB611 .C44"
        assert res["callNumberTypeId"] == "95467209-6d7b-468b-94df-0f5d7ad2747d"


def test_setup_boundwith_relationship_map_missing_entries():
    with pytest.raises(TransformationProcessError) as tpe:
        file_mock = [{}]
        mock_task_configuration = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
        mock_task_configuration.default_call_number_type_name = "Dewey Decimal classification"
        mock_task_configuration.fallback_holdings_type_id = "03c9c400-b9e3-4a07-ac0e-05ab470233ed"
        mock_task_configuration.create_source_records = False
        RulesMapperHoldings(mocked_classes.mocked_folio_client(), [], mock_task_configuration, mocked_classes.get_mocked_library_config(), {}, file_mock)
    assert "Column MFHD_ID missing from" in str(tpe.value)


def test_setup_boundwith_relationship_map_missing_bib_id_entries():
    with pytest.raises(TransformationProcessError) as tpe:
        file_mock = [{"MFHD_ID": "H1"}]
        mock_task_configuration = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
        mock_task_configuration.default_call_number_type_name = "Dewey Decimal classification"
        mock_task_configuration.fallback_holdings_type_id = "03c9c400-b9e3-4a07-ac0e-05ab470233ed"
        mock_task_configuration.create_source_records = False
        RulesMapperHoldings(mocked_classes.mocked_folio_client(), [], mock_task_configuration, mocked_classes.get_mocked_library_config(), {}, file_mock)
    assert "Column BIB_ID missing from" in str(tpe.value)


def test_setup_boundwith_relationship_map_missing_entries_2():
    mocked_mapper = Mock(spec=RulesMapperHoldings)
    file_mock = []
    res = RulesMapperHoldings.setup_boundwith_relationship_map(mocked_mapper, file_mock)
    assert res == {}


def test_setup_boundwith_relationship_map_empty_entries():
    with pytest.raises(TransformationProcessError) as tpe:
        mock_task_configuration = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
        mock_task_configuration.default_call_number_type_name = "Dewey Decimal classification"
        mock_task_configuration.fallback_holdings_type_id = "03c9c400-b9e3-4a07-ac0e-05ab470233ed"
        mock_task_configuration.create_source_records = False
        file_mock = [{"MFHD_ID": "", "BIB_ID": ""}]
        RulesMapperHoldings(mocked_classes.mocked_folio_client(), [], mock_task_configuration, mocked_classes.get_mocked_library_config(), {}, file_mock)
    assert "Column MFHD_ID missing from" in str(tpe.value)


def test_setup_boundwith_relationship_map_empty_bib_id_entries():
    with pytest.raises(TransformationProcessError) as tpe:
        mock_task_configuration = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
        mock_task_configuration.default_call_number_type_name = "Dewey Decimal classification"
        mock_task_configuration.fallback_holdings_type_id = "03c9c400-b9e3-4a07-ac0e-05ab470233ed"
        mock_task_configuration.create_source_records = False
        file_mock = [{"MFHD_ID": "H1", "BIB_ID": ""}]
        RulesMapperHoldings(mocked_classes.mocked_folio_client(), [], mock_task_configuration, mocked_classes.get_mocked_library_config(), {}, file_mock)
    assert "Column BIB_ID missing from" in str(tpe.value)


def test_setup_boundwith_relationship_map_with_entries():
    file_mock = [
        {"MFHD_ID": "H1", "BIB_ID": "B1"},
        {"MFHD_ID": "H1", "BIB_ID": "B2"},
        {"MFHD_ID": "H2", "BIB_ID": "B3"},
        {"MFHD_ID": "H2", "BIB_ID": "B4"},
    ]
    mock_task_configuration = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mock_task_configuration.default_call_number_type_name = "Dewey Decimal classification"
    mock_task_configuration.fallback_holdings_type_id = "03c9c400-b9e3-4a07-ac0e-05ab470233ed"
    mock_task_configuration.create_source_records = False
    parent_id_map = {
        "B1": ("B1", "ae0c833c-e76f-53aa-975a-7ac4c2be7972", "in00000000001"),
        "B2": ("B2", "fae73ef8-b546-5310-b4ee-c2d68fed48c5", "in00000000002"),
        "B3": ("B3", "096b0057-fb32-519c-9e6d-58974d64a154", "in00000000003"),
        "B4": ("B4", "9f6d7e45-9489-5b56-ab8e-5e22ba523856", "in00000000004"),
    }

    mapper = RulesMapperHoldings(mocked_classes.mocked_folio_client(), [], mock_task_configuration, mocked_classes.get_mocked_library_config(), parent_id_map, file_mock, {})

    assert len(mapper.boundwith_relationship_map) == 2
    assert mapper.boundwith_relationship_map["bcddbd83-a6aa-5904-888b-13b46f0b1fcb"] == [
        "ae0c833c-e76f-53aa-975a-7ac4c2be7972",
        "fae73ef8-b546-5310-b4ee-c2d68fed48c5",
    ]


def test_edit852(mapper: RulesMapperHoldings, caplog):
    path = "./tests/test_data/mfhd/holding.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        new_code = "JOUR"
        record["852"]["b"] = new_code
        assert record["852"]["b"] == new_code


def test_get_legacy_ids_001():
    record = Record()
    mock_mapper = Mock(spec=RulesMapperHoldings)
    record.add_field(Field(tag="001", data="0001"))
    mocked_config = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mocked_config.legacy_id_marc_path = "001"
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = mocked_config
    legacy_ids = RulesMapperHoldings.get_legacy_ids(mock_mapper, record, 1)
    assert legacy_ids == ["0001"]


def test_get_legacy_id_001_wrong_order():
    record = Record()
    record.add_field(Field(tag="001", data="0001"))
    record.add_field(
        Field(
            tag="951",
            subfields=[Subfield(code="b", value="bid")],
        )
    )
    record.add_field(
        Field(
            tag="951",
            subfields=[Subfield(code="c", value="cid")],
        )
    )
    record.add_field(Field(tag="001", data="0001"))
    mocked_config = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mocked_config.legacy_id_marc_path = "951$c"
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = mocked_config

    legacy_ids = RulesMapperHoldings.get_legacy_ids(mock_mapper, record, 1)
    assert legacy_ids == ["cid"]


def test_get_legacy_id_001_right_order():
    record = Record()
    record.add_field(Field(tag="001", data="0001"))
    record.add_field(
        Field(
            tag="951",
            subfields=[Subfield(code="c", value="cid")],
        )
    )
    record.add_field(
        Field(
            tag="951",
            subfields=[Subfield(code="b", value="bid")],
        )
    )
    record.add_field(Field(tag="001", data="0001"))
    mocked_config = Mock(spec=HoldingsMarcTransformer.TaskConfiguration)
    mocked_config.legacy_id_marc_path = "951$c"
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = mocked_config
    legacy_ids = RulesMapperHoldings.get_legacy_ids(mock_mapper, record, 1)
    assert legacy_ids == ["cid"]


def test_get_holdings_schema():
    folio = mocked_classes.mocked_folio_client()
    mock_mapper = Mock(spec=RulesMapperHoldings)
    schema = RulesMapperHoldings.fetch_holdings_schema(mock_mapper, folio)
    assert schema["required"]


def test_get_marc_textual_stmt_correct_order_and_not_deduped():
    path = "./tests/test_data/mfhd/c_record_repeated_holdings_statements.mrc"
    with open(path, "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record = next(reader)
        res = HoldingsStatementsParser.get_holdings_statements(
            record, "853", "863", "866", ["apa"], False
        )
        rec = {"holdingsStatements": res["statements"]}
        props_to_not_dedupe = [
            "holdingsStatements",
            "holdingsStatementsForIndexes",
            "holdingsStatementsForSupplements",
        ]
        RulesMapperHoldings.dedupe_rec(rec, props_to_not_dedupe)
        all_stmts = [f["statement"] for f in rec["holdingsStatements"]]
        all_94s = [f for f in all_stmts if f == "1994-1998."]
        assert len(all_94s) == 2


def test_remove_from_id_map():
    mocked_rules_mapper_holdings = Mock(spec=RulesMapperHoldings)
    mocked_rules_mapper_holdings.id_map = {
        "h15066915": "5a0af31f-aa4a-5215-8a60-712b38cd6cb6",
        "h14554914": "c9c44650-11e2-5534-ae50-01a1aa0fbd66",
    }

    former_ids = ["h15066915"]

    RulesMapperHoldings.remove_from_id_map(mocked_rules_mapper_holdings, former_ids)

    # The ids in the former_ids have been removed, any others are still there
    assert "h15066915" not in mocked_rules_mapper_holdings.id_map.keys()
    assert "h14554914" in mocked_rules_mapper_holdings.id_map.keys()


def test_set_default_call_number_type_if_empty():
    mocked_rules_mapper_holdings = Mock(spec=RulesMapperHoldings)
    mocked_rules_mapper_holdings.conditions = Mock(spec=Conditions)
    mocked_rules_mapper_holdings.conditions.default_call_number_type = {
        "id": "b8992e1e-1757-529f-9238-147703864635"
    }

    without_callno_type_specified = {"callNumberTypeId": ""}
    with_callno_type_specified = {"callNumberTypeId": "22156b02-785f-51c0-8723-416512cd42d9"}

    # The default callNumberTypeId is assigned when no callNumberTypeId is specified
    RulesMapperHoldings.set_default_call_number_type_if_empty(
        mocked_rules_mapper_holdings, without_callno_type_specified
    )
    assert (
        without_callno_type_specified["callNumberTypeId"] == "b8992e1e-1757-529f-9238-147703864635"
    )

    # The default callNumberTypeId is not replaced with the default if one is already specified
    RulesMapperHoldings.set_default_call_number_type_if_empty(
        mocked_rules_mapper_holdings, with_callno_type_specified
    )
    assert with_callno_type_specified["callNumberTypeId"] == "22156b02-785f-51c0-8723-416512cd42d9"


def test_create_source_records_equals_false():
    folio = mocked_classes.mocked_folio_client()
    lib = LibraryConfiguration(
        okapi_url=folio.gateway_url,
        tenant_id=folio.tenant_id,
        okapi_username=folio.username,
        okapi_password=folio.password,
        folio_release=FolioRelease.ramsons,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    conf = HoldingsMarcTransformer.TaskConfiguration(
        name="test",
        migration_task_type="HoldingsTransformer",
        hrid_handling=HridHandling.default,
        files=[],
        ils_flavour=IlsFlavour.voyager,
        legacy_id_marc_path="001",
        location_map_file_name="",
        default_call_number_type_name="Dewey Decimal classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
        create_source_records=False,
    )
    parent_id_map: dict[str, tuple] = {}
    location_map = [
        {"legacy_code": "jnlDesk", "folio_code": "KU/CC/DI/2"},
        {"legacy_code": "*", "folio_code": "KU/CC/DI/2"},
    ]
    mapper = RulesMapperHoldings(folio, location_map, conf, lib, parent_id_map, [])
    mapper.integrate_supplemental_mfhd_mappings()
    mapper.folio_client = folio
    mapper.migration_report = MigrationReport()
    path = "./tests/test_data/mfhd/holding.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        mapper.parent_id_map = {
            "7611780": (
                "7611780",
                "9d1673a3-a546-5afa-b0fb-5ab971f73eca",
                "in00000000005",
            )
        }
        record = next(reader)
        ids = RulesMapperHoldings.get_legacy_ids(mapper, record, 1)
        res = mapper.parse_record(
            record, FileDefinition(file_name="", suppressed=False, staff_suppressed=False), ids
        )[0]
        assert res
        assert res["permanentLocationId"] == "f34d27c6-a8eb-461b-acd6-5dea81771e70"
        assert res.get("hrid", False) is False
        assert len(res["administrativeNotes"]) > 0
        assert res["callNumber"] == "QB611 .C44"
        assert res["callNumberTypeId"] == "95467209-6d7b-468b-94df-0f5d7ad2747d"


def test_create_source_records_equals_false_preserve001():
    folio = mocked_classes.mocked_folio_client()
    lib = LibraryConfiguration(
        okapi_url=folio.gateway_url,
        tenant_id=folio.tenant_id,
        okapi_username=folio.username,
        okapi_password=folio.password,
        folio_release=FolioRelease.ramsons,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    conf = HoldingsMarcTransformer.TaskConfiguration(
        name="test",
        migration_task_type="HoldingsTransformer",
        hrid_handling=HridHandling.preserve001,
        files=[],
        ils_flavour=IlsFlavour.voyager,
        legacy_id_marc_path="001",
        location_map_file_name="",
        default_call_number_type_name="Dewey Decimal classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
        create_source_records=False,
    )
    parent_id_map: dict[str, tuple] = {}
    location_map = [
        {"legacy_code": "jnlDesk", "folio_code": "KU/CC/DI/2"},
        {"legacy_code": "*", "folio_code": "KU/CC/DI/2"},
    ]
    mapper = RulesMapperHoldings(folio, location_map, conf, lib, parent_id_map, [])
    mapper.integrate_supplemental_mfhd_mappings()
    mapper.folio_client = folio
    mapper.migration_report = MigrationReport()
    path = "./tests/test_data/mfhd/holding.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        mapper.parent_id_map = {
            "7611780": (
                "7611780",
                "9d1673a3-a546-5afa-b0fb-5ab971f73eca",
                "in00000000005",
            )
        }
        record = next(reader)
        ids = RulesMapperHoldings.get_legacy_ids(mapper, record, 1)
        res = mapper.parse_record(
            record, FileDefinition(file_name="", suppressed=False, staff_suppressed=False), ids
        )[0]
        assert res
        assert res["permanentLocationId"] == "f34d27c6-a8eb-461b-acd6-5dea81771e70"
        assert res.get("hrid", False) == "000000167"
        assert len(res["administrativeNotes"]) > 0
        assert res["callNumber"] == "QB611 .C44"
        assert res["callNumberTypeId"] == "95467209-6d7b-468b-94df-0f5d7ad2747d"


def test_collect_mrk_statement_notes(mapper):
    mapper.task_configuration.include_mrk_statements = True
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="853",
            subfields=[
                pymarc.Subfield(code="8", value="1"),
                pymarc.Subfield(code="a", value="v."),
                pymarc.Subfield(code="b", value="no."),
                pymarc.Subfield(code="i", value="(year)"),
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="863",
            subfields=[
                pymarc.Subfield(code="8", value="1.1"),
                pymarc.Subfield(code="a", value="1-16"),
                pymarc.Subfield(code="b", value="1-16"),
                pymarc.Subfield(code="i", value="1994-1998"),
            ],
        ),
    )
    record.add_field(
        pymarc.Field(
            tag="866",
            subfields=[
                pymarc.Subfield(code="8", value="1.2"),
                pymarc.Subfield(code="a", value="v.17no.17 (1999)-v.32no.32 (2003)"),
            ],
        ),
    )
    folio_holdings = {}
    mapper.collect_mrk_statement_notes(record, folio_holdings, "1")
    assert len(folio_holdings["notes"]) == 1
    assert len(folio_holdings["notes"][0]['note'].split("\n")) == 3
    assert folio_holdings["notes"][0]['note'] == (
        "=853  \\\\$81$av.$bno.$i(year)\n"
        "=863  \\\\$81.1$a1-16$b1-16$i1994-1998\n"
        "=866  \\\\$81.2$av.17no.17 (1999)-v.32no.32 (2003)"
    )
    assert json.dumps(folio_holdings) == '{"notes": [{"note": "=853  \\\\\\\\$81$av.$bno.$i(year)\\n=863  \\\\\\\\$81.1$a1-16$b1-16$i1994-1998\\n=866  \\\\\\\\$81.2$av.17no.17 (1999)-v.32no.32 (2003)", "holdingsNoteTypeId": "841d1873-015b-4bfb-a69f-6cbb41d925ba", "staffOnly": true}]}'


def test_add_mfhd_as_mrk_note(mapper):
    mapper.task_configuration.include_mfhd_mrk_as_note = True
    with open("./tests/test_data/mfhd/holding.mrc", "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, hide_utf8_warnings=True)
        record = next(reader)
    folio_holdings = {}
    mapper.add_mfhd_as_mrk_note(record, folio_holdings, "1")
    mfhd_str = """=LDR  00183nx  a22000854n 4500\n=001  000000167\n=004  7611780\\\\\\\\\n=005  20190827122500.0\n=008  1601264|00008|||1001|||||0901128\n=852  0\\$bjnlDesk$hQB611$i.C44\n"""
    assert len(folio_holdings["notes"]) == 1
    assert folio_holdings["notes"][0]['note'] == mfhd_str
    assert folio_holdings["notes"][0]['holdingsNoteTypeId'] == "09c1e5c9-6f11-432e-bcbe-b9e733ccce57"
    assert json.dumps(folio_holdings) == '{"notes": [{"note": "=LDR  00183nx  a22000854n 4500\\n=001  000000167\\n=004  7611780\\\\\\\\\\\\\\\\\\n=005  20190827122500.0\\n=008  1601264|00008|||1001|||||0901128\\n=852  0\\\\$bjnlDesk$hQB611$i.C44\\n", "holdingsNoteTypeId": "09c1e5c9-6f11-432e-bcbe-b9e733ccce57", "staffOnly": true}]}'

def test_add_mfhd_as_mrc_note(mapper):
    mapper.task_configuration.include_mfhd_mrc_as_note = True
    with open("./tests/test_data/mfhd/holding.mrc", "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, hide_utf8_warnings=True)
        record = next(reader)
    folio_holdings = {}
    mapper.add_mfhd_as_mrc_note(record, folio_holdings, "1")
    mfhd_str = '00183nx  a22000854n 4500001001000000004001200010005001700022008003300039852002500072\x1e000000167\x1e7611780    \x1e20190827122500.0\x1e1601264|00008|||1001|||||0901128\x1e0 \x1fbjnlDesk\x1fhQB611\x1fi.C44\x1e\x1d'
    assert len(folio_holdings["notes"]) == 1
    assert folio_holdings["notes"][0]['note'] == mfhd_str
    assert folio_holdings["notes"][0]['holdingsNoteTypeId'] == "474120b0-d64e-4a6f-9c9c-e7d3e76f3cf5"
    assert json.dumps(folio_holdings) == '{"notes": [{"note": "00183nx  a22000854n 4500001001000000004001200010005001700022008003300039852002500072\\u001e000000167\\u001e7611780    \\u001e20190827122500.0\\u001e1601264|00008|||1001|||||0901128\\u001e0 \\u001fbjnlDesk\\u001fhQB611\\u001fi.C44\\u001e\\u001d", "holdingsNoteTypeId": "474120b0-d64e-4a6f-9c9c-e7d3e76f3cf5", "staffOnly": true}]}'


def test_collect_mrk_statement_notes_false(mapper):
    mapper.task_configuration.include_mrk_statements = False
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="853",
            subfields=[
                pymarc.Subfield(code="8", value="1"),
                pymarc.Subfield(code="a", value="v."),
                pymarc.Subfield(code="b", value="no."),
                pymarc.Subfield(code="i", value="(year)"),
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="863",
            subfields=[
                pymarc.Subfield(code="8", value="1.1"),
                pymarc.Subfield(code="a", value="1-16"),
                pymarc.Subfield(code="b", value="1-16"),
                pymarc.Subfield(code="i", value="1994-1998"),
            ],
        ),
    )
    record.add_field(
        pymarc.Field(
            tag="866",
            subfields=[
                pymarc.Subfield(code="8", value="1.2"),
                pymarc.Subfield(code="a", value="v.17no.17 (1999)-v.32no.32 (2003)"),
            ],
        ),
    )
    folio_holdings = {}
    mapper.collect_mrk_statement_notes(record, folio_holdings, "1")
    assert "notes" not in folio_holdings


def test_add_mfhd_as_mrk_note_false(mapper):
    mapper.task_configuration.include_mfhd_mrk_as_note = False
    with open("./tests/test_data/mfhd/holding.mrc", "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, hide_utf8_warnings=True)
        record = next(reader)
    folio_holdings = {}
    mapper.add_mfhd_as_mrk_note(record, folio_holdings, "1")
    assert "notes" not in folio_holdings


def test_add_mfhd_as_mrc_note_false(mapper):
    mapper.task_configuration.include_mfhd_mrc_as_note = False
    with open("./tests/test_data/mfhd/holding.mrc", "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, hide_utf8_warnings=True)
        record = next(reader)
    folio_holdings = {}
    mapper.add_mfhd_as_mrc_note(record, folio_holdings, "1")
    assert "notes" not in folio_holdings


def test_statistical_code_map_from_single_marc_field_single_subfield(mapper):
    mapper.task_configuration.statistical_code_mapping_fields = ["998$a"]
    file_def = Mock(spec=FileDefinition)
    file_def.statistical_code = ""
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="998",
            subfields=[
                pymarc.Subfield(code="a", value="arch^-^audstream"),
            ],
        )
    )
    folio_holdings = {}
    mapper.map_statistical_codes(folio_holdings, file_def, record)
    assert len(folio_holdings["statisticalCodeIds"]) == 2
    assert "998_a:arch" in folio_holdings["statisticalCodeIds"]
    assert "998_a:audstream" in folio_holdings["statisticalCodeIds"]


def test_statistical_code_map_from_single_marc_field_single_subfield_repeated_single_code(mapper):
    mapper.task_configuration.statistical_code_mapping_fields = ["998$a"]
    file_def = Mock(spec=FileDefinition)
    file_def.statistical_code = ""
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="998",
            subfields=[
                pymarc.Subfield(code="a", value="arch"),
                pymarc.Subfield(code="a", value="audstream"),
            ],
        )
    )
    folio_holdings = {}
    mapper.map_statistical_codes(folio_holdings, file_def, record)
    assert len(folio_holdings["statisticalCodeIds"]) == 2
    assert "998_a:arch" in folio_holdings["statisticalCodeIds"]
    assert "998_a:audstream" in folio_holdings["statisticalCodeIds"]


def test_statistical_code_map_from_one_field_no_subfields_non_control_to_id(mapper):
    mapper.task_configuration.statistical_code_mapping_fields = ["990"]
    file_def = Mock(spec=FileDefinition)
    file_def.statistical_code = ""
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="990",
            subfields=[
                pymarc.Subfield(code="a", value="xoclc"),
            ],
        )
    )
    folio_holdings = {}
    mapper.map_statistical_codes(folio_holdings, file_def, record)
    assert len(folio_holdings["statisticalCodeIds"]) == 1
    assert "990:xoclc" in folio_holdings["statisticalCodeIds"]
    mapper.map_statistical_code_ids("1", folio_holdings)
    assert len(folio_holdings["statisticalCodeIds"]) == 1
    assert "264c4f94-1538-43a3-8b40-bed68384b31b" in folio_holdings["statisticalCodeIds"]


def test_statistical_code_map_from_single_marc_field_multiple_subfields_to_stat_code_id(mapper):
    mapper.task_configuration.statistical_code_mapping_fields = ["998$a$b$c", "003"]
    file_def = Mock(spec=FileDefinition)
    file_def.statistical_code = ""
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="998",
            subfields=[
                pymarc.Subfield(code="a", value="arch^-^audstream"),
                pymarc.Subfield(code="b", value="ebooks"),
                pymarc.Subfield(code="c", value="compfiles"),
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="003",
            data="FoD",
        )
    )
    folio_holdings = {}
    mapper.map_statistical_codes(folio_holdings, file_def, record)
    assert len(folio_holdings["statisticalCodeIds"]) == 5
    assert "998_a:arch" in folio_holdings["statisticalCodeIds"]
    assert "998_a:audstream" in folio_holdings["statisticalCodeIds"]
    assert "998_b:ebooks" in folio_holdings["statisticalCodeIds"]
    assert "998_c:compfiles" in folio_holdings["statisticalCodeIds"]
    assert "003:FoD" in folio_holdings["statisticalCodeIds"]
    mapper.map_statistical_code_ids("1", folio_holdings)
    assert len(folio_holdings["statisticalCodeIds"]) == 5
    assert "b6b46869-f3c1-4370-b603-29774a1e42b1" in folio_holdings["statisticalCodeIds"]
    assert "e10796e0-a594-47b7-b748-3a81b69b3d9b" in folio_holdings["statisticalCodeIds"]
    assert "9d8abbe2-1a94-4866-8731-4d12ac09f7a8" in folio_holdings["statisticalCodeIds"]
    assert "bb76b1c1-c9df-445c-8deb-68bb3580edc2" in folio_holdings["statisticalCodeIds"]
    assert "6d584d0e-3dbc-46c4-a1bd-e9238dd9a6be" in folio_holdings["statisticalCodeIds"]


def test_statistical_code_map_with_unmapped_codes(mapper, caplog):
    mapper.task_configuration.statistical_code_mapping_fields = ["998$a$b$c", "003"]
    file_def = Mock(spec=FileDefinition)
    file_def.statistical_code = ""
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="998",
            subfields=[
                pymarc.Subfield(code="a", value="arch^-^audstream"),
                pymarc.Subfield(code="b", value="ebooks"),
                pymarc.Subfield(code="c", value="compfiles"),
                pymarc.Subfield(code="c", value="unmapped"),
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="003",
            data="FoD",
        )
    )
    folio_holdings = {}
    caplog.set_level(logging.DEBUG)
    mapper.map_statistical_codes(folio_holdings, file_def, record)
    assert len(folio_holdings["statisticalCodeIds"]) == 6
    assert "998_a:arch" in folio_holdings["statisticalCodeIds"]
    assert "998_a:audstream" in folio_holdings["statisticalCodeIds"]
    assert "998_b:ebooks" in folio_holdings["statisticalCodeIds"]
    assert "998_c:compfiles" in folio_holdings["statisticalCodeIds"]
    assert "003:FoD" in folio_holdings["statisticalCodeIds"]
    assert "998_c:unmapped" in folio_holdings["statisticalCodeIds"]
    mapper.map_statistical_code_ids("1", folio_holdings)
    assert len(folio_holdings["statisticalCodeIds"]) == 5
    assert "b6b46869-f3c1-4370-b603-29774a1e42b1" in folio_holdings["statisticalCodeIds"]
    assert "e10796e0-a594-47b7-b748-3a81b69b3d9b" in folio_holdings["statisticalCodeIds"]
    assert "9d8abbe2-1a94-4866-8731-4d12ac09f7a8" in folio_holdings["statisticalCodeIds"]
    assert "bb76b1c1-c9df-445c-8deb-68bb3580edc2" in folio_holdings["statisticalCodeIds"]
    assert "6d584d0e-3dbc-46c4-a1bd-e9238dd9a6be" in folio_holdings["statisticalCodeIds"]
    assert "Statistical code '998_c:unmapped' not found in FOLIO" in "".join(caplog.messages)


def test_set_instance_id_by_map_success():
    # Setup
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="004", data="B123"))
    legacy_ids = ["legacy1"]
    folio_holding = {"formerIds": []}
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.bib_id_template = ""
    mock_mapper.parent_id_map = {"B123": ("B123", "instance-uuid", "in00000000001")}

    RulesMapperHoldings.set_instance_id_by_map(mock_mapper, legacy_ids, folio_holding, record)

    assert folio_holding["instanceId"] == "instance-uuid"
    assert "B123" in folio_holding["formerIds"]


def test_set_instance_id_by_map_missing_004():
    record = pymarc.Record()
    legacy_ids = ["legacy1"]
    folio_holding = {"formerIds": []}
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.bib_id_template = ""
    mock_mapper.parent_id_map = {}

    with pytest.raises(TransformationProcessError) as excinfo:
        RulesMapperHoldings.set_instance_id_by_map(mock_mapper, legacy_ids, folio_holding, record)
    assert "No 004 in record" in str(excinfo.value)


def test_set_instance_id_by_map_multiple_004(caplog):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="004", data="B123"))
    record.add_field(pymarc.Field(tag="004", data="B456"))
    legacy_ids = ["legacy1"]
    folio_holding = {"formerIds": []}
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.bib_id_template = ""
    mock_mapper.parent_id_map = {"B123": ("B123", "instance-uuid", "in00000000001")}
    RulesMapperHoldings.set_instance_id_by_map(mock_mapper, legacy_ids, folio_holding, record)
    assert folio_holding["instanceId"] == "instance-uuid"
    assert "More than one linked bib (004) found in record" in "".join(caplog.messages)


def test_set_instance_id_by_map_instance_id_not_in_map():
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="004", data="B999"))
    legacy_ids = ["legacy1"]
    folio_holding = {"formerIds": []}
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.bib_id_template = ""
    mock_mapper.parent_id_map = {}

    with pytest.raises(Exception) as excinfo:
        RulesMapperHoldings.set_instance_id_by_map(mock_mapper, legacy_ids, folio_holding, record)
    assert "Old instance id not in map" in str(excinfo.value)
