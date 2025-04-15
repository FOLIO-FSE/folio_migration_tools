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
from folio_migration_tools.test_infrastructure import mocked_classes


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> RulesMapperHoldings:
    folio = mocked_classes.mocked_folio_client()
    lib = LibraryConfiguration(
        okapi_url=folio.okapi_url,
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
    )
    parent_id_map: dict[str, tuple] = {}
    location_map = [
        {"legacy_code": "jnlDesk", "folio_code": "KU/CC/DI/2"},
        {"legacy_code": "*", "folio_code": "KU/CC/DI/2"},
    ]
    mapper = RulesMapperHoldings(folio, location_map, conf, lib, parent_id_map, [])
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
    with pytest.raises(TransformationProcessError):
        mocked_mapper = Mock(spec=RulesMapperHoldings)
        file_mock = [{}]
        RulesMapperHoldings.setup_boundwith_relationship_map(mocked_mapper, file_mock)


def test_setup_boundwith_relationship_map_missing_entries_2():
    mocked_mapper = Mock(spec=RulesMapperHoldings)
    file_mock = []
    res = RulesMapperHoldings.setup_boundwith_relationship_map(mocked_mapper, file_mock)
    assert res == {}


def test_setup_boundwith_relationship_map_empty_entries():
    with pytest.raises(TransformationProcessError):
        mocked_mapper = Mock(spec=RulesMapperHoldings)
        file_mock = [{"MFHD_ID": "", "BIB_ID": ""}]
        RulesMapperHoldings.setup_boundwith_relationship_map(mocked_mapper, file_mock)


def test_setup_boundwith_relationship_map_with_entries():
    mocked_mapper = Mock(spec=RulesMapperHoldings)
    mocked_mapper.folio_client = mocked_classes.mocked_folio_client()
    mocked_mapper.parent_id_map = {"B1": [], "B2": [], "B3": [], "B4": []}

    file_mock = [
        {"MFHD_ID": "H1", "BIB_ID": "B1"},
        {"MFHD_ID": "H1", "BIB_ID": "B2"},
        {"MFHD_ID": "H2", "BIB_ID": "B3"},
        {"MFHD_ID": "H2", "BIB_ID": "B4"},
    ]
    res = RulesMapperHoldings.setup_boundwith_relationship_map(mocked_mapper, file_mock)
    assert len(res) == 2
    assert res["66db04ef-fbfb-5c45-9ed7-65a1f2495eaf"] == [
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
        okapi_url=folio.okapi_url,
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
        okapi_url=folio.okapi_url,
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
