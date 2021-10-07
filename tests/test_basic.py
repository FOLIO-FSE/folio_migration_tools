# content of test_sample.py
from migration_tools.mapping_file_transformation import mapping_file_mapper_base
from migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.mapping_file_transformation.ref_data_mapping import RefDataMapping
from unittest.mock import Mock, patch
import pymarc

from migration_tools.report_blurbs import Blurbs


def func(x):
    return x + 1


def test_answer2():
    assert func(4) == 5


def test_is_hybrid_default_mapping():
    mappings = [{"location": "*", "loan_type": "*", "material_type": "*"}]
    mock = Mock(spec=RefDataMapping)
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.is_hybrid_default_mapping(mock, mappings[0])
    assert res == False


def test_get_hybrid_mapping():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "*"},
        {"location": "l_1", "loan_type": "*", "material_type": "mt_1"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {"location": "l_1", "loan_type": "lt_1", "material_type": "mt_1"}
    with patch(
        "migration_tools.mapping_file_transformation.ref_data_mapping.RefDataMapping"
    ) as mock_rdm:
        instance = mock_rdm.return_value
        instance.hybrid_mappings = mappings
        instance.mapped_legacy_keys = ["location", "loan_type", "material_type"]
        res = mapping_file_mapper_base.MappingFileMapperBase.get_hybrid_mapping(
            legacy_object, instance
        )
        assert res == mappings[1]


def test_get_hybrid_mapping2():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "*"},
        {"location": "l_1", "loan_type": "*", "material_type": "mt_1"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {"location": "l_2", "loan_type": "apa", "material_type": "papa"}
    with patch(
        "migration_tools.mapping_file_transformation.ref_data_mapping.RefDataMapping"
    ) as mock_rdm:
        instance = mock_rdm.return_value
        instance.hybrid_mappings = mappings
        instance.mapped_legacy_keys = ["location", "loan_type", "material_type"]
        res = mapping_file_mapper_base.MappingFileMapperBase.get_hybrid_mapping(
            legacy_object, instance
        )
        assert res == mappings[0]


def test_get_hybrid_mapping3():
    mappings = [
        {"location": "sprad", "loan_type": "*", "material_type": "*"},
        {"location": "L_1", "loan_type": "Lt1", "material_type": "Mt_1"},
        {"location": "l_1", "loan_type": "lt1 ", "material_type": "mt2"},
    ]
    legacy_object = {"location": "sprad", "loan_type": "0", "material_type": "0"}
    with patch(
        "migration_tools.mapping_file_transformation.ref_data_mapping.RefDataMapping"
    ) as mock_rdm:
        instance = mock_rdm.return_value
        instance.mapped_legacy_keys = ["location", "loan_type", "material_type"]
        instance.hybrid_mappings = mappings
        res = mapping_file_mapper_base.MappingFileMapperBase.get_hybrid_mapping(
            legacy_object, instance
        )
        assert res == mappings[0]


def test_normal_refdata_mapping_strip():
    mappings = [
        {"location": "l_2", "loan_type": "lt2", "material_type": "mt_1"},
        {"location": "L_1", "loan_type": "Lt1", "material_type": "Mt_1"},
        {"location": "l_1", "loan_type": "lt1 ", "material_type": "mt2"},
    ]
    legacy_object = {"location": "l_1 ", "loan_type": "lt1", "material_type": "mt2"}
    with patch(
        "migration_tools.mapping_file_transformation.ref_data_mapping.RefDataMapping"
    ) as mock_rdm:
        instance = mock_rdm.return_value
        instance.mapped_legacy_keys = ["location", "loan_type", "material_type"]
        instance.regular_mappings = mappings
        res = mapping_file_mapper_base.MappingFileMapperBase.get_ref_data_mapping(
            legacy_object, instance
        )
        assert res == mappings[2]


def test_blurbs():
    b = Blurbs.Introduction
    print(b)
    assert b[0] == "Introduction"


def test_get_marc_record():
    file_path = "./tests/test_data/default/test_get_record.xml"
    record = pymarc.parse_xml_to_array(file_path)[0]
    assert record["001"].value() == "21964516"


def test_get_marc_textual_stmt():
    file_path = "./tests/test_data/default/test_mfhd_holdings_statements.xml"
    record = pymarc.parse_xml_to_array(file_path)[0]
    res = HoldingsStatementsParser.get_holdings_statements(record, "853", "863", "866")
    # print(json.dumps(res, indent=4))
    stmt = "v.1:no. 1(1943:July 3)-v.1:no.52(1944:June 24)"
    stmt2 = "Some statement without note"
    stmt3 = "v.29 (2011)"
    stmt4 = "v.1 (1948)-v.27 (2007)"
    assert any(res["statements"])
    assert any(stmt in f["statement"] for f in res["statements"])
    assert any(stmt3 in f["statement"] for f in res["statements"])
    assert any(stmt4 in f["statement"] for f in res["statements"])
    assert any("Some note" in f["note"] for f in res["statements"])
    assert any(stmt2 in f["statement"] for f in res["statements"])
    assert any("Missing linked fields for 853" in f[1] for f in res["migration_report"])
