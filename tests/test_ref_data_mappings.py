from unittest.mock import Mock, patch

from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.mapping_file_transformation import (
    mapping_file_mapper_base,
)


def test_is_hybrid_default_mapping():
    mappings = [{"location": "*", "loan_type": "*", "material_type": "*"}]
    mock = Mock(spec=RefDataMapping)
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.is_hybrid_default_mapping(mock, mappings[0])
    assert res is False


def test_get_hybrid_mapping():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "*"},
        {"location": "l_1", "loan_type": "*", "material_type": "mt_1"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {"location": "l_1", "loan_type": "lt_1", "material_type": "mt_1"}
    mock = Mock(spec=RefDataMapping)
    mock.hybrid_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_hybrid_mapping(mock, legacy_object)
    assert res == mappings[1]


def test_get_hybrid_mapping2():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "*"},
        {"location": "l_1", "loan_type": "*", "material_type": "mt_1"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {"location": "l_2", "loan_type": "apa", "material_type": "papa"}
    mock = Mock(spec=RefDataMapping)
    mock.hybrid_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_hybrid_mapping(mock, legacy_object)
    assert res == mappings[0]


def test_get_hybrid_mapping3_4():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "papa"},
        {"location": "l_1", "loan_type": "lt_1", "material_type": "*"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {"location": "l_1", "loan_type": "lt_1", "material_type": "papa"}
    mock = Mock(spec=RefDataMapping)
    mock.hybrid_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_hybrid_mapping(mock, legacy_object)
    assert res == mappings[1]


def test_get_hybrid_mapping_none():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "papa"},
        {"location": "l_1", "loan_type": "lt_1", "material_type": "*"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
        # {"location": "*", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {
        "location": "l_55",
        "loan_type": "lt_55",
        "material_type": "papapp",
    }
    mock = Mock(spec=RefDataMapping)
    mock.hybrid_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_hybrid_mapping(mock, legacy_object)
    assert res is None


def test_get_hybrid_mapping3():
    mappings = [
        {"location": "L_1", "loan_type": "Lt1", "material_type": "Mt_1"},
        {"location": "l_1", "loan_type": "lt1 ", "material_type": "mt2"},
    ]
    legacy_object = {"location": "sprad", "loan_type": "0", "material_type": "0 "}
    mock = Mock(spec=RefDataMapping)
    mock.regular_mappings = mappings
    mock.hybrid_mappings = [
        {"location": "sprad", "loan_type": "* ", "material_type": "*"}
    ]
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_hybrid_mapping(mock, legacy_object)
    assert res is None


def test_normal_refdata_mapping_strip():
    mappings = [
        {"location": "l_2", "loan_type": "lt2", "material_type": "mt_1"},
        {"location": "L_1", "loan_type": "Lt1", "material_type": "Mt_1"},
        {"location": "l_1", "loan_type": "lt1", "material_type": "mt2"},
    ]
    legacy_object = {"location": "l_1 ", "loan_type": "lt1", "material_type": "mt2 "}
    mock = Mock(spec=RefDataMapping)
    mock.regular_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_ref_data_mapping(mock, legacy_object)
    assert res == mappings[2]
