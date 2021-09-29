# content of test_sample.py
from marc_to_folio.mapping_file_transformation import mapper_base
from marc_to_folio.mapping_file_transformation.ref_data_mapping import RefDataMapping
from unittest.mock import Mock, patch


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
        "marc_to_folio.mapping_file_transformation.ref_data_mapping.RefDataMapping"
    ) as mock_rdm:
        instance = mock_rdm.return_value
        instance.hybrid_mappings = mappings
        instance.mapped_legacy_keys = ["location", "loan_type", "material_type"]
        res = mapper_base.MapperBase.get_hybrid_mapping(legacy_object, instance)
        assert res == mappings[1]


def test_get_hybrid_mapping2():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "*"},
        {"location": "*", "loan_type": "*", "material_type": "mt_1"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {"location": "l_1", "loan_type": "lt_1", "material_type": "mt_1"}
    with patch(
        "marc_to_folio.mapping_file_transformation.ref_data_mapping.RefDataMapping"
    ) as mock_rdm:
        instance = mock_rdm.return_value

        instance.mapped_legacy_keys = ["location", "loan_type", "material_type"]
        instance.hybrid_mappings = mappings
        res = mapper_base.MapperBase.get_hybrid_mapping(legacy_object, instance)
        assert res == mappings[1]
