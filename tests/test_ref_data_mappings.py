from unittest.mock import Mock

from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
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


def test_get_hybrid_mapping3_5():
    mappings = [
        {"location": "l_2", "loan_type": "*", "material_type": "papa"},
        {"location": "l_1", "loan_type": "lt_1", "material_type": "*"},
        {"location": "l_1", "loan_type": "*", "material_type": "*"},
    ]
    legacy_object = {"location": "l_1", "loan_type": "lt_44", "material_type": "papa"}
    mock = Mock(spec=RefDataMapping)
    mock.hybrid_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_hybrid_mapping(mock, legacy_object)
    assert res == mappings[2]


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
    mock.hybrid_mappings = [{"location": "sprad", "loan_type": "* ", "material_type": "*"}]
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


def test_mapping_for_multiple_fields():
    mappings = [
        {
            "email1_categories": "tspt",
            "email2_categories": "*",
            "folio_value": "Technical Support",
        },
        {"email1_categories": "sls", "email2_categories": "*", "folio_value": "Sales"},
        {
            "email1_categories": "*",
            "email2_categories": "tspt",
            "folio_value": "Technical Support",
        },
        {"email1_categories": "*", "email2_categories": "sls", "folio_value": "Sales"},
        {"email1_categories": "*", "email2_categories": "*", "folio_value": "General"},
    ]

    legacy_object = {
        "EMAIL": "email1@abebooks.com",
        "email1_categories": "sls",
        "EMAIL2": "email2@abebooks.com",
        "email2_categories": "tspt",
    }

    mock = Mock(spec=RefDataMapping)
    mock.regular_mappings = [
        {
            "email1_categories": "heja",
            "email2_categories": "Sverige",
            "folio_value": "Nationalizm",
        },
    ]
    mock.hybrid_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["email1_categories", "email2_categories"]
    res = RefDataMapping.get_hybrid_mapping(mock, legacy_object)
    assert res == mappings[1]

def test_multiple_emails_array_objects_with_different_ref_data_mappings("The code doesn't handle this case."):
    # This test demonstrates that, when you have multiple mapped fields,
    # the ref data mapping for the same legacy value will
    # differ depending on the order of the rows in the ref data mapping

    # See def test_multiple_emails_array_objects in organization_mapper.py
    # to see what happens when you have multiple array items with different
    # mapped reference data values and where some ref data mapping rows
    # effectively become unreachable as all properties are taken into account
    # for all array items

    
    # The source data contains two email objects
    # The FOLIO category of the email1 object should always be "Sales"
    # The FOLIO category of the email2 object should always be "Technical Support"
    legacy_object = {
        "EMAIL": "email1@abebooks.com",
        "email1_categories": "sls",
        "EMAIL2": "email2@abebooks.com",
        "email2_categories": "tspt",
    }

    # In this ref data mapping, the first row which matches the condition
    # email1_categories == sls/* and email2_categories == tsp/*
    # is row number 2, which maps to Sales
    mapping_a = [
        {
            "email1_categories": "tspt",
            "email2_categories": "*",
            "folio_value": "Technical Support",
        },
        {"email1_categories": "sls", "email2_categories": "*", "folio_value": "Sales"},
        {
            "email1_categories": "*",
            "email2_categories": "tspt",
            "folio_value": "Technical Support",
        },
        {"email1_categories": "*", "email2_categories": "sls", "folio_value": "Sales"},
        {"email1_categories": "*", "email2_categories": "*", "folio_value": "General"},
    ]

    # In this ref data mapping, the first row which matches the condition
    # email1_categories == sls/* and email2_categories == tsp/*
    # is row number 3, which maps to Support
    mapping_b = [
        {
            "email1_categories": "tspt",
            "email2_categories": "*",
            "folio_value": "Technical Support",
        },
        {
            "email1_categories": "*",
            "email2_categories": "tspt",
            "folio_value": "Technical Support",
        },
        {"email1_categories": "sls", "email2_categories": "*", "folio_value": "Sales"},
        {"email1_categories": "*", "email2_categories": "sls", "folio_value": "Sales"},
        {"email1_categories": "*", "email2_categories": "*", "folio_value": "General"},
    ]


    mock = Mock(spec=RefDataMapping)

    mock.hybrid_mappings = mapping_a
    mock.cache = {}
    mock.mapped_legacy_keys = ["email1_categories", "email2_categories"]
    res_1 = RefDataMapping.get_hybrid_mapping(mock, legacy_object)

    mock.hybrid_mappings = mapping_b
    mock.cache = {}
    mock.mapped_legacy_keys = ["email1_categories", "email2_categories"]
    res_2 = RefDataMapping.get_hybrid_mapping(mock, legacy_object)

    assert res_1 == res_2
