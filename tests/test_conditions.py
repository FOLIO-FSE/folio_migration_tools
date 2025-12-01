from unittest.mock import Mock, create_autospec, patch

import pytest
from folioclient import FolioClient
from pymarc import Field, Indicators, Subfield

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import RulesMapperBase
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
# from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
#     RulesMapperHoldings,
# )
from folio_migration_tools.migration_report import MigrationReport
from tests.test_rules_mapper_base import folio_client as folio_client_fixture
from .test_infrastructure.mocked_classes import mocked_folio_client


def test_condition_trim_period():
    mock = Mock(spec=Conditions)
    res = Conditions.condition_trim_period(mock, None, "value with period.", None, None)
    assert res == "value with period"


def test_condition_trim_punctuation():
    mock = Mock(spec=Conditions)
    res = Conditions.condition_trim_punctuation(mock, None, "Rockefeller, John D.", None, None)
    res2 = Conditions.condition_trim_punctuation(
        mock, None, "Rockefeller, John D., 1893-, ", None, None
    )
    res3 = Conditions.condition_trim_punctuation(mock, None, "Rockefeller, John.  ", None, None)
    res4 = Conditions.condition_trim_punctuation(mock, None, "Rockefeller, John D.,", None, None)
    assert res == "Rockefeller, John D."
    assert res2 == "Rockefeller, John D., 1893-"
    assert res3 == "Rockefeller, John"
    assert res4 == "Rockefeller, John D."


def test_condition_concat_subfields_by_name():
    mock = Mock(spec=Conditions)
    parameter = {"subfieldsToConcat": ["q"], "subfieldsToStopConcat": ["z"]}
    legacy_id = "legacy_id"
    marc_field = Field(
        tag="245",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="a", value="value"),
            Subfield(code="b", value="from journeyman to master /"),
            Subfield(code="q", value="stuff to concatenate"),
            Subfield(code="z", value="stop here"),
            Subfield(code="q", value="more stuff to concatenate"),
        ],
    )
    res = Conditions.condition_concat_subfields_by_name(
        mock, legacy_id, "value", parameter, marc_field
    )
    assert res == "value stuff to concatenate"


def test_condition_set_contributor_type_text():
    mock = Mock(spec=Conditions)
    folio = Mock(spec=FolioClient)
    folio.contributor_types = [{"code": "ed", "name": "editor"}]
    mock.folio = folio
    legacy_id = "legacy_id"
    marc_fields = [
        Field(
            tag="100",
            indicators=["1", ""],
            subfields=[
                Subfield(code="a", value="Schmitt, John Jacob Jingleheimer"),
                Subfield(code="e", value="singer,"),
                Subfield(code="e", value="shouter."),
            ],
        ),
        Field(
            tag="700",
            indicators=["1", ""],
            subfields=[
                Subfield(code="a", value="Scmitt, John Jacob Jingleheimer"),
                Subfield(code="e", value="editor."),
            ],
        ),
    ]
    value_100 = " ".join(marc_fields[0].get_subfields("e"))
    value_700 = " ".join(marc_fields[1].get_subfields("e"))
    res_100 = Conditions.condition_set_contributor_type_text(
        mock, legacy_id, value_100, {}, marc_fields[0]
    )
    assert res_100 == "singer, shouter."
    res_700 = Conditions.condition_set_contributor_type_text(
        mock, legacy_id, value_700, {}, marc_fields[1]
    )
    assert res_700 == "editor"


def test_condition_set_note_staff_only_via_indicator():
    mock = Mock(spec=Conditions)
    mock.mapper = Mock(spec=BibsRulesMapper)
    mock.mapper.migration_report = Mock(spec=MigrationReport)
    legacy_id = "legacy_id"
    marc_field = Field(
        tag="541",
        indicators=["0", "0"],
        subfields=[
            Subfield(code="a", value="Note 1"),
            Subfield(code="b", value="Note 2"),
        ],
    )
    res_true = Conditions.condition_set_note_staff_only_via_indicator(
        mock, legacy_id, "value", {}, marc_field
    )
    assert res_true == "true"

    marc_field.indicators = ["1", "0"]
    res_false = Conditions.condition_set_note_staff_only_via_indicator(
        mock, legacy_id, "value", {}, marc_field
    )
    assert res_false == "false"


def test_condition_set_subject_type_id():
    mock = create_autospec(Conditions)
    parameter = {"name": "Topical term"}
    mock.mapper = Mock(spec=BibsRulesMapper)
    mock.mapper.migration_report = Mock(spec=MigrationReport)
    # mock.get_ref_data_tuple_by_name = Conditions.get_ref_data_tuple_by_name
    # mock.get_ref_data_tuple = Conditions.get_ref_data_tuple
    mock.folio = Mock(spec=FolioClient)
    mock.folio.subject_types = [{'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b1', 'name': 'Personal name', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b2', 'name': 'Corporate name', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b3', 'name': 'Meeting name', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b4', 'name': 'Uniform title', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b5', 'name': 'Named event', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b6', 'name': 'Chronological term', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b7', 'name': 'Topical term', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b8', 'name': 'Geographic name', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b9', 'name': 'Uncontrolled', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff510', 'name': 'Faceted topical terms', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff511', 'name': 'Genre/form', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff512', 'name': 'Occupation', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff513', 'name': 'Function', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff514', 'name': 'Curriculum objective', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff515', 'name': 'Hierarchical place name', 'source': 'folio'}, {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff516', 'name': 'Type of entity unspecified', 'source': 'folio'}]
    mock.ref_data_dicts = {"subject_types": mock.folio.subject_types}
    legacy_id = "legacy_id"
    marc_field = Field(
        tag="650",
        indicators=["0", "0"],
        subfields=[
            Subfield(code="a", value="Subject 1")
        ],
    )
    with patch.object(mock, "get_ref_data_tuple_by_name", return_value=("d6488f88-1e74-40ce-81b5-b19a928ff5b7", "Topical term")):
        res = Conditions.condition_set_subject_type_id(mock, legacy_id, "", parameter, marc_field)
        assert res == "d6488f88-1e74-40ce-81b5-b19a928ff5b7"

    with patch.object(mock, "get_ref_data_tuple", return_value=("d6488f88-1e74-40ce-81b5-b19a928ff5b7", "Topical term")):
        res = Conditions.get_ref_data_tuple_by_name(mock, mock.folio.subject_types, "subject_types", parameter["name"])
        assert res == ("d6488f88-1e74-40ce-81b5-b19a928ff5b7", "Topical term")

    res = Conditions.get_ref_data_tuple(mock, mock.folio.subject_types, "subject_types", "Topical term", "name")
    assert res == ("d6488f88-1e74-40ce-81b5-b19a928ff5b7", "Topical term")


def test_condition_set_subject_source_id():
    mock = create_autospec(Conditions)
    parameter = {"name": "Library of Congress Subject Headings"}
    mock.mapper = Mock(spec=BibsRulesMapper)
    mock.mapper.migration_report = Mock(spec=MigrationReport)
    mock.folio = Mock(spec=FolioClient)

    # Mock the folio_get_all method to return the desired subject sources
    mock.folio.folio_get_all.return_value = [
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b1', 'name': 'Library of Congress Subject Headings', 'code': 'LCSH', 'source': 'folio'}
    ]
    mock.ref_data_dicts = {"subject_sources": mock.folio.folio_get_all.return_value}
    mock.get_ref_data_tuple_by_name.return_value = Conditions.get_ref_data_tuple(mock, mock.folio.folio_get_all.return_value, "subject_sources", "Library of Congress Subject Headings", "name")

    with patch.object(FolioClient, 'folio_get_all', return_value=mock.folio.folio_get_all.return_value):
        res = Conditions.condition_set_subject_source_id(mock, "legacy_id", "", parameter, Field(
            tag="650",
            indicators=["0", "0"],
            subfields=[
                Subfield(code="a", value="Subject 1")
            ],
        ))
        assert res == "d6488f88-1e74-40ce-81b5-b19a928ff5b1"


def test_condition_set_subject_source_id_no_match():
    mock = create_autospec(Conditions)
    parameter = {"name": "Medical Subject Headings"}
    mock.mapper = Mock(spec=BibsRulesMapper)
    mock.mapper.migration_report = Mock(spec=MigrationReport)
    mock.folio = Mock(spec=FolioClient)

    # Mock the folio_get_all method to return the desired subject sources
    mock.folio.folio_get_all.return_value = [
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b1', 'name': 'Library of Congress Subject Headings', 'code': 'LCSH', 'source': 'folio'}
    ]
    mock.ref_data_dicts = {"subject_sources": mock.folio.folio_get_all.return_value}
    mock.get_ref_data_tuple_by_name.return_value = Conditions.get_ref_data_tuple(mock, mock.folio.folio_get_all.return_value, "subject_sources", "Medical Subject Headings", "name")

    with pytest.raises(TransformationProcessError) as res_error:
        res = Conditions.condition_set_subject_source_id(mock, "legacy_id", "", parameter, Field(
            tag="650",
            indicators=["0", "0"],
            subfields=[
                Subfield(code="a", value="Subject 1")
            ],
        ))
    assert res_error.type is TransformationProcessError
    assert res_error.value.message.startswith("Subject source not found for Medical Subject Headings")


def test_condition_set_subject_source_id_by_code():
    mock = create_autospec(Conditions)
    value = "LCSH"
    mock.mapper = Mock(spec=BibsRulesMapper)
    mock.mapper.migration_report = Mock(spec=MigrationReport)
    mock.folio = Mock(spec=FolioClient)

    # Mock the folio_get_all method to return the desired subject sources
    mock.folio.folio_get_all.return_value = [
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b1', 'name': 'Library of Congress Subject Headings', 'code': 'LCSH', 'source': 'folio'}
    ]
    mock.ref_data_dicts = {"subject_sources": mock.folio.folio_get_all.return_value}
    mock.get_ref_data_tuple_by_code.return_value = Conditions.get_ref_data_tuple(mock, mock.folio.folio_get_all.return_value, "subject_sources", value, "code")

    with patch.object(FolioClient, 'folio_get_all', return_value=mock.folio.folio_get_all.return_value):
        res = Conditions.condition_set_subject_source_id_by_code(mock, "legacy_id", value, None, Field(
            tag="650",
            indicators=["0", "0"],
            subfields=[
                Subfield(code="a", value="Subject 1"),
                Subfield(code="2", value="LCSH")
            ],
        ))
        assert res == "d6488f88-1e74-40ce-81b5-b19a928ff5b1"


def test_condition_set_subject_source_id_by_code_no_match():
    mock = create_autospec(Conditions)
    value = "MeSH"
    mock.mapper = Mock(spec=BibsRulesMapper)
    mock.mapper.migration_report = Mock(spec=MigrationReport)
    mock.folio = Mock(spec=FolioClient)

    # Mock the folio_get_all method to return the desired subject sources
    mock.folio.folio_get_all.return_value = [
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b1', 'name': 'Library of Congress Subject Headings', 'code': 'LCSH', 'source': 'folio'}
    ]
    mock.ref_data_dicts = {"subject_sources": mock.folio.folio_get_all.return_value}
    mock.get_ref_data_tuple_by_code.return_value = Conditions.get_ref_data_tuple(mock, mock.folio.folio_get_all.return_value, "subject_sources", value, "code")

    with pytest.raises(TransformationProcessError) as res_error:
        res = Conditions.condition_set_subject_source_id_by_code(mock, "legacy_id", value, None, Field(
            tag="650",
            indicators=["0", "0"],
            subfields=[
                Subfield(code="a", value="Subject 1"),
                Subfield(code="2", value="MeSH")
            ],
        ).get('2'))
    assert res_error.type is TransformationProcessError
    assert res_error.value.message.startswith("Subject source not found for MeSH")


def test_condition_remove_prefix_by_indicator():
    mock = Mock(spec=Conditions)
    marc_fields = [
        Field(
            tag="100",
            indicators=["1", "0"],
            subfields=[
                Subfield(code="a", value="Subject 1")
            ],
        ),
        Field(
            tag="200",
            indicators=["1", "5"],
            subfields=[
                Subfield(code="b", value="Subject 2")
            ],
        ),
    ]
    res100 = Conditions.condition_remove_prefix_by_indicator(
        mock, "", "some value: /", {}, marc_fields[0]
    )
    assert res100 == "some value"
    res200 = Conditions.condition_remove_prefix_by_indicator(
        mock, "", "some value: /", {}, marc_fields[1]
    )
    assert res200 == "value"


def test_condition_set_electronic_access_relations_id(folio_client_fixture):
    mock = create_autospec(Conditions)
    parameter = {"name": "Component part(s) of resource"}
    mock.mapper = Mock(spec=BibsRulesMapper)
    mock.mapper.migration_report = Mock(spec=MigrationReport)
    mock.folio = folio_client_fixture
    mock.folio.folio_get_all.return_value = [
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b1', 'name': 'Component part(s) of resource', 'source': 'folio'},
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b2', 'name': 'Other version', 'source': 'folio'},
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b3', 'name': 'Version of resource', 'source': 'folio'},
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b4', 'name': 'Related resource', 'source': 'folio'},
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b5', 'name': 'No display constant generated', 'source': 'folio'},
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b6', 'name': 'Resource', 'source': 'folio'},
    ]
    mock.object_type = "holdings"
    mock.ref_data_dicts = {"electronic_access_relations": mock.folio.folio_get_all.return_value}
    mock.get_ref_data_tuple_by_name.return_value = Conditions.get_ref_data_tuple(mock, mock.folio.folio_get_all.return_value, "electronicAccessRelationships", "Component part(s) of resource", "name")

    # Mock the folio_get_all method to return the desired electronic access relations
    mock.folio.folio_get_all.return_value = [
        {'id': 'd6488f88-1e74-40ce-81b5-b19a928ff5b1', 'name': 'Electronic access', 'source': 'folio'}
    ]
    mock.ref_data_dicts = {"electronic_access_relations": mock.folio.folio_get_all.return_value}
    mock.get_ref_data_tuple_by_name.return_value = Conditions.get_ref_data_tuple(mock, mock.folio.folio_get_all.return_value, "electronic_access_relations", "Electronic access", "name")

    with patch.object(FolioClient, 'folio_get_all', return_value=mock.folio.folio_get_all.return_value):
        res = Conditions.condition_set_electronic_access_relations_id(mock, "legacy_id", "", parameter, Field(
            tag="856",
            indicators=["0", "0"],
            subfields=[
                Subfield(code="u", value="http://example.com"),
                Subfield(code="z", value="Electronic access")
            ],
        ))
        assert res == "d6488f88-1e74-40ce-81b5-b19a928ff5b1"

    # mock.object_type = "instance"
    # with patch.object(FolioClient, 'folio_get_all', return_value=mock.folio.folio_get_all.return_value):
    #     res = Conditions.condition_set_electronic_access_relations_id(mock, "legacy_id", "", parameter, Field(
    #         tag="856",
    #         indicators=["0", "0"],
    #         subfields=[
    #             Subfield(code="u", value="http://example.com"),
    #             Subfield(code="z", value="Electronic access")
    #         ],
    #     ))
    #     assert res == "d6488f88-1e74-40ce-81b5-b19a928ff5b6"
