from operator import le
from unittest.mock import Mock, create_autospec, patch

from folioclient import FolioClient
from pymarc import Field, Indicators, Subfield

from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from tests.test_rules_mapper_base import folio_client


def test_condition_trim_period():
    mock = Mock(spec=Conditions)
    res = Conditions.condition_trim_period(None, mock, "value with period.", None, None)
    assert res == "value with period"


def test_condition_trim_punctuation():
    mock = Mock(spec=Conditions)
    res = Conditions.condition_trim_punctuation(None, mock, "Rockefeller, John D.", None, None)
    res2 = Conditions.condition_trim_punctuation(
        None, mock, "Rockefeller, John D., 1893-, ", None, None
    )
    res3 = Conditions.condition_trim_punctuation(None, mock, "Rockefeller, John.  ", None, None)
    res4 = Conditions.condition_trim_punctuation(None, mock, "Rockefeller, John D.,", None, None)
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