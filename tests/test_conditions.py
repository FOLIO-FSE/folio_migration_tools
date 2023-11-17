from unittest.mock import Mock

from folioclient import FolioClient
from pymarc import Field
from pymarc import Subfield

from folio_migration_tools.marc_rules_transformation.conditions import Conditions


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
