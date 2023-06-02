from unittest.mock import Mock

from pymarc import Field
from pymarc import Subfield

from folio_migration_tools.marc_rules_transformation.conditions import Conditions


def test_condition_trim_period():
    mock = Mock(spec=Conditions)
    res = Conditions.condition_trim_period(None, mock, "value with period.", None, None)
    assert res == "value with period"


def test_condition():
    mock = Mock(spec=Conditions)
    parameter = {"subfieldsToConcat": ["q"]}
    legacy_id = "legacy_id"
    marc_field = Field(
        tag="245",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="a", value="value"),
            Subfield(code="b", value="from journeyman to master /"),
            Subfield(code="q", value="stuff to concatenate"),
        ],
    )
    res = Conditions.condition_concat_subfields_by_name(
        legacy_id, mock, "value", parameter, marc_field
    )
    assert res == "value stuff to concatenate"
