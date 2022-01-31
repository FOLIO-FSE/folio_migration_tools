from unittest.mock import Mock
from pymarc import Field

from migration_tools.marc_rules_transformation.conditions import Conditions


def test_is_hybrid_default_mapping():

    mock = Mock(spec=Conditions)
    parameter = {"subfieldsToConcat": ["q"]}
    marc_field = Field(
        tag="245",
        indicators=["0", "1"],
        subfields=[
            "a",
            "value",
            "b",
            "from journeyman to master /",
            "q",
            "stuff to concatenate",
        ],
    )
    res = Conditions.condition_concat_subfields_by_name(
        mock, "value", parameter, marc_field
    )
    assert res == "value stuff to concatenate"
