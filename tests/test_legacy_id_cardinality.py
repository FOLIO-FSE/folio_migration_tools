"""Pin down when the MARC mappers return more than one legacy id for a record.

The report store normalizes single-member lists to a bare id and keeps multi-member
lists in a side table, so these cases document which mapper shapes need that.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pymarc import Field, Record, Subfield

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)


def holdings_ids(marc_path: str, record: Record) -> list:
    stub = SimpleNamespace(task_configuration=SimpleNamespace(legacy_id_marc_path=marc_path))
    return RulesMapperHoldings.get_legacy_ids(stub, record, 0)


def record_with(*fields: Field) -> Record:
    record = Record()
    for field in fields:
        record.add_field(field)
    return record


def data_field(tag: str, value: str) -> Field:
    return Field(tag=tag, indicators=[" ", " "], subfields=[Subfield("a", value)])


def test_control_field_path_gives_single_id():
    record = record_with(Field(tag="001", data="h100"))
    assert holdings_ids("001", record) == ["h100"]


def test_repeated_data_field_gives_multiple_ids():
    record = record_with(data_field("852", "b1"), data_field("852", "b2"))
    assert holdings_ids("852$a", record) == ["b1", "b2"]


def test_single_data_field_gives_single_member_list():
    record = record_with(data_field("852", "b1"))
    assert holdings_ids("852$a", record) == ["b1"]


def test_missing_id_fails_the_record():
    with pytest.raises(TransformationRecordFailedError):
        holdings_ids("852$a", record_with(data_field("245", "title")))


def bib_ids(flavour: str, record: Record, **config) -> list:
    stub = SimpleNamespace(
        task_configuration=SimpleNamespace(ils_flavour=flavour, **config),
        migration_report=Mock(),
    )
    stub.get_aleph_bib_id = lambda rec: BibsRulesMapper.get_aleph_bib_id(stub, rec)
    return BibsRulesMapper.get_legacy_ids(stub, record, 0)


def sub_field(tag: str, *pairs: tuple) -> Field:
    return Field(tag=tag, indicators=[" ", " "], subfields=[Subfield(c, v) for c, v in pairs])


def test_bib_flavours_with_a_single_id():
    record = record_with(Field(tag="001", data="b1"), sub_field("907", ("a", "s1")))
    assert bib_ids("sierra", record) == ["s1"]
    assert bib_ids("tag001", record) == ["b1"]
    assert bib_ids("custom", record, custom_bib_id_field="907$a") == ["s1"]


def test_907y_flavour_can_return_two_ids():
    record = record_with(sub_field("907", ("a", "a1"), ("y", "y1")))
    assert sorted(bib_ids("tag907y", record)) == ["a1", "y1"]


def test_990a_flavour_returns_990a_values_and_the_001():
    record = record_with(
        Field(tag="001", data="b1"), sub_field("990", ("a", "x1")), sub_field("990", ("a", "x2"))
    )
    assert sorted(bib_ids("tagf990a", record)) == ["b1", "x1", "x2"]


def test_aleph_flavour_returns_every_998b_and_otherwise_the_001():
    record = record_with(
        Field(tag="001", data="b1"), sub_field("998", ("b", "k1")), sub_field("998", ("b", "k2"))
    )
    assert bib_ids("aleph", record) == ["k1", "k2"]
    assert bib_ids("aleph", record_with(Field(tag="001", data="b1"))) == ["b1"]
