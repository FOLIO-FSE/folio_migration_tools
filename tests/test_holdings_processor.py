from pymarc import Field, Record

from migration_tools.marc_rules_transformation.holdings_processor import (
    HoldingsProcessor,
)


def test_get_legacy_id_001():
    record = Record()
    record.add_field(Field(tag="001", data="0001"))
    legacy_id = HoldingsProcessor.get_legacy_id("001", record)
    assert legacy_id == "0001"


def test_get_legacy_id_001_wrong_order():
    record = Record()
    record.add_field(Field(tag="001", data="0001"))
    record.add_field(
        Field(
            tag="951",
            subfields=["b", "bid"],
        )
    )
    record.add_field(
        Field(
            tag="951",
            subfields=["c", "cid"],
        )
    )
    record.add_field(Field(tag="001", data="0001"))
    legacy_id = HoldingsProcessor.get_legacy_id("951$c", record)
    assert legacy_id == "cid"


def test_get_legacy_id_001_right_order():
    record = Record()
    record.add_field(Field(tag="001", data="0001"))
    record.add_field(
        Field(
            tag="951",
            subfields=["c", "cid"],
        )
    )
    record.add_field(
        Field(
            tag="951",
            subfields=["b", "bid"],
        )
    )
    record.add_field(Field(tag="001", data="0001"))
    legacy_id = HoldingsProcessor.get_legacy_id("951$c", record)
    assert legacy_id == "cid"
