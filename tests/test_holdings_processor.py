from pymarc import Field, Record

from migration_tools.marc_rules_transformation.holdings_processor import (
    HoldingsProcessor,
)


def test_chop_008():
    record = Record()
    record.add_field(Field(tag="008", data="2112204p    8   0001baeng0              "))
    assert len(record["008"].data) == 40
    remain, rest = record["008"].data[:32], record["008"].data[32:]
    assert len(remain) == 32
    assert len(rest) == 8
    record["008"].data = remain
    assert len(record["008"].data) == 32


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
