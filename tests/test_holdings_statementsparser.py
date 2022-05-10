# content of test_sample.py
import pymarc
from pymarc import Field

from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)


def test_get_marc_textual_stmt():
    file_path = "./tests/test_data/default/test_mfhd_holdings_statements.xml"
    record = pymarc.parse_xml_to_array(file_path)[0]
    res = HoldingsStatementsParser.get_holdings_statements(record, "853", "863", "866", ["apa"])
    stmt = "v.1:no. 1(1943:July 3)-v.1:no.52(1944:June 24)"
    stmt2 = "Some statement without note"
    stmt3 = "v.29 (2011)"
    stmt4 = "v.1 (1948)-v.27 (2007)"
    stmt5 = "v.253:no.2 (2006:Jan. 09)"
    # stmt6 = "v.34:no.48(2005:Nov.)-v.35:no.2(2006:Jan.)"
    # stmt7 = "2009-"
    print(res["statements"])
    assert any(res["statements"])
    assert any(stmt in f["statement"] for f in res["statements"])
    assert any(stmt3 in f["statement"] for f in res["statements"])
    assert any(stmt4 in f["statement"] for f in res["statements"])
    assert any("Some note" in f["note"] for f in res["statements"])
    assert any(stmt2 in f["statement"] for f in res["statements"])
    assert any(stmt5 in f["statement"] for f in res["statements"])
    # assert any(stmt7 in f["statement"] for f in res["statements"])
    # assert any(stmt6 in f["statement"] for f in res["statements"])

    assert any("Missing linked fields for 853" in f[1] for f in res["migration_report"])


def test_linked_fields_1():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=["8", "1", "a", "v.", "b", "no.", "i", "(year)", "j", "(month)", "k", "(day)"],
    )
    linked_value_field = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=["8", "1.1", "a", "253", "b", "2", "i", "2006", "j", "01", "k", "09"],
    )

    stmt = "v.253:no.2 (2006:Jan. 09)"  # "v.253:no.2 (Jan. 09, 2006)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field])
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_2():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=["8", "1", "a", "v.", "b", "no.", "i", "(year)", "j", "(month)"],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=["8", "1.1", "a", "34", "b", "48", "i", "2005", "j", "11"],
    )
    linked_value_field2 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=["8", "1.2", "a", "35", "b", "2", "i", "2006", "j", "01"],
    )

    stmt = "v.34:no.48 (2005:Nov.)"
    stmt2 = "v.35:no.2 (2006:Jan.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field1])
    assert ret["statement"]["statement"] == stmt
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field2])
    assert ret["statement"]["statement"] == stmt2


def test_linked_fields_3():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=["8", "1", "a", "v.", "b", "", "i", "(year)", "j", "(month)"],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            "8",
            "1.1",
            "a",
            "110-111",
            "b",
            "3-3",
            "i",
            "2003-2004",
            "j",
            "05/06",
            "w",
            "n",
        ],
    )

    stmt = "v.110:3 (2003:May./Jun.)-v.111:3 (2004:May./Jun.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field1])
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_4():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=["8", "1", "a", "v.", "b", "no.", "i", "(year)", "j", "(season)"],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            "8",
            "1.1",
            "a",
            "22-41",
            "b",
            "1-4",
            "i",
            "1992-2011",
            "j",
            "21-23",
            "z",
            "Print copy cancelled in 2011.",
        ],
    )

    stmt = "v.22:no.1 (1992 Spring)-v.41:no.4 (2011 Fall)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field1])
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_5():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=["8", "1", "i", "(year)"],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            "8",
            "1.1",
            "i",
            "1946-1947",
        ],
    )

    stmt = "1946-1947"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field1])
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_6():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=["8", "1", "a", "v.", "b", "no.", "i", "(year)", "j", "(month)"],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=["8", "1.1", "a", "1-48", "b", "1-4", "i", "1966-2014", "j", "11"],
    )

    stmt = "v.1:no.1 (1966:Nov.)-v.48:no.4 (2014:Nov.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field1])
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_7():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=["8", "1", "a", "v.", "b", "", "i", "(year)", "j", "(month)"],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=["8", "1.1", "a", "111-111", "b", "5-6", "i", "2004", "j", "09/10-11/12"],
    )

    stmt = "v.111:5 (2004:Sep./Oct.)-v.111:6 (2004:Nov./Dec.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, [linked_value_field1])
    assert ret["statement"]["statement"] == stmt
