# content of test_sample.py
import pymarc
from pymarc import Field
from pymarc import Subfield
from pymarc import Field, Subfield

from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)


def get_sfs(fieldstring: str) -> list[str]:
    sfs = []
    s = fieldstring.split("$")[1:]
    for sf in s:
        sfs.extend([Subfield(code=sf[0], value=sf[1:])])
    tag = fieldstring[1:4]
    ind1 = fieldstring[5]
    ind1 = " " if ind1 == "\\" else ind1
    ind2 = fieldstring[6]
    ind2 = " " if ind2 == "\\" else ind2
    return Field(
        tag=tag,
        indicators=[ind1, ind2],
        subfields=sfs,
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
    stmt6 = "v.34:no.48 (2005:Nov.)"
    stmt6_1 = "v.35:no.2 (2006:Jan.)"
    stmt7 = "2009-"
    print(res["statements"])
    assert any(res["statements"])
    assert any(stmt in f["statement"] for f in res["statements"])
    assert any(stmt3 in f["statement"] for f in res["statements"])
    assert any(stmt4 in f["statement"] for f in res["statements"])
    assert any("Some note" in f["note"] for f in res["statements"])
    assert any(stmt2 in f["statement"] for f in res["statements"])
    assert any(stmt5 in f["statement"] for f in res["statements"])
    assert any(stmt7 in f["statement"] for f in res["statements"])
    assert any(stmt6 in f["statement"] for f in res["statements"])
    assert any(stmt6_1 in f["statement"] for f in res["statements"])

    assert any("Missing linked fields for 853" in f[1] for f in res["migration_report"])


def test_get_marc_textual_stmt_correct_order():
    path = "./tests/test_data/default/mfhd_with_dupe_866s.mrc"
    with open(path, "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record = next(reader)
        res = HoldingsStatementsParser.get_holdings_statements(
            record, "853", "863", "866", ["apa"], False
        )
        stmt = "1945:Jan. 31,"
        all_stmts = [f["statement"] for f in res["statements"]]
        all_41s = [f for f in all_stmts if stmt == f]
        assert len(all_41s) == 2
        assert stmt in all_stmts
        assert all_stmts[33] == stmt
        assert all_stmts[22] == stmt


def test_get_marc_textual_stmt_correct_order_and_not_deduped():
    path = "./tests/test_data/mfhd/c_record_repeated_holdings_statements.mrc"
    with open(path, "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record = next(reader)
        res = HoldingsStatementsParser.get_holdings_statements(
            record, "853", "863", "866", ["apa"], False
        )
        stmt = "1994-1998."
        all_stmts = [f["statement"] for f in res["statements"]]
        all_94s = [f for f in all_stmts if stmt == f]
        assert len(all_94s) == 2


def test_linked_fields_1():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1"),
            Subfield(code="a", value="v."),
            Subfield(code="b", value="no."),
            Subfield(code="i", value="(year)"),
            Subfield(code="j", value="(month)"),
            Subfield(code="k", value="(day)"),
        ],
    )
    linked_value_field = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.1"),
            Subfield(code="a", value="253"),
            Subfield(code="b", value="2"),
            Subfield(code="i", value="2006"),
            Subfield(code="j", value="01"),
            Subfield(code="k", value="09"),
        ],
    )

    stmt = "v.253:no.2 (2006:Jan. 09)"  # "v.253:no.2 (Jan. 09, 2006)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_2():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1"),
            Subfield(code="a", value="v."),
            Subfield(code="b", value="no."),
            Subfield(code="i", value="(year)"),
            Subfield(code="j", value="(month)"),
        ],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.1"),
            Subfield(code="a", value="34"),
            Subfield(code="b", value="48"),
            Subfield(code="i", value="2005"),
            Subfield(code="j", value="11"),
        ],
    )
    linked_value_field2 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.2"),
            Subfield(code="a", value="35"),
            Subfield(code="b", value="2"),
            Subfield(code="i", value="2006"),
            Subfield(code="j", value="01"),
        ],
    )

    stmt = "v.34:no.48 (2005:Nov.)"
    stmt2 = "v.35:no.2 (2006:Jan.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field2)
    assert ret["statement"]["statement"] == stmt2


def test_linked_fields_3():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1"),
            Subfield(code="a", value="v."),
            Subfield(code="b", value=""),
            Subfield(code="i", value="(year)"),
            Subfield(code="j", value="(month)"),
        ],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.1"),
            Subfield(code="a", value="110-111"),
            Subfield(code="b", value="3-3"),
            Subfield(code="i", value="2003-2004"),
            Subfield(code="j", value="05/06"),
            Subfield(code="w", value="n"),
        ],
    )

    stmt = "v.110:3 (2003:May/June)-v.111:3 (2004:May/June);"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_4():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1"),
            Subfield(code="a", value="v."),
            Subfield(code="b", value="no."),
            Subfield(code="i", value="(year)"),
            Subfield(code="j", value="(season)"),
        ],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.1"),
            Subfield(code="a", value="22-41"),
            Subfield(code="b", value="1-4"),
            Subfield(code="i", value="1992-2011"),
            Subfield(code="j", value="21-23"),
            Subfield(code="z", value="Print copy cancelled in 2011."),
        ],
    )

    stmt = "v.22:no.1 (1992 Spring)-v.41:no.4 (2011 Fall)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_5():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=[Subfield(code="8", value="1"), Subfield(code="i", value="(year)")],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.1"),
            Subfield(code="i", value="1946-1947"),
        ],
    )

    stmt = "(1946)-(1947)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_6():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1"),
            Subfield(code="a", value="v."),
            Subfield(code="b", value="no."),
            Subfield(code="i", value="(year)"),
            Subfield(code="j", value="(month)"),
        ],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.1"),
            Subfield(code="a", value="1-48"),
            Subfield(code="b", value="1-4"),
            Subfield(code="i", value="1966-2014"),
            Subfield(code="j", value="11"),
        ],
    )

    stmt = "v.1:no.1 (1966:Nov.)-v.48:no.4 (2014:Nov.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_7():
    pattern_field = Field(
        tag="853",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1"),
            Subfield(code="a", value="v."),
            Subfield(code="b", value=" "),
            Subfield(code="i", value="(year)"),
            Subfield(code="j", value="(month)"),
        ],
    )
    linked_value_field1 = Field(
        tag="863",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="8", value="1.1"),
            Subfield(code="a", value="111-111"),
            Subfield(code="b", value="5-6"),
            Subfield(code="i", value="2004"),
            Subfield(code="j", value="09/10-11/12"),
        ],
    )

    stmt = "v.111:5 (2004:Sep./Oct.)-v.111:6 (2004:Nov./Dec.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_8():
    pattern_field = get_sfs(r"=853  \\$81$av.$b $i(year)$j(month)")
    linked_value_field1 = get_sfs(r"=863  \\$81.2$a111-111$b5-6$i2004$j09/10-11/12")
    stmt = "v.111:5 (2004:Sep./Oct.)-v.111:6 (2004:Nov./Dec.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_9():
    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(season)")
    linked_value_field1 = get_sfs(r"=863  \\$81.1$a1-24$b1-3$i1985-2009$j24-24")
    stmt = "v.1:no.1 (1985 Winter)-v.24:no.3 (2009 Winter)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_10():
    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(month)")
    linked_value_field1 = get_sfs(
        r"=863  \\$81.1$a1-51$b1-6$i1963-2014$j06-12$zPrint copy canceled in 2014."
    )
    stmt = "v.1:no.1 (1963:June)-v.51:no.6 (2014:Dec.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_11():
    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(season)")
    linked_value_field1 = get_sfs(
        r"=863  \\$81.1$a2-35$b1-4$i1979-2013$j21-24$zPrint copy canceled in 2014."
    )
    stmt = "v.2:no.1 (1979 Spring)-v.35:no.4 (2013 Winter)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_12():
    pattern_field = get_sfs(r"=853  \\$81$i(year)$j(month)")
    linked_value_field1 = get_sfs(r"=863  \\$81.1$i2000-2003$j01-07")
    stmt = "(2000:Jan.)-(2003:July)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt
    pattern_field = get_sfs(r"=853  \\$81$i(year)")
    linked_value_field1 = get_sfs(r"=863  \\$81.3$i1980-")
    stmt = "(1980)-"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt

    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(month)")
    linked_value_field1 = get_sfs(r"=863  \\$81.1$a1-$b1-$i1972-$j09-")
    stmt = "v.1:no.1 (1972:Sep.)-"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_15():
    pattern_field = get_sfs(r"=853  20$81$av.$bno.$i(year)$j(month)")
    linked_value_field1 = get_sfs(r"=863  40$81.1$a64-69$b7-1$i1985-1990$j07-01/02$wg")
    stmt1 = "v.64:no.7 (1985:July)-v.69:no.1 (1990:Jan./Feb.),"
    linked_value_field2 = get_sfs(r"=863  40$81.2$a69-70$b3-6$i1990-1991$j05/06-11/12$wg")
    stmt2 = "v.69:no.3 (1990:May/June)-v.70:no.6 (1991:Nov./Dec.),"
    linked_value_field3 = get_sfs(r"=863  41$81.3$a71$b8$i1992$j03/04$wn")
    stmt3 = "v.71:no.8 (1992:Mar./Apr.);"
    linked_value_field4 = get_sfs(r"=863  40$81.4$a72-74$b9-1$i1992-1994$jsummer issue,-01/02$wg")
    stmt4 = "v.72:no.9 (1992:summer issue,)-v.74:no.1 (1994:Jan./Feb.),"
    linked_value_field5 = get_sfs(r"=863  41$81.5$a74$b3$i1994$j05/06$wg")
    stmt5 = "v.74:no.3 (1994:May/June),"
    linked_value_field6 = get_sfs(r"=863  41$81.6$a75$b1$i1995$j01/02")
    stmt6 = "v.75:no.1 (1995:Jan./Feb.)"

    ret1 = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret1["statement"]["statement"] == stmt1

    ret2 = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field2)
    assert ret2["statement"]["statement"] == stmt2

    ret3 = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field3)
    assert ret3["statement"]["statement"] == stmt3

    ret4 = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field4)
    assert ret4["statement"]["statement"] == stmt4

    ret5 = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field5)
    assert ret5["statement"]["statement"] == stmt5

    ret6 = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field6)
    assert ret6["statement"]["statement"] == stmt6


def test_linked_fields_16():
    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(season)")
    linked_value_field1 = get_sfs(
        r"=863  \\$81.1$a22-41$b1-4$i1992-2011$j21-23$zPrint copy cancelled in 2011."
    )
    stmt = "v.22:no.1 (1992 Spring)-v.41:no.4 (2011 Fall)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_17():
    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(season)")
    linked_value_field1 = get_sfs(r"=863  \\$81.1$a1-24$b1-3$i1985-2009$j24-24")
    stmt = "v.1:no.1 (1985 Winter)-v.24:no.3 (2009 Winter)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_18():
    pattern_field = get_sfs(r"=853  \\$81$i(year)")
    linked_value_field1 = get_sfs(r"=863  \\$81.1$i2000-")
    stmt = "(2000)-"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_19():
    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(season)")
    linked_value_field1 = get_sfs(
        r"=863  \\$81.1$a1-64$b1-1$i1948-2014$j23-08$zPrint copy canceled in 2014."
    )
    stmt = "v.1:no.1 (1948 Fall)-v.64:no.1 (2014 Aug.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_20():
    pattern_field = get_sfs(r"=853  20$82$av.$b(*)$u6$vr$g(*)$i(year)$wb")
    linked_value_field1 = get_sfs(r"=863  41$81.1$a2$b1/2$i1982$wg")
    stmt = "v.2:1/2 (1982),"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_21():
    pattern_field = get_sfs(r"=853  20$82$av.$b(*)$u6$vr$g(*)$i(year)$wb")
    linked_value_field1 = get_sfs(r"=863  41$81.2$a6$b4$i1986$wg")
    stmt = "v.6:4 (1986),"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_22():
    pattern_field = get_sfs(r"=853  20$82$av.$b(*)$u6$vr$g(*)$i(year)$wb")
    linked_value_field1 = get_sfs(r"=863  41$81.3$a7$b4$i1987$wg")
    stmt = "v.7:4 (1987),"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_23():
    pattern_field = get_sfs(r"=853  20$82$av.$b(*)$u6$vr$g(*)$i(year)$wb")
    linked_value_field1 = get_sfs(r"=863  40$81.4$a14-16$b1-4$i1991-1993$wg")
    stmt = "v.14:1 (1991)-v.16:4 (1993),"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_24():
    pattern_field = get_sfs(r"=853  20$82$av.$b(*)$u6$vr$g(*)$i(year)$wb")
    linked_value_field1 = get_sfs(r"=863  40$81.5$a17-26$b3/4-4$i1993-1999$wg")
    stmt = "v.17:3/4 (1993)-v.26:4 (1999),"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_25():
    pattern_field = get_sfs(r"=853  20$82$av.$b(*)$u6$vr$g(*)$i(year)$wb")
    linked_value_field1 = get_sfs(r"=863  40$81.6$a28-40$b1-2$i1999-2005$wg")
    stmt = "v.28:1 (1999)-v.40:2 (2005),"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_26():
    pattern_field = get_sfs(r"=853  20$82$av.$b(*)$u6$vr$g(*)$i(year)$wb")
    linked_value_field1 = get_sfs(r"=863  40$81.7$a41-46$b1-4$i2005-2008")
    stmt = "v.41:1 (2005)-v.46:4 (2008)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_linked_fields_27():
    pattern_field = get_sfs(r"=853  \\$81$av.$bno.$i(year)$j(season)")
    linked_value_field1 = get_sfs(
        r"=863  \\$81.1$a1-64$b1-1$i1948-2014$j23-08$zPrint copy canceled in 2014."
    )
    stmt = "v.1:no.1 (1948 Fall)-v.64:no.1 (2014 Aug.)"
    ret = HoldingsStatementsParser.parse_linked_field(pattern_field, linked_value_field1)
    assert ret["statement"]["statement"] == stmt


def test_g_s():
    r = HoldingsStatementsParser.g_s("21")
    assert r == "Spring"


def test_g_m():
    r = HoldingsStatementsParser.g_m(1)
    assert r == "Jan."


def test_get_season():
    r = HoldingsStatementsParser.get_season("21")
    assert r == "Spring"


def test_get_month():
    r = HoldingsStatementsParser.get_month("01")
    assert r == "Jan."
    r = HoldingsStatementsParser.get_month("05")
    assert r == "May"


def test_get_textual_statements():
    path = "./tests/test_data/default/mfhd_with_dupe_866s.mrc"
    with open(path, "rb") as openfile:
        reader = pymarc.MARCReader(openfile, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record = next(reader)
        return_dict = {
            "statements": [],
            "migration_report": [],
        }
        HoldingsStatementsParser.get_textual_statements(
            record,
            "866",
            return_dict,
            "1"
        )
        statements = set(['1943:Sept. 30,', '1943:Oct. 31.', '1943:Nov. 30.', '1943:Dec. 31,', '1944:Feb. 29,', '1944:Mar.,', '1944:Apr. 30,', '1944:May 31,', '1944:Sept. 30,', '1944:Oct.-1944:Dec. 31,', '1945:Jan.-1945:May,', '1945:Sept.-1946:May,', '1946:Apr. 30,', '1946:May 31,', '1946:Sept.,', '1946:Oct.-1946:Dec.,', '1947:Jan. 31-1947:May 31,', '1947:Sept. 30-1947:Oct. 31,', '1947:Dec.', 'COPY 2:', '1944:Mar. 31,', '1944:Oct. 31-1944:Nov. 30,', '1945:Jan. 31,', '1945:Feb. 28,', '1945:Mar. 31-1945:May 31,', '1945:Sept. 30-1945:Dec. 31,', '1946:Jan.-1946:Apr,', '1946:Oct.-1946:Dec.,', '1947:Feb. 28,', '1947:Apr. 30-1947:May 31,', '1947:Oct. 31,', '1947:Dec.', 'COPY 3:', '1945:Jan. 31,', '1945:Mar. 31,', '1945:Apr. 30-1945:May 31,', '1946:Oct.', '1947:May 31,', '1947:Oct. 31.', '1947:Dec. 31.'])
        assert statements == set([x["statement"] for x in return_dict["statements"]])


def make_field(subfields_dict):
    """Helper to create a pymarc Field with subfields from a dict."""
    subfields = []
    for code, value in subfields_dict.items():
        subfields.append(Subfield(code=code, value=value))
    return Field(tag="863", indicators=[" ", " "], subfields=subfields)


def make_pattern_field(subfields_dict):
    """Helper to create a pymarc Field for pattern with subfields from a dict."""
    subfields = []
    for code, value in subfields_dict.items():
        subfields.append(Subfield(code=code, value=value))
    return Field(tag="853", indicators=[" ", " "], subfields=subfields)


def test_get_from_to_simple():
    pattern = make_pattern_field({"a": "v.", "b": "no."})
    value = make_field({"a": "1", "b": "2"})
    _from, _to, is_span = HoldingsStatementsParser.get_from_to(pattern, value)
    assert _from == "v.1:no.2"
    assert _to == ""
    assert not is_span


def test_get_from_to_with_span():
    pattern = make_pattern_field({"a": "v.", "b": "no."})
    value = make_field({"a": "1-2", "b": "3-4"})
    _from, _to, is_span = HoldingsStatementsParser.get_from_to(pattern, value)
    assert _from == "v.1:no.3"
    assert _to == "v.2:no.4"
    assert is_span


def test_get_from_to_with_empty_desc():
    pattern = make_pattern_field({"a": "", "b": ""})
    value = make_field({"a": "10-20", "b": "5-6"})
    _from, _to, is_span = HoldingsStatementsParser.get_from_to(pattern, value)
    assert _from == "10:5"
    assert _to == "20:6"
    assert is_span


def test_get_from_to_with_partial_span():
    pattern = make_pattern_field({"a": "v.", "b": "no."})
    value = make_field({"a": "5", "b": "1-2"})
    _from, _to, is_span = HoldingsStatementsParser.get_from_to(pattern, value)
    assert _from == "v.5:no.1"
    assert _to == "no.2"
    assert is_span


def test_get_from_to_no_enum_levels():
    pattern = make_pattern_field({})
    value = make_field({})
    _from, _to, is_span = HoldingsStatementsParser.get_from_to(pattern, value)
    assert _from == ""
    assert _to == ""
    assert not is_span


def test_get_from_to_multiple_levels():
    pattern = make_pattern_field({"a": "v.", "b": "no.", "c": "pt."})
    value = make_field({"a": "1-2", "b": "3-4", "c": "5-6"})
    _from, _to, is_span = HoldingsStatementsParser.get_from_to(pattern, value)
    # Should concatenate all levels
    assert _from == "v.1:no.3:pt.5"
    assert _to == "v.2:no.4:pt.6"
    assert is_span


def test_get_from_to_with_parentheses_in_desc():
    pattern = make_pattern_field({"a": "v.", "b": "(number)"})
    value = make_field({"a": "1-2", "b": "3-4"})
    _from, _to, is_span = HoldingsStatementsParser.get_from_to(pattern, value)
    # desc should be empty for b because of parentheses
    assert _from == "v.1:3"
    assert _to == "v.2:4"
    assert is_span


def test_get_cron_from_to_month_span():
    # pattern: i(year), j(month)
    pattern_field = make_pattern_field({"i": "(year)", "j": "(month)"})
    value_field = make_field({"i": "2000-2001", "j": "01-07"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == "2000:Jan."
    assert cron_to == "2001:July"
    assert hlm_stmt == "2000-2001"
    assert is_span


def test_get_cron_from_to_month_single():
    pattern_field = make_pattern_field({"i": "(year)", "j": "(month)"})
    value_field = make_field({"i": "2010", "j": "05"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == "2010:May"
    assert cron_to == ""
    assert hlm_stmt == "2010"
    assert not is_span


def test_get_cron_from_to_season_span():
    pattern_field = make_pattern_field({"i": "(year)", "j": "(season)"})
    value_field = make_field({"i": "2015-2016", "j": "21-24"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == "2015 Spring"
    assert cron_to == "2016 Winter"
    assert hlm_stmt == "2015-2016"
    assert is_span


def test_get_cron_from_to_season_single():
    pattern_field = make_pattern_field({"i": "(year)", "j": "(season)"})
    value_field = make_field({"i": "2018", "j": "23"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == "2018 Fall"
    assert cron_to == ""
    assert hlm_stmt == "2018"
    assert not is_span


def test_get_cron_from_to_multiple_chron_levels():
    pattern_field = make_pattern_field({"i": "(year)", "j": "(month)", "k": "(day)"})
    value_field = make_field({"i": "2020-2021", "j": "12-01", "k": "31-01"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    # Should combine all levels
    assert cron_from == "2020:Dec. 31"
    assert cron_to == "2021:Jan. 01"
    assert cron_from.startswith("2020:Dec.")
    assert cron_to.startswith("2021:Jan.")
    assert hlm_stmt == "2020-2021"
    assert is_span


def test_get_cron_from_to_no_chron_levels():
    pattern_field = make_pattern_field({})
    value_field = make_field({})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == ""
    assert cron_to == ""
    assert hlm_stmt == ""
    assert not is_span


def test_get_cron_from_to_nonstandard_chron_level():
    # Only 'l' present
    pattern_field = make_pattern_field({"l": "(other)"})
    value_field = make_field({"l": "foo-bar"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == "foo"
    assert cron_to == "bar"
    assert hlm_stmt == ""
    assert is_span


def test_get_cron_from_to_month_with_slash():
    pattern_field = make_pattern_field({"i": "(year)", "j": "(month)"})
    value_field = make_field({"i": "2022", "j": "01/02"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == "2022:Jan./Feb."
    assert cron_to == ""
    assert hlm_stmt == "2022"
    assert not is_span


def test_get_cron_from_to_month_with_empty_pattern_desc():
    pattern_field = make_pattern_field({"i": "(year)", "j": ""})
    value_field = make_field({"i": "2023", "j": "03"})
    cron_from, cron_to, hlm_stmt, is_span = HoldingsStatementsParser.get_cron_from_to(pattern_field, value_field)
    assert cron_from == "2023 03"
    assert cron_to == ""
    assert hlm_stmt == "2023"
    assert not is_span


def test_parse_linked_field_basic():
    pattern_field = make_pattern_field({"a": "v.", "b": "no.", "i": "(year)", "j": "(month)"})
    value_field = make_field({"a": "1", "b": "2", "i": "2020", "j": "05"})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert isinstance(result, dict)
    assert "statement" in result
    assert "hlm_stmt" in result
    assert result["statement"]["statement"] == "v.1:no.2 (2020:May)"
    assert result["statement"]["note"] == ""
    assert result["statement"]["staffNote"] == ""


def test_parse_linked_field_with_span():
    pattern_field = make_pattern_field({"a": "v.", "b": "no.", "i": "(year)", "j": "(month)"})
    value_field = make_field({"a": "1-2", "b": "3-4", "i": "2020-2021", "j": "05-06"})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert "-" in result["statement"]["statement"]
    assert result["statement"]["statement"].startswith("v.1:no.3")
    assert result["statement"]["statement"].endswith("v.2:no.4 (2021:June)")


def test_parse_linked_field_with_span_open():
    pattern_field = make_pattern_field({"a": "v.", "b": "no.", "i": "(year)", "j": "(month)"})
    value_field = make_field({"a": "1-", "b": "3-", "i": "2020-", "j": "05-"})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    # assert "-" in result["statement"]["statement"]
    assert result["statement"]["statement"].startswith("v.1:no.3")
    assert result["statement"]["statement"].endswith("-")


def test_parse_linked_field_with_notes():
    pattern_field = make_pattern_field({"a": "v.", "b": "no.", "i": "(year)"})
    value_field = make_field({"a": "10", "b": "5", "i": "2015", "z": "Some note", "x": "Staff note"})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert result["statement"]["note"] == "Some note"
    assert result["statement"]["staffNote"] == "Staff note"


def test_parse_linked_field_with_break_indicator():
    pattern_field = make_pattern_field({"a": "v.", "i": "(year)"})
    value_field = make_field({"a": "3", "i": "2010", "w": "g"})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert result["statement"]["statement"].endswith(",")


def test_parse_linked_field_empty_fields():
    pattern_field = make_pattern_field({})
    value_field = make_field({})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert result["statement"]["statement"] == ""
    assert result["statement"]["note"] == ""
    assert result["statement"]["staffNote"] == ""
    assert result["hlm_stmt"] == ""


def test_parse_linked_field_only_cron():
    pattern_field = make_pattern_field({"i": "(year)", "j": "(month)"})
    value_field = make_field({"i": "2022", "j": "01"})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert result["statement"]["statement"] == "(2022:Jan.)"
    assert result["hlm_stmt"] == "2022"


def test_parse_linked_field_with_multiple_notes():
    pattern_field = make_pattern_field({"i": "(year)"})
    value_field = make_field({"i": "2020", "z": "Note1", "x": "Staff1"})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert result["statement"]["statement"] == "(2020)"
    assert result["statement"]["note"] == "Note1"
    assert result["statement"]["staffNote"] == "Staff1"


def test_parse_linked_field_with_no_from():
    pattern_field = make_pattern_field({"i": "(year)"})
    value_field = make_field({"i": ""})
    result = HoldingsStatementsParser.parse_linked_field(pattern_field, value_field)
    assert result["statement"]["statement"] == ""
