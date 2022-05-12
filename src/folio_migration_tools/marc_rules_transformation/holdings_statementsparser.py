import calendar
import contextlib
import logging
import re

from pymarc import Field
from pymarc import Record

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError


class HoldingsStatementsParser:
    @staticmethod
    def get_holdings_statements(
        marc_record: Record,
        pattern_tag: str,
        value_tag: str,
        field_textual: str,
        legacy_id: str,
        dedupe_results: bool = True,
    ) -> dict:
        """The main method

        Args:
            marc_record (Record): _description_
            pattern_tag (str): _description_
            value_tag (str): _description_
            field_textual (str): _description_
            legacy_id (str): _description_
            dedupe_results (bool, optional): _description_. Defaults to True.

        Raises:
            TransformationFieldMappingError: _description_

        Returns:
            dict: _description_
        """

        # Textual holdings statements
        return_dict: dict = {"statements": [], "migration_report": [], "hlm_stmts": []}
        HoldingsStatementsParser.get_textual_statements(
            marc_record, field_textual, return_dict, legacy_id
        )

        value_fields = marc_record.get_fields(value_tag)
        for pattern_field in marc_record.get_fields(pattern_tag):
            if "8" not in pattern_field:
                raise TransformationFieldMappingError(
                    legacy_id,
                    f"{pattern_tag} subfield 8 not in field",
                    pattern_field.format_field(),
                )
            linked_value_fields = [
                value_field
                for value_field in value_fields
                if "8" in value_field and value_field["8"].split(".")[0] == pattern_field["8"]
            ]

            if not any(linked_value_fields):
                return_dict["migration_report"].append(
                    (
                        "Holdings statements",
                        f"Missing linked fields for {pattern_tag}",
                    )
                )

            else:
                for linked_value_field in linked_value_fields:
                    parsed_dict = HoldingsStatementsParser.parse_linked_field(
                        pattern_field, linked_value_field
                    )
                    if parsed_dict["hlm_stmt"]:
                        return_dict["hlm_stmts"].append(parsed_dict["hlm_stmt"])
                    if parsed_dict["statement"]:
                        logging.info(
                            f"HOLDINGS STATEMENT PATTERN\t{legacy_id}\t{pattern_field}"
                            f"\t{linked_value_field}"
                            f"\t{parsed_dict['statement']['statement']}"
                            f"\t{parsed_dict['statement']['note']}"
                            f"\t{parsed_dict['statement']['staffNote']}"
                        )
                        return_dict["migration_report"].append(
                            (
                                "Holdings statements",
                                f"From {pattern_tag}",
                            )
                        )
                        return_dict["statements"].append(parsed_dict["statement"])

        if dedupe_results:
            return_dict["statements"] = dedupe_list_of_dict(return_dict["statements"])
        return return_dict

    @staticmethod
    def parse_linked_field(pattern_field: Field, linked_value_fields: Field):
        break_ind = get_break_indicator(linked_value_fields)
        return_dict = {
            "hlm_stmt": "",
            "statement": {
                "statement": "",
                "note": "",
                "staffNote": "",
            },
        }
        _from, _to, is_span = get_from_to(pattern_field, linked_value_fields)
        cron_from, cron_to, hlm_stmt, is_cron_span = get_cron_from_to(
            pattern_field, linked_value_fields
        )
        return_dict["hlm_stmt"] = hlm_stmt
        if _from and cron_from:
            _from = f"{_from} ({cron_from})"
        if not _from and cron_from:
            _from = cron_from
        if _to and cron_to:
            _to = f"{_to} ({cron_to})"
        if _to and cron_from and not cron_to:
            _to = f"{_to} ({cron_from})"
        if not _to and cron_to:
            _to = cron_to
        if _to == cron_to and _to:
            _to = f"({cron_to})"
        if _from and _from == cron_from:
            _from = f"({cron_from})"
        span = " - " if is_span or is_cron_span else ""
        stmt = f"{_from}{span}{_to}{break_ind}" if _from else ""
        stmt = stmt.strip()
        if "z" in linked_value_fields:
            return_dict["statement"]["note"] = linked_value_fields["z"]
        if "x" in linked_value_fields:
            return_dict["statement"]["staffNote"] = linked_value_fields["x"]
        stmt = re.sub(" +", " ", stmt)
        return_dict["statement"]["statement"] = stmt
        return return_dict

    @staticmethod
    def get_textual_statements(
        marc_record: Record, field_textual: str, return_dict: dict, legacy_id: str
    ):
        """Returns the textual statements from the relevant marc fields

        Args:
            marc_record (Record): _description_
            field_textual (str): _description_
            return_dict (dict): _description_
        """
        for f in marc_record.get_fields(field_textual):
            if "a" not in f and "z" not in f:
                raise TransformationFieldMappingError(
                    legacy_id,
                    f"{field_textual} subfield a or z not in field",
                    f.format_field(),
                )
            if not (f["a"] or f["z"]):
                raise TransformationFieldMappingError(
                    legacy_id,
                    f"{field_textual} Both a or z are empty",
                    f.format_field(),
                )
            return_dict["statements"].append(
                {"statement": (f["a"] or ""), "note": (f["z"] or ""), "staffNote": ""}
            )
            return_dict["migration_report"].append(
                ("Holdings statements", f"From {field_textual}")
            )


def get_break_indicator(field: Field):
    if "w" not in field or field["w"] not in ["g", "n"]:
        return ""
    elif field["w"] == "g":
        return ","
    elif field["w"] == "n":
        return ";"
    else:
        return ""


def get_season(val: str):
    try:
        val_int = int(val)
        if val_int == 21:
            return "Spring"
        elif val_int == 22:
            return "Summer"
        elif val_int == 23:
            return "Fall"
        elif val_int == 24:
            return "Winter"
        else:
            return val
    except Exception:
        return val


def dedupe_list_of_dict(list_of_dict):
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dict}]


def g_m(m: int):
    if m == 5:
        return "May"
    elif m == 6:
        return "June"
    elif m == 7:
        return "July"
    else:
        return f"{calendar.month_abbr[m]}."


def get_month(month_str: str):
    month_str = month_str.strip()
    if "/" in month_str:
        return "/".join([g_m(int(m)) for m in month_str.split("/")])
    else:
        return g_m(int(month_str))


def get_cron_from_to(pattern_field: Field, linked_value_field: Field):
    cron_from = ""
    cron_to = ""
    hlm_stmt = ""
    year = False
    is_span = False
    for chron_level in [cl for cl in "ijkl" if cl in linked_value_field]:
        desc = pattern_field[chron_level] or ""
        if linked_value_field[chron_level]:
            if chron_level == "i" and desc == "(year)":
                hlm_stmt = linked_value_field[chron_level]
            if desc == "(year)":
                year = True
            val, *val_rest = linked_value_field[chron_level].split("-")
            is_span = "-" in linked_value_field[chron_level] or is_span
            if desc == "(month)":
                with contextlib.suppress(Exception):
                    val = get_month(val)
                with contextlib.suppress(Exception):
                    val_rest = get_month(val_rest[0])
                if "".join(val_rest):
                    with contextlib.suppress(Exception):
                        val_rest = get_month("".join(val_rest))
                elif cron_to.strip() and val:
                    val_rest = val
                if year:
                    spill_year = f"{hlm_stmt}:" if "-" not in hlm_stmt else ""
                    cron_from = f"{cron_from.strip()}:{val} "
                    if cron_to and "".join(val_rest):
                        cron_to = f"{cron_to}:{''.join(val_rest)} "
                    elif not cron_to and "".join(val_rest):
                        cron_to = f"{spill_year}{''.join(val_rest)}"

            else:
                if "season" in desc:
                    val = get_season(val)
                    if "".join(val_rest):
                        val_rest = get_season("".join(val_rest))
                cron_from = f"{cron_from} {val}".strip()
                cron_to = f"{cron_to} {''.join(val_rest)}".strip()
    return (f"{cron_from.strip()}", cron_to.strip(), hlm_stmt, is_span)


def get_from_to(pattern_field: Field, linked_value_field: Field):
    _from = ""
    _to = ""
    is_span = False
    for enum_level in [el for el in "abcdef" if el in linked_value_field]:
        desc = pattern_field[enum_level] or ""
        desc = desc.strip() if "(" not in desc else ""
        if linked_value_field[enum_level]:
            val, *val_rest = linked_value_field[enum_level].split("-")
            is_span = "-" in linked_value_field[enum_level] or is_span
            _from = f"{_from}{(':' if _from else '')}{desc}{val}"
            temp_to = "".join(val_rest)
            if temp_to.strip():
                _to = f"{_to}{(':' if _to else '')}{desc}{temp_to}"
    return (f"{_from.strip()}", _to.strip(), is_span)
