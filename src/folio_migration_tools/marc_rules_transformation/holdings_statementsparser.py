import calendar
import logging
import re

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
from pymarc import Field, Record


class HoldingsStatementsParser:
    @staticmethod
    def get_textual_statements(marc_record, field_textual, return_dict):
        for f in marc_record.get_fields(field_textual):
            return_dict["statements"].append(
                {"statement": (f["a"] or ""), "note": (f["z"] or ""), "staffNote": ""}
            )
            return_dict["migration_report"].append(
                ("Holdings statements", f"From {field_textual}")
            )

    @staticmethod
    def get_holdings_statements(
        marc_record: Record, pattern_tag, value_tag, field_textual, legacy_id: str
    ):
        # Textual holdings statements
        return_dict = {"statements": [], "migration_report": [], "hlm_stmts": []}
        HoldingsStatementsParser.get_textual_statements(
            marc_record,
            field_textual,
            return_dict,
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
                if "8" in value_field
                and value_field["8"].split(".")[0] == pattern_field["8"]
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

        return_dict["statements"] = dedupe_list_of_dict(return_dict["statements"])
        return return_dict

    @staticmethod
    def parse_linked_field(pattern_field: Field, linked_value_field: Field):
        return_dict = {
            "hlm_stmt": "",
            "statement": {
                "statement": "",
                "note": "",
                "staffNote": "",
            },
        }
        _from, _to = get_from_to(pattern_field, linked_value_field)
        cron_from, cron_to, hlm_stmt = get_cron_from_to(
            pattern_field, linked_value_field
        )
        return_dict["hlm_stmt"] = hlm_stmt
        if cron_from:
            _from = f"{_from} ({cron_from})"
        if _to and cron_to:
            _to = f"{_to} ({cron_to})"
        if _to and cron_from and not cron_to:
            _to = f"{_to} ({cron_from})"
        stmt = f"{_from}-{_to}" if _from else ""
        stmt = stmt.strip("-")
        if "z" in linked_value_field:
            return_dict["statement"]["note"] = linked_value_field["z"]
        if "x" in linked_value_field:
            return_dict["statement"]["staffNote"] = linked_value_field["x"]
        stmt = re.sub(" +", " ", stmt)
        return_dict["statement"]["statement"] = stmt
        return return_dict


def get_season(val):
    try:
        val = int(val)
        if val == 21:
            return "Spring"
        elif val == 22:
            return "Summer"
        elif val == 23:
            return "Fall"
        elif val == 24:
            return "Winter"
        else:
            return val
    except Exception:
        return val


def dedupe_list_of_dict(list_of_dict):
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dict}]


def get_cron_from_to(pattern_field: Field, linked_value_field: Field):
    cron_from = ""
    cron_to = ""
    hlm_stmt = ""
    year = False

    for chron_level in [cl for cl in "ijkl" if cl in linked_value_field]:
        desc = pattern_field[chron_level] or ""
        if linked_value_field[chron_level]:
            if chron_level == "i" and desc == "(year)":
                hlm_stmt = linked_value_field[chron_level]
            if desc == "(year)":
                year = True
            val, *val_rest = linked_value_field[chron_level].split("-")
            if desc == "(month)":
                try:
                    val = f"{calendar.month_abbr[int(val)]}."
                except Exception:
                    pass
                if "".join(val_rest):
                    try:
                        val_rest = calendar.month_abbr[int("".join(val_rest))]
                    except Exception:
                        pass
                if year:
                    cron_from = f"{cron_from.strip()}:{val} "
                    cron_to = f"{cron_to}:{''.join(val_rest)} "
            else:
                if "season" in desc:
                    val = get_season(val)
                cron_from = f"{cron_from} {val} "
                cron_to = f"{cron_to} {''.join(val_rest)}"
    return (cron_from.strip(), cron_to.strip(), hlm_stmt)


def get_from_to(pattern_field: Field, linked_value_field: Field):
    _from = ""
    _to = ""
    for enum_level in [el for el in "abcdef" if el in linked_value_field]:
        desc = pattern_field[enum_level] or ""
        desc = desc if "(" not in desc else ""
        if linked_value_field[enum_level]:
            val, *val_rest = linked_value_field[enum_level].split("-")
            _from = f"{_from}{(':' if _from else '')}{desc}{val}"
            temp_to = "".join(val_rest)
            if temp_to.strip():
                _to = f"{_to}{(':' if _to else '')}{desc}{temp_to}"
    return (_from.strip(), _to.strip())
