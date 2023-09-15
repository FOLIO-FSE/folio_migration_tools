import json
import logging
import i18n
from uuid import uuid4

from folio_migration_tools import custom_exceptions
from folio_migration_tools import helper
from folio_migration_tools.migration_report import MigrationReport


class HoldingsHelper:
    @staticmethod
    def to_key(
        holdings_record: dict,
        fields_criterias: list[str],
        migration_report: MigrationReport,
        holdings_type_id_to_exclude_from_merging: str = "Not set",
    ) -> str:
        """Creates a key from values determined by the fields_crieteria in a holding
        record to determine uniquenes

        fields_criterias are limited to the strings and UUID properties on the first level of
        the object. If the property is not found, or empty, it will be ignored

        IF the holdings type id is matched to holdings_type_id_to_exclude_from_merging,
        the key will be added with a uuid to prevent merging of this holding

        Args:
            holdings_record (dict): The Holdingsrecord
            fields_criterias (list[str]): names of the properties of the holdingsrecord.
            migration_report (MigrationReport): Report to help reporting merge
            holdings_type_id_to_exclude_from_merging: (str): the holdings type UUID to exclude

        Raises:
            Exception: _description_

        Returns:
            str: _description_
        """
        try:
            values = []
            for fields_criteria in fields_criterias:
                v = holdings_record.get(fields_criteria, "")
                if not v:
                    migration_report.add(
                        "HoldingsMerging",
                        i18n.t(
                            "%{fields_criteria} empty or not set", fields_criteria=fields_criteria
                        ),
                    )
                values.append(v)

            if holdings_record.get("holdingsTypeId") == holdings_type_id_to_exclude_from_merging:
                values.append(str(uuid4()))
                migration_report.add(
                    "HoldingsMerging",
                    i18n.t("Holding prevented from merging by holdingsTypeId"),
                )
            return "-".join(values)
        except Exception as exception:
            logging.error(json.dumps(holdings_record, indent=4))
            raise exception from exception

    @staticmethod
    def load_previously_generated_holdings(
        holdings_file_path,
        fields_criteria,
        migration_report: MigrationReport,
        holdings_type_id_to_exclude_from_merging: str = "Not set",
    ):
        if not holdings_file_path.is_file():
            raise custom_exceptions.TransformationProcessError(
                "", "File not found", holdings_file_path
            )
        logging.info(
            "Holdings type id to exclude is set to %s",
            holdings_type_id_to_exclude_from_merging,
        )
        with open(holdings_file_path) as holdings_file:
            prev_holdings = {}
            for row in holdings_file:
                stored_holding = json.loads(row.split("\t")[-1])
                stored_key = HoldingsHelper.to_key(
                    stored_holding,
                    fields_criteria,
                    migration_report,
                    holdings_type_id_to_exclude_from_merging,
                )
                if stored_key in prev_holdings:
                    message = (
                        f"Previously stored holdings key already exists in the list of previously"
                        f" stored Holdings. You have likely not used the same matching criterias"
                        f" ({fields_criteria}) as you did in the previous process"
                    )
                    helper.Helper.log_data_issue(stored_holding["formerIds"], message, stored_key)
                    logging.warn(message)
                    prev_holdings[stored_key] = HoldingsHelper.merge_holding(
                        prev_holdings[stored_key], stored_holding
                    )
                    migration_report.add(
                        "HoldingsMerging",
                        i18n.t("Duplicate key based on current merge criteria. Records merged"),
                    )
                else:
                    migration_report.add(
                        "HoldingsMerging",
                        i18n.t("Previously transformed holdings record loaded"),
                    )
                    prev_holdings[stored_key] = stored_holding
            return prev_holdings

    @staticmethod
    def merge_holding(holdings_record: dict, incoming_holdings: dict) -> dict:
        extend_list("holdingsStatementsForIndexes", holdings_record, incoming_holdings, True)
        extend_list("holdingsStatements", holdings_record, incoming_holdings, True)
        extend_list("holdingsStatementsForSupplements", holdings_record, incoming_holdings, True)
        extend_list("notes", holdings_record, incoming_holdings)
        holdings_record["notes"] = dedupe(holdings_record.get("notes", []))
        extend_list("formerIds", holdings_record, incoming_holdings)
        extend_list("electronicAccess", holdings_record, incoming_holdings)
        HoldingsHelper.remove_empty_holdings_statements(holdings_record)
        merge_boolean("discoverySuppress", holdings_record, incoming_holdings)
        return holdings_record

    @staticmethod
    def remove_empty_holdings_statements(holdings_record: dict):
        keys = [
            "holdingsStatements",
            "holdingsStatementsForIndexes",
            "holdingsStatementsForSupplements",
        ]

        for key in keys:
            if key in holdings_record:
                temp_l = [stmt for stmt in holdings_record[key] if any(stmt.values())]
                holdings_record[key] = temp_l
            if key in holdings_record and not holdings_record.get(key, []):
                del holdings_record[key]

    @staticmethod
    def handle_notes(folio_object):
        if folio_object.get("notes", []):
            filtered_notes = []
            for note_obj in folio_object.get("notes", []):
                if not note_obj.get("holdingsNoteTypeId", ""):
                    raise custom_exceptions.TransformationProcessError(
                        folio_object.get("legacyIds", ""),
                        "Missing note type id mapping",
                        json.dumps(note_obj),
                    )
                elif note_obj.get("note", ""):
                    filtered_notes.append(note_obj)
            if filtered_notes:
                folio_object["notes"] = filtered_notes
            else:
                del folio_object["notes"]


def extend_list(
    prop_name: str, holdings_record: dict, incoming_holdings: dict, accept_dupe_items: bool = False
):
    temp = holdings_record.get(prop_name, [])
    all_already_in = all(i in temp for i in incoming_holdings.get(prop_name, []))
    for f in incoming_holdings.get(prop_name, []):
        if not all_already_in and (accept_dupe_items or f not in temp):
            temp.append(f)
    if temp:
        holdings_record[prop_name] = temp


def dedupe(list_of_dicts):
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]


def merge_boolean(prop_name: str, holdings_record: dict, incoming_holdings: dict):
    if (
        holdings_record.get(prop_name, False) is True
        and incoming_holdings.get(prop_name, False) is True
    ):
        holdings_record[prop_name] = True
    else:
        holdings_record[prop_name] = False
