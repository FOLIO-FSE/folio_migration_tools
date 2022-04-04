import json
import sys
import logging
from uuid import uuid4

from folio_migration_tools import custom_exceptions
from folio_migration_tools import helper
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_blurbs import Blurbs


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

        fields_criterias are limited to the strings and UUID properties on the first level of the object.
        If the property is not found, or empty, it will be ignored

        IF the holdings type id is matched to holdings_type_id_to_exclude_from_merging,
        the key will be added with a uuid to prevent merging of this holding

        Args:
            holdings_record (dict): The Holdingsrecord
            fields_criteria (list[str]): names of the properties of the holdingsrecord.
            migration_report (MigrationReport): Report to help reporting merge
            holdings_type_id_to_exclude_from_merging: (str): the holdings type UUID to exclude

        Raises:
            exception: _description_

        Returns:
            str: _description_
        """
        try:
            values = []
            for fields_criteria in fields_criterias:
                v = holdings_record.get(fields_criteria, "")
                if not v:
                    migration_report.add(
                        Blurbs.HoldingsMerging, f"{fields_criteria} empty or not set"
                    )
                values.append(v)

            if (
                holdings_record.get("holdingsTypeId")
                == holdings_type_id_to_exclude_from_merging
            ):
                values.append(str(uuid4()))
                migration_report.add(
                    Blurbs.HoldingsMerging,
                    "Holding prevented from merging by holdingsTypeId",
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
                        f"Previously stored holdings key already exists in the "
                        f"list of previously stored Holdings. You have likely not used the same "
                        f"matching criterias ({fields_criteria}) as you did in the previous process"
                    )
                    helper.Helper.log_data_issue(
                        stored_holding["formerIds"], message, stored_key
                    )
                    logging.warn(message)
                    prev_holdings[stored_key] = HoldingsHelper.merge_holding(
                        prev_holdings[stored_key], stored_holding
                    )
                    migration_report.add(
                        Blurbs.HoldingsMerging,
                        "Duplicate key based on current merge criteria. Records merged",
                    )
                else:
                    migration_report.add(
                        Blurbs.HoldingsMerging,
                        "Previously transformed holdings record loaded",
                    )
                    prev_holdings[stored_key] = stored_holding
            return prev_holdings

    @staticmethod
    def merge_holding(holdings_record: dict, incoming_holdings: dict) -> dict:
        extend_list("holdingsStatementsForIndexes", holdings_record, incoming_holdings)
        extend_list("holdingsStatements", holdings_record, incoming_holdings)
        extend_list(
            "holdingsStatementsForSupplements", holdings_record, incoming_holdings
        )
        extend_list("notes", holdings_record, incoming_holdings)
        holdings_record["notes"] = dedupe(holdings_record.get("notes", []))
        extend_list("formerIds", holdings_record, incoming_holdings)
        holdings_record["formerIds"] = list(set(holdings_record["formerIds"]))
        extend_list("electronicAccess", holdings_record, incoming_holdings)
        return holdings_record

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


def extend_list(prop_name: str, holdings_record: dict, incoming_holdings: dict):

    temp = holdings_record.get(prop_name, [])
    for f in incoming_holdings.get(prop_name, []):
        if f not in temp:
            temp.append(f)
    holdings_record[prop_name] = temp


def dedupe(list_of_dicts):
    # TODO: Move to interface or parent class
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]
