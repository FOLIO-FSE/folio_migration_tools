import json
import logging

from migration_tools import custom_exceptions
from migration_tools import helper
from migration_tools.migration_report import MigrationReport
from migration_tools.report_blurbs import Blurbs


class HoldingsHelper:
    @staticmethod
    def to_key(
        holdings_record: dict,
        fields_criterias: list[str],
        migration_report: MigrationReport,
    ) -> str:
        """Creates a key from values determined by the fields_crieteria in a holding
        record to determine uniquenes

        Args:
            holdings_record (dict): The Holdingsrecord
            fields_criteria (list[str]): names of the properties of the holdingsrecord.
            Limited to the strings and UUID properties on the first level of the object.
            If the property is not found, or empty, it will be ignored
            migration_report (MigrationReport): Report to help reporting merge

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
            return "-".join(values)
        except Exception as exception:
            logging.error(json.dumps(holdings_record, indent=4))
            raise exception from exception

    @staticmethod
    def load_previously_generated_holdings(
        holdings_file_path, fields_criteria, migration_report: MigrationReport
    ):
        with open(holdings_file_path) as holdings_file:
            prev_holdings = {}
            for row in holdings_file:
                stored_holding = json.loads(row.split("\t")[-1])
                stored_key = HoldingsHelper.to_key(
                    stored_holding, fields_criteria, migration_report
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


def extend_list(prop_name: str, holdings_record: dict, incoming_holdings: dict):

    temp = holdings_record.get(prop_name, [])
    for f in incoming_holdings.get(prop_name, []):
        if f not in temp:
            temp.append(f)
    holdings_record[prop_name] = temp


def dedupe(list_of_dicts):
    # TODO: Move to interface or parent class
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]
