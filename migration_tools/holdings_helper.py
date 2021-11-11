import json
import logging

from migration_tools import custom_exceptions
from migration_tools.helper import Helper


class HoldingsHelper:
    @staticmethod
    def to_key(holding, fields_criteria):
        """creates a key if key values in holding record
        to determine uniquenes"""
        try:
            call_number = (
                "".join(holding.get("callNumber", "").split())
                if "c" in fields_criteria
                else ""
            )
            instance_id = holding["instanceId"] if "b" in fields_criteria else ""
            location_id = (
                holding["permanentLocationId"] if "l" in fields_criteria else ""
            )
            return "-".join([instance_id, call_number, location_id, ""])
        except Exception as ee:
            logging.error(json.dumps(holding, indent=4))
            raise ee

    @staticmethod
    def merge_holding(old_holdings_record: dict, new_holdings_record: dict):
        # TODO: Move to interface or parent class and make more generic
        if old_holdings_record.get("notes"):
            old_holdings_record["notes"].extend(new_holdings_record.get("notes", []))
            old_holdings_record["notes"] = dedupe(old_holdings_record.get("notes", []))
        if old_holdings_record.get("holdingsStatements"):
            old_holdings_record["holdingsStatements"].extend(
                new_holdings_record.get("holdingsStatements", [])
            )
            old_holdings_record["holdingsStatements"] = dedupe(
                old_holdings_record["holdingsStatements"]
            )
        if old_holdings_record.get("formerIds"):
            old_holdings_record["formerIds"].extend(
                new_holdings_record.get("formerIds", [])
            )
            old_holdings_record["formerIds"] = list(
                set(old_holdings_record["formerIds"])
            )

    @staticmethod
    def load_previously_generated_holdings(holdings_file_path, fields_criteria):
        with open(holdings_file_path) as holdings_file:
            prev_holdings = {}
            for row in holdings_file:
                stored_holding = json.loads(row.split("\t")[-1])
                stored_key = HoldingsHelper.to_key(stored_holding, fields_criteria)
                if stored_key in prev_holdings:
                    message = (
                        f"Previously stored holdings key {stored_key} already exists in the "
                        f"list of previously stored Holdings. You have likely not used the same "
                        f"matching criterias ({fields_criteria}) as you did in the previous process"
                    )
                    raise custom_exceptions.TransformationRecordFailedError(message)
                prev_holdings[stored_key] = stored_holding
            return prev_holdings

    @staticmethod
    def setup_holdings_id_map(result_path):
        holdings_id_dict_path = Helper.setup_path(result_path, "holdings_id_map.json")
        with open(holdings_id_dict_path, "r") as holdings_id_map_file:
            holdings_id_map = json.load(holdings_id_map_file)
            logging.info("Loaded %s holdings ids", len(holdings_id_map))
            return holdings_id_map


def dedupe(list_of_dicts):
    # TODO: Move to interface or parent class
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]
