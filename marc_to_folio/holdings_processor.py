""" Class that processes each MARC record """
import time
import json
import traceback
import logging
import os
from datetime import datetime as dt
from jsonschema import ValidationError, validate


class HoldingsProcessor:
    """the processor"""

    def __init__(self, mapper, folio_client, results_file, args):
        self.results_file = results_file
        self.records_count = 0
        self.missing_instance_id_count = 0
        self.mapper = mapper
        self.args = args
        self.start = time.time()
        self.suppress = args.suppress
        logging.info(
            f'map will be saved to {os.path.join(self.args.result_folder, "holdings_id_map.json")}'
        )

    def process_record(self, marc_record):
        """processes a marc holdings record and saves it"""
        try:
            self.records_count += 1
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.mapper.parse_hold(marc_record)
            if not folio_rec.get("instanceId", ""):
                self.missing_instance_id_count += 1
                if self.missing_instance_id_count > 1000:
                    raise Exception(f"More than 1000 missing instance ids. Something is wrong. Last 004: {marc_record['004']}")


            write_to_file(self.results_file, self.args.postgres_dump, folio_rec)
            add_stats(self.mapper.stats, "Holdings records written to disk")
            # Print progress
            if self.records_count % 10000 == 0:
                logging.info(self.mapper.stats)
                elapsed = self.records_count / (time.time() - self.start)
                elapsed_formatted = "{0:.4g}".format(elapsed)
                logging.info(f"{elapsed_formatted}\t\t{self.records_count}")
        except ValueError as value_error:
            add_stats(self.mapper.stats, "Value errors")
            add_stats(self.mapper.stats, "Failed records")
            logging.debug(marc_record)
            logging.error(value_error)
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
        except ValidationError as validation_error:
            add_stats(self.mapper.stats, "Validation errors")
            add_stats(self.mapper.stats, "Failed records")
            logging.error(validation_error)
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
        except Exception as inst:
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
            traceback.print_exc()
            logging.error(type(inst))
            logging.error(inst.args)
            logging.error(inst)
            logging.error(marc_record)
            raise inst

    def wrap_up(self):
        """Finalizes the mapping by writing things out."""
        id_map = self.mapper.holdings_id_map
        path = os.path.join(self.args.result_folder, "holdings_id_map.json")
        logging.warning(
            "Saving map of {} old and new IDs to {}".format(len(id_map), path)
        )
        with open(path, "w+") as id_map_file:
            json.dump(id_map, id_map_file)
        logging.warning(f"{self.records_count} records processed")
        mrf = os.path.join(self.args.result_folder, "holdings_transformation_report.md")
        with open(mrf, "w+") as report_file:
            report_file.write(f"# MFHD records transformation results   \n")
            report_file.write(f"Time Finished: {dt.isoformat(dt.utcnow())}   \n")
            report_file.write(f"## MFHD records transformation counters   \n")
            self.mapper.print_dict_to_md_table(
                self.mapper.stats, report_file, "Measure","Count",
            )
            self.mapper.write_migration_report(report_file)
            self.mapper.print_mapping_report(report_file)
        logging.info(f"Done. Transformation report written to {report_file}")


def write_to_file(file, pg_dump, folio_record):
    """Writes record to file. pg_dump=true for importing directly via the
    psql copy command"""
    if pg_dump:
        file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
    else:
        file.write("{}\n".format(json.dumps(folio_record)))


def add_stats(stats, a):
    # TODO: Move to interface or parent class
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1
