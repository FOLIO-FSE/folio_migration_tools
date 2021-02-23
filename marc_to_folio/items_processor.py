""" Class that processes each MARC record """
import time
import traceback
import json
import os
from datetime import datetime as dt
from jsonschema import ValidationError, validate


class ItemsProcessor:
    """the processor"""

    def __init__(self, mapper, folio_client, results_file, args):
        self.results_file = results_file
        self.item_schema = folio_client.get_item_schema()
        self.stats = {}
        self.migration_report = {}
        self.records_count = 0
        self.mapper = mapper
        self.instance_id_map = {}
        self.holdings_id_map = {}
        self.args = args
        self.start = time.time()

    def process_record(self, record):
        """processes a marc item record and saves it"""
        try:
            self.records_count += 1
            # Transform the item to a FOLIO record
            folio_rec = self.mapper.parse_item(record)
            if self.args.validate:
                validate(folio_rec, self.item_schema)
            # write record to file
            if folio_rec:
                write_to_file(self.results_file, self.args.postgres_dump, folio_rec)
                add_stats(self.stats, "Number of Items written to disk")
            # Print progress
            if self.records_count % 10000 == 0:
                elapsed = self.records_count / (time.time() - self.start)
                elapsed_formatted = "{}".format(elapsed)
                print(
                    "{}\t\t{}".format(elapsed_formatted, self.records_count), flush=True
                )
        except ValueError as value_error:
            # print(marc_record)
            print(value_error)
            # print(marc_record)
            print("Removing record from idMap")
            raise value_error
        except ValidationError as validation_error:
            print("Error validating record. Halting...")
            raise validation_error
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print(inst)
            traceback.print_exc()
            print(record)
            raise inst

    def wrap_up(self):
        """Finalizes the mapping by writing things out."""
        id_map = self.mapper.item_id_map
        self.mapper.stats = {**self.stats, **self.mapper.stats}
        path = os.path.join(self.args.result_path, "item_id_map.json")
        print("Saving map of {} old and new IDs to {}".format(len(id_map), path))
        with open(path, "w+") as id_map_file:
            json.dump(id_map, id_map_file, indent=4)
        mrf = os.path.join(self.args.result_path, "items_transformation_report.md")
        with open(mrf, "w+") as report_file:
            report_file.write(f"# Item records transformation results   \n")
            report_file.write(f"Time Finished: {dt.isoformat(dt.utcnow())}   \n")
            report_file.write(f"## Item records transformation counters   \n")
            self.mapper.print_dict_to_md_table(
                self.mapper.stats, report_file, "  Measure  ", "Count   \n",
            )
            self.mapper.write_migration_report(report_file)
            self.mapper.print_mapping_report(report_file)

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)


def write_to_file(file, pg_dump, folio_record):
    """Writes record to file. pg_dump=true for importing directly via the
    psql copy command"""
    if pg_dump:
        file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
    else:
        file.write("{}\n".format(json.dumps(folio_record)))


def add_stats(stats, a):
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1