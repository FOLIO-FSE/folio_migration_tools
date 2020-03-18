""" Class that processes each MARC record """
import time
import traceback
import json
import os
from jsonschema import ValidationError, validate


class ItemsProcessor:
    """the processor"""

    def __init__(self, mapper, folio_client, results_file, args):
        self.results_file = results_file
        self.item_schema = folio_client.get_item_schema()
        self.records_count = 0
        self.written_items = 0
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
            # raise value_error
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
        path = os.path.join(self.args.result_path, "item_id_map.json")
        print("Saving map of {} old and new IDs to {}".format(len(id_map), path))
        with open(path, "w+") as id_map_file:
            json.dump(id_map, id_map_file, indent=4)
        print(self.mapper.folio.missing_location_codes)
        self.mapper.wrap_up()
        print(f"{self.written_items} written")


def write_to_file(file, pg_dump, folio_record):
    """Writes record to file. pg_dump=true for importing directly via the
    psql copy command"""
    if pg_dump:
        file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
    else:
        file.write("{}\n".format(json.dumps(folio_record)))
