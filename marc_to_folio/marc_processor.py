""" Class that processes each MARC record """
import time
import json
from jsonschema import ValidationError, validate


class MarcProcessor:
    """the processor"""

    def __init__(self, mapper, folio_client, results_file, args):
        self.results_folder = args.results_folder
        self.results_file = results_file
        self.instance_schema = folio_client.get_instance_json_schema()
        self.stats = {
            "bibs_processed": 0,
            "failed_bibs": 0,
            "successful_bibs": 0,
            "holdings": 0,
        }
        self.mapper = mapper
        self.args = args
        self.start = time.time()

    def process_record(self, marc_record, inventory_only, num_filtered):
        """processes a marc record and saves it"""
        folio_rec = None
        try:
            self.stats["bibs_processed"] += 1
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.mapper.parse_bib(
                marc_record, self.args.data_source, inventory_only
            )
            if self.args.validate:
                validate(folio_rec, self.instance_schema)
            # write record to file
            write_to_file(self.results_file, self.args.postgres_dump, folio_rec)
            # Print progress
            if self.stats["bibs_processed"] % 10000 == 0:
                elapsed = self.stats["bibs_processed"] / (time.time() - self.start)
                elapsed_formatted = int(elapsed)
                print(
                    f'{elapsed_formatted}\t{self.stats["bibs_processed"]}\tFiltered:{num_filtered}',
                    flush=True,
                )
            self.stats["successful_bibs"] += 1
        except ValueError as value_error:
            self.stats["failed_bibs"] += 1
            #  print(marc_record)
            print(f"{value_error} for {marc_record['001']} ")
            print("Removing record from idMap")
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
            # raise value_error
        except ValidationError as validation_error:
            self.stats["failed_bibs"] += 1
            print("Error validating record. Halting...")
            raise validation_error
        except Exception as inst:
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
            self.stats["failed_bibs"] += 1
            print(type(inst))
            print(inst.args)
            print(inst)
            if folio_rec:
                print(folio_rec)
            # print(marc_record)
            raise inst

    def wrap_up(self):
        """Finalizes the mapping by writing things out."""
        try:
            self.mapper.wrap_up()
        except Exception as exception:
            print(f"error when flushing last srs recs {exception}")
        print(self.stats)
        print("Saving map of old and new IDs")
        print("Number of Instances in map:\t{}".format(len(self.mapper.id_map)))
        if self.mapper.misc_stats:
            print("Misc stats:")
            print(json.dumps(self.mapper.misc_stats, sort_keys=True, indent=4))
        if self.mapper.id_map:
            map_path = self.results_folder + "/instance_id_map.json"
            with open(map_path, "w+") as id_map_file:
                json.dump(self.mapper.id_map, id_map_file, sort_keys=True, indent=4)
        print("Saving holdings created from bibs")
        if any(self.mapper.holdings_map):
            holdings_path = self.results_folder + "/folio_holdings.json"
            with open(holdings_path, "w+") as holdings_file:
                for key, holding in self.mapper.holdings_map.items():
                    write_to_file(holdings_file, False, holding)


def write_to_file(file, pg_dump, folio_record):
    """Writes record to file. pg_dump=true for importing directly via the
    psql copy command"""
    if pg_dump:
        file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
    else:
        file.write("{}\n".format(json.dumps(folio_record)))
