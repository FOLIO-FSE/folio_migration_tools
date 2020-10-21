""" Class that processes each MARC record """
import time
import json
from datetime import datetime as dt
import os.path
from jsonschema import ValidationError, validate


class BibsProcessor:
    """the processor"""

    def __init__(self, mapper, folio_client, results_file, args):
        self.migration_report = {}
        self.stats = {}
        self.ils_flavour = args.ils_flavour
        self.results_folder = args.results_folder
        self.results_file = results_file
        self.instance_schema = folio_client.get_instance_json_schema()
        self.mapper = mapper
        self.args = args
        self.start = time.time()

    def process_record(self, marc_record, inventory_only):
        """processes a marc record and saves it"""
        folio_rec = None
        try:
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.mapper.parse_bib(marc_record, inventory_only)
            if self.args.validate:
                validate(folio_rec, self.instance_schema)
            # write record to file
            if not folio_rec.get("title", ""):
                s = f"No title in {marc_record['001'].format_field()}"
                self.add_to_migration_report("Records without titles", s)
                print(s)
                add_stats(self.stats, "Bib records that faile transformation")
            if not folio_rec.get("instanceTypeId", ""):
                s = f"No Instance Type Id in {marc_record['001'].format_field()}"
                self.add_to_migration_report("Records without Instance Type Ids", s)
                print(s)
                add_stats(self.stats, "Bib records that faile transformation")
            else:
                write_to_file(self.results_file, self.args.postgres_dump, folio_rec)
                add_stats(self.stats, "Successfully transformed bibs")

            # Print progress

        except ValueError as value_error:
            self.add_to_migration_report(
                "Records failed to migrate due to Value errors found in Transformation",
                f"{value_error} for {marc_record['001']} ",
            )
            add_stats(self.stats, "Value Errors")
            add_stats(self.stats, "Bib records that faile transformation")
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
        except ValidationError as validation_error:
            add_stats(self.stats, "Validation Errors")
            add_stats(self.stats, "Bib records that failed transformation")
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
        except Exception as inst:
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
            add_stats(self.stats, "Bib records that failed transformation")
            add_stats(self.stats, "Transformation exceptions")
            print(type(inst))
            print(inst.args)
            print(inst)
            print(marc_record)
            if folio_rec:
                print(folio_rec)
            raise inst

    def wrap_up(self):
        """Finalizes the mapping by writing things out."""
        try:
            self.mapper.wrap_up()
        except Exception as exception:
            print(f"error during wrap up {exception}")
        print("Saving map of old and new IDs")
        if self.mapper.id_map:
            map_path = os.path.join(self.results_folder, "instance_id_map.json")
            with open(map_path, "w+") as id_map_file:
                json.dump(self.mapper.id_map, id_map_file, sort_keys=True, indent=4)
            self.stats["Number of Instances in map"] = len(self.mapper.id_map)
        print("Saving holdings created from bibs")
        if any(self.mapper.holdings_map):
            holdings_path = os.path.join(self.results_folder, "folio_holdings.json")
            with open(holdings_path, "w+") as holdings_file:
                for key, holding in self.mapper.holdings_map.items():
                    write_to_file(holdings_file, False, holding)

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)

    def write_migration_report(self):
        for a in self.migration_report:
            print(f"## {a}")
            for b in self.migration_report[a]:
                print(f"{b}\\")


def add_stats(stats, a):
    # TODO: Move to interface or parent class
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1


def write_to_file(file, pg_dump, folio_record):
    """Writes record to file. pg_dump=true for importing directly via the
    psql copy command"""
    if pg_dump:
        file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
    else:
        file.write("{}\n".format(json.dumps(folio_record)))
