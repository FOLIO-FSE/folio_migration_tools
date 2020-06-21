'''Main "script."'''
import argparse
import json
import logging
import csv
import copy
import sys
import traceback
from os import listdir
from os.path import isfile, join
from datetime import datetime as dt
import time

from folioclient.FolioClient import FolioClient
from pymarc import MARCReader
from marc_to_folio import RulesMapper

from marc_to_folio.bibs_processor import BibsProcessor


class Worker:
    """Class that is responsible for the acutal work"""

    def __init__(self, folio_client, results_file, args):
        # msu special case
        self.args = args
        self.stats = {}
        self.migration_report = {}
        self.results_file_path = results_file
        self.allowed_locs = self.setup_allowed(args)
        self.discovery_suppress_locations = self.setup_discovery_suppress_locations(
            args
        )

        self.inventory_only = self.setup_inventory_only(args)
        self.num_filtered = 0
        self.files = [
            f
            for f in listdir(args.source_folder)
            if isfile(join(args.source_folder, f))
        ]
        self.folio_client = folio_client
        print(f"Files to process: {len(self.files)}")
        print(json.dumps(self.files, sort_keys=True, indent=4))

        self.mapper = RulesMapper(
            folio_client, args.results_folder, self.discovery_suppress_locations,
        )
        self.processor = None
        print("Rec./s\t\tTot. recs\t\t")
        self.failed_files = list()
        self.filtered_out_locations = {}
        self.bibids = set()
        print("Init done")

    def work(self):
        print("Starting....")
        with open(self.results_file_path, "w+") as results_file:
            self.processor = BibsProcessor(
                self.mapper, self.folio_client, results_file, self.args,
            )
            self.start = time.time()
            for file_name in self.files:
                try:
                    with open(join(sys.argv[1], file_name), "rb") as marc_file:
                        reader = MARCReader(marc_file, "rb", permissive=True)
                        reader.hide_utf8_warnings = True
                        if self.args.force_utf_8:
                            print("FORCE UTF-8 is set to TRUE")
                            reader.force_utf8 = True
                        print(f"running {file_name}")
                        self.read_records(reader)
                except Exception as exception:
                    print(exception)
                    traceback.print_exc()
                    print(file_name)
            # wrap up
            self.wrap_up()

    def read_records(self, reader):
        for record in reader:
            add_stats(self.stats, "MARC21 records in file before parsing")
            if record is None:
                self.add_to_migration_report(
                    "Bib records that failed to parse. -",
                    f"{reader.current_exception} {reader.current_chunk}",
                )
                add_stats(
                    self.stats, "MARC21 Records with encoding errors - parsing failed"
                )
            else:
                add_stats(self.stats, "MARC21 Records successfully parsed")
                if self.keep_and_clean_for_msu(record):
                    inventory_only = self.check_inventory_only(record)
                    self.processor.process_record(record, False)
                    add_stats(self.stats, "MARC21 Records filtering - remaining")
                else:
                    add_stats(self.stats, "MARC21 Records filtering - filtered out")

    def wrap_up(self):
        print("Done. Wrapping up...")
        self.processor.wrap_up()
        print("Failed files:")
        self.stats = {**self.stats, **self.mapper.stats, **self.processor.stats}

        print("# Bibliographic records migration")
        print(f"Time Run: {dt.isoformat(dt.now())}")
        print("## Bibliographic records migration counters")
        print_dict_to_md_table(
            {k: self.stats[k] for k in sorted(self.stats)}, "    ", "Count"
        )
        print("## filter_by_location_results")
        print_dict_to_md_table(
            {
                k: self.filtered_out_locations[k]
                for k in sorted(self.filtered_out_locations)
            },
            "Location",
            "Count",
        )
        print("## Unmapped MARC tags")
        print_dict_to_md_table(
            {
                k: self.mapper.unmapped_tags[k]
                for k in sorted(self.mapper.unmapped_tags)
            },
            "Tag",
            "Count",
        )

        self.write_migration_report(self.mapper.migration_report)
        self.write_migration_report(self.processor.migration_report)
        self.write_migration_report()
        print("done")

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)

    def check_inventory_only(self, record):
        f945s = record.get_fields("945")
        if any(f945s):
            marc_locs = get_subfield_contents(record, "945", "l")
            inventory_only = any(
                marc_loc
                for marc_loc in marc_locs
                if marc_loc in self.inventory_only and marc_loc
            )
            if inventory_only:
                add_stats(self.stats, "Inventory only")
            return inventory_only
        return False

    def keep_and_clean_for_msu(self, record):
        bib_id = record["907"]["a"]

        # filter out or not?
        # has record any of the allowed locations? If yes, migrate
        marc_locs = get_subfield_contents(record, "945", "l")
        has_allowed = any(
            marc_loc
            for marc_loc in marc_locs
            if marc_loc in self.allowed_locs and marc_loc
        )
        # report on duplicates
        if bib_id in self.bibids:
            add_stats(self.stats, "Duplicate MARC21 records")
            return False
        # Not a dupe, add to list
        self.bibids.add(bib_id)

        # if no items are attached, return the record
        f945s = record.get_fields("945")
        if not any(f945s):
            add_stats(self.stats, "Bibs without any Items")
            return True

        for f856 in record.get_fields("856"):
            for sf in f856.get_subfields("5"):
                if sf and sf.strip() != "6mosu":
                    try:
                        record.remove_field(f856)
                    except Exception as ee:
                        print(f"could not delete {f856} from {record['001']} - {ee}")
                    add_stats(self.stats, f"856 Removed - {sf}")
                    add_stats(self.stats, f"856 Removed Total")
                else:
                    add_stats(self.stats, f"856 Preserved Total")
                    add_stats(self.stats, f"856 Preserved - {sf}")
            if "5" not in f856:
                add_stats(self.stats, f"856 Preserved Total")
                add_stats(self.stats, "856 without $5. Preserving")

        for f945 in record.get_fields("945"):
            ls = f945.get_subfields("l")
            if not any(l for l in ls if l in self.allowed_locs):
                add_stats(self.stats, "945 not in allowed location, removed")
                record.remove_field(f945)
                # print(f"{l} - {[str(f) for f in record.get_fields('945')]}")
            else:
                add_stats(self.stats, "945 in allowed location. Updating to FOLIO one")
                allowed = ""
                run = True
                while run:
                    l = f945.delete_subfield("l")
                    if not l:
                        run = False
                    if l in self.allowed_locs:
                        allowed = l
                f945.add_subfield("l", self.allowed_locs[allowed])

        # report on locations in records
        for loc in set(marc_locs):
            if loc in self.allowed_locs:
                add_stats(
                    self.filtered_out_locations, f"Items w/ location allowed - {loc}"
                )
            else:
                add_stats(
                    self.filtered_out_locations,
                    f"Items w/ location filtered out - {loc}",
                )

        return has_allowed

    def setup_discovery_suppress_locations(self, args):
        if args.msu_locations_path:
            csv.register_dialect("tsv", delimiter="\t")
            with open(args.msu_locations_path) as loc_file:
                self.locations_map = list(csv.DictReader(loc_file, dialect="tsv"))
                locs = [
                    l["folio_code"]
                    for l in self.locations_map
                    if l["suppress_from_discovery"] and l["folio_code"]
                ]
                print(
                    f"{len(locs)} Discovery suppress locations fetched:\n{json.dumps(locs, sort_keys=True, indent=4)}"
                )
                self.stats["Discovery Suppress Locations"] = len(locs)
                return locs

    def setup_allowed(self, args):
        if args.msu_locations_path:
            csv.register_dialect("tsv", delimiter="\t")
            with open(args.msu_locations_path) as loc_file:
                self.locations_map = list(csv.DictReader(loc_file, dialect="tsv"))
                allowed_locs = dict(
                    [
                        (l["iii_code"], l["folio_code"])
                        for l in self.locations_map
                        if l["barcode_handling"]
                        != "Do not import instances/holdings/or item records"
                        and l["iii_code"]
                    ]
                )
                print(
                    f"{len(allowed_locs)} allowed locations fetched:\n{json.dumps(allowed_locs, sort_keys=True, indent=4)}"
                )
                self.stats["Allowed Locations"] = len(allowed_locs)
                return allowed_locs
        else:
            return []

    def setup_inventory_only(self, args):
        if args.msu_locations_path:
            csv.register_dialect("tsv", delimiter="\t")
            with open(args.msu_locations_path) as loc_file:
                self.locations_map = list(csv.DictReader(loc_file, dialect="tsv"))
                inventory_only = list(
                    l["iii_code"]
                    for l in self.locations_map
                    if l["barcode_handling"] == "Inventory only plus holdings, items"
                    and l["iii_code"]
                )
                print(
                    f"{len(inventory_only)} inventory only locations fetched:"
                    f"\n{json.dumps(inventory_only, sort_keys=True, indent=4)}"
                )
                self.stats["Inventory-only Locations"] = len(inventory_only)
                return inventory_only
        else:
            return []

    def write_migration_report(self, other_report=None):
        if other_report:
            for a in other_report:
                print(f"## {a}")
                for b in other_report[a]:
                    print(f"{b}\\")
        else:
            for a in self.migration_report:
                print(f"## {a}")
                for b in self.migration_report[a]:
                    print(f"{b}\\")


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    parser.add_argument("results_folder", help="path to Instance results folder")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("data_source", help=("name of this data set"))
    parser.add_argument("-mapper", "-m", help=("The mapper of choice"))
    parser.add_argument(
        "-force_utf_8",
        "-utf8",
        help=("forcing UTF8 when pasing marc records"),
        action="store_true",
    )
    parser.add_argument(
        "-msu_locations_path", "-f", help=("filter records based on MSU rules")
    )
    parser.add_argument(
        "-postgres_dump",
        "-p",
        help=("results will be written out for Postgres" "ingestion. Default is JSON"),
        action="store_true",
    )
    parser.add_argument(
        "-marcxml", "-x", help=("DATA is in MARCXML format"), action="store_true"
    )
    parser.add_argument(
        "-validate",
        "-v",
        help=("Validate JSON data against JSON Schema"),
        action="store_true",
    )
    args = parser.parse_args()
    return args


def main():
    """Main Method. Used for bootstrapping. """
    # Parse CLI Arguments
    args = parse_args()

    logging.basicConfig(level=logging.CRITICAL)

    results_file = join(args.results_folder, "folio_instances.json")
    print("\tresults will be saved at:\t", args.results_folder)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    print("\tRecord source:\t", args.data_source)
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    # Iniiate Worker
    worker = Worker(folio_client, results_file, args)
    worker.work()


def get_subfield_contents(record, marc_tag, subfield_code):
    fields = record.get_fields(marc_tag)
    res = []
    for f in fields:
        for sf in f.get_subfields(subfield_code):
            res.append(sf)
    return res


def add_stats(stats, a):
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1


def print_dict_to_md_table(my_dict, h1="Measure", h2="Number"):
    # TODO: Move to interface or parent class
    print(f"{h1} | {h2}")
    print("--- | ---:")
    for k, v in my_dict.items():
        print(f"{k} | {v}")


if __name__ == "__main__":
    main()
