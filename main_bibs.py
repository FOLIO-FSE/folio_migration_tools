'''Main "script."'''
import argparse
import json
import logging
import csv
import sys
import traceback
from os import listdir
from os.path import isfile, join

from folioclient.FolioClient import FolioClient
from pymarc import MARCReader
from marc_to_folio import RulesMapper

from marc_to_folio.marc_processor import MarcProcessor


class Worker:
    """Class that is responsible for the acutal work"""

    def __init__(self, folio_client, results_file, args):
        # msu special case
        self.args = args
        self.results_file_path = results_file
        self.allowed_locs = self.setup_allowed(args)
        self.files = [
            f
            for f in listdir(args.source_folder)
            if isfile(join(args.source_folder, f))
        ]
        self.folio_client = folio_client
        print(f"Files to process: {len(self.files)}")
        print(json.dumps(self.files, sort_keys=True, indent=4))

        self.mapper = RulesMapper(folio_client, args.results_folder)
        print("Rec./s\t\tTot. recs\t\t")
        self.failed_records = list()
        self.failed_files = list()
        self.filtered_out_locations = {}
        print("Init done")

    def work(self):
        print("Starting....")
        with open(self.results_file_path, "w+") as results_file:
            processor = MarcProcessor(
                self.mapper, self.folio_client, results_file, self.args
            )
            for file_name in self.files:
                try:
                    with open(join(sys.argv[1], file_name), "rb") as marc_file:
                        reader = MARCReader(marc_file, "rb", permissive=True)
                        reader.hide_utf8_warnings = True
                        if self.args.force_utf_8:
                            print("FORCE UTF-8 is set to TRUE")
                            reader.force_utf8 = True
                        print(f"running {file_name}")
                        self.read_records(reader, processor)
                except Exception as exception:
                    print(exception)
                    traceback.print_exc()
                    print(file_name)
            # wrap up
            self.wrap_up(processor)

    def read_records(self, reader, processor):
        for record in reader:
            if record is None:
                print(
                    f"Current chunk: {reader.current_chunk} "
                    " was ignored because the following exception: ",
                    f"{reader.current_exception}",
                )
                self.failed_records.append(reader.current_chunk)
            else:
                if not self.allowed_locs or not self.msu_filter(record):
                    processor.process_record(record)

    def wrap_up(self, processor):
        print("Done. Wrapping up...")
        processor.wrap_up()
        print("Failed files:")
        print(json.dumps(self.failed_files, sort_keys=True, indent=4))
        print(f"Failed records ({len(self.failed_records)}):")
        print(json.dumps(self.failed_records, indent=4))
        print("filter_by_location_results:")
        print(json.dumps(self.filtered_out_locations, indent=4))
        print("done")

    def msu_filter(self, record):
        f945s = record.get_fields("945")
        marc_locs = []
        for f in f945s:
            for sf in f.get_subfields("l"):
                marc_locs.append(sf)
        has_allowed = any(
            marc_loc for marc_loc in marc_locs if marc_loc in self.allowed_locs
        )
        for loc in marc_locs:
            if loc in self.allowed_locs:
                add_stats(self.filtered_out_locations, f"{loc} - ALLOWED")
            else:
                add_stats(self.filtered_out_locations, f"{loc} - FILTERED OUT")
        if has_allowed:
            return False
        else:
            add_stats(self.filtered_out_locations, "TOTAL FILTERED OUT")
            return True

    def setup_allowed(self, args):
        if args.msu_locations_path:
            csv.register_dialect("tsv", delimiter="\t")
            with open(args.msu_locations_path) as loc_file:
                self.locations_map = list(csv.DictReader(loc_file, dialect="tsv"))
                allowed_locs = list(
                    l["iii_code"]
                    for l in self.locations_map
                    if l["folio_code"] != "remove" and l["iii_code"]
                )
                print(
                    f"{len(allowed_locs)} allowed locations fetched:\n{json.dumps(allowed_locs)}"
                )
                return allowed_locs
        else:
            return []


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


def add_stats(stats, a):
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1


if __name__ == "__main__":
    main()
