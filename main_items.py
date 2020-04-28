'''Main "script."'''
import argparse
import csv

import traceback
import json
import logging
import os
import pymarc
from os import listdir
from os.path import isfile, join
from folioclient.FolioClient import FolioClient
from marc_to_folio.items_default_mapper import ItemsDefaultMapper
from marc_to_folio.items_processor import ItemsProcessor


def main():
    """Main method. Magic starts here."""
    # TODO: räknare på allt!
    csv.register_dialect("tsv", delimiter="\t")
    logging.basicConfig(level=logging.CRITICAL)
    module = __import__("marc_to_folio")
    mappers = [cls.__name__ for cls in ItemsDefaultMapper.__subclasses__()]
    print(mappers)
    parser = argparse.ArgumentParser()
    parser.add_argument("records_file", help="path to items file")
    parser.add_argument("result_path", help="path to Instance results file")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("-holdings_id_dict_path", "-ih", help=(""))
    parser.add_argument("-location_map_path", "-l", help=("path of location map"))
    parser.add_argument("-items_mapper_file", "-it", help=(""))
    parser.add_argument(
        "-postgres_dump",
        "-p",
        help=("results will be written out for Postgres" "ingestion. Default is JSON"),
        action="store_true",
    )
    parser.add_argument(
        "-validate",
        "-v",
        help=("Validate JSON data against JSON Schema"),
        action="store_true",
    )
    args = parser.parse_args()
    print("\tresults file:\t", args.result_path)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    print("\thold idMap will get stored at:\t", args.holdings_id_dict_path)
    files = [
        f for f in listdir(args.records_file) if isfile(join(args.records_file, f))
    ]
    print("File to process: {}".format(args.records_file))
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    holdings_id_map = {}
    items_map = {}
    location_map = {}
    with open(args.holdings_id_dict_path, "r") as json_file:
        holdings_id_map = json.load(json_file)
    print("Number of holdings in ID map: {}".format(len(holdings_id_map)))

    if args.location_map_path:
        with open(args.location_map_path) as location_map_f:
            location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
        print(f"Locations in map: {len(location_map)}")

    with open(args.items_mapper_file) as items_mapper_file:
        items_map = json.load(items_mapper_file)
    try:
        mapper_name = next(
            (m for m in mappers if args.mapper and args.mapper in m),
            "ItemsDefaultMapper",
        )
        print(mapper_name)
        class_ = getattr(module, mapper_name)
        mapper = class_(folio_client, items_map, holdings_id_map, location_map)

    except Exception as ee:
        print("could not instantiate mapper")
        raise ee
    print("Starting")
    print("Rec./s\t\tTot. recs\t\t")
    failed_files = list()
    with open(os.path.join(args.result_path, "folio_items.json"), "w+") as results_file:
        processor = ItemsProcessor(mapper, folio_client, results_file, args)
        f = 0
        i = 0
        for file_name in files:
            f += 1
            print(f"running {file_name}")
            try:
                with open(
                    join(args.records_file, file_name),
                    "r+",
                    errors="replace",
                    encoding="utf-8-sig",
                ) as items_file:
                    for rec in mapper.get_records(items_file):
                        i += 1
                        processor.process_record(rec)
            except UnicodeDecodeError as decode_error:
                print(f"UnicodeDecodeError in {file_name} for index")
                print("UnicodeDecodeError in {}:\t {}".format(file_name, decode_error))
                print("File {} needs fixing".format(file_name))
                failed_files.append(file_name)
            except Exception as ee:
                raise ee
    # wrap up
    print("Done. Wrapping up...")
    processor.wrap_up()
    print("Failed files:")
    print(json.dumps(failed_files, sort_keys=True, indent=4))
    print("done")
    print(f"processt {i} records in {f} files")


if __name__ == "__main__":
    main()
