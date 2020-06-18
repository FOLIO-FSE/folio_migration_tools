'''Main "script."'''
import argparse
import os
import csv
import logging
import json
import pymarc
from folioclient.FolioClient import FolioClient
from marc_to_folio.holdings_processor import HoldingsProcessor
from marc_to_folio import HoldingsDefaultMapper


def main():
    """Main method. Magic starts here."""
    # TODO: räknare på allt!
    logging.basicConfig(level=logging.CRITICAL)
    module = __import__("marc_to_folio")
    mappers = [cls.__name__ for cls in HoldingsDefaultMapper.__subclasses__()]
    print(mappers)
    parser = argparse.ArgumentParser()
    parser.add_argument("records_file", help="path to marc records folder")
    parser.add_argument("result_folder", help="path to results folder")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument(
        "-postgres_dump",
        "-p",
        help=("results will be written out for Postgres" "ingestion. Default is JSON"),
        action="store_true",
    )
    parser.add_argument("-map_path", "-m", help=("path of location map"))
    parser.add_argument(
        "-marcxml", "-x", help=("DATA is in MARCXML format"), action="store_true"
    )
    args = parser.parse_args()
    print("\tresults are stored at:\t", args.result_folder)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:\t", args.username)
    print("\tPassword:\tSecret")
    print(f"File to process: {args.records_file}")
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    instance_id_map = {}
    location_map = {}
    print(f"Locations in FOLIO: {len(folio_client.locations)}")
    csv.register_dialect("tsv", delimiter="\t")
    if args.location_map_path:
        with open(
            os.path.join(args.location_map_path, "locations.tsv")
        ) as location_map_f:
            location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
        print(f"Locations in map: {len(location_map)}")
    with open(
        os.path.join(args.result_folder, "instance_id_map.json"), "r"
    ) as json_file:
        instance_id_map = json.load(json_file)
    print(len(instance_id_map))
    mapper = HoldingsDefaultMapper(folio_client, instance_id_map, location_map)
    print(f"Number of instances in ID map: {len(instance_id_map)}")
    print("Rec./s\t\tTot. recs\t\t")

    with open(
        os.path.join(args.result_folder, "folio_holdings.json"), "w+"
    ) as results_file:
        processor = HoldingsProcessor(mapper, folio_client, results_file, args)
        if args.marcxml:
            pymarc.map_xml(processor.process_record, args.records_file)
        else:
            with open(args.records_file, "rb") as marc_file:
                pymarc.map_records(processor.process_record, marc_file)
    # wrap up
    print("Done. Wrapping up...")
    processor.wrap_up()
    print("done")


if __name__ == "__main__":
    main()
