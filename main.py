'''Main "script."'''
import argparse
import json
import sys
from os import listdir
from os.path import isfile, join

import pymarc
from marc_to_folio.chalmers_mapper import ChalmersMapper
from marc_to_folio.five_collages_mapper import FiveCollagesMapper
from marc_to_folio.alabama_mapper import AlabamaMapper
from marc_to_folio.default_mapper import DefaultMapper
from marc_to_folio.folio_client import FolioClient
from marc_to_folio.marc_processor import MarcProcessor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    parser.add_argument("result_path", help="path to Instance results file")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("okapi_token", help=("the x-okapi-token."))
    parser.add_argument("data_source", help=("name of this data set"))
    parser.add_argument("holdings_map_path", help=("path to new holdings"))
    parser.add_argument("-id_dict_path", "-i",
                        help=("path to file saving a dictionary of Sierra ids "
                              "and new InstanceIds to be used for matching the"
                              "holdings and items to the right instance."))
    parser.add_argument("-postgres_dump", "-p",
                        help=("results will be written out for Postgres"
                              "ingestion. Default is JSON"),
                        action="store_true")
    parser.add_argument("-chalmers_stuff", "-c",
                        help=("Do special stuff according to Chalmers"),
                        action="store_true")
    parser.add_argument("-five_collages_stuff", "-f",
                        help=("Do special stuff according to Five Collages"),
                        action="store_true")
    parser.add_argument("-alabama_stuff", "-a",
                        help=("Do special stuff according to Alabama"),
                        action="store_true")
    parser.add_argument("-marcxml", "-x", help=("DATA is in MARCXML format"),
                        action="store_true")
    args = parser.parse_args()

    print('\tresults file:\t', args.result_path)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tToken:   \t", args.okapi_token)
    print("\tRecord source:\t", args.data_source)
    print("\tidMap will get stored at:\t", args.id_dict_path)
    print("\tHoldings will get stored at\t", args.holdings_map_path)

    files = [f for f in listdir(args.source_folder)
             if isfile(join(args.source_folder, f))]
    print("Files to process:")
    print(json.dumps(files, sort_keys=True, indent=4))
    folio_client = FolioClient(args)
    default_mapper = DefaultMapper(folio_client)
    if args.chalmers_stuff:
        extra_mapper = ChalmersMapper(folio_client)
    elif args.alabama_stuff:
        extra_mapper = AlabamaMapper(folio_client)
    elif args.five_collages_stuff:
        extra_mapper = FiveCollagesMapper(folio_client)
    else:
        extra_mapper = None
    print("Starting")
    print("Rec./s\t\tHolds\t\tTot. recs\t\t")

    with open(args.result_path, 'w+') as results_file:
        processor = MarcProcessor(default_mapper, extra_mapper, folio_client,
                                  results_file, args)
        for file_name in files:
            f_path = sys.argv[1]+file_name
            # print("loading MARC21 records from {}".format(f_path))
            if args.marcxml:
                pymarc.map_xml(processor.process_record, f_path)
            else:
                with open(f_path, 'rb') as marc_file:
                    pymarc.map_records(processor.process_record, marc_file)
    # wrap up
    print("Done. Wrapping up...")
    processor.wrap_up()
    print("done")


if __name__ == '__main__':
    main()
