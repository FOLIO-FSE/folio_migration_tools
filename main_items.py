'''Main "script."'''
import argparse
import csv
import json

import pymarc
from folioclient.FolioClient import FolioClient
from marc_to_folio.items_default_mapper import ItemsDefaultMapper
from marc_to_folio.items_processor import ItemsProcessor


def main():
    '''Main method. Magic starts here.'''
    # TODO: räknare på allt!
    parser = argparse.ArgumentParser()
    parser.add_argument("records_file", help="path to marc records folder")
    parser.add_argument("result_path", help="path to Instance results file")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("-holdings_id_dict_path", "-ih",
                        help=(""))
    parser.add_argument("-items_id_dict_path", "-it",
                        help=(""))
    parser.add_argument("-postgres_dump", "-p",
                        help=("results will be written out for Postgres"
                              "ingestion. Default is JSON"),
                        action="store_true")
    parser.add_argument("-validate", "-v", help=("Validate JSON data against JSON Schema"),
                        action="store_true")
    args = parser.parse_args()
    print('\tresults file:\t', args.result_path)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t",args.username)
    print("\tPassword:   \tSecret")
    print("\titems idMap will get stored at:\t", args.items_id_dict_path)
    print("\thold idMap will get stored at:\t", args.holdings_id_dict_path)

    print("File to process: {}".format(args.records_file))
    folio_client = FolioClient(args.okapi_url,
                               args.tenant_id,
                               args.username,
                               args.password)
    holdings_id_map = {}
    with open(args.holdings_id_dict_path, 'r') as json_file:
            holdings_id_map = json.load(json_file)
    print("Number of holdings in ID map: {}".format(len(holdings_id_map)))
    default_mapper = ItemsDefaultMapper(folio_client, holdings_id_map)
    print("Starting")
    print("Rec./s\t\tTot. recs\t\t")

    with open(args.result_path, 'w+') as results_file:
        processor = ItemsProcessor(default_mapper,
                                   folio_client,
                                   results_file, args)
        with open(args.records_file, 'r+') as items_file:
            recs = csv.DictReader(items_file, delimiter='|')
            for rec in recs:
                processor.process_record(rec)
    # wrap up
    print("Done. Wrapping up...")
    processor.wrap_up()
    print("done")


if __name__ == '__main__':
    main()
