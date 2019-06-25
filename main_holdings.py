'''Main "script."'''
import argparse
import json
import pymarc
from marc_to_folio.folio_client import FolioClient
from marc_to_folio.holdings_marc_processor import HoldingsMarcProcessor
from marc_to_folio.holdings_default_mapper import HoldingsDefaultMapper


def main():
    '''Main method. Magic starts here.'''
    parser = argparse.ArgumentParser()
    parser.add_argument("records_file", help="path to marc records folder")
    parser.add_argument("result_path", help="path to Instance results file")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("-holdings_id_dict_path", "-ih",
                        help=(""))
    parser.add_argument("-instance_id_dict_path", "-i",
                        help=(""))
    parser.add_argument("-postgres_dump", "-p",
                        help=("results will be written out for Postgres"
                              "ingestion. Default is JSON"),
                        action="store_true")
    parser.add_argument("-marcxml", "-x", help=("DATA is in MARCXML format"),
                        action="store_true")
    args = parser.parse_args()
    print('\tresults file:\t', args.result_path)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t",args.username)
    print("\tPassword:   \tSecret")
    print("\tinstance idMap will get stored at:\t", args.instance_id_dict_path)
    print("\thold idMap will get stored at:\t", args.holdings_id_dict_path)

    print("File to process: {}".format(args.records_file))
    folio_client = FolioClient(args)
    instance_id_map = {}
    with open(args.instance_id_dict_path, 'r') as json_file:
            instance_id_map = json.load(json_file)
    print("Number of instances in ID map: {}".format(len(instance_id_map)))
    default_mapper = HoldingsDefaultMapper(folio_client, instance_id_map)
    print("Starting")
    print("Rec./s\t\tTot. recs\t\t")

    with open(args.result_path, 'w+') as results_file:
        processor = HoldingsMarcProcessor(default_mapper,
                                          folio_client,
                                          results_file, args)
        if args.marcxml:
            pymarc.map_xml(processor.process_record, args.records_file)
        else:
            with open(args.records_file, 'rb') as marc_file:
                pymarc.map_records(processor.process_record, marc_file)
    # wrap up
    print("Done. Wrapping up...")
    processor.wrap_up()
    print("done")


if __name__ == '__main__':
    main()
