'''Main "script."'''
import argparse
import json
import logging
import traceback
import sys
from os import listdir
from os.path import isfile, join

import pymarc
from folioclient.FolioClient import FolioClient
from pymarc import MARCReader

from marc_to_folio import (AlabamaMapper, ChalmersMapper, DefaultMapper,
                           FiveCollagesMapper)
from marc_to_folio.marc_processor import MarcProcessor


def main():
    # TODO: räknare på allt!
    logging.basicConfig(level=logging.CRITICAL)
    mappers = [cls.__name__ for cls in DefaultMapper.__subclasses__()]
    module = __import__("marc_to_folio")
    parser = argparse.ArgumentParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    parser.add_argument("results_folder", help="path to Instance results folder")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("data_source", help=("name of this data set"))
    parser.add_argument("-mapper", "-m",
                        help=("The mapper of choice"))
    parser.add_argument("-postgres_dump", "-p",
                        help=("results will be written out for Postgres"
                              "ingestion. Default is JSON"),
                        action="store_true")
    parser.add_argument("-marcxml", "-x", help=("DATA is in MARCXML format"),
                        action="store_true")
    parser.add_argument("-validate", "-v", help=("Validate JSON data against JSON Schema"),
                        action="store_true")    
        
    args = parser.parse_args()
    results_file = args.results_folder + '/folio_instances.json'
    print('\tresults will be saved at:\t', args.results_folder)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    print("\tRecord source:\t", args.data_source)

    files = [f for f in listdir(args.source_folder)
             if isfile(join(args.source_folder, f))]
    print("Files to process: {}".format(len(files)))
    # print(json.dumps(files, sort_keys=True, indent=4))
    folio_client = FolioClient(args.okapi_url,
                               args.tenant_id,
                               args.username,
                               args.password)
    try:
        mapper_name = next((m for m in mappers
                            if args.mapper and args.mapper in m), "DefaultMapper")
        print(mapper_name)
        class_ = getattr(module, mapper_name)
        mapper = class_(folio_client, args.results_folder)
    except Exception as ee:
        print("could not instantiate mapper")
        raise ee

    print("Starting")
    print("Rec./s\t\tTot. recs\t\t")

    with open(results_file, 'w+') as results_file:
        processor = MarcProcessor(mapper, folio_client,
                                  results_file, args)
        for file_name in files:
            try:
                with open(sys.argv[1]+file_name, 'rb') as marc_file:
                    reader = MARCReader(marc_file, 'rb')
                    reader.hide_utf8_warnings = True
                    print("running {}".format(file_name))
                    for marc_record in reader:
                        try:
                            processor.process_record(marc_record)
                            #f_path = sys.argv[1]+file_name
                            # print("loading MARC21 records from {}".format(f_path))
                            # if args.marcxml:
                            #    pymarc.map_xml(processor.process_record, f_path)
                            # else:
                            #    with open(f_path, 'rb') as marc_file:
                            #        pymarc.map_records(processor.process_record, marc_file)
                        except UnicodeDecodeError as decode_error:
                            print("UnicodeDecodeError in {}:\t {}"
                                .format(file_name, exception))
                            print("File {} needs fixing".format(file_name))
                        except Exception as exception:
                            print(exception)
                            traceback.print_exc()
            except Exception as exception:
                print(exception)
                traceback.print_exc()
                print(file_name)
        # wrap up
    print("Done. Wrapping up...")
    processor.wrap_up()
    print("done")


if __name__ == '__main__':
    main()
