'''Main "script."'''
import argparse
import json
import logging
import sys
import traceback
from os import listdir
from os.path import isfile, join

from folioclient.FolioClient import FolioClient
from pymarc import MARCReader
from marc_to_folio import RulesMapper

from marc_to_folio.marc_processor import MarcProcessor


def main():
    logging.basicConfig(level=logging.CRITICAL)
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
    results_file = join(args.results_folder, "folio_instances.json")
    print("\tresults will be saved at:\t", args.results_folder)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    print("\tRecord source:\t", args.data_source)

    files = [
        f for f in listdir(args.source_folder) if isfile(join(args.source_folder, f))
    ]
    print("Files to process: {}".format(len(files)))
    # print(json.dumps(files, sort_keys=True, indent=4))
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    mapper = RulesMapper(folio_client, args.results_folder)
    print("Starting")
    print("Rec./s\t\tTot. recs\t\t")
    failed_records = list()
    failed_files = list()
    with open(results_file, "w+") as results_file:
        processor = MarcProcessor(mapper, folio_client, results_file, args)
        for file_name in files:
            try:
                with open(join(sys.argv[1], file_name), "rb") as marc_file:
                    reader = MARCReader(marc_file, "rb", permissive=True)
                    reader.hide_utf8_warnings = True
                    if args.force_utf_8:
                        print("FORCE UTF-8 is set to TRUE")
                        reader.force_utf8 = True
                    print("running {}".format(file_name))
                    for record in reader:
                        if record is None:
                            print(
                                "Current chunk: ",
                                reader.current_chunk,
                                " was ignored because the following exception raised: ",
                                reader.current_exception,
                            )
                            failed_records.append(reader.current_chunk)
                        else:
                            processor.process_record(record)
            except Exception as exception:
                print(exception)
                traceback.print_exc()
                print(file_name)
        # wrap up
    print("Done. Wrapping up...")
    processor.wrap_up()
    print("Failed files:")
    print(json.dumps(failed_files, sort_keys=True, indent=4))
    print(f"Failed records ({len(failed_records)}):")
    print(json.dumps(failed_records, indent=4))
    print("done")


if __name__ == "__main__":
    main()
