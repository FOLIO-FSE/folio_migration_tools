'''Main script.'''
import argparse
import json
import sys
import time
from os import listdir
from os.path import isfile, join

import requests
from jsonschema import ValidationError, validate
from marc_to_folio.chalmers_mapper import ChalmersMapper
from marc_to_folio.default_mapper import DefaultMapper
from marc_to_folio.folio_client import FolioClient
from pymarc import MARCReader


def write_to_file(file, pg_dump, folio_record):
    '''Writes record to file. pg_dump=true for importing directly via the 
    psql copy command'''
    if(pg_dump):
        file.write('{}\t{}\n'.format(folio_record['id'],
                                     json.dumps(folio_record)))
    else:
        file.write('{}\n'.format(json.dumps(folio_record)))


# Fetches the JSON Schema for holdings
def get_holdings_schema():
    url = 'https://raw.github.com'
    path = '/folio-org/mod-inventory-storage/master/ramls/holdingsrecord.json'
    req = requests.get(url+path)
    return json.loads(req.text)


# Fetches the JSON Schema for instances
def get_instance_json_schema():
    url = 'https://raw.github.com'
    path = '/folio-org/mod-inventory-storage/master/ramls/instance.json'
    req = requests.get(url+path)
    return json.loads(req.text)


parser = argparse.ArgumentParser()
parser.add_argument("source_folder",
                    help="path of the folder where the marc files resides")
parser.add_argument("result_path",
                    help="path and name of the results file")
parser.add_argument("okapi_url",
                    help=("url of your FOLIO OKAPI endpoint. See settings->"
                          "software version in FOLIO"))
parser.add_argument("tenant_id",
                    help=("id of the FOLIO tenant. See settings->software "
                          "version in FOLIO"))
parser.add_argument("okapi_token",
                    help=("the x-okapi-token. Easiest optained via F12 in "
                          "the webbrowser"))
parser.add_argument("data_source",
                    help=("name of the source system or collection from "
                          "which the records are added"))
parser.add_argument("holdings_map_path",
                    help=("path to file saving a dictionary of holdings "
                          "created from bib records"))
parser.add_argument("-id_dict_path", "-i",
                    help=("path to file saving a dictionary of Sierra ids "
                          "and new InstanceIds to be used for matching the"
                          "right holdings and items to the right instance."))
parser.add_argument("-postgres_dump", "-p",
                    help=("results will be written out for Postgres ingestion."
                          " Default is JSON"),
                    action="store_true")
parser.add_argument("-chalmers_stuff", "-c",
                    help=("Do special stuff with the data according to"
                          " Chalmers"),
                    action="store_true")
parser.add_argument("-MARCXML", "-x",
                    help=("Do special stuff with the data according to"
                          " Chalmers"),
                    action="store_true")
args = parser.parse_args()

print('Will post data to')
print('\tresults file:\t', args.result_path)
print("\tOkapi URL:\t", args.okapi_url)
print("\tTenanti Id:\t", args.tenant_id)
print("\tToken:   \t", args.okapi_token)
print("\tRecord source:\t", args.data_source)
print("\tidMap will get stored at:\t", args.id_dict_path)
print("\tHoldings will get stored at\t", args.holdings_map_path)
json_schema = get_instance_json_schema()
holdings_schema = get_holdings_schema()
id_dict_path = args.id_dict_path
holdings = 0
records = 0
files = [f for f in listdir(args.source_folder)
         if isfile(join(args.source_folder, f))]
print("Files to process:")
print(json.dumps(files, sort_keys=True, indent=4))
idMap = {}
folio_client = FolioClient(args)
default_mapper = DefaultMapper(folio_client)
chalmers_mapper = ChalmersMapper(folio_client, idMap)
holdings_map = {}
print("Starting")
start = time.time()
print("Rec./s\t\tHolds\t\tTot. recs\t\tFile\t\t")
with open(args.result_path, 'w+') as results_file:
    for f in files:
        with open(sys.argv[1]+f, 'rb') as fh:
            reader = MARCReader(fh, 'rb',
                                hide_utf8_warnings=True,
                                utf8_handling='replace')
            for marc_record in reader:
                try:
                    records += 1
                    if marc_record['004']:
                        holdings += 1
                    else:
                        # Transform the MARC21 to a FOLIO record
                        folio_rec = default_mapper.parse_bib(marc_record,
                                                             args.data_source)
                        if args.chalmers_stuff:
                            # NOTE: Add the folio record's id to a list of
                            # ID-mappings. This is to be handled differently
                            # by different systems.
                            chalmers_mapper.parse_bib(marc_record, folio_rec,
                                                      holdings_map,
                                                      holdings_schema)

                        # validate against json schema
                        validate(folio_rec, json_schema)

                        write_to_file(results_file,
                                      args.postgres_dump,
                                      folio_rec)
                        # Print progress
                        if records % 1000 == 0:
                            e = records/(time.time() - start)
                            elapsed = '{0:.3g}'.format(e)
                            print_template = "{}\t\t{}\t\t{}\t\t{}\t\t{}"
                            print(print_template.format(elapsed,
                                                        holdings,
                                                        records,
                                                        f,
                                                        len(idMap)),
                                  flush=True)
                except ValueError as ve:
                    # print(marc_record)
                    print(ve)
                    print("Removing record from idMap")
                    chalmers_mapper.remove_from_id_map(marc_record)
                except ValidationError as ve:
                    print("Error validating record. Halting...")
                    raise ve
                except Exception as inst:
                    print(type(inst))
                    print(inst.args)
                    print(inst)
                    print(marc_record)
                    raise inst
    print("Saving id map")
    with open(id_dict_path, 'w+') as json_file:
        json.dump(idMap, json_file, sort_keys=True, indent=4)
    print("Saving holdings created from bibs")
    if any(holdings_map):
        with open(args.holdings_map_path, 'w+') as json_file:
            for key, holding in holdings_map.items():
                write_to_file(json_file, False, holding)
    print("done")
