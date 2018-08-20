import json
import argparse
import sys
import time
from marc_to_folio.MtFMapper import MtFMapper
from pymarc import MARCReader
from os import listdir
from os.path import isfile, join


def write_to_file(f, pg_dump, folio_record):
    if(pg_dump):
        f.write('{}\t{}\n'.format(folio_record['id'],
                                  json.dumps(folio_record)))
    else:
        f.write('{}\n'.format(json.dumps(folio_record)))


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
parser.add_argument("record_source",
                    help=("name of the source system or collection from "
                          "which the records are added"))
parser.add_argument("-id_dict_path", "-i",
                    help=("path to file saving a dictionary of Sierra ids "
                          "and new InstanceIds to be used for matching the"
                          "right holdings and items to the right instance."))
parser.add_argument("-postgres_dump",
                    "-p",
                    help=("results will be written out for Postgres ingestion."
                          " Default is JSON"),
                    action="store_true")
args = parser.parse_args()

print('Will post data to')
print('\tresults file:\t', args.result_path)
print("\tOkapi URL:\t", args.okapi_url)
print("\tTenanti Id:\t", args.tenant_id)
print("\tToken:   \t", args.okapi_token)
print("\tRecord source:\t", args.record_source)
print("\tidMap will get stored at:\t", args.id_dict_path)
id_dict_path = args.id_dict_path
holdings = 0
records = 0
start = time.time()
files = [f for f in listdir(args.source_folder)
         if isfile(join(args.source_folder, f))]
print("Files to process:")
print(json.dumps(files, sort_keys=True, indent=4))
idMap = {}
mapper = MtFMapper(args)
print("Starting")
print("Rec./s\t\tHolds\t\tTot. recs\t\tFile\t\t")
with open(args.result_path, 'w+') as results_file:
    for f in files:
        with open(sys.argv[1]+f, 'rb') as fh:
            reader = MARCReader(fh, 'rb',
                                hide_utf8_warnings=True,
                                utf8_handling='replace')
            for record in reader:
                try:
                    records += 1
                    if record['004']:
                        holdings += 1
                    else:
                        folio_rec = mapper.parse_bib_record(record,
                                                            args.record_source)
                        if(record['907']['a']):
                            sierra_id = record['907']['a'].replace('.b', '')[:-1]
                            idMap[sierra_id] = folio_rec['id']
                        write_to_file(results_file,
                                      args.postgres_dump,
                                      folio_rec)
                        if records % 1000 == 0:
                            elapsed = '{0:.3g}'.format(records/(time.time() - start))
                            print_template = "{}\t\t{}\t\t{}\t\t{}\t\t{}"
                            print(print_template.format(elapsed,
                                                        holdings,
                                                        records,
                                                        f,
                                                        len(idMap)), end='\r')
                except Exception as inst:
                    print(type(inst))
                    print(inst.args)
                    print(inst)
    with open(id_dict_path, 'w+') as json_file:
        json.dump(idMap, json_file, sort_keys=True, indent=4)
    print("done")
