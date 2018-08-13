import json
import argparse
import sys
import requests
import time
import uuid
from pymarc import MARCReader
from os import listdir
from os.path import isfile, join
from jsonschema import validate
from random import randint
import xml.etree.ElementTree as ET


class Mapper:

    def __init__(self, config):
        cql_all = '?limit=100&query=cql.allRecords=1%20sortby%20name'
        self.okapi_url = config.okapi_url
        self.okapi_headers = {'x-okapi-token': config.okapi_token,
                             'x-okapi-tenant': config.tenant_id,
                             'content-type': 'application/json'}
        self.identifier_types = self.folio_get("/identifier-types"+cql_all,
                                                 "identifier_types")
        print("Fetching ContributorTypes...")
        self.contributor_types = self.folio_get("/contributor-types"+cql_all,
                                              "contributor_types")
        print("Fetching ContributorNameTypes...")
        self.contributor_name_types = self.folio_get("/contributor-name-types"+cql_all,
                                                  "contributor_name_types")
        print("Fetching JSON schema for Instances...")
        self.instance_json_schema = self.get_instance_json_schema()

        print("Fetching Instance types...")
        self.instance_types = self.folio_get("/instance-types"+cql_all,
                                            "instanceTypes")

        print("Fetching valid language codes...")
        self.language_codes = self.fetch_language_codes()

        print("Fetching Instance formats...")
        self.instance_formats = self.folio_get("/instance-formats"+cql_all,
                                              "instanceFormats")

    # Parses a bib recod into a FOLIO Inventory instance object
    def parse_bib_record(self, marc_record, folio_record, record_source):
        folio_record['title'] = marc_record.title() or ''
        folio_record['source'] = record_source
        folio_record['contributors'] = list(self.get_contributors(marc_record,
                                                                 self.contributor_name_types))
        folio_record['identifiers'] = list(self.get_identifiers(marc_record,
                                                               self.identifier_types))
        # TODO: Add instanceTypeId
        folio_record['instanceTypeId'] = self.instanceTypes[randint(0, len(self.instanceTypes)-1)]['id']
        folio_record['alternativeTitles'] = self.get_alternative_titles(marc_record)
        folio_record['series'] = list(set(self.get_series(marc_record)))
        folio_record['edition'] = (marc_record['250'] and
                                   marc_record['250']['a']) or ''
        folio_record['subjects'] = list(set(self.get_subjects(marc_record)))
        # TODO: add classification
        # folio_record['classification'] = [{'classificationNumber':'a',
        # 'classificationTypeId':''}]
        folio_record['publication'] = list((self.get_publication(marc_record)))
        folio_record['urls'] = list(set(self.get_urls(marc_record)))
        # TODO: add instanceFormatId
        folio_record['instanceFormatId'] = self.instanceFormats[randint(0, len(self.instanceFormats)-1)]['id']
        # TODO: add physical description
        folio_record['physicalDescriptions'] = ['pd1', 'pd2']
        # TODO: add languages
        folio_record['languages'] = self.get_languages(marc_record,
                                                       self.language_codes)
        # TODO: add notes
        folio_record['notes'] = ['', '']

        validate(fRec, self.instance_json_schema)
        return folio_record

    def folio_record_template(id):
        return {'id': str(id)}

    def folio_get(self, path, key):
        print("Fetching {} from {}".format(key, self.okapi_url+key))
        req = requests.get(self.okapi_url+path,
                           headers=self.okapi_headers)
        return json.loads(req.text)[key]

    # Fetches the JSON Schema for instances
    def get_instance_json_schema():
        url = 'https://raw.github.com'
        path = '/folio-org/mod-inventory-storage/master/ramls/instance.json'
        req = requests.get(url+path)
        return json.loads(req.text)

    # Collects contributors from the record and adds the apropriate Ids
    def get_contributors(self, marc_record):
        a = {'100': next(f['id'] for f in self.contributor_name_types
                         if f['name'] == 'Personal name'),
             '110': next(f['id'] for f in self.contributor_name_types
                         if f['name'] == 'Corporate name'),
             '111': next(f['id'] for f in self.contributor_name_types
                         if f['name'] == 'Meeting name')}
        first = 0
        for field_tag in a.keys():
            for field in marc_record.get_fields(field_tag):
                first += 1
                yield {'name': field.format_field(),
                       'contributorNameTypeId': a[field_tag],
                       'contributorTypeText': next(f['name'] for f
                                                   in self.contributor_name_types
                                                   if f['id'] == a[field_tag]),
                       'primary': first < 2}

    def get_urls(marc_record):
        for field in marc_record.get_fields('856'):
            yield field['u'] or ''

    def get_subjects(marc_record):
        tags = ['600', '610', '611', '630', '647', '648', '650', '651'
                '653', '654', '655', '656', '657', '658', '662']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield field.format_field()

    def get_alternative_titles(marc_record):
        tags = ['246', '247']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield field.format_field()

    def get_publication(marc_record):
        tags = ['260', '264']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield {'publisher': field['b'] or '',
                       'place': field['a'] or '',
                       'dateOfPublication': field['c'] or ''}

    def get_series(marc_record):
        tags = ['440', '490']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield field['a'] or ''

    def get_languages(self, marc_record):
        languages = (marc_record['041'] and marc_record['041']
                     .get_subfields('a', 'b', 'd', 'e', 'f', 'g', 'h',
                                    'j', 'k', 'm', 'n')) or []
        from_008 = marc_record['008'][35:37]
        if from_008 not in ['###', 'zxx']:
            languages.append(from_008)
        languages = list(set(filter(None, languages)))
        # TODO: test agianist valide language codes
        return languages

    def fetch_language_codes():
        url = "https://www.loc.gov/standards/codelists/languages.xml"
        tree = ET.fromstring(requests.get(url).content)
        ns = "{info:lc/xmlns/codelist-v1}"
        xp = "{0}languages/{0}language/{0}code".format(ns)
        for code in tree.findall(xp):
            yield code.text

    # Collects Identifiers from the record and adds the appropriate metadata"""
    def get_identifiers(self, marc_record):
        a = {'020': next(f['id'] for f
                         in self.identifier_types if f['name'] == 'ISBN'),
             '022': next(f['id'] for f
                         in self.identifier_types if f['name'] == 'ISSN'),
             '907': '7e591197-f335-4afb-bc6d-a6d76ca3bace'}
        for field_tag in a.keys():
            for field in record.get_fields(field_tag):
                yield {'identifierTypeId': a[field_tag],
                       'value': field.format_field()}


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
mapper = Mapper(args)
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
                        fRec = args.parse_bib_record(record,
                                                  mapper.folio_record_template(uuid.uuid4()),
                                                  args.record_source)
                        if(record['907']['a']):
                            sierraId = record['907']['a'].replace('.b', '')[:-1]
                            idMap[sierraId] = fRec['id']
                        if(args.postgres_dump):
                            results_file.write('{}\t{}\n'.format(fRec['id'], json.dumps(fRec)))
                        else:
                            results_file.write('{}\n'.format(json.dumps(fRec)))
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
