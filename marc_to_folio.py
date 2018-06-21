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
print("Script starting")


# Collects contributors from the record and adds the apropriate Ids
def getContributors(record, contributorNameTypes):
    a = {'100': next(f['id'] for f in contributorNameTypes
                     if f['name'] == 'Personal name'),
         '110': next(f['id'] for f in contributorNameTypes
                     if f['name'] == 'Corporate name'),
         '111': next(f['id'] for f in contributorNameTypes
                     if f['name'] == 'Meeting name')}
    first = 0
    for fieldTag in a.keys():
        for field in record.get_fields(fieldTag):
            first += 1
            yield {'name': field.format_field(),
                   'contributorNameTypeId': a[fieldTag],
                   'contributorTypeText': next(f['name'] for f
                                               in contributorNameTypes
                                               if f['id'] == a[fieldTag]),
                   'primary': first < 2}


def get_languages(marc_record, valid_languages):
    languages = (marc_record['041'] and marc_record['041']
                 .get_subfields('a', 'b', 'd', 'e', 'f', 'g', 'h',
                                'j', 'k', 'm', 'n')) or []
    from_008 = marc_record['008'][35:37]
    if from_008 not in ['###', 'zxx']:
        languages.append(from_008)
    languages = list(set(filter(None, languages)))
    return languages


# Collects Identifiers from the record and adds the appropriate metadata"""
def getIdentifiers(marcRecord, identifierTypes):
    a = {'020': next(f['id'] for f
                     in identifierTypes if f['name'] == 'ISBN'),
         '022': next(f['id'] for f
                     in identifierTypes if f['name'] == 'ISSN')}
    for fieldTag in a.keys():
        for field in record.get_fields(fieldTag):
            yield {'identifierTypeId': a[fieldTag],
                   'value': field.format_field()}


# Fetches identifierTypes from FOLIO
def get_folio_instance_formats(okapiUrl, okapiHeaders):
    path = '/instance-formats?limit=100&query=cql.allRecords=1%20sortby%20name'
    req = requests.get(okapiUrl+path,
                       headers=okapiHeaders)
    return json.loads(req.text)['instanceFormats']


# Fetches identifierTypes from FOLIO
def get_folio_instance_types(okapiUrl, okapiHeaders):
    path = '/instance-types?limit=100&query=cql.allRecords=1%20sortby%20name'
    req = requests.get(okapiUrl+path,
                       headers=okapiHeaders)
    return json.loads(req.text)['instanceTypes']


# Fetches identifierTypes from FOLIO
def getFoliIdentifierTypes(okapiUrl, okapiHeaders):
    path = '/identifier-types?limit=100&query=cql.allRecords=1%20sortby%20name'
    req = requests.get(okapiUrl+path,
                       headers=okapiHeaders)
    return json.loads(req.text)['identifierTypes']


# Fetches contribuor types from FOLIO
def getFolioContributorTypes(okapiUrl, okapiHeaders):
    path = '/contributor-types?limit=100&query=cql.allRecords=1 sortby name'
    req = requests.get(okapiUrl+path,
                       headers=okapiHeaders)
    return json.loads(req.text)


# Fetches contributorNameTypes from FOLIO
def getFolioContributorNameTypes(okapiUrl, okapiHeaders):
    path = '''/contributor-name-types?
            limit=100&query=cql.allRecords=1 sortby ordering'''
    req = requests.get(okapiUrl+path,
                       headers=okapiHeaders)
    return json.loads(req.text)['contributorNameTypes']


# Fetches the JSON Schema for instances
def getInstanceJSONSchema():
    url = 'https://raw.github.com'
    path = '/folio-org/mod-inventory-storage/master/ramls/instance.json'
    req = requests.get(url+path)
    return json.loads(req.text)


# Parses a bib recod into a FOLIO Inventory instance object
def parseBibRecord(marcRecord, folioRecord,
                   contributorNameTypes, identifierTypes, recordSource,
                   instance_types, instance_formats):
    folioRecord['title'] = marcRecord.title() or ''
    folioRecord['source'] = recordSource
    folioRecord['contributors'] = list(getContributors(marcRecord,
                                                       contributorNameTypes))
    folioRecord['identifiers'] = list(getIdentifiers(marcRecord,
                                                     identifierTypes))
    # TODO: Add instanceTypeId
    folioRecord['instanceTypeId'] = instance_types[randint(0, len(instance_types)-1)]['id']
    # TODO:add alternative titles
    folioRecord['alternativeTitles'] = ["Alternative title1",
                                        "Alternative title 2"]
    # TODO: add series
    folioRecord['series'] = ['Series 1', 'Series 2']
    # TODO: add edtion info
    folioRecord['edition'] = 'Edition'
    # TODO: add subjects
    folioRecord['subjects'] = ['Subject 1', 'Subject 2']
    # TODO: add classification
    # folioRecord['classification'] = [{'classificationNumber':'a',
    # 'classificationTypeId':''}]
    # TODO: add publication
    folioRecord['publication'] = [{'publisher': 'a',
                                   'place': 'b',
                                   'dateOfPublication': 'c'}]
    # TODO: add urls
    folioRecord['urls'] = ['http://dn.se', 'https://svd.se']
    # TODO: add instanceFormatId
    folioRecord['instanceFormatId'] = instance_formats[randint(0, len(instance_formats)-1)]['id']
    # TODO: add physical description
    folioRecord['physicalDescriptions'] = ['pd1', 'pd2']
    # TODO: add languages
    folioRecord['languages'] = get_languages(marcRecord, "")
    # TODO: add notes
    folioRecord['notes'] = ['', '']
    return folioRecord


parser = argparse.ArgumentParser()
parser.add_argument("source_folder", help="path of the folder where the marc files resides")
parser.add_argument("result_path", help="path and name of the results file")
parser.add_argument("okapi_url", help="url of your FOLIO OKAPI endpoint. See settings->software version in FOLIO")
parser.add_argument("tenant_id", help="id of the FOLIO tenant. See settings->software version in FOLIO")
parser.add_argument("okapi_token", help="the x-okapi-token. Easiest optained via F12 in the webbrowser")
parser.add_argument("record_source", help="name of the source system or collection from which the records are added")
parser.add_argument("-id_dict_path", "-i", help="path to file saving a dictionary of Sierra ids and new InstanceIds to be used for matching the right holdings and items to the right instance.")
parser.add_argument("-postgres_dump",
                    "-p",
                    help="results will be written out for Postgres ingestion. Default is JSON",
                    action="store_true")
args = parser.parse_args()


def folioRecordTemplate(id):
    return {'id': str(id)}


print('Will post data to')
print('\tresults file:\t', args.result_path)
print("\tOkapi URL:\t", args.okapi_url)
print("\tTenanti Id:\t", args.tenant_id)
print("\tToken:   \t", args.okapi_token)
okapiHeaders = {'x-okapi-token': args.okapi_token,
                'x-okapi-tenant': args.tenant_id,
                'content-type': 'application/json'}
print("\tRecord source:\t", args.record_source)

print("\tidMap will get stored at:\t", args.id_dict_path)
id_dict_path = args.id_dict_path
holdings = 0
records = 0
start = time.time()
files = [f for f in listdir(args.source_folder)
         if isfile(join(args.source_folder, f))]
bufsize = 1
print("Files to process:")
print(json.dumps(files, sort_keys=True, indent=4))
print("Fetching foliIdentifierTypes...", end='')
foliIdentifierTypes = getFoliIdentifierTypes(args.okapi_url,
                                             okapiHeaders)
print("done")

print("Fetching CreatorTypes...", end='')
contributorNameTypes = getFolioContributorNameTypes(args.okapi_url,
                                                    okapiHeaders)
print("done")
print("Fetching JSON schema for Instances...", end='')
instanceJSONSchema = getInstanceJSONSchema()
print("done")

print("Fetching Instance types...", end='')
instance_types = get_folio_instance_types(args.okapi_url,
                                          okapiHeaders)
print(len(instance_types))
print(instance_types[randint(0, len(instance_types)-1)]['id'])
print("done")

print("Fetching valid language codes...")
#ns = {'ns': 'http://www.loc.gov/standards/codelists/codelist.xsd'}
#req = requests.get('https://www.loc.gov/standards/codelists/languages.xml')
#tree = ET.parse(req.raw)
#root = tree.getroot()
#for lang in root.findall("./ns:languages/ns:language/ns:code"):
#    print(type(lang))
#    print(len(lang))
#    print(lang.text)
print("done")
print("Fetching Instance formats...", end='')
instance_formats = get_folio_instance_formats(args.okapi_url,
                                              okapiHeaders)
print("done")

idMap = {}
print("Starting")
print("Rec./s\t\tHolds\t\tTot. recs\t\tFile\t\t")
with open(args.result_path, 'a') as resultsFile:
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
                        fRec = parseBibRecord(record,
                                              folioRecordTemplate(uuid.uuid4()),
                                              contributorNameTypes,
                                              foliIdentifierTypes,
                                              args.record_source,
                                              instance_types,
                                              instance_formats)
                        if(record['907']['a']):
                            sierraId = record['907']['a'].replace('.b', '')[:-1]
                            idMap[sierraId] = fRec['id']
                        validate(fRec, instanceJSONSchema)
                        if(args.postgres_dump):
                            resultsFile.write('{}\t{}\n'.format(fRec['id'], json.dumps(fRec)))
                        else:
                            resultsFile.write('{}\n'.format(json.dumps(fRec)))
                        if records % 1000 == 0:
                            elapsed = '{0:.3g}'.format(records/(time.time() - start))
                            printTemplate = "{}\t\t{}\t\t{}\t\t{}\t\t{}"
                            print(printTemplate.format(elapsed,
                                                       holdings,
                                                       records,
                                                       f,
                                                       len(idMap)), end='\r')
                except Exception as inst:
                    print(type(inst))
                    print(inst.args)
                    print(inst)
    with open(id_dict_path, 'w') as json_file:
        json.dump(idMap, json_file, sort_keys=True, indent=4)
    print("done")
