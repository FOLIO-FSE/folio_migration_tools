import json
import sys
import requests
import time
import uuid
from pymarc import MARCReader
from os import listdir
from os.path import isfile, join
from jsonschema import validate

print("Script starting")


# Collects contributors from the record and adds the apropriate Ids
def getContributors(marcRecord, contributorNameTypes):
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
                   contributorNameTypes, identifierTypes, recordSource):
    folioRecord['title'] = marcRecord.title() or ''
    folioRecord['source'] = recordSource
    folioRecord['contributors'] = list(getContributors(marcRecord,
                                                       contributorNameTypes))
    folioRecord['identifiers'] = list(getIdentifiers(marcRecord,
                                                     identifierTypes))
    # TODO: Add instanceTypeId
    folioRecord['instanceTypeId'] = '464102a7-1527-4bd6-9bca-886597cebf29'
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
    folioRecord['instanceFormatId'] = 'e0474071-2d1d-4898-b226-226bd060aa55'
    # TODO: add physical description
    folioRecord['physicalDescriptions'] = ['pd1', 'pd2']
    # TODO: add languages
    folioRecord['languages'] = ['lang1', 'lang2']
    # TODO: add notes
    folioRecord['notes'] = ['', '']
    return folioRecord


def folioRecordTemplate(id):
    return {'id': str(id)}


print('Will post data to')
resultPath = sys.argv[2]
print('\tresults file:\t', resultPath)
okapiUrl = sys.argv[3]
print("\tOkapi URL:\t", okapiUrl)
tenantId = sys.argv[4]
print("\tTenanti Id:\t", tenantId)
okapiToken = sys.argv[5]
print("\tToken:   \t", okapiToken)
okapiHeaders = {'x-okapi-token': okapiToken,
                'x-okapi-tenant': tenantId,
                'content-type': 'application/json'}
recordSource = sys.argv[6]
print("\tRecord source:\t", recordSource)
holdings = 0
records = 0
start = time.time()
files = [f for f in listdir(sys.argv[1])
         if isfile(join(sys.argv[1], f))]
bufsize = 1
print("Files to process:")
print(json.dumps(files, sort_keys=True, indent=4))
print("fetching foliIdentifierTypes")
foliIdentifierTypes = getFoliIdentifierTypes(okapiUrl,
                                             okapiHeaders)
print("done")
print("fetching CreatorTypes")
contributorNameTypes = getFolioContributorNameTypes(okapiUrl,
                                                    okapiHeaders)
print("done")
print("Fetching JSON schema for Instances")
instanceJSONSchema = getInstanceJSONSchema()
with open(resultPath, 'a') as resultsFile:
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
                                              recordSource)
                        validate(fRec, instanceJSONSchema)
                        resultsFile.write('{}\n'.format(json.dumps(fRec)))
                        if records % 1000 == 0:
                            elapsed = '{0:.3g}'.format(records/(time.time() - start))
                            printTemplate = """Records/second: {}\t
                                               Holdings: {}\tTotal records:{}\t
                                               Current file:{}"""
                            print(printTemplate.format(elapsed,
                                                       holdings,
                                                       records, f))
                except Exception as inst:
                    print(type(inst))
                    print(inst.args)
                    print(inst)
    print("done")
