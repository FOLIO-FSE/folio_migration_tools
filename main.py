import json, sys, requests,pymarc,time, glob, uuid
print("Script starting")

def getContributors(marcRecord, contributorNameTypes):
    a = { '100' : next(f['id'] for f in contributorNameTypes if f['name'] == 'Personal name') 
         ,'110' : next(f['id'] for f in contributorNameTypes if f['name'] == 'Corporate name')
         ,'111' : next(f['id'] for f in contributorNameTypes if f['name'] == 'Meeting name') }    
    for fieldTag in a.keys():
         for field in record.get_fields(fieldTag):
             yield {'name':field.format_field(), 'contributorNameTypeId': a[fieldTag], 'primary':'false'}

def getIdentifiers(marcRecord, identifierTypes):
    a = {'020' : next(f['id'] for f in identifierTypes if f['name'] == 'ISBN')
         ,'022' : next(f['id'] for f in identifierTypes if f['name'] == 'ISSN')}
    for fieldTag in a.keys():
        for field in record.get_fields(fieldTag):
            yield {'identifierTypeId':a[fieldTag], 'value' : field.format_field() }

def getFoliIdentifierTypes(okapiUrl, okapiHeaders):
    path = '/identifier-types?limit=100&query=cql.allRecords=1%20sortby%20name'
    req =  requests.get(okapiUrl+path, headers = okapiHeaders)
    return json.loads(req.text)['identifierTypes']

def getFolioContributorTypes(okapiUrl, okapiHeaders):
    path = '/contributor-types?limit=100&query=cql.allRecords=1 sortby name'
    req =  requests.get(okapiUrl+path, headers = okapiHeaders)
    return json.loads(req.text)

def getFolioContributorNameTypes(okapiUrl, okapiHeaders):
    path = '/contributor-name-types?limit=100&query=cql.allRecords=1 sortby ordering'
    req =  requests.get(okapiUrl+path, headers = okapiHeaders)
    return json.loads(req.text)['contributorNameTypes']

def parseBibRecord(marcRecord,folioRedord, contributorNameTypes, identifierTypes):
    folioRedord['title'] = marcRecord.title()
    folioRedord['source'] = "Test source"
    folioRedord['contributors'] = list(getContributors(marcRecord, contributorNameTypes))
    folioRedord['identifiers'] = list(getIdentifiers(marcRecord, identifierTypes))
    return folioRedord
    
def folioRecordTemplate(id):
    return {'id':str(id),
            'source': '', 
            'title' : '', 
            'contributors' : [{'name':'', 'contributorNameTypeId':'', 'primary':True}],
            'identifiers':[ {'identifierTypeId':'','value':''}
            ],
            'instanceTypeId': ''
           }


from os import listdir
from os.path import isfile, join

print('Will post data to')
okapiUrl = sys.argv[3]
print("\tOkapi URL:\t",okapiUrl)
tenantId = sys.argv[4]
print("\tTenanti Id:\t",tenantId)
okapiToken = sys.argv[5]
print("\tToken:   \t",okapiToken)
okapiHeaders =  {'x-okapi-token': okapiToken,'x-okapi-tenant': tenantId, 'content-type': 'application/json'}

holdings = 0
records = 0
start = time.time()
files = onlyfiles = [f for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f))]
bufsize = 1

print("Files to process:")
print(json.dumps(files, sort_keys=True, indent=4))
print("fetching foliIdentifierTypes")
foliIdentifierTypes = getFoliIdentifierTypes(okapiUrl, okapiHeaders)
print(foliIdentifierTypes)
print("done")
print("fetching CreatorTypes")
contributorNameTypes = getFolioContributorNameTypes(okapiUrl, okapiHeaders)
print(contributorNameTypes)
print("done")


for f in files:
    from pymarc import MARCReader
    with open(sys.argv[1]+f, 'rb') as fh:
        reader = MARCReader(fh,'rb')
        for record in reader:
            try:
                records += 1
                if record['004']:
                    holdings += 1
                else:
                    fRec = parseBibRecord(record, folioRecordTemplate(uuid.uuid4()), contributorNameTypes, foliIdentifierTypes)
                    if records % 10000 == 0 :
                        elapsed = records/(time.time() - start)
                        print(json.dumps(fRec, sort_keys=True, indent=4))
                        printTemplate = "Records/second: {}\tHoldings: {}\tTotal records:{}\tCurrent file:{}" 
                        print(printTemplate.format(elapsed, holdings, records,f)) #,end='\r')
            except:
                print('error!')
print("Script finished")
