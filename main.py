import json, sys, requests,pymarc,time, glob, uuid
print("Script starting")

def getContributors(marcRecord):
    primary = True
    for fieldName in ['100','110','111']:
         for field in record.get_fields(fieldName):
             yield {'name':field.format_field(), 'contributorNameTypeId':'', 'primary':primary}

def getFoliIdentifierTypes(okapiUrl, okapiHeaders):
    path = '/identifier-types?limit=100&query=cql.allRecords=1%20sortby%20name'
    req =  requests.get(okapiUrl+path, headers = okapiHeaders)
    return json.loads(req.text)

def getFolioContributorTypes(okapiUrl, okapiHeaders):
    path = '/contributor-types?limit=100&query=cql.allRecords=1 sortby name'
    req =  requests.get(okapiUrl+path, headers = okapiHeaders)
    return json.loads(req.text)

def getFolioContributorNameTypes(okapiUrl, okapiHeaders):
    path = '/contributor-name-types?limit=100&query=cql.allRecords=1 sortby ordering'
    req =  requests.get(okapiUrl+path, headers = okapiHeaders)
    return json.loads(req.text)

def parseBibRecord(marcRecord,folioRedord):
    folioRedord['title'] = marcRecord.title()
    folioRedord['source'] = "Test source"
    folioRedord['contributors'] = list(getContributors(marcRecord))
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
foliIdentifierTypes = getFoliIdentifierTypes(okapiUrl, okapiHeaders)
files = onlyfiles = [f for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f))]
bufsize = 1 
print("Files to process:")
print(json.dumps(files, sort_keys=True, indent=4))
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
                    fRec = parseBibRecord(record, folioRecordTemplate(uuid.uuid4()))
                    if records % 10000 == 0 :
                        elapsed = records/(time.time() - start)
                        print(json.dumps(fRec, sort_keys=True, indent=4))
                        printTemplate = "Records/second: {}\tHoldings: {}\tTotal records:{}\tCurrent file:{}" 
                        print(printTemplate.format(elapsed, holdings, records,f)) #,end='\r')
            except:
                println('error!')
print("Script finished")
