import json
import requests


class FolioClient:
    '''handles communication and getting values from FOLIO'''
    def __init__(self, config):
        '''# Bootstrapping (loads data needed later in the script.)'''
        cql_all = '?limit=100&query=cql.allRecords=1 sortby name'
        self.okapi_url = config.okapi_url
        self.okapi_headers = {'x-okapi-token': config.okapi_token,
                              'x-okapi-tenant': config.tenant_id,
                              'content-type': 'application/json'}
        self.identifier_types = self.folio_get("/identifier-types",
                                               "identifierTypes",
                                               cql_all)
        print("Fetching ContributorTypes...")
        self.contributor_types = self.folio_get("/contributor-types",
                                                "contributorTypes",
                                                cql_all)
        print("Fetching ContributorNameTypes...")
        self.contrib_name_types = self.folio_get("/contributor-name-types",
                                                 "contributorNameTypes",
                                                 cql_all)
        print("Fetching Instance types...")
        self.instance_types = self.folio_get("/instance-types",
                                             "instanceTypes",
                                             cql_all)

        print("Fetching Instance formats...")
        self.instance_formats = self.folio_get("/instance-formats",
                                               "instanceFormats",
                                               cql_all)

    def folio_get(self, path, key, query=''):
        '''Fetches data from FOLIO and turns it into a json object'''
        url = self.okapi_url+path+query
        print("Fetching {} from {}".format(key, url))
        req = requests.get(url,
                           headers=self.okapi_headers)
        req.raise_for_status()
        result = json.loads(req.text)[key]
        if not any(result):
            print("No {} setup in tenant".format(key))
        return result

    def get_instance_json_schema(self):
        '''Fetches the JSON Schema for instances'''
        url = 'https://raw.github.com'
        path = '/folio-org/mod-inventory-storage/master/ramls/instance.json'
        req = requests.get(url+path)
        return json.loads(req.text)

    def get_holdings_schema(self):
        '''Fetches the JSON Schema for holdings'''
        url = 'https://raw.github.com'
        path = '/folio-org/mod-inventory-storage/master/ramls/holdingsrecord.json'
        req = requests.get(url+path)
        return json.loads(req.text)
