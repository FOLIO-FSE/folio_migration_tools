import json
import requests


class FolioClient:
    '''handles communication and getting values from FOLIO'''
    def __init__(self, config):
        self.missing_location_codes = set()
        '''# Bootstrapping (loads data needed later in the script.)'''
        cql_all = '?limit=100&query=cql.allRecords=1 sortby name'
        self.okapi_url = config.okapi_url
        self.tenant_id = config.tenant_id
        self.username = config.username
        self.password = config.password
        print("Authenticating user {} to FOLIO...".format(self.username))
        self.login()
        print("Logged in. Token acquired.")
        self.okapi_headers = {'x-okapi-token': self.okapi_token,
                              'x-okapi-tenant': self.tenant_id,
                              'content-type': 'application/json'}
        self.identifier_types = self.folio_get("/identifier-types",
                                               "identifierTypes",
                                               cql_all)
        print(len(self.identifier_types))

        print("Fetching ContributorTypes...")
        self.contributor_types = self.folio_get("/contributor-types",
                                                "contributorTypes",
                                                cql_all)
        print(len(self.contributor_types))

        print("Fetching ContributorNameTypes...")
        self.contrib_name_types = self.folio_get("/contributor-name-types",
                                                 "contributorNameTypes",
                                                 cql_all)
        print(len(self.contrib_name_types))
        print("Fetching Instance types...")
        self.instance_types = self.folio_get("/instance-types",
                                             "instanceTypes",
                                             cql_all)
        print(len(self.instance_types))

        print("Fetching Instance formats...")
        self.instance_formats = self.folio_get("/instance-formats",
                                               "instanceFormats",
                                               cql_all)
        print(len(self.instance_formats))

        print("Fetching Alternative title types...")
        self.alt_title_types = self.folio_get("/alternative-title-types",
                                              "alternativeTitleTypes",
                                              cql_all)
        print(len(self.alt_title_types))

        print("Fetching locations...")
        self.locations = self.folio_get("/locations",
                                        "locations",
                                        cql_all)
        print(len(self.locations))

        print("Fetching Classification types...")
        self.class_types = self.folio_get("/classification-types",
                                          "classificationTypes",
                                          cql_all)
        print(len(self.class_types))

    def login(self):
        '''Logs into FOLIO in order to get the okapi token'''
        try:
            headers = {
                'x-okapi-tenant': self.tenant_id,
                'content-type': 'application/json'}
            payload = {"username": self.username,
                       "password": self.password}
            path = "/authn/login"
            url = self.okapi_url + path
            req = requests.post(url, data=json.dumps(payload), headers=headers)
            if req.status_code != 201:
                print(req.text)
                raise ValueError("Request failed {}".format(req.status_code))
            self.okapi_token = req.headers.get('x-okapi-token')
            self.refresh_token = req.headers.get('refreshtoken')
        except Exception as exception:
            print("Failed login request. No login token acquired.")
            raise exception

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

    def get_item_schema(self):
        '''Fetches the JSON Schema for holdings'''
        url = 'https://raw.github.com'
        path = '/folio-org/mod-inventory-storage/master/ramls/item.json'
        req = requests.get(url+path)
        return json.loads(req.text)

    def get_location_id(self, location_code):
        try:
            return next(l['id'] for l in self.locations
                        if location_code.strip() == l['code'])
        except Exception as exception:
            print("No location with code '{}' in locations"
                  .format(location_code.strip()))
            print(exception)
            self.missing_location_codes.add(location_code)
            return next(l['id'] for l in self.locations
                        if l['code'] == 'catch_all')
