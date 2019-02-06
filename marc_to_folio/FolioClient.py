import json
import uuid
import xml.etree.ElementTree as ET
from random import randint

import requests
from jsonschema import ValidationError, validate

class FolioClient:
# Bootstrapping (loads data needed later in the script.)
    def __init__(self, config):
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

# Fetches data from FOLIO and turns it into a json object
    def folio_get(self, path, key, query=''):
        u = self.okapi_url+path+query
        print("Fetching {} from {}".format(key, u))
        req = requests.get(u,
                           headers=self.okapi_headers)
        req.raise_for_status()
        return json.loads(req.text)[key]
