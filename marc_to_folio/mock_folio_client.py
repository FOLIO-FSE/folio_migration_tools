''''''
import json
import requests


class MockFolioClient:

    def __init__(self, config):
        print("Mock FOLIO client init...")

    def folio_get(self, path, key, query=''):
        result = json.loads('')[key]
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
