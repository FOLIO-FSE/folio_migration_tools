import json
import uuid
import requests
from jsonschema import validate
from random import randint
import xml.etree.ElementTree as ET


class MtFMapper:

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
        print("Fetching JSON schema for Instances...")
        self.instance_json_schema = self.get_instance_json_schema()

        print("Fetching Instance types...")
        self.instance_types = self.folio_get("/instance-types",
                                             "instanceTypes",
                                             cql_all)

        print("Fetching valid language codes...")
        self.language_codes = self.fetch_language_codes()

        print("Fetching Instance formats...")
        self.instance_formats = self.folio_get("/instance-formats",
                                               "instanceFormats",
                                               cql_all)

    # Parses a bib recod into a FOLIO Inventory instance object
    def parse_bib_record(self, marc_record, record_source):
        folio_record = self.folio_record_template(uuid.uuid4())
        folio_record['title'] = marc_record.title() or ''
        folio_record['source'] = record_source
        folio_record['contributors'] = list(self.get_contributors(marc_record))
        folio_record['identifiers'] = list(self.get_identifiers(marc_record))
        # TODO: Add instanceTypeId
        rand_idx = randint(0, len(self.instance_types)-1)
        folio_record['instanceTypeId'] = self.instance_types[rand_idx]['id']
        folio_record['alternativeTitles'] = list(set(self.get_alt_titles(marc_record)))
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
        rand_idx = randint(0, len(self.instance_formats)-1)
        folio_record['instanceFormatId'] = self.instance_formats[rand_idx]['id']
        # TODO: add physical description
        folio_record['physicalDescriptions'] = ['pd1', 'pd2']
        # TODO: add languages
        folio_record['languages'] = self.get_languages(marc_record)
        # TODO: add notes
        folio_record['notes'] = ['', '']

        validate(folio_record, self.instance_json_schema)
        return folio_record

    def folio_record_template(self, id):
        return {'id': str(id)}

    def folio_get(self, path, key, query=''):
        u = self.okapi_url+path+query
        print("Fetching {} from {}".format(key, u))
        req = requests.get(u,
                           headers=self.okapi_headers)
        req.raise_for_status()
        return json.loads(req.text)[key]

    # Fetches the JSON Schema for instances
    def get_instance_json_schema(self):
        url = 'https://raw.github.com'
        path = '/folio-org/mod-inventory-storage/master/ramls/instance.json'
        req = requests.get(url+path)
        return json.loads(req.text)

    # Collects contributors from the record and adds the apropriate Ids
    def get_contributors(self, marc_record):
        a = {'100': next(f['id'] for f in self.contrib_name_types
                         if f['name'] == 'Personal name'),
             '110': next(f['id'] for f in self.contrib_name_types
                         if f['name'] == 'Corporate name'),
             '111': next(f['id'] for f in self.contrib_name_types
                         if f['name'] == 'Meeting name')}
        first = 0
        for field_tag in a.keys():
            for field in marc_record.get_fields(field_tag):
                first += 1
                yield {'name': field.format_field(),
                       'contributorNameTypeId': a[field_tag],
                       'contributorTypeText': next(f['name'] for f
                                                   in self.contrib_name_types
                                                   if f['id'] == a[field_tag]),
                       'primary': first < 2}

    def get_urls(self, marc_record):
        for field in marc_record.get_fields('856'):
            yield field['u'] or ''

    def get_subjects(self, marc_record):
        tags = ['600', '610', '611', '630', '647', '648', '650', '651'
                '653', '654', '655', '656', '657', '658', '662']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield field.format_field()

    def get_alt_titles(self, marc_record):
        tags = ['246', '247']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield field.format_field()

    def get_publication(self, marc_record):
        tags = ['260', '264']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield {'publisher': field['b'] or '',
                       'place': field['a'] or '',
                       'dateOfPublication': field['c'] or ''}

    def get_series(self, marc_record):
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

    def fetch_language_codes(self):
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
            for field in marc_record.get_fields(field_tag):
                yield {'identifierTypeId': a[field_tag],
                       'value': field.format_field()}
