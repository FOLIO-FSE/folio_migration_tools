import json
import uuid
import xml.etree.ElementTree as ET
from random import randint

import requests
from jsonschema import ValidationError, validate

class DefaultMapper:

    # Bootstrapping (loads data needed later in the script.)
    def __init__(self, folio):
        self.folio = folio
        print("Fetching valid language codes...")
        self.language_codes = self.fetch_language_codes()

    # Parses a bib recod into a FOLIO Inventory instance object
    # Community mapping suggestion: https://bit.ly/2S7Gyp3
    # This is the main function
    def parse_bib_record(self, marc_record, record_source):
            # not mapped. Randomizes an instance type:
            rand_idx = randint(0, len(self.folio.instance_types)-1)
            rec = {
                'id': str(uuid.uuid4()),
                # This should be the new Libris ID?
                'hrid': str(marc_record['001'].format_field()),
                'title': self.get_title(marc_record),
                'source': record_source,
                'contributors': list(self.get_contributors(marc_record)),
                'identifiers': list(self.get_identifiers(marc_record)),
                # TODO: Add instanceTypeId
                'instanceTypeId': self.folio.instance_types[rand_idx]['id'],
                # 'alternativeTitles': list(self.get_alt_titles(marc_record)),
                'series': list(set(self.get_series(marc_record))),
                'editions': [(marc_record['250'] and
                              marc_record['250']['a']) or ''],
                'subjects': list(set(self.get_subjects(marc_record))),
                # TODO: add classification
                # 	'classification': [{'classificationNumber':'a',
                # 'classificationTypeId':''}]
                'publication': list((self.get_publication(marc_record))),
                # TODO: add instanceFormatId
                'instanceFormatIds': [self.folio.instance_formats[0]['id']],
                # TODO: add physical description
                # 'physicalDescriptions': ['pd1', 'pd2'],
                # TODO: add languages
                'languages': self.get_languages(marc_record)}
            # TODO: add notes
            # 'notes': ['', '']}
            return rec

    # get title or raise exception
    # uses pymarcs built in title function.
    # Needs to be improved
    def get_title(self, marc_record):
        title = marc_record.title()
        if title:
            return title
        else:
            raise ValueError("No title for {}".format(marc_record['001']))

    # Create a new folio record from template
    def folio_record_template(self, id):
        # if created from json schema validation could happen earlier...
        return {'id': str(id)}

    

    # Collects contributors from the marc record and adds the apropriate Ids
    def get_contributors(self, marc_record):
        a = {'100': next(f['id'] for f in self.folio.contrib_name_types
                         if f['name'] == 'Personal name'),
             '110': next(f['id'] for f in self.folio.contrib_name_types
                         if f['name'] == 'Corporate name'),
             '111': next(f['id'] for f in self.folio.contrib_name_types
                         if f['name'] == 'Meeting name')}
        first = 0
        for field_tag in a.keys():
            for field in marc_record.get_fields(field_tag):
                ctype = self.get_contrib_type_id(marc_record)
                first += 1
                yield {'name': field.format_field(),
                       'contributorNameTypeId': a[field_tag],
                       'contributorTypeId': ctype,
                       'primary': first < 2}

    # Maps type of contribution to the right FOLIO
    # Contributor types
    def get_contrib_type_id(self, marc_record):
        # TODO: create logic here...
        ret = '5daa3848-958c-4dd8-9724-b7ae83a99a27'
        return ret

    # No urls wanted from Chalmers?.
    # def get_urls(self, marc_record):
    #     for field in marc_record.get_fields('856'):
    #        yield field['u'] or ''

    # Get subject headings from the marc record.
    def get_subjects(self, marc_record):
        tags = ['600', '610', '611', '630', '647', '648', '650', '651'
                '653', '654', '655', '656', '657', '658', '662']
        for tag in tags:
            for field in marc_record.get_fields(tag):
                yield field.format_field()

    # Alternative titles. 
    # Currently all alt titles to one type
    def get_alt_titles(self, marc_record):
        tags = ['240', '246', '247']
        alt_titles = set()
        for tag in tags:
            for field in marc_record.get_fields(tag):
                alt_titles.add(field.format_field())
        for t in alt_titles:
            tid = '0fe58901-183e-4678-a3aa-0b4751174ba8'
            yield {'alternativeTitleTypeId': tid,
                   'alternativeTitle': t}

    # Publication 
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

    # Fteches the language codes from LoC and returns a generator of them
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
                         in self.folio.identifier_types if f['name'] == 'ISBN'),
             '022': next(f['id'] for f
                         in self.folio.identifier_types if f['name'] == 'ISSN')}
        for field_tag in a.keys():
            for field in marc_record.get_fields(field_tag):
                yield {'identifierTypeId': a[field_tag],
                       'value': field.format_field()}