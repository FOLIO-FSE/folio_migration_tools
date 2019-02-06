import json
import uuid
import xml.etree.ElementTree as ET
from random import randint


class ChalmersMapper:

    # Bootstrapping (loads data needed later in the script.)
    def __init__(self, folio):
        self.folio = folio

    # Parses a bib recod into a FOLIO Inventory instance object
    # Community mapping suggestion: https://bit.ly/2S7Gyp3
    # This is the main function
    def parse_bib_record(self, marc_record, folio_record, idMap):
        s_or_p = self.s_or_p(marc_record)
        idMap[self.get_source_id(marc_record)] = {
             'id': folio_record['id'],
             's_or_p': s_or_p} 
        folio_record['identifiers'] += self.get_identifiers(marc_record)
        if s_or_p == 'p':  # create holdings record from sierra bib
            if '852' not in marc_record:
                raise ValueError("missing 852")
            elif len(marc_record.get_fields('852')) != len(marc_record.get_fields('866')):
                raise ValueError("not same number of 852s and 866s")
            else:
                pairs = self.get_holding_pairs(marc_record)
                if len(pairs) == 0:
                    raise ValueError("No pairs")
                for pair in pairs:
                    
    def get_holding_pairs(self, marc_record):
        f866s = marc_record.get_fields('866')
        for f852 in marc_record.get_fields('852'):
            f866 = next(f for f in f866s if f['5'] == f852['5'])
            yield [f852, f866]


    # No urls wanted from Chalmers?.
    # def get_urls(self, marc_record):
    #     for field in marc_record.get_fields('856'):
    #        yield field['u'] or ''

    # Add chalmers subjects

    def s_or_p(self, marc_record):
        if '907' in marc_record and 'c' in marc_record['907']:
            if marc_record['907']['c'] == 'p':
               return 'p'
            elif marc_record['907']['c'] == 's':
                return 's'
            else:
                return ValueError("neither s or p")
        else:
            return ValueError("neither s or p")

    # Adds sierra Id
    def get_identifiers(self, marc_record):
        yield {'identifierTypeId': '3187432f-9434-40a8-8782-35a111a1491e',
               'value': self.get_source_id(marc_record)}

    # Gets the system Id from sierra
    def get_source_id(self, marc_record):
        # if SIERRA:
        if marc_record['907']['a']:
            return marc_record['907']['a'].replace('.b', '')[:-1]
        else:
            raise ValueError("No Sierra record id found")
