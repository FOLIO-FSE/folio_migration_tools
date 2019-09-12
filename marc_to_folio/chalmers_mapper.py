'''Mapper for specific Chalmers requirements'''
import json
import re
import uuid
from io import BytesIO, StringIO
from itertools import islice

import requests
from jsonschema import validate
from pymarc import JSONWriter, MARCReader, MARCWriter, Field

from marc_to_folio.default_mapper import DefaultMapper


class ChalmersMapper(DefaultMapper):
    '''Extra mapper specific for Chalmer requirements'''
    # TODO: Add Chalmers specific subjects

    def __init__(self, folio):
        super().__init__(folio)
        self.filter_last_isbd_chars = r'[,/]$'
        ''' Bootstrapping (loads data needed later in the script.)'''
        self.folio = folio
        self.holdings_map = {}
        self.id_map = {}
        self.holdings_schema = folio.get_holdings_schema()

    def parse_bib(self, marc_record, record_source):
        folio_record = super().parse_bib(marc_record, record_source)
        '''Parses a bib recod into a FOLIO Inventory instance object
        Community mapping suggestion: https://bit.ly/2S7Gyp3'''
        s_or_p = self.s_or_p(marc_record)
        save_source_record = folio_record['hrid'] == 'FOLIOstorage'
        del folio_record['hrid']
        if save_source_record:
            folio_record['statisticalCodeIds'] = [
                '67e08311-90f8-4639-82cc-f7085c6511d8']
        else:
            folio_record['statisticalCodeIds'] = ['55326d56-4466-43d7-83ed-73ffd4d4221f']
            # self.save_source_record(marc_record, folio_record['id'])
        self.id_map[self.get_source_id(marc_record)] = {
            'id': folio_record['id'],
            'libris_001': marc_record['001'].format_field(),
            's_or_p': s_or_p}
        folio_record['identifiers'] = self.get_identifiers(marc_record)
        if s_or_p == 'p':  # create holdings record from sierra bib
            if '852' not in marc_record:
                print(marc_record)
                raise ValueError("missing 852 for {}"
                                 .format(self.get_source_id(marc_record)))
            sigels_in_852 = set()
            for f852 in marc_record.get_fields('852'):
                if f852['5'] not in sigels_in_852:
                    sigels_in_852.add(f852['5'])
                else:
                    print("Duplicate sigel amongst 852s for {}"
                          .format(self.get_source_id(marc_record)))
                f866s = [f866 for f866
                         in marc_record.get_fields('866')
                         if '5' in f866 and
                         # ('a' in f866 or 'z' in f866) and
                         (f866['5']).upper() == f852['5'].upper()]
                holding = self.create_holding([f852, f866s],
                                              folio_record['id'],
                                              self.get_source_id(marc_record))
                key = self.to_key(holding)
                if key not in self.holdings_map:
                    validate(holding, self.holdings_schema)
                    self.holdings_map[self.to_key(holding)] = holding
            else:
                print("Holdings already saved {}".format(key))
            # TODO: check for unhandled 866s
        return folio_record

    def save_source_record(self, marc_record, instance_id):
        '''Saves the source Marc_record to the Source record Storage module'''
        print("Will save {} to SRS".format(self.get_source_id(marc_record)))
        marc_record.add_field(Field(tag='999',
                                    indicators=['f', 'f'],
                                    subfields=['i', instance_id]))
        json_string = StringIO()
        writer = JSONWriter(json_string)
        writer.write(marc_record)
        writer.close(close_fh=False)
        a = {
            "id": str(uuid.uuid4()),
            "snapshotId": "67dfac11-1caf-4470-9ad1-d533f6360bdd",
            "matchedProfileId": str(uuid.uuid4()),
            "matchedId": str(uuid.uuid4()),
            "generation": 1,
            "recordType": "MARC",
            "rawRecord": {
                "id": str(uuid.uuid4()),
                "content": marc_record.as_json()
            },
            "parsedRecord": {
                "id": str(uuid.uuid4()),
                "content": marc_record.as_json()
            }
        }
        try:
            self.post_new_source_storage_record(json.dumps(a))
        except Exception as ee:
            # print(json.dumps(a))
            print(ee)

    def post_new_source_storage_record(self, loan):
        okapi_headers = self.folio.okapi_headers
        host = self.folio.okapi_url
        path = ("{}/source-storage/records".format(host))
        response = requests.post(path,
                                 data=loan,
                                 headers=okapi_headers)
        if response.status_code != 201:
            print("Something went wrong. HTTP {}\nMessage:\t{}"
                  .format(response.status_code, response.text))

    def remove_from_id_map(self, marc_record):
        ''' removes the ID from the map in case parsing failed'''
        id_key = self.get_source_id(marc_record)
        if id_key in self.id_map:
            del self.id_map[id_key]

    def to_key(self, holding):
        '''creates a key if key values in holding record
        to determine uniquenes'''
        inst_id = (holding['instanceId']
                   if 'instanceId' in holding else '')
        call_number = (holding['callNumber']
                       if 'callNumber' in holding else '')
        loc_id = (holding['permanentLocationId']
                  if 'permanentLocationId' in holding else '')
        return '-'.join([inst_id, call_number, loc_id])

    def create_holding(self, pair, instance_id, source_id):
        holding = {
            'instanceId': instance_id,
            'id': str(uuid.uuid4()),
            'permanentLocationId': self.loc_id_from_sigel(pair[0]['5'],
                                                          source_id),
            'callNumber': " ".join(filter(None, [
                pair[0]['c'],
                pair[0]['h'],
                pair[0]['j']])),
            'notes': [],
            'holdingsStatements': []
        }
        # if str(holding['callNumber']).strip() == "":
        #     raise ValueError("Empty callnumber for {}".format(source_id))
        if 'z' in pair[0]:
            h_n_t_id = 'de14ac4e-f705-404a-8027-4a69d9dc8075'
            for subfield in pair[0].get_subfields('z'):  # Repeatable
                holding['notes'].append({'note': subfield,
                                         'staffOnly': False,
                                         'holdingNoteTypeId': h_n_t_id})
        for hold_stm in pair[1]:
            sub_a = hold_stm['a'] if 'a' in hold_stm else ''
            sub_z = " ".join(hold_stm.get_subfields('z'))
            if not sub_a and not sub_z:
                raise ValueError("No $a or $z in 866: {} for {}"
                                 .format(hold_stm, source_id))
            holding['holdingsStatements'].append({
                'statement': (sub_a if sub_a else sub_z),
                'note': (sub_z if sub_a else sub_z)})
        return holding

    def loc_id_from_sigel(self, sigel, source_id):
        # TODO: replace with proper map file
        locs = {'enll': '553ffd9b-26b5-4aa6-a426-187f8bbb77a3',
                'z': '921e0666-fdd3-4e54-a4c4-a20d8d2333fd',
                'za': 'ddbeedde-a75a-4d76-bc16-43b292910ca9',
                'zl': 'e2e4b00a-fbe7-4c2a-ac50-361062949d56'}                
    
        if sigel and sigel.lower() in locs:
            return locs[sigel.lower()]
        else:
            raise ValueError("Error parsing {} as sigel for {}"
                             .format(sigel, source_id))

    def s_or_p(self, marc_record):
        if '907' in marc_record and 'c' in marc_record['907']:
            if marc_record['907']['c'] == 'p':
                return 'p'
            elif marc_record['907']['c'] == 's':
                return 's'
            else:
                return ValueError("neither s or p for {}"
                                  .format(self.get_source_id(marc_record)))
        else:
            return ValueError("neither s or pfor {}"
                              .format(self.get_source_id(marc_record)))

    def get_identifiers(self, marc_record):
        identifiers = list(super().get_identifiers(marc_record))

        '''Adds sierra Id and Libris ids. If no modern Libris ID, take 001'''
        # SierraId
        identifiers.append({'identifierTypeId': '5fc83ef4-7572-40cf-9f64-79c41e9ccf8b',
                            'value': self.get_source_id(marc_record)})
        # "LIBRIS XL ID"
        identifiers.append({'identifierTypeId': '925c7fb9-0b87-4e16-8713-7f4ea71d854b',
                            'value': (self.get_xl_id_long(marc_record))})
        # "LIBRIS BIB ID"
        identifiers.append({'identifierTypeId': '28c170c6-3194-4cff-bfb2-ee9525205cf7',
                            'value': marc_record['001'].format_field()})
        # LIBRIS XL ID (kort)
        identifiers.append({'identifierTypeId': '4f3c4c2c-8b04-4b54-9129-f732f1eb3e14',
                            'value': self.get_xl_id(marc_record)})
        return list(identifiers)

    def get_xl_id(self, marc_record):
        f001 = str(marc_record['001'].format_field())
        if '887' not in marc_record and not f001.isnumeric():
            return f001
        for id_placeholder in marc_record.get_fields('887'):
            if '5' not in id_placeholder:
                # Get that broken libris holdings id out of there!
                a = id_placeholder['a']
                jstring = a.replace('{lrub}', '{')
                jstring = jstring.replace('{lcub}', '}')
                xl_id = json.loads(jstring)['@id']
                return xl_id
        return ''

    def get_xl_id_long(self, marc_record):
        short_id = self.get_xl_id(marc_record)
        if short_id != '':
            return "https://libris.kb.se/{}".format(short_id)
        f001 = marc_record['001'].format_field()
        if f001.isnumeric():
            return "http://libris.kb.se/bib/{}".format(f001)
        else:
            return "https://libris.kb.se/{}".format(f001)
        raise ValueError('No libris id parsed!')

    def get_source_id(self, marc_record):
        '''Gets the system Id from sierra'''

        if '907' not in marc_record:
            print(marc_record)
            raise ValueError("No Sierra record id found")
        return marc_record['907']['a'].replace('.b', '')[:-1]

    def get_title(self, marc_record):
        if '245' not in marc_record:
            return ''
        '''Get title or raise exception.'''
        title = " ".join(marc_record['245'].get_subfields(*list('abknp')))
        if title:
            return re.sub(self.filter_last_isbd_chars, str(''), title).strip()
        else:
            raise ValueError("No title for {}\n{}"
                             .format(marc_record['001'], marc_record))

    def get_subjects(self, marc_record):
        ''' Get subject headings from the marc record.'''
        tags = {'600': 'abcdq',
                '610': 'abcdn',
                '611': 'acde',
                '630': 'adfhklst',
                '647': 'acdvxyz',
                '648': 'avxyz',
                '650': 'abcdvxyz',
                '651': 'avxyz',
                '653': 'a',
                '655': 'abcvxyz'}
        non_mapped_tags = {
            '654': '',
            '656': '',
            '657': '',
            '658': '',
            '662': ''}
        for tag in list(non_mapped_tags.keys()):
            if any(marc_record.get_fields(tag)):
                print("CM: Unmapped Subject field {} in {}"
                      .format(tag, marc_record['001']))
        for key, value in tags.items():
            for field in marc_record.get_fields(key):
                yield " ".join(field.get_subfields(*value)).strip()
