'''Mapper for specific Chalmers requirements'''
import uuid
from jsonschema import validate


class ChalmersMapper:
    '''Extra mapper specific for Chalmer requirements'''
    # TODO: Add Chalmers specific subjects

    def __init__(self, folio):
        ''' Bootstrapping (loads data needed later in the script.)'''
        self.folio = folio
        self.holdings_map = {}
        self.id_map = {}
        self.holdings_schema = folio.get_holdings_schema()

    def parse_bib(self, marc_record, folio_record):
        '''Parses a bib recod into a FOLIO Inventory instance object
        Community mapping suggestion: https://bit.ly/2S7Gyp3'''
        s_or_p = self.s_or_p(marc_record)
        save_source_record = folio_record['hrid'] == 'FOLIOstorage'
        if save_source_record:
            del folio_record['hrid']

        if save_source_record:
            self.save_source_record(marc_record)

        self.id_map[self.get_source_id(marc_record)] = {
            'id': folio_record['id'],
            's_or_p': s_or_p}
        folio_record['identifiers'] += self.get_identifiers(marc_record)
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
                         (f866['5']).upper() == f852['5']]
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

    def save_source_record(self, marc_record):
        '''Saves the source Marc_record to the Source record Storage module'''
        # TODO: Monitor SRS development
        print("Will save {} to SRS".format(self.get_source_id(marc_record)))

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
        locs = {'enll': 'af39ff72-b375-43e9-926d-283a445633d4',
                'z': 'a67560fb-ec45-4ee4-8b47-4b2498449eae',
                'za': '9fd580d6-43e3-4e0e-8e70-089536e3ea73',
                'zl': '3187484c-877a-4b25-9c48-f80edb3e239f'}
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
        '''Adds sierra Id'''
        yield {'identifierTypeId': '3187432f-9434-40a8-8782-35a111a1491e',
               'value': self.get_source_id(marc_record)}

    def get_source_id(self, marc_record):
        '''Gets the system Id from sierra'''
        if marc_record['907']['a']:
            return marc_record['907']['a'].replace('.b', '')[:-1]
        else:
            raise ValueError("No Sierra record id found")
