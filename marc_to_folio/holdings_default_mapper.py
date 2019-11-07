'''The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications'''
import uuid
import logging


class HoldingsDefaultMapper:
    '''Maps a MARC record to inventory instance format according to
    the FOLIO community convention'''
    # Bootstrapping (loads data needed later in the script.)

    def __init__(self, folio, instance_id_map):
        self.folio = folio
        self.instance_id_map = instance_id_map
        self.holdings_id_map = {}
        self.stats = {
            'bib id not in map': 0,
            'multiple 852s': 0
        }
        self.note_tags = {'506': 'abcdefu23568',
                          '538': 'aiu3568',
                          '541': 'abcdefhno3568',
                          '561': 'au3568',
                          '562': 'abcde3568',
                          '563': 'au3568',
                          '583': 'abcdefhijklnouxz23568',
                          '843': 'abcdefmn35678',
                          '845': 'abcdu3568',
                          '852': 'x',  # TODO: nonpublic notes
                          '852': 'z'}

    def parse_hold(self, marc_record):
        '''Parses a holdings record into a FOLIO Inventory Holdings object
        community mapping suggestion: '''
        rec = {
            'id': str(uuid.uuid4()),
            'hrid': marc_record['001'].format_field(),
            'instanceId': self.get_instance_id(marc_record),
            'permanentLocationId': self.get_location(marc_record),
            'holdingsStatements': list(self.get_holdingsStatements(marc_record)),
            # 'holdingsTypeId': '',
            # 'formerIds': list(set()),
            # 'temporaryLocationId': '',
            # 'electronicAccess':,
            # 'acquisitionFormat': '',
            # 'acquisitionMethod': '',
            # 'receiptStatus': '',
            # 'receiptStatus': '',
            'notes': list(self.get_notes(marc_record)),
            # 'illPolicyId': '',
            # 'retentionPolicy': '',
            # 'digitizationPolicy': '',            #
            # 'numberOfItems': '',
            # 'discoverySuppress': '',
            # 'statisticalCodeIds': [],
            # 'holdingsItems': [],
            # holdingsInstance
            # receivingHistory
            # holdingsStatementsForSupplements
            # holdingsStatementsForIndexes
        }
        rec.update(self.get_callnumber_data(marc_record))
        self.holdings_id_map[marc_record['001'].format_field()] = rec['id']
        return rec

    def get_instance_id(self, marc_record):
        try:
            old_instance_id = marc_record['LKR']['b']
            if old_instance_id in self.instance_id_map:
                return self.instance_id_map[old_instance_id]['id']
            else:
                logging.warn(
                    "Old instance id not in map: {}".format(old_instance_id))
                raise Exception(
                    "Old instance id not in map: {}".format(old_instance_id))
        except Exception as ee:
            self.stats['bib id not in map'] += 1
            raise ValueError("Error getting new FOLIO Instance {} - {}"
                             .format(marc_record['001'], ee))

    def get_callnumber_data(self, marc_record):
        # TODO: handle repeated 852s
        # read and implement http://www.loc.gov/marc/holdings/hd852.html
        # TODO: UA does not use $2
        # TODO: UA First 852 will have a location the later could be set as notes
        # TODO: print call number type id sources (852 ind1 and $2)
        if '852' not in marc_record:
            raise ValueError("852 missing for {}".format(marc_record['001']))
        f852 = marc_record.get_fields('852')
        if len(f852) > 1:
            self.stats['multiple 852s'] += 1
            # TODO: add the second and following
            raise ValueError("Multiple 852:s in {}".format(marc_record['001']))
        return {
            'callNumberTypeId': 'fccfdd98-e061-49ed-8185-c13979fbfaa2',
            'callNumberPrefix': " ".join(f852[0].get_subfields('k')),
            'callNumber': " ".join(f852[0].get_subfields(*'hij')),
            'callNumberSuffix': " ".join(f852[0].get_subfields(*'m')),
            'shelvingTitle': " ".join(f852[0].get_subfields(*'l')),
            'copyNumber': " ".join(f852[0].get_subfields(*'t'))
        }

    def get_notes(self, marc_record):
        '''returns the various notes fields from the marc record'''
        for key, value in self.note_tags.items():
            for field in marc_record.get_fields(key):
                yield {
                    # TODO: add logic for noteTypeId
                    'holdingsNoteTypeId': 'b160f13a-ddba-4053-b9c4-60ec5ea45d56',
                    "note": " ".join(field.get_subfields(*value)),
                    # TODO: Map staffOnly according to field
                    "staffOnly": True
                }

    def get_location(self, marc_record):
        # TODO: handle repeated 852s
        '''returns the location mapped and translated'''
        if '852' in marc_record and 'c' in marc_record['852']:
            return self.folio.get_location_id(marc_record['852']['c'])
        return self.folio.get_location_id('ATDM')

    def get_holdingsStatements(self, marc_record):
        '''returns the various holdings statements'''
        yield {'statement': 'Some statement',
               'note': 'some note'}
