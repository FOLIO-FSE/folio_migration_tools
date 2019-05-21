'''The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications'''
import uuid


class HoldingsDefaultMapper:
    '''Maps a MARC record to inventory instance format according to
    the FOLIO community convention'''
    # Bootstrapping (loads data needed later in the script.)
    def __init__(self, folio, instance_id_map):
        self.folio = folio
        self.instance_id_map = instance_id_map
        self.holdings_id_map = {}

    def parse_hold(self, marc_record):
        '''Parses a holdings record into a FOLIO Inventory Holdings object
        community mapping suggestion: '''
        rec = {
            'id': str(uuid.uuid4()),
            'instanceId': self.get_instance_id(marc_record),
            'permanentLocationId': self.get_location(marc_record),
            'holdingsStatements': list(self.get_holdingsStatements(marc_record)),
            # 'holdingsTypeId': '',
            # 'formerIds': [],
            # 'temporaryLocationId': '',
            # 'electronicAccess':
            # 'callNumberTypeId': '',
            # 'callNumberPrefix': '',
            # 'callNumber': '',
            # 'callNumberSuffix': '',
            # 'shelvingTitle': '',
            # 'acquisitionFormat': '',
            # 'acquisitionMethod': '',
            # 'receiptStatus': '',
            'notes': list(self.get_notes(marc_record)),
            # 'illPolicyId': '',
            # 'retentionPolicy': '',
            # 'digitizationPolicy': '',
            # 'copyNumber': '',
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
            return self.instance_id_map[old_instance_id]['id']
        except Exception as ee:
            raise ValueError("Error getting new FOLIO Instance {} - {}"
                             .format(marc_record['001'], ee))

    def get_callnumber_data(self, marc_record):
        # read and implement http://www.loc.gov/marc/holdings/hd852.html
        return {}

    def get_notes(self, marc_record):
        '''returns the various notes fields from the marc record'''
        yield {'holdingsNoteTypeId': 'b160f13a-ddba-4053-b9c4-60ec5ea45d56',
               'note': 'Some note',
               'staffOnly': True}

    def get_location(self, marc_record):
        '''returns the location mapped and translated'''
        if '852' in marc_record and 'c' in marc_record['852']:
            return self.folio.get_location_id(marc_record['852']['c'])
        return self.folio.get_location_id('catch_all')

    def get_holdingsStatements(self, marc_record):
        '''returns the various holdings statements'''
        yield {'statement': 'Some statement',
               'note': 'some note'}
