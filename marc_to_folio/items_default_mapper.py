'''The default mapper, responsible for parsing Items acording to the
FOLIO community specifications'''
import uuid


class ItemsDefaultMapper:
    '''Maps an Item to inventory Item format according to
    the FOLIO community convention'''
    # Bootstrapping (loads data needed later in the script.)
    def __init__(self, folio, holdings_id_map):
        self.folio = folio
        self.item_id_map = {}
        self.holdings_id_map = holdings_id_map

    def parse_item(self, record):
        '''Parses a holdings record into a FOLIO Inventory Holdings object
        community mapping suggestion: '''
        rec = {
            'id': str(uuid.uuid4()),
            'holdingsRecordId': self.get_holdings_id(record),
            # 'formerIds': [],
            # 'discoverySuppress': False,
            # 'accessionNumber': '',
            'barcode': self.get_barcode(record).strip(),
            'itemLevelCallNumber': self.get_or_empty('Z30_CALL_NO', record),
            # 'itemLevelCallNumberPrefix': 
            # 'itemLevelCallNumberSuffix': '',
            # 'itemLevelCallNumberTypeId': '',
            # 'volume': '',
            # 'enumeration': '',
            # 'chronology': '',
            # 'yearCaption': [''],
            # 'itemIdentifier': '',
            # 'copyNumbers': [''],
            # 'numberOfPieces': '',
            # 'descriptionOfPieces': '',
            # 'numberOfMissingPieces': '',
            # 'missingPieces': '',
            # 'missingPiecesDate': '',
            # 'itemDamagedStatusId': '',
            # 'itemDamagedStatusDate': '',
            'notes': list(self.get_notes(record)),
            # 'circulationNotes': [{'noteType': 'Check in',
            #                     'note': '',
            #                     'staffOnly': False}],
            'status': [{'name': 'Available'}],
            'materialTypeId': '6a63d094-191c-4535-bf23-a1a4cf387759',
            'permanentLoanTypeId': '76ed1db8-a995-46bc-bed2-f9b21f8b9358',
            # 'temporaryLoanTypeId': '',
            'permanentLocationId': self.folio.get_location_id(record['Z30_COLLECTION']),
            # 'temporaryLocationId': '',
            # 'electronicAccess': [{
            # 'uri': '',
            # 'linkText': '',
            # 'materialsSpecification': '',
            # 'publicNote': '',
            # 'relationshipId': ''}],
            # 'inTransitDestinationServicePointId': '',
            # 'purchaseOrderLineIdentifier': ''
            #
        }
        self.item_id_map[str(record['Z30_HOL_DOC_NUMBER_X'])] = rec['id']
        return rec

    def get_or_empty(self, code, record):
        val = record[code].strip()
        if len(val) < 1 or val is "null":
            return val
        return ''

    def get_holdings_id(self, record):
        try:
            old_holdings_id = str(record['Z30_HOL_DOC_NUMBER_X'])
            return self.holdings_id_map[old_holdings_id]
        except Exception as ee:
            print(ee)
            raise ValueError("Error getting new FOLIO holding id from item {}"
                             .format(record['Z30_REC_KEY']))

    def get_barcode(self, record):
        return (record['Z30_BARCODE'] if 'Z30_BARCODE' in record else '')

    def get_notes(self, record):
        '''returns the various notes fields from the marc record'''
        note_fields = ['Z30_NOTE_CIRCULATION', 'Z30_NOTE_INTERNAL',
                       'Z30_NOTE_OPAC']
        for note_field in [record[n] for n in note_fields if record[n]]:
            if note_field != "null":
                yield {
                    'itemsNoteTypeId': '2c93c2c2-a7c3-4e35-a5cb-04b2713cdbc2',
                    'note': note_field,
                    'staffOnly': True}

    def get_location(self, marc_record):
        '''returns the location mapped and translated'''
        if '852' in marc_record and 'c' in marc_record['852']:
            return marc_record['852']['c']
        return 'catch_all'

    def get_holdingsStatements(self, marc_record):
        '''returns the various holdings statements'''
        yield {'statement': 'Some statement',
               'note': 'some note'}
