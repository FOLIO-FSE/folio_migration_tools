'''The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications'''
import uuid
import logging
import json
from datetime import datetime


class HoldingsDefaultMapper:
    '''Maps a MARC record to inventory instance format according to
    the FOLIO community convention'''
    # Bootstrapping (loads data needed later in the script.)

    def __init__(self, folio, instance_id_map, location_map=None):
        self.folio = folio
        self.instance_id_map = instance_id_map
        self.holdings_id_map = {}
        self.folio_locations = {}
        self.legacy_locations = {}
        self.unmapped_locations = {}
        self.unmapped_location_id = ""
        self.locations = self.folio.folio_get_all("/locations",
                                                  "locations")
        print(f"LOCATIONS: {len(self.locations)}")
        self.stats = {
            'number of bib id not in map': 0,
            'multiple 852s': 0
        }
        self.locations_map = {}
        if any(location_map):
            for item in location_map:
                if ',' in item['legacy_code']:
                    leg_codes = item['legacy_code'].split(',')
                    for lc in leg_codes:
                        self.locations_map[lc.strip()] = self.get_loc_id(
                            item['folio_code'])
                else:
                    self.locations_map[item['legacy_code'].strip()] = self.get_loc_id(
                        item['folio_code'])
            print((f"{len(location_map)} locations rows and "
                   f"{len(self.locations_map)} legacy locations to be mapped "
                   f"to {len(self.folio.locations)} folio locations"
                   f"to {len(self.locations)} folio all locations"))
        else:
            print("No locations map supplied")
        self.unmapped_location_id = self.get_loc_id('ATDM')
        self.note_tags = {'506': 'abcdefu23568',
                          '538': 'aiu3568',
                          '541': 'abcdefhno3568',
                          '561': 'au3568',
                          '562': 'abcde3568',
                          '563': 'au3568',
                          '583': 'abcdefhijklnouxz23568',
                          '843': 'abcdefmn35678',
                          '845': 'abcdu3568',
                          '852': 'z'}
        self.nonpublic_note_tags = {'852': 'x'}

    def remove_from_id_map(self, marc_record):
        ''' removes the ID from the map in case parsing failed'''
        id_key = marc_record['001'].format_field()
        if id_key in self.instance_id_map:
            del self.instance_id_map[id_key]

    def get_loc_id(self, loc_code):
        loc = next((l['id'] for l in self.locations
                    if loc_code == l['code']), '')
        if not loc:
            self.unmapped_locations[loc_code] = self.unmapped_locations.get(
                loc_code, 0) + 1
            return self.unmapped_location_id
        else:
            return loc

    def wrap_up(self):
        print("STATS:")
        print(json.dumps(self.stats, indent=4))
        print("LEGACY LOCATIONS:")
        print(json.dumps(self.legacy_locations, indent=4))
        print("FOLIO LOCATIONS:")
        print(json.dumps(self.folio_locations, indent=4))
        print("UNMAPPED LOCATION CODES:")
        print(json.dumps(self.unmapped_locations, indent=4))

    def get_metadata_construct(self, user_id):
        df = '%Y-%m-%dT%H:%M:%S.%f+0000'
        return {
            "createdDate": datetime.now().strftime(df),
            "createdByUserId": user_id,
            "updatedDate": datetime.now().strftime(df),
            "updatedByUserId": user_id}

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
            # holdingsStatementsForIndexes,
            'metadata': self.folio.get_metadata_construct()
        }
        if not rec['permanentLocationId']:
            raise ValueError(
                f"No .permanentLocationId for {marc_record['001'].format_field()}  {marc_record['852']}")
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
            self.stats['number of bib id not in map'] += 1
            raise ValueError("Error getting new FOLIO Instance {} - {}"
                             .format(marc_record['001'], ee))

    def get_callnumber_data(self, marc_record):
        # read and implement http://www.loc.gov/marc/holdings/hd852.html
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
        # TODO: UA First 852 will have a location the later could be set as notes

        for key, value in self.note_tags.items():
            for field in marc_record.get_fields(key):
                yield {
                    # TODO: add logic for noteTypeId
                    'holdingsNoteTypeId': 'b160f13a-ddba-4053-b9c4-60ec5ea45d56',
                    "note": " ".join(field.get_subfields(*value)),
                    "staffOnly": False
                }
        for key, value in self.nonpublic_note_tags.items():
            for field in marc_record.get_fields(key):
                yield {
                    # TODO: add logic for noteTypeId
                    'holdingsNoteTypeId': 'b160f13a-ddba-4053-b9c4-60ec5ea45d56',
                    "note": " ".join(field.get_subfields(*value)),
                    "staffOnly": True
                }

    def get_location(self, marc_record):
        '''returns the location mapped and translated'''
        if '852' in marc_record and 'c' in marc_record['852']:
            loc_code = marc_record['852']['c']
            self.legacy_locations[loc_code] = self.legacy_locations.get(
                loc_code, 0) + 1
            folio_id = self.locations_map.get(loc_code.strip())
            if folio_id in [self.unmapped_location_id, '', None]:
                self.unmapped_locations[loc_code] = self.unmapped_locations.get(
                    loc_code, 0) + 1
                print(f"loc_code {loc_code} not mapped")
                raise ValueError(
                    f"Location code {loc_code} not found in {marc_record['001']}")
            self.folio_locations[folio_id] = self.folio_locations.get(folio_id,
                                                                      0) + 1
            return folio_id
        else:
            raise ValueError("no 852 $c in {}. Unable to parse location".format(
                marc_record['001'].format_field()))

    def get_holdingsStatements(self, marc_record):
        '''returns the various holdings statements'''
        yield {'statement': 'Some statement',
               'note': 'some note'}
