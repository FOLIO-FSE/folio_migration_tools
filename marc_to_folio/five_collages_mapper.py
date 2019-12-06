'''Mapper for specific Five Collages requirements'''
from marc_to_folio.default_mapper import DefaultMapper


class FiveCollagesMapper(DefaultMapper):
    '''Extra mapper specific for Alabama requirements'''
    # TODO: Add Alabama specific subjects

    def __init__(self, folio, results_path):
        ''' Bootstrapping (loads data needed later in the script.)'''
        super().__init__(folio, results_path)
        self.migration_user_id = 'd916e883-f8f1-4188-bc1d-f0dce1511b50'
        self.folio = folio
        self.id_map = {}
        self.results_path = results_path
        self.holdings_schema = folio.get_holdings_schema()
        # raise Exception("Mode of issuance ids?")
        # raise Exception("Instance type ids?")

    def wrap_up(self):
        super().wrap_up()

    def parse_bib(self, marc_record, record_source):
        '''Performs extra parsing, based on local requirements'''
        folio_record = super().parse_bib(marc_record, record_source)
        folio_record['metadata'] = super().get_metadata_construct(
            self.migration_user_id)
        legacy_id = marc_record['001'].format_field()
        if '852' in marc_record:
            print("852 found for{}. Holdings record?".format(legacy_id))
        srs_id = (super().save_source_record(marc_record, folio_record['id']))
        folio_record['identifiers'].append(
            {'identifierTypeId': '8e258acc-7dc5-4635-b581-675ac4c510e3',
                'value': srs_id})
        self.id_map[legacy_id] = {'id': folio_record['id']}
        folio_record['identifiers'].append(
            {'identifierTypeId': '2433e5d8-e5de-4cd4-d54c-28f3836df4e9',
             'value': legacy_id})
        return folio_record

    def remove_from_id_map(self, marc_record):
        ''' removes the ID from the map in case parsing failed'''
        id_key = marc_record['001'].format_field()
        if id_key in self.id_map:
            del self.id_map[id_key]

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
                '654': 'abcevyz01234',
                '655': 'abcdvxyz',
                '656': 'akvxyz0132',
                '657': 'avxyz',
                '658': 'abcd2',
                '662': 'abcdefgh0124'
                }
        non_mapped_tags = {}
        for tag in list(non_mapped_tags.keys()):
            if any(marc_record.get_fields(tag)):
                print("Unmapped Subject field {} in {}"
                      .format(tag, marc_record['001']))
        for key, value in tags.items():
            for field in marc_record.get_fields(key):
                yield " ".join(field.get_subfields(*value)).strip()
