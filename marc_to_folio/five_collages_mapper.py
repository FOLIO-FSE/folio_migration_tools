'''Mapper for specific Five Collages requirements'''
from marc_to_folio.default_mapper import DefaultMapper


class FiveCollagesMapper(DefaultMapper):
    '''Extra mapper specific for Alabama requirements'''
    # TODO: Add Alabama specific subjects

    def __init__(self, folio, results_path):
        ''' Bootstrapping (loads data needed later in the script.)'''
        super().__init__(folio, results_path)
        self.folio = folio
        self.id_map = {}
        self.results_path = results_path
        self.holdings_schema = folio.get_holdings_schema()
        # raise Exception("Mode of issuance ids?")
        #raise Exception("Instance type ids?")

    def parse_bib(self, marc_record, record_source):
        '''Performs extra parsing, based on local requirements'''
        folio_record = super().parse_bib(marc_record, record_source)
        legacy_id = marc_record['001'].format_field()
        self.id_map[legacy_id] = {'id': folio_record['id']}
        return folio_record

    def remove_from_id_map(self, marc_record):
        ''' removes the ID from the map in case parsing failed'''
        id_key = marc_record['001']
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
