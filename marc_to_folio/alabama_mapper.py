'''Mapper for specific Alabama requirements'''
from marc_to_folio.default_mapper import DefaultMapper


class AlabamaMapper(DefaultMapper):
    '''Extra mapper specific for Alabama requirements'''
    # TODO: Add Alabama specific subjects
    # TODO: Add 090 as LC classification
    # TODO: Add mappings for 653, 655

    def __init__(self, folio, results_path):
        ''' Bootstrapping (loads data needed later in the script.)'''
        super().__init__(folio, results_path)
        self.folio = folio
        self.results_path = results_path
        self.holdings_map = {}
        self.id_map = {}
        self.holdings_schema = folio.get_holdings_schema()

    def parse_bib(self, marc_record, record_source):
        # raise Exception("001:s with BHI as prefix")
        # raise Exception("FIX boundwidths")
        '''Performs extra parsing, based on local requirements'''
        folio_record = super().parse_bib(marc_record, record_source)
        legacy_id = marc_record['001'].format_field()
        self.id_map[legacy_id] = {'id': folio_record['id']}
        if '852' in marc_record:
            print("852 found for{}. Holdings record?"
                  .format(marc_record['001'].format_field()))
        srs_id = super().save_source_record(
            self.results_path, marc_record, folio_record['id'])
        folio_record['identifiers'].append(
            {'identifierTypeId': '8e258acc-7dc5-4635-b581-675ac4c510e3',
             'value': srs_id})
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
                '655': 'abcvxyz'}
        non_mapped_tags = {
            '654': '',
            '656': '',
            '657': '',
            '658': '',
            '662': ''}
        for tag in list(non_mapped_tags.keys()):
            if any(marc_record.get_fields(tag)):
                print("Unmapped Subject field {} in {}"
                      .format(tag, marc_record['001']))
        for key, value in tags.items():
            for field in marc_record.get_fields(key):
                yield " ".join(field.get_subfields(*value)).strip()
