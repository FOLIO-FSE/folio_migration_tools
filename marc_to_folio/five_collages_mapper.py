'''Mapper for specific Five Collages requirements'''


class FiveCollagesMapper:
    '''Extra mapper specific for Alabama requirements'''
    # TODO: Add Alabama specific subjects

    def __init__(self, folio):
        ''' Bootstrapping (loads data needed later in the script.)'''
        self.folio = folio
        self.holdings_map = {}
        self.id_map = {}
        self.holdings_schema = folio.get_holdings_schema()

    def parse_bib(self, marc_record, folio_record):
        '''Performs extra parsing, based on local requirements'''
        self.id_map[marc_record['001'].format_field()] = {'id': folio_record['id']}
        return folio_record

    def remove_from_id_map(self, marc_record):
        ''' removes the ID from the map in case parsing failed'''
        id_key = marc_record['001']
        if id_key in self.id_map:
            del self.id_map[id_key]
