'''Mapper for specific Alabama requirements'''
import uuid
from jsonschema import validate


class AlabamaMapper:
    '''Extra mapper specific for Alabama requirements'''
    # TODO: Add Alabama specific subjects

    def __init__(self, folio):
        ''' Bootstrapping (loads data needed later in the script.)'''
        self.folio = folio
        self.holdings_map = {}
        self.id_map = {}
        self.holdings_schema = folio.get_holdings_schema()

    def parse_bib(self, marc_record, folio_record):
        return folio_record
