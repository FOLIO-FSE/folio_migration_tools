"""Mapper for specific Alabama requirements"""
from deprecated import deprecated
from marc_to_folio.default_mapper import DefaultMapper


@deprecated(reason="Use folio_rules_mapper instead")
class AlabamaMapper(DefaultMapper):
    """Extra mapper specific for Alabama requirements"""

    # TODO: Add Alabama specific subjects
    # TODO: Add 090 as LC classification
    # TODO: Add mappings for 653, 655

    def __init__(self, folio, results_path):
        """ Bootstrapping (loads data needed later in the script.)"""
        super().__init__(folio, results_path)
        self.folio = folio
        self.migration_user_id = "d916e883-f8f1-4188-bc1d-f0dce1511b50"
        self.results_path = results_path
        self.holdings_map = {}
        self.id_map = {}
        self.rec_with_852 = 0
        self.holdings_schema = folio.get_holdings_schema()

    def wrap_up(self):
        super().wrap_up()
        print(f"records with 852s: {self.rec_with_852}")

    def parse_bib(self, marc_record, record_source):
        # raise Exception("001:s with BHI as prefix")
        # raise Exception("FIX boundwidths")
        """Performs extra parsing, based on local requirements"""
        folio_record = super().parse_bib(marc_record, record_source)
        folio_record["metadata"] = super().get_metadata_construct(
            self.migration_user_id
        )
        legacy_id = marc_record["001"].format_field()
        self.id_map[legacy_id] = {"id": folio_record["id"]}
        if "852" in marc_record:
            self.rec_with_852 += 1
            # print("852 found for{}. Holdings record?"
            #      .format(marc_record['001'].format_field()))

        folio_record["identifiers"].append(
            {
                "identifierTypeId": "7e591197-f335-4afb-bc6d-a6d76ca3bace",
                "value": legacy_id,
            }
        )

        return folio_record

    def remove_from_id_map(self, marc_record):
        """ removes the ID from the map in case parsing failed"""
        id_key = marc_record["001"].format_field()
        if id_key in self.id_map:
            del self.id_map[id_key]

    def get_subjects(self, marc_record):
        """ Get subject headings from the marc record."""
        tags = {
            "600": "abcdq",
            "610": "abcdn",
            "611": "acde",
            "630": "adfhklst",
            "647": "acdvxyz",
            "648": "avxyz",
            "650": "abcdvxyz",
            "651": "avxyz",
            "653": "a",
            "655": "abcvxyz",
        }
        non_mapped_tags = {"654": "", "656": "", "657": "", "658": "", "662": ""}
        for tag in list(non_mapped_tags.keys()):
            if any(marc_record.get_fields(tag)):
                print("Unmapped Subject field {} in {}".format(tag, marc_record["001"]))
        for key, value in tags.items():
            for field in marc_record.get_fields(key):
                yield " ".join(field.get_subfields(*value)).strip()
