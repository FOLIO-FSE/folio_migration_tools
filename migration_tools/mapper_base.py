import logging
import collections


class MapperBase:
    def __init__(self):
        logging.info("MapperBase initiating")
        self.mapped_folio_fields = {}
        self.mapped_legacy_fields = {}
        self.stats = {}
        self.schema_properties = None

    def report_legacy_mapping(self, field_name, present, mapped):
        if field_name not in self.mapped_legacy_fields:
            self.mapped_legacy_fields[field_name] = [int(present), int(mapped)]
        else:
            self.mapped_legacy_fields[field_name][0] += int(present)
            self.mapped_legacy_fields[field_name][1] += int(mapped)

    def add_stats(self, measure_to_add, number=1):
        # TODO: Move to interface or parent class
        if measure_to_add not in self.stats:
            self.stats[measure_to_add] = number
        else:
            self.stats[measure_to_add] += number

    def report_folio_mapping(self, folio_record, schema):
        try:
            flattened = flatten(folio_record)
            for field_name, v in flattened.items():
                mapped = 0
                if isinstance(v, str) and v.strip():
                    mapped = 1
                elif isinstance(v, list) and any(v):
                    l = len([a for a in v if a])
                    mapped = l
                if field_name not in self.mapped_folio_fields:
                    self.mapped_folio_fields[field_name] = [mapped]
                else:
                    self.mapped_folio_fields[field_name][0] += mapped
            if not self.schema_properties:
                self.schema_properties = schema["properties"].keys()
            unmatched_properties = (
                p for p in self.schema_properties if p not in folio_record.keys()
            )
            for p in unmatched_properties:
                self.mapped_folio_fields[p] = [0]
        except Exception as ee:
            logging.error(ee)


def flatten(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
