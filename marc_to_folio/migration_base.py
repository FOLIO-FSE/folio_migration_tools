import logging
import time
from typing import Dict, List


class MigrationBase:
    def __init__(self):
        self.migration_report = {}
        self.stats = {}
        self.mapped_folio_fields = {}
        self.mapped_legacy_fields = {}

    def report_legacy_mapping(self, field_name, was_mapped, was_empty=False):
        if field_name not in self.mapped_legacy_fields:
            self.mapped_legacy_fields[field_name] = [int(was_mapped), int(was_empty)]
        else:
            self.mapped_legacy_fields[field_name][0] += int(was_mapped)
            self.mapped_legacy_fields[field_name][1] += int(was_empty)

    def report_folio_mapping(self, field_name, was_mapped, was_empty=False):
        if field_name not in self.mapped_folio_fields:
            self.mapped_folio_fields[field_name] = [int(was_mapped), int(was_empty)]
        else:
            self.mapped_folio_fields[field_name][0] += int(was_mapped)
            self.mapped_folio_fields[field_name][1] += int(was_empty)

    def print_mapping_report(self):
        total_records = self.stats["Number of records in file(s)"]
        print("\n## Mapped FOLIO fields")
        d_sorted = {
            k: self.mapped_folio_fields[k] for k in sorted(self.mapped_folio_fields)
        }
        print(f"FOLIO Field | Mapped | Empty | Unmapped")
        print("--- | --- | --- | ---:")
        for k, v in d_sorted.items():
            unmapped = total_records - v[0]
            mapped = v[0] - v[1]
            unmapped_per = "{:.1%}".format(unmapped / total_records)
            mp = mapped / total_records
            mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
            print(
                f"{k} | {mapped if mapped > 0 else 0} ({mapped_per}) | {v[1]} | {unmapped}"
            )
        print("\n## Mapped Legacy fields")
        d_sorted = {
            k: self.mapped_legacy_fields[k] for k in sorted(self.mapped_legacy_fields)
        }
        print(f"Legacy Field | Mapped | Empty | Unmapped")
        print("--- | --- | --- | ---:")
        for k, v in d_sorted.items():
            unmapped = total_records - v[0]
            mapped = v[0] - v[1]
            unmapped_per = "{:.1%}".format(unmapped / total_records)
            mp = mapped / total_records
            mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
            print(
                f"{k} | {mapped if mapped > 0 else 0} ({mapped_per}) | {v[1]} | {unmapped}"
            )

    def add_to_migration_report(self, header, measure_to_add):
        if header not in self.migration_report:
            self.migration_report[header] = {}
        if measure_to_add not in self.migration_report[header]:
            self.migration_report[header][measure_to_add] = 1
        else:
            self.migration_report[header][measure_to_add] += 1

    def write_migration_report(self, report_file):
        for a in self.migration_report:
            report_file.write(f"   \n")
            report_file.write(f"## {a} - {len(self.migration_report[a])} things   \n")
            report_file.write(f"Measure | Count   \n")
            report_file.write(f"--- | ---:   \n")
            b = self.migration_report[a]
            sortedlist = [(k, b[k]) for k in sorted(b, key=as_str)]
            for b in sortedlist:
                report_file.write(f"{b[0]} | {b[1]}   \n")

    def print_progress(self):
        i = self.stats["Records processed"]
        if i % 1000 == 0:
            elapsed = i / (time.time() - self.start)
            elapsed_formatted = int(elapsed)
            print(
                f"{elapsed_formatted}\t{i}", flush=True,
            )

    def print_dict_to_md_table(self, my_dict, report_file, h1="Measure", h2="Number"):
        # TODO: Move to interface or parent class
        d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
        report_file.write(f"{h1} | {h2}   \n")
        report_file.write(f"--- | ---:   \n")
        for k, v in d_sorted.items():
            report_file.write(f"{k} | {v:,}   \n")

    def add_stats(self, stats, a):
        if a not in stats:
            stats[a] = 1
        else:
            stats[a] += 1

    def count_unmapped_fields(self, schema, folio_object):
        schema_properties = schema["properties"].keys()
        unmatched_properties = (
            p for p in schema_properties if p not in folio_object.keys()
        )
        for p in unmatched_properties:
            self.report_folio_mapping(p, False, True)

    def count_mapped_fields(self, folio_object):
        keys_to_delete = []
        for key, value in folio_object.items():
            if isinstance(value, str):
                self.report_folio_mapping(key, True, not value)
                if not value:
                    keys_to_delete.append(key)
            elif isinstance(value, list):
                self.report_folio_mapping(key, True, any(value))
                if not any(value):
                    keys_to_delete.append(key)
            elif isinstance(value, dict):
                self.report_folio_mapping(key, True, any(value))
                if not any(value):
                    keys_to_delete.append(key)
            else:
                logging.info(type(value))
        for mykey in keys_to_delete:
            del folio_object[mykey]


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
