import json
import logging


class Helper:
    @staticmethod
    def print_mapping_report(
        report_file, total_records: int, mapped_folio_fields, mapped_legacy_fields
    ):
        details_start = (
            "<details><summary>Click to expand field report</summary>     \n\n"
        )
        details_end = "</details>   \n"
        report_file.write("\n## Mapped FOLIO fields\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_folio_fields[k] for k in sorted(mapped_folio_fields)}
        report_file.write(details_start)

        report_file.write("FOLIO Field | Mapped | Unmapped  \n")
        report_file.write("--- | --- | ---:  \n")
        for k, v in d_sorted.items():
            unmapped = max(total_records - v[0], 0)
            mapped = v[0]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            report_file.write(
                f"{k} | {max(mapped, 0):,} ({mapped_per}) | {unmapped:,}  \n"
            )
        report_file.write(details_end)

        report_file.write("\n## Mapped Legacy fields\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_legacy_fields[k] for k in sorted(mapped_legacy_fields)}
        report_file.write(details_start)
        report_file.write("Legacy Field | Present | Mapped | Unmapped  \n")
        report_file.write("--- | --- | --- | ---:  \n")
        for k, v in d_sorted.items():
            present = v[0]
            present_per = "{:.1%}".format(
                present / total_records if total_records else 0
            )
            unmapped = present - v[1]
            mapped = v[1]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            report_file.write(
                f"{k} | {max(present, 0):,} ({present_per}) | {max(mapped, 0):,} ({mapped_per}) | {unmapped:,}  \n"
            )
        report_file.write(details_end)

    @staticmethod
    def log_data_issue(index_or_id, message, legacy_value):
        logging.log(26, "DATA ISSUE\t%s\t%s\t%s", index_or_id, message, legacy_value)

    @staticmethod
    def write_to_file(file, folio_record, pg_dump=False):
        """Writes record to file. pg_dump=true for importing directly via the
        psql copy command"""
        if pg_dump:
            file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
        else:
            file.write("{}\n".format(json.dumps(folio_record)))
