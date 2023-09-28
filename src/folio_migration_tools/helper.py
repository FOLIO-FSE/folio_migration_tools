import json
import logging
import i18n


class Helper:
    @staticmethod
    def print_mapping_report(
        report_file, total_records: int, mapped_folio_fields, mapped_legacy_fields
    ):
        details_start = (
            "<details><summary>" + i18n.t("Click to expand field report") + "</summary>\n\n"
        )
        details_end = "</details>\n"
        report_file.write("\n## " + i18n.t("Mapped FOLIO fields") + "\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_folio_fields[k] for k in sorted(mapped_folio_fields)}
        report_file.write(details_start)
        columns = [i18n.t("FOLIO Field"), i18n.t("Mapped"), i18n.t("Unmapped")]
        report_file.write(" | ".join(columns) + "\n")
        report_file.write("|".join(len(columns) * ["---"]) + "\n")
        for k, v in d_sorted.items():
            unmapped = max(total_records - v[0], 0)
            mapped = v[0]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            up = unmapped / total_records if total_records else 0
            unmapped_per = "{:.0%}".format(max(up, 0))
            report_file.write(
                f"{k} | {max(mapped, 0):,} ({mapped_per}) | {unmapped:,} ({unmapped_per}) \n"
            )
        report_file.write(details_end)

        report_file.write("\n## " + i18n.t("Mapped Legacy fields") + "\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_legacy_fields[k] for k in sorted(mapped_legacy_fields)}
        report_file.write(details_start)
        columns = [i18n.t("Legacy Field"), i18n.t("Present"), i18n.t("Mapped"), i18n.t("Unmapped")]
        report_file.write("|".join(columns) + "\n")
        report_file.write("|".join(len(columns) * ["---"]) + "\n")
        for k, v in d_sorted.items():
            present = v[0]
            present_per = "{:.1%}".format(present / total_records if total_records else 0)
            unmapped = present - v[1]
            mapped = v[1]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            report_file.write(
                f"{k} | {max(present, 0):,} ({present_per}) | {max(mapped, 0):,} "
                f"({mapped_per}) | {unmapped:,}  \n"
            )
        report_file.write(details_end)

    @staticmethod
    def log_data_issue(index_or_id, message, legacy_value):
        logging.log(26, "DATA ISSUE\t%s\t%s\t%s", index_or_id, message, legacy_value)

    @staticmethod
    def write_to_file(file, folio_record):
        """Writes record to file.

        Args:
            file (_type_): _description_
            folio_record (_type_): _description_
        """
        file.write(f"{json.dumps(folio_record)}\n")
