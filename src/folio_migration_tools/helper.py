"""General helper utilities for migration tasks.

Provides the Helper class with static utility methods for data validation,
mapping report generation, and common transformation operations used across
multiple migration tasks.
"""

import json
import logging
from datetime import datetime, timezone

from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.report_store import get_active_store


class Helper:
    @staticmethod
    def print_mapping_report(
        report_file, total_records: int, mapped_folio_fields, mapped_legacy_fields
    ):
        details_start = (
            "<details><summary>" + i18n_t("Click to expand field report") + "</summary>\n\n"
        )
        details_end = "</details>\n"
        report_file.write("\n## " + i18n_t("Mapped FOLIO fields") + "\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_folio_fields[k] for k in sorted(mapped_folio_fields)}
        report_file.write(details_start)
        columns = [i18n_t("FOLIO Field"), i18n_t("Mapped"), i18n_t("Unmapped")]
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

        report_file.write("\n## " + i18n_t("Mapped Legacy fields") + "\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_legacy_fields[k] for k in sorted(mapped_legacy_fields)}
        report_file.write(details_start)
        columns = [i18n_t("Legacy Field"), i18n_t("Present"), i18n_t("Mapped"), i18n_t("Unmapped")]
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
    def log_data_issue(index_or_id, message, legacy_value, location=None):
        logging.log(26, "DATA ISSUE\t%s\t%s\t%s", index_or_id, message, legacy_value)
        Helper._store_data_issue("DATA ISSUE", index_or_id, message, legacy_value, location)

    @staticmethod
    def log_data_issue_failed(index_or_id, message, legacy_value, location=None):
        logging.log(26, "RECORD FAILED\t%s\t%s\t%s", index_or_id, message, legacy_value)
        Helper._store_data_issue("RECORD FAILED", index_or_id, message, legacy_value, location)

    @staticmethod
    def _store_data_issue(issue_type, index_or_id, message, legacy_value, location=None):
        """Mirror a data issue into the report database. Never raises.

        location is an optional (kind, path) tuple, see report_store.marc_location.
        """
        kind, path = location or (None, None)
        if store := get_active_store():
            try:
                store.add_issue(
                    issue_type,
                    index_or_id,
                    message,
                    legacy_value,
                    kind,
                    path,
                    ts=datetime.now(timezone.utc).isoformat(),
                )
            except Exception as ex:  # a reporting failure must not stop a migration
                logging.getLogger(__name__).warning("Could not store data issue: %s", ex)

    @staticmethod
    def write_to_file(file, folio_record):
        """Writes record to file.

        Args:
            file (_type_): _description_
            folio_record (_type_): _description_
        """
        file.write(f"{json.dumps(folio_record)}\n")
