"""Migration reporting and statistics tracking.

Provides the MigrationReport class for tracking migration statistics, errors,
and warnings during transformation and loading tasks. Generates markdown and
JSON formatted reports with categorized statistics.
"""

import json
import logging
from datetime import datetime, timezone

from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.report_store import get_active_store

logger = logging.getLogger(__name__)


class MigrationReport:
    """Class responsible for handling the migration report."""

    def __init__(self):
        """Initialize a new migration report for tracking statistics and issues."""
        self.report = {}
        self.stats = {}
        if store := get_active_store():
            store.register_report(self)

    def add(self, blurb_id, measure_to_add, number=1):
        """Add section header and values to migration report.

        Args:
            blurb_id (string): ID of Blurb in translations file
            measure_to_add (_type_): _description_
            number (int, optional): _description_. Defaults to 1.
        """
        try:
            self.report[blurb_id][measure_to_add] += number
        except KeyError:
            if blurb_id not in self.report:
                self.report[blurb_id] = {"blurb_id": blurb_id}
            if measure_to_add not in self.report[blurb_id]:
                self.report[blurb_id][measure_to_add] = number

    def set(self, blurb_id, measure_to_add: str, number: int):
        """Set a section value to a specific number.

        Args:
            blurb_id: The report section identifier.
            measure_to_add (str): The measure name to set.
            number (int): The value to set.
        """
        if blurb_id not in self.report:
            self.report[blurb_id] = {}
        self.report[blurb_id][measure_to_add] = number

    def add_general_statistics(self, measure_to_add: str):
        """Shortcut for adding to the first breakdown.

        Args:
            measure_to_add (str): _description_
        """
        self.add("GeneralStatistics", measure_to_add)

    def write_json_report(self, report_file):
        """Writes the raw migration report data to a JSON file.

        Args:
            report_file: An open file object to write the JSON data to
        """
        json.dump(self.report, report_file, indent=2)

    def write_migration_report(
        self,
        report_title,
        report_file,
        time_started: datetime,
    ):
        """Writes the migration report, including section headers, section blurbs, and values.

        Args:
            report_title (_type_):the header of the report.
            report_file (_type_):path to file
            time_started (datetime): The datetime stamp (in utc), of when the process started
        """
        time_finished = datetime.now(timezone.utc)
        if store := get_active_store():
            store.set_report_title(report_title)
        sections = []
        for a in self.report:
            # Sections created via set() have no blurb_id entry; the section key is the blurb id
            blurb_id = self.report[a].get("blurb_id") or a
            sections.append(
                (blurb_id, [(k, v) for k, v in self.report[a].items() if k != "blurb_id"])
            )
        report_file.write(
            render_report_markdown(report_title, time_started, time_finished, sections)
        )
        logger.info(f"Elapsed time: {time_finished - time_started}")

    def log_me(self):
        for a in self.report:
            blurb_id = self.report[a].get("blurb_id") or ""
            logger.info(f"{blurb_id}    ")
            logger.info("_______________")
            b = self.report[a]
            sortedlist = [(k, b[k]) for k in sorted(b, key=as_str) if k != "blurb_id"]
            for b in sortedlist:
                logger.info(f"{b[0] or 'EMPTY'} \t\t{b[1]:,}   ")


def render_report_markdown(
    report_title, time_started: datetime, time_finished: datetime, sections: list
) -> str:
    """Build the markdown report text.

    Shared by the in-memory report and the database renderer, so both produce the
    same layout.

    Args:
        report_title: The header of the report.
        time_started: When the process started.
        time_finished: When the process finished.
        sections: A list of (blurb_id, [(measure, count), ...]) in display order.
    """
    lines = [
        "# " + report_title,
        i18n_t("blurbs.Introduction.description"),
        "## " + i18n_t("Timings"),
        "",
        i18n_t("Measure") + " | " + i18n_t("Value"),
        "--- | ---:",
        i18n_t("Time Started:") + " | " + datetime.isoformat(time_started),
        i18n_t("Time Finished:") + " | " + datetime.isoformat(time_finished),
        i18n_t("Elapsed time:") + " | " + str(time_finished - time_started),
    ]
    text = "\n".join(lines)
    for blurb_id, measures in sections:
        text += "\n".join(
            [
                "",
                "## " + i18n_t(f"blurbs.{blurb_id}.title"),
                i18n_t(f"blurbs.{blurb_id}.description"),
                "<details><summary>"
                + i18n_t("Click to expand all %{count} things", count=len(measures))
                + "</summary>",
                "",
                i18n_t("Measure") + " | " + i18n_t("Count"),
                "--- | ---:",
            ]
            + [
                f"{k or 'EMPTY'} | {count:,}"
                for k, count in sorted(measures, key=lambda m: as_str(m[0]))
            ]
            + ["</details>", ""]
        )
    return text


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
