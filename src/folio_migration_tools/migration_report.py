import logging
import i18n
from datetime import datetime
from datetime import timezone


class MigrationReport:
    """Class responsible for handling the migration report"""

    def __init__(self):
        self.report = {}
        self.stats = {}

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
        """Set a section value  to a specific number

        Args:
            blurb (_type_): _description_
            measure_to_add (str): _description_
            number (int): _description_
        """
        if blurb_id not in self.report:
            self.report[blurb_id] = {}
        self.report[blurb_id][measure_to_add] = number

    def add_general_statistics(self, measure_to_add: str):
        """Shortcut for adding to the first breakdown

        Args:
            measure_to_add (str): _description_
        """
        self.add("GeneralStatistics", measure_to_add)

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
        report_file.write(
            "\n".join(
                [
                    "# " + report_title,
                    i18n.t("blurbs.Introduction.description"),
                    "## " + i18n.t("Timings"),
                    "",
                    i18n.t("Measure") + " | " + i18n.t("Value"),
                    "--- | ---:",
                    i18n.t("Time Started:") + " | " + datetime.isoformat(time_started),
                    i18n.t("Time Finished:") + " | " + datetime.isoformat(time_finished),
                    i18n.t("Elapsed time:") + " | " + str(time_finished - time_started),
                ]
            )
        )
        logging.info(f"Elapsed time: {time_finished-time_started}")
        for a in self.report:
            blurb_id = self.report[a].get("blurb_id") or ""
            report_file.write(
                "\n".join(
                    [
                        "",
                        "## " + i18n.t(f"blurbs.{blurb_id}.title"),
                        i18n.t(f"blurbs.{blurb_id}.description"),
                        "<details><summary>"
                        + i18n.t("Click to expand all %{count} things", count=len(self.report[a]))
                        + "</summary>",
                        "",
                        i18n.t("Measure") + " | " + i18n.t("Count"),
                        "--- | ---:",
                    ]
                    + [
                        f"{k or 'EMPTY'} | {self.report[a][k]:,}"
                        for k in sorted(self.report[a], key=as_str)
                        if k != "blurb_id"
                    ]
                    + ["</details>", ""]
                )
            )

    def log_me(self):
        for a in self.report:
            blurb_id = self.report[a].get("blurb_id") or ""
            logging.info(f"{blurb_id}    ")
            logging.info("_______________")
            b = self.report[a]
            sortedlist = [(k, b[k]) for k in sorted(b, key=as_str) if k != "blurb_id"]
            for b in sortedlist:
                logging.info(f"{b[0] or 'EMPTY'} \t\t{b[1]:,}   ")


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
