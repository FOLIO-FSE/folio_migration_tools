import logging
import i18n
from datetime import datetime
from datetime import timezone

from folio_migration_tools.report_blurbs import Blurbs


class MigrationReport:
    """Class responsible for handling the migration report"""

    def __init__(self):
        self.report = {}
        self.stats = {}

    def add(self, blurb_tuple: tuple, measure_to_add, number=1):
        """Add section header and values to migration report.

        Args:
            blurb_tuple (tuple): _description_
            measure_to_add (_type_): _description_
            number (int, optional): _description_. Defaults to 1.
        """
        try:
            self.report[blurb_tuple[0]][measure_to_add] += number
        except KeyError:
            if blurb_tuple[0] not in self.report:
                self.report[blurb_tuple[0]] = {"blurb_tuple": blurb_tuple}
            if measure_to_add not in self.report[blurb_tuple[0]]:
                self.report[blurb_tuple[0]][measure_to_add] = number

    def set(self, blurb, measure_to_add: str, number: int):
        """Set a section value  to a specific number

        Args:
            blurb (_type_): _description_
            measure_to_add (str): _description_
            number (int): _description_
        """
        if blurb[0] not in self.report:
            self.report[blurb[0]] = {}
        self.report[blurb[0]][measure_to_add] = number

    def add_general_statistics(self, measure_to_add: str):
        """Shortcut for adding to the first breakdown

        Args:
            measure_to_add (str): _description_
        """
        self.add(Blurbs.GeneralStatistics, measure_to_add)

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
                    Blurbs.Introduction[1],
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
            blurb = self.report[a].get("blurb_tuple") or ("", "")
            report_file.write(
                "\n".join(
                    [
                        "",
                        "## " + blurb[0],
                        blurb[1],
                        "<details><summary>"
                        + i18n.t(
                            "Click to expand all %{number} things", number=len(self.report[a])
                        )
                        + "</summary>",
                        "",
                        i18n.t("Measure") + " | " + i18n.t("Count"),
                        "--- | ---:",
                    ]
                    + [
                        f"{k or 'EMPTY'} | {self.report[a][k]:,}"
                        for k in sorted(self.report[a], key=as_str)
                        if k != "blurb_tuple"
                    ]
                    + ["</details>"]
                )
            )

    def log_me(self):
        for a in self.report:
            blurb = self.report[a].get("blurb_tuple") or ("", "")
            logging.info(f"{blurb[0]}    ")
            logging.info("_______________")
            b = self.report[a]
            sortedlist = [(k, b[k]) for k in sorted(b, key=as_str) if k != "blurb_tuple"]
            for b in sortedlist:
                logging.info(f"{b[0] or 'EMPTY'} \t\t{b[1]:,}   ")


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
