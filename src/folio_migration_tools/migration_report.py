from folio_migration_tools.report_blurbs import Blurbs


class MigrationReport:
    """Class responsible for handling the migration report"""

    def __init__(self):
        self.report = {}
        self.stats = {}

    def add(self, blurb_tuple: tuple, measure_to_add, number=1):
        """Add section header and values to migration report."""
        try:
            self.report[blurb_tuple[0]][measure_to_add] += number
        except KeyError:
            if blurb_tuple[0] not in self.report:
                self.report[blurb_tuple[0]] = {"blurb_tuple": blurb_tuple}
            if measure_to_add not in self.report[blurb_tuple[0]]:
                self.report[blurb_tuple[0]][measure_to_add] = number

    def set(self, blurb, measure_to_add: str, number: int):
        """set a section value  to a specific number"""
        if blurb[0] not in self.report:
            self.report[blurb[0]] = {}
        self.report[blurb[0]][measure_to_add] = number

    def add_general_statistics(self, measure_to_add: str):
        """Shortcut for adding to the first breakdown"""
        self.add(Blurbs.GeneralStatistics, measure_to_add)

    def write_migration_report(self, report_file):
        """Writes the migration report, including section headers, section blurbs, and values."""
        report_file.write(f"{Blurbs.Introduction[1]}\n")

        for a in self.report:
            blurb = self.report[a].get("blurb_tuple") or ("", "")
            report_file.write("   \n")
            report_file.write(f"## {blurb[0]}    \n")
            report_file.write(f"{blurb[1]}    \n")
            report_file.write(
                f"<details><summary>Click to expand all {len(self.report[a])} things</summary>     \n"
            )
            report_file.write("   \n")
            report_file.write("Measure | Count   \n")
            report_file.write("--- | ---:   \n")
            b = self.report[a]
            sortedlist = [
                (k, b[k]) for k in sorted(b, key=as_str) if k != "blurb_tuple"
            ]
            for b in sortedlist:
                report_file.write(f"{b[0] or 'EMPTY'} | {b[1]:,}   \n")
            report_file.write("</details>   \n")


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
