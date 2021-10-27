from migration_tools.report_blurbs import Blurbs


class MigrationReport:
    """Class responsible for handling the migration report"""

    def __init__(self):
        self.report = {}
        self.stats = {}

    def add(self, blurb_tuple: tuple, measure_to_add, number=1):
        """Add section header and values to migration report."""
        if blurb_tuple[0] not in self.report:
            self.report[blurb_tuple[0]] = {"blurb_tuple": blurb_tuple}
        if measure_to_add not in self.report[blurb_tuple[0]]:
            self.report[blurb_tuple[0]][measure_to_add] = number
        else:
            self.report[blurb_tuple[0]][measure_to_add] += number

    def set(self, blurb, measure_to_add: str, number: int):
        """set a section value  to a specific number"""
        if blurb[0] not in self.report:
            self.report[blurb[0]] = {}
        self.report[blurb[0]][measure_to_add] = number

    def add_general_statistics(self, measure_to_add: str):
        """Shortcut for adding to the first breakdown"""
        self.add(Blurbs.GeneralStatistics, measure_to_add)
