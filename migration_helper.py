import time


class MigrationHelper:
    def __init__(self):
        self.stats = {}
        self.migration_report = {}
        self.start = time.time()

    def print_progress(self):
        i = self.stats["Records processed"]
        if i % 1000 == 0:
            elapsed = i / (time.time() - self.start)
            elapsed_formatted = int(elapsed)
            print(
                f"{elapsed_formatted}\t{i}", flush=True,
            )

    def add_stats(self, a):
        if a not in self.stats:
            self.stats[a] = 1
        else:
            self.stats[a] += 1

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)

    def write_migration_report(self, other_report=None):
        if other_report:
            for a in other_report:
                print(f"## {a} - {len(other_report[a])} things")
                for b in other_report[a]:
                    print(f"{b}\\")
        else:
            for a in self.migration_report:
                print(f"## {a} - {len(self.migration_report[a])} things")
                for b in self.migration_report[a]:
                    print(f"{b}\\")

    @staticmethod
    def print_dict_to_md_table(my_dict, h1="Measure", h2="Number"):
        # TODO: Move to interface or parent class
        d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
        print(f"{h1} | {h2}")
        print("--- | ---:")
        for k, v in d_sorted.items():
            print(f"{k} | {v:,}")
