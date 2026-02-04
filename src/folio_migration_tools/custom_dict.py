"""Custom CSV DictReader with case-insensitive field names.

Provides the InsensitiveDictReader class that normalizes CSV field names to
lowercase and strips whitespace, making CSV parsing more forgiving of
inconsistent header formatting.
"""

import csv


class InsensitiveDictReader(csv.DictReader):
    # This class overrides the csv.fieldnames property, which converts all
    # fieldnames without leading and trailing
    # spaces and to lower case.
    @property
    def fieldnames(self):
        return [field.strip().lower() for field in csv.DictReader.fieldnames.fget(self)]  # type: ignore

    def next(self):
        return InsensitiveDict(csv.DictReader.next(self))  # type: ignore


class InsensitiveDict(dict):
    # This class overrides the __getitem__ method to automatically strip()
    # and lower() the input key
    def __getitem__(self, key):
        return dict.__getitem__(self, key.strip().lower())
