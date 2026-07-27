"""Generic utility helpers shared across folio_migration_tools modules."""

import re


def normalize_for_compare(value):
    """Normalize values for case/whitespace-insensitive comparisons."""
    if value is None:
        return ""
    return re.sub(r"[\s\x1c-\x1f]+", "", str(value)).lower()
