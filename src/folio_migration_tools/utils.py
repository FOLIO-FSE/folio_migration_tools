"""Generic utility helpers shared across folio_migration_tools modules."""

import re


def normalize_for_compare(value):
    """Normalize values for case/whitespace-insensitive comparisons."""
    if value is None:
        return ""
    return re.sub(
        r'[\s\x1c-\x1f\u200B\uFEFF"\u2018\u2019\u201A\u201B\u201C\u201D\u201E\u201F]+',
        "",
        str(value),
    ).lower()
