"""Generic utility helpers shared across folio_migration_tools modules."""

import logging
import re
import uuid

logger = logging.getLogger(__name__)


def is_uuid(value: str) -> bool:
    """Check if a string value is a valid UUID."""
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def resolve_service_point_value(
    source_dict: dict,
    source_key: str,
    fallback_service_point_id: str,
    service_point_mapping,
    row: int,
    errors: list,
) -> str:
    """Resolve a service point value from source data, fallback, or mapping.

    Args:
        source_dict: Dictionary containing the source record data.
        source_key: Key to look up the service point value in source_dict.
        fallback_service_point_id: Fallback value when source data is empty.
        service_point_mapping: Optional RefDataMapping for code-to-UUID resolution.
        row: Row number for error reporting.
        errors: Error list to append to on failure.

    Returns:
        str: Resolved service point value (UUID, code, or empty on error).
    """
    raw_value = source_dict.get(source_key, "").strip()

    if not raw_value:
        raw_value = fallback_service_point_id

    if is_uuid(raw_value):
        return raw_value

    if service_point_mapping:
        try:
            mapping = service_point_mapping.get_ref_data_mapping({"service_point_id": raw_value})
            if mapping and "folio_id" in mapping:
                return mapping["folio_id"]
            else:
                errors.append(
                    (
                        f"Service point code not found in mapping in row {row}",
                        raw_value,
                    )
                )
                return ""
        except Exception as e:
            logger.warning(f"Error resolving service point '{raw_value}' in row {row}: {e}")
            errors.append(
                (
                    f"Error resolving service point code in row {row}",
                    raw_value,
                )
            )
            return ""

    return raw_value


def normalize_for_compare(value):
    """Normalize values for case/whitespace-insensitive comparisons."""
    if value is None:
        return ""
    return re.sub(
        r'[\s\x1c-\x1f\u200B\uFEFF"\u2018\u2019\u201A\u201B\u201C\u201D\u201E\u201F]+',
        "",
        str(value),
    ).lower()
