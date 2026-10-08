"""Shared pre-validation of patron and item barcodes for circulation migration tasks.

Used by the loans, requests and course reserves migrators to verify legacy barcodes
against records that already exist in FOLIO before any transactions are posted.
"""

import asyncio
import json
import logging

import folioclient
from folioclient import FolioClient

from folio_migration_tools.helper import Helper
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    get_from_path,
)

logger = logging.getLogger(__name__)

DEFAULT_PATRON_IDENTIFIERS = ["barcode"]


def normalize_identifier_fields(identifier_config: object) -> list[str]:
    """Flatten a prefPatronIdentifier setting (string, list or dict) into field paths."""
    if isinstance(identifier_config, str):
        return [p.strip() for p in identifier_config.split(",") if p and p.strip()]
    if isinstance(identifier_config, list):
        normalized = []
        for val in identifier_config:
            normalized.extend(normalize_identifier_fields(val))
        return normalized
    if isinstance(identifier_config, dict):
        normalized = []
        for val in identifier_config.values():
            normalized.extend(normalize_identifier_fields(val))
        return normalized
    return []


def flatten_identifier_values(value: object) -> list[str]:
    """Flatten a (possibly nested) patron field value into a list of identifier strings."""
    if value is None:
        return []
    if isinstance(value, (str, int, float, bool)):
        text = str(value).strip()
        return [text] if text else []
    if isinstance(value, list):
        flattened = []
        for val in value:
            flattened.extend(flatten_identifier_values(val))
        return flattened
    if isinstance(value, dict):
        flattened = []
        for key in ["barcode", "value", "id", "identifier", "externalSystemId"]:
            if key in value:
                flattened.extend(flatten_identifier_values(value[key]))
        if flattened:
            return flattened
        for nested in value.values():
            flattened.extend(flatten_identifier_values(nested))
        return flattened
    return []


def load_patron_identifiers(folio_client: FolioClient) -> list[str]:
    """Read the tenant's patron lookup identifiers from the CHECKOUT other_settings.

    Falls back to ["barcode"] if the setting is missing or cannot be read.
    """
    endpoint = (
        "/configurations/entries?query=(module==CHECKOUT%20and%20configName==other_settings)"
    )
    try:
        settings = folio_client.folio_get_single_object(endpoint) or {}
        value = settings.get("configs", [{}])[0].get("value", "{}")
        configured = json.loads(value).get("prefPatronIdentifier", DEFAULT_PATRON_IDENTIFIERS)
        identifiers = normalize_identifier_fields(configured) or list(DEFAULT_PATRON_IDENTIFIERS)
        logger.info(
            "Patron lookup identifiers available for this tenant: %s", ", ".join(identifiers)
        )
        return identifiers
    except (ValueError, KeyError, TypeError, IndexError, folioclient.FolioClientError) as e:
        if hasattr(e, "response"):
            logger.exception("Error retrieving circulation settings: %s", e.response.text)
        else:
            logger.exception("Error retrieving circulation settings: %s", str(e))
        return list(DEFAULT_PATRON_IDENTIFIERS)


def get_patron_lookup_value(
    patron: dict, original_barcode: str, patron_identifiers: list[str]
) -> str | None:
    """Return the identifier value to use for this patron when posting transactions."""
    for path in ["barcode", *patron_identifiers]:
        value = get_from_path(patron, path, None)
        if value is None and path in patron:
            value = patron.get(path)
        values = flatten_identifier_values(value)
        if values:
            if original_barcode in values:
                return original_barcode
            return values[0]
    return None


def validate_item_barcodes(
    folio_client: FolioClient, barcodes: set[str], batch_size: int = 1000
) -> set[str]:
    """Check which barcodes match an item in FOLIO, logging a data issue for each miss.

    Items are fetched in batches to avoid exceeding query size limits.

    Returns:
        set[str]: The subset of barcodes that exist as items in FOLIO.
    """
    logger.info("Pre-validating item barcodes for %s unique barcodes", len(barcodes))
    fetch_items = []
    barcode_list = list(barcodes)
    for i in range(0, len(barcode_list), batch_size):
        batch = barcode_list[i : i + batch_size]
        try:
            response = folio_client.folio_post(  # type: ignore[misc]
                "/item-storage/items/retrieve",
                {
                    "query": " OR ".join([f'barcode=="{barcode}"' for barcode in batch]),
                    "limit": len(batch),
                },
            )
            if not isinstance(response, dict):
                response = {}
            fetch_items.extend(response.get("items", []))
        except folioclient.FolioClientError as e:
            logger.exception(
                "Error fetching items batch %s: %s", i // batch_size + 1, e.response.text
            )
        logger.info(
            "Batch %s/%s: fetched %s items",
            i // batch_size + 1,
            (len(barcode_list) + batch_size - 1) // batch_size,
            len(fetch_items),
        )
    logger.info("Fetched %s items matching legacy barcodes", len(fetch_items))
    valid_barcodes = {item["barcode"] for item in fetch_items if "barcode" in item}
    for barcode in barcodes - valid_barcodes:
        logger.warning("No item found for barcode: %s", barcode)
        Helper.log_data_issue_failed("", "No item found for barcode", f"Barcode: {barcode}")
    return valid_barcodes


async def validate_patron_barcodes(
    folio_client: FolioClient,
    barcodes: set[str],
    patron_identifiers: list[str],
    require_barcode: bool = False,
    max_concurrent: int = 10,
) -> dict[str, str]:
    """Check legacy patron barcodes against FOLIO users.

    A patron is valid if exactly one user matches, the user has a patron group, and an
    identifier value can be resolved for it.

    Args:
        folio_client: FOLIO API client.
        barcodes: Unique legacy patron barcodes to check.
        patron_identifiers: User fields to search on (the tenant's prefPatronIdentifier).
        require_barcode: If True, the resolved value must be the user's own barcode. Use this
            for transactions that can only reference a patron by barcode (e.g. check-out).
        max_concurrent: Maximum number of concurrent user lookups.

    Returns:
        dict[str, str]: Map of legacy barcode to the value to use when posting transactions.
    """
    logger.info("Pre-validating %s unique patron barcodes (async)", len(barcodes))
    semaphore = asyncio.Semaphore(max_concurrent)
    valid_patron_map: dict[str, str] = {}
    counter = 0
    num_invalid = 0

    async def check_one(barcode: str):
        nonlocal num_invalid
        nonlocal counter
        query = " OR ".join(f"{field.strip()}=={barcode}" for field in patron_identifiers)
        async with semaphore:
            try:
                fetch_patron = await folio_client.folio_get_async(
                    "/users", key="users", query=query
                )
            except Exception as e:
                if hasattr(e, "response"):
                    logger.exception(
                        "Error fetching patron for barcode %s: %s", barcode, e.response.text
                    )
                else:
                    logger.exception("Error fetching patron for barcode %s: %s", barcode, str(e))
                fetch_patron = []
            counter += 1
        if not fetch_patron:
            logger.warning("No patron found for barcode: %s", barcode)
            Helper.log_data_issue_failed("", "No patron found for barcode", f"Barcode: {barcode}")
            num_invalid += 1
        elif len(fetch_patron) > 1:
            logger.warning("Multiple patrons found for barcode: %s", barcode)
            Helper.log_data_issue_failed(
                "",
                "Multiple patrons found for barcode",
                f"Barcode: {barcode} - {json.dumps(fetch_patron)}",
            )
            num_invalid += 1
        elif not fetch_patron[0].get("patronGroup"):
            logger.warning("Patron exists but has no group: %s", barcode)
            Helper.log_data_issue_failed(
                "",
                "Fetched patron has no group assigned",
                f"Barcode: {barcode} - {json.dumps(fetch_patron)}",
            )
            num_invalid += 1
        else:
            patron = fetch_patron[0]
            if require_barcode:
                lookup_value = patron.get("barcode")
            else:
                lookup_value = get_patron_lookup_value(patron, barcode, patron_identifiers)
            if lookup_value:
                valid_patron_map[barcode] = lookup_value
            else:
                logger.warning("Patron exists but has no usable identifier value: %s", barcode)
                Helper.log_data_issue(
                    "",
                    "Fetched patron has no barcode assigned"
                    if require_barcode
                    else "Fetched patron has no lookupable identifier value",
                    f"Barcode: {barcode} - {json.dumps(patron)}",
                )
                num_invalid += 1
        if counter % 100 == 0:
            logger.info(
                "Pre-validation progress: %s/%s barcodes checked. %s valid, %s not found.",
                counter,
                len(barcodes),
                len(valid_patron_map),
                num_invalid,
            )

    # Without a /retrieve POST query endpoint for Users, the only sensible way to check
    # patron barcodes is to query them one by one. This is still faster than trying to
    # match them in Python after fetching all users for most systems.
    await asyncio.gather(*(check_one(bc) for bc in barcodes))
    logger.info(
        "Pre-validation progress: %s/%s barcodes checked. %s valid, %s not found.",
        counter,
        len(barcodes),
        len(valid_patron_map),
        num_invalid,
    )
    return valid_patron_map
