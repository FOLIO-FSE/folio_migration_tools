"""Shared pre-validation of patron and item barcodes for circulation migration tasks.

Used by the loans, requests and course reserves migrators to verify legacy barcodes
against records that already exist in FOLIO before any transactions are posted.
"""

import asyncio
import json
import logging
import time

import httpx
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.helper import Helper
from folio_migration_tools.utils import cql_quote, get_from_path

logger = logging.getLogger(__name__)

DEFAULT_PATRON_IDENTIFIERS = ["barcode"]
RETRY_DELAY_SECONDS = 2


def _error_text(error: Exception) -> str:
    """Return the response body of an HTTP error, or the error itself if there is none."""
    response = getattr(error, "response", None)
    return response.text if response is not None else str(error)


def _lookup_failed(message: str, data_value: str, error: Exception) -> TransformationProcessError:
    """Build the error that halts the task when a lookup still fails after the retry.

    A failed lookup cannot be told apart from a missing record, so carrying on would
    discard every transaction in the batch as if its record did not exist.
    """
    return TransformationProcessError("", message, f"{data_value}: {_error_text(error)}")


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


def _flatten_dict_values(value: dict) -> list[str]:
    """Prefer well-known identifier keys; otherwise flatten every nested value."""
    flattened = []
    for key in ["barcode", "value", "id", "identifier", "externalSystemId"]:
        if key in value:
            flattened.extend(flatten_identifier_values(value[key]))
    if flattened:
        return flattened
    for nested in value.values():
        flattened.extend(flatten_identifier_values(nested))
    return flattened


def flatten_identifier_values(value: object) -> list[str]:
    """Flatten a (possibly nested) patron field value into a list of identifier strings."""
    if isinstance(value, (str, int, float, bool)):
        text = str(value).strip()
        return [text] if text else []
    if isinstance(value, list):
        return [text for val in value for text in flatten_identifier_values(val)]
    if isinstance(value, dict):
        return _flatten_dict_values(value)
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
    except (ValueError, KeyError, TypeError, IndexError, httpx.HTTPError) as e:
        logger.exception("Error retrieving circulation settings: %s", _error_text(e))
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


def _fetch_items_batch(folio_client: FolioClient, batch: list[str], batch_number: int) -> list:
    """Fetch the items matching a batch of barcodes, retrying once on an HTTP error.

    Raises:
        TransformationProcessError: If the retry fails too.
    """
    # Item barcodes are unique in FOLIO, so each exact-match term finds at most one item.
    payload = {
        "query": " OR ".join(f"barcode=={cql_quote(barcode)}" for barcode in batch),
        "limit": len(batch),
    }
    try:
        response = folio_client.folio_post("/item-storage/items/retrieve", payload)  # type: ignore[misc]
    except httpx.HTTPError as e:
        logger.warning("Error fetching items batch %s, retrying: %s", batch_number, _error_text(e))
        time.sleep(RETRY_DELAY_SECONDS)
        try:
            response = folio_client.folio_post("/item-storage/items/retrieve", payload)  # type: ignore[misc]
        except httpx.HTTPError as retry_error:
            raise _lookup_failed(
                "Item barcode pre-validation failed. Halting.",
                f"Batch {batch_number}",
                retry_error,
            ) from retry_error
    return response.get("items", []) if isinstance(response, dict) else []


def validate_item_barcodes(
    folio_client: FolioClient, barcodes: set[str], batch_size: int = 1000
) -> set[str]:
    """Check which barcodes match an item in FOLIO, logging a data issue for each miss.

    Items are fetched in batches to avoid exceeding query size limits. Barcodes are
    compared case-insensitively, the same way FOLIO matches them.

    Returns:
        set[str]: The subset of barcodes (as spelled in the legacy data) that exist as
            items in FOLIO.

    Raises:
        TransformationProcessError: If a batch cannot be fetched, even after a retry.
    """
    logger.info("Pre-validating item barcodes for %s unique barcodes", len(barcodes))
    fetch_items = []
    barcode_list = list(barcodes)
    num_batches = (len(barcode_list) + batch_size - 1) // batch_size
    for i in range(0, len(barcode_list), batch_size):
        batch_number = i // batch_size + 1
        batch = barcode_list[i : i + batch_size]
        fetch_items.extend(_fetch_items_batch(folio_client, batch, batch_number))
        logger.info("Batch %s/%s: fetched %s items", batch_number, num_batches, len(fetch_items))
    logger.info("Fetched %s items matching legacy barcodes", len(fetch_items))
    found_barcodes = {item["barcode"].lower() for item in fetch_items if item.get("barcode")}
    valid_barcodes = {barcode for barcode in barcodes if barcode.lower() in found_barcodes}
    for barcode in barcodes - valid_barcodes:
        logger.warning("No item found for barcode: %s", barcode)
        Helper.log_data_issue_failed("", "No item found for barcode", f"Barcode: {barcode}")
    return valid_barcodes


async def _fetch_patrons(
    folio_client: FolioClient, barcode: str, patron_identifiers: list[str]
) -> list[dict]:
    """Fetch the users matching a legacy barcode on any of the patron identifiers.

    Retries once on an HTTP error.

    Raises:
        TransformationProcessError: If the retry fails too.
    """
    query = " OR ".join(f"{field.strip()}=={cql_quote(barcode)}" for field in patron_identifiers)
    try:
        return await folio_client.folio_get_async("/users", key="users", query=query)
    except httpx.HTTPError as e:
        logger.warning(
            "Error fetching patron for barcode %s, retrying: %s", barcode, _error_text(e)
        )
    await asyncio.sleep(RETRY_DELAY_SECONDS)
    try:
        return await folio_client.folio_get_async("/users", key="users", query=query)
    except httpx.HTTPError as retry_error:
        raise _lookup_failed(
            "Patron barcode pre-validation failed. Halting.",
            f"Barcode {barcode}",
            retry_error,
        ) from retry_error


def _resolve_patron_value(
    patrons: list[dict],
    barcode: str,
    patron_identifiers: list[str],
    require_barcode: bool,
) -> str | None:
    """Return the value to post transactions with, or None (after logging why) if invalid."""
    if not patrons:
        logger.warning("No patron found for barcode: %s", barcode)
        Helper.log_data_issue_failed("", "No patron found for barcode", f"Barcode: {barcode}")
        return None
    if len(patrons) > 1:
        logger.warning("Multiple patrons found for barcode: %s", barcode)
        Helper.log_data_issue_failed(
            "",
            "Multiple patrons found for barcode",
            f"Barcode: {barcode} - {json.dumps(patrons)}",
        )
        return None
    patron = patrons[0]
    if not patron.get("patronGroup"):
        logger.warning("Patron exists but has no group: %s", barcode)
        Helper.log_data_issue_failed(
            "",
            "Fetched patron has no group assigned",
            f"Barcode: {barcode} - {json.dumps(patrons)}",
        )
        return None
    if require_barcode:
        lookup_value = patron.get("barcode")
        issue = "Fetched patron has no barcode assigned"
    else:
        lookup_value = get_patron_lookup_value(patron, barcode, patron_identifiers)
        issue = "Fetched patron has no lookupable identifier value"
    if not lookup_value:
        logger.warning("Patron exists but has no usable identifier value: %s", barcode)
        Helper.log_data_issue("", issue, f"Barcode: {barcode} - {json.dumps(patron)}")
        return None
    return lookup_value


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
    not_found = 0

    def log_progress():
        logger.info(
            "Pre-validation progress: %s/%s barcodes checked. %s valid, %s not found, "
            "%s rejected.",
            counter,
            len(barcodes),
            len(valid_patron_map),
            not_found,
            counter - len(valid_patron_map) - not_found,
        )

    async def check_one(barcode: str):
        nonlocal counter, not_found
        async with semaphore:
            patrons = await _fetch_patrons(folio_client, barcode, patron_identifiers)
            counter += 1
        if not patrons:
            not_found += 1
        lookup_value = _resolve_patron_value(patrons, barcode, patron_identifiers, require_barcode)
        if lookup_value:
            valid_patron_map[barcode] = lookup_value
        if counter % 100 == 0:
            log_progress()

    # Without a /retrieve POST query endpoint for Users, the only sensible way to check
    # patron barcodes is to query them one by one. This is still faster than trying to
    # match them in Python after fetching all users for most systems.
    await asyncio.gather(*(check_one(bc) for bc in barcodes))
    log_progress()
    return valid_patron_map
