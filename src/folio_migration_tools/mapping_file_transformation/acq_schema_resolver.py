"""Shared schema resolution for acquisitions objects (orders, organizations, ...).

FOLIO's acquisitions schemas live in the ``folio-org/acq-models`` repository, which is a git
submodule of the modules that use them (``ramls/acq-models``). Raw GitHub URLs cannot traverse
submodules, so the submodule commit is looked up from the module's release tree and schemas are
then fetched from ``acq-models`` at that commit. All ``$ref``s (including ``../../common`` and
``raml-util`` ones) are then resolved by ``folioclient``'s deterministic dereferencer.
"""

import json
import logging
import os
import sys
from typing import Any, Optional

import httpx
from folioclient import FolioClient
from folioclient._schema_refs import dereference

logger = logging.getLogger(__name__)

RAW_GITHUB = "https://raw.githubusercontent.com"
GITHUB_API = "https://api.github.com/repos"
DEPRECATED = "Deprecated"


def _github_headers() -> dict:
    headers = {
        "content-type": "application/json",
        "User-Agent": "FOLIO Migration Tools (https://github.com/FOLIO-FSE/folio_migration_tools/)",  # noqa: E501
    }
    if os.environ.get("GITHUB_TOKEN"):
        headers["authorization"] = f"token {os.environ['GITHUB_TOKEN']}"
    return headers


def get_submodules_of_release(owner, repo, github_headers, release_tag=None) -> list:
    """Return the submodule entries under ``ramls`` for a release (latest if no tag given)."""
    if not release_tag:
        req = httpx.get(
            f"{GITHUB_API}/{owner}/{repo}/releases/latest",
            headers=github_headers,
            follow_redirects=True,
            timeout=None,
        )
        req.raise_for_status()
        release_tag = req.json()["tag_name"]

    logger.info("Using schemas from %s release: %s", repo, release_tag)

    def get_tree(ref: str) -> dict:
        resp = httpx.get(
            f"{GITHUB_API}/{owner}/{repo}/git/trees/{ref}",
            headers=github_headers,
            follow_redirects=True,
            timeout=None,
        )
        resp.raise_for_status()
        return resp.json()

    release_tree = get_tree(release_tag)
    ramls_sha = next(item["sha"] for item in release_tree["tree"] if item["path"] == "ramls")
    return [item for item in get_tree(ramls_sha)["tree"] if item["mode"] == "160000"]


def _mark_unresolved(node: Any) -> Any:
    """Replace circular-reference markers left by the dereferencer with a skippable stub.

    ``skip_property`` in the mapper base ignores properties whose description is "Deprecated".
    """
    if isinstance(node, dict):
        if "$ref" in node:
            return {"type": "object", "description": DEPRECATED}
        result = {key: _mark_unresolved(value) for key, value in node.items()}
        # Some acq-models enums (e.g. interface type) are mis-declared as objects
        if result.get("type") == "object" and "enum" in result and "properties" not in result:
            result["type"] = "string"
        return result
    if isinstance(node, list):
        return [_mark_unresolved(item) for item in node]
    return node


def resolve_acq_schema(
    module: str,
    object_name: str,
    release_tag: Optional[str] = None,
    owner: str = "folio-org",
    repo: Optional[str] = None,
) -> dict:
    """Fetch an acq-models schema and return it fully dereferenced.

    Args:
        module: Directory in acq-models (e.g. ``mod-orders``, ``mod-orgs``).
        object_name: Schema file name without extension (e.g. ``composite_purchase_order``).
        release_tag: Release of the owning module whose acq-models commit to use. Latest if None.
        owner: GitHub owner.
        repo: Repository holding the acq-models submodule. Defaults to ``module``.

    Returns:
        dict: Plain nested dict with no ``$ref`` keys. Circular references become stubs that
        the mapper skips.
    """
    repo = repo or module
    try:
        headers = _github_headers()
        submodules = get_submodules_of_release(owner, repo, headers, release_tag)
        acq_models_sha = next(item["sha"] for item in submodules if item["path"] == "acq-models")
        base = f"{RAW_GITHUB}/{owner}/acq-models/{acq_models_sha}/{module}/schemas"
        url = f"{base}/{object_name}.json"
        schema = FolioClient.fetch_github_schema(url)
        return _mark_unresolved(dereference(schema, url, FolioClient.fetch_github_schema))
    except httpx.HTTPError as http_error:
        logger.critical("Halting! \t%s", http_error)
        sys.exit(2)
    except json.decoder.JSONDecodeError as json_error:
        logger.critical(json_error)
        sys.exit(2)
