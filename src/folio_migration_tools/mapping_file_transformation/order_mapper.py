"""Mapper for transforming order data to FOLIO Orders format.

Provides the OrderMapper class for mapping legacy purchase order data to FOLIO
Orders and PurchaseOrderLines. Handles composite orders with embedded lines,
acquisition units, fund distributions, and vendor references.
"""

import json
import logging
import re
import uuid
from typing import Any

from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.acq_schema_resolver import (
    resolve_acq_schema,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.notes_mapper import NotesMapper
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.utils import normalize_for_compare

logger = logging.getLogger(__name__)

# Notes mapped under a purchase order line, e.g. compositePoLines[0].notes[0].content
POL_NOTE_PREFIX = re.compile(r"^(?:compositePoLines|poLines)\[\d+\]\.(?=notes\[)")


class CompositeOrderMapper(MappingFileMapperBase):
    TENANT_ADDRESSES_QUERY = '?query=(module=="TENANT" and configName=="tenant.addresses")'

    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        task_configuration,
        composite_order_map: dict,
        organizations_id_map: dict,
        instance_id_map: dict,
        acquisition_method_map,
        payment_status_map,
        receipt_status_map,
        workflow_status_map,
        location_map,
        funds_map,
        funds_expense_class_map=None,
    ):
        """Initialize CompositeOrderMapper for purchase order transformations.

        Args:
            folio_client (FolioClient): FOLIO API client.
            library_configuration (LibraryConfiguration): Library configuration.
            task_configuration: Task configuration for orders migration.
            composite_order_map (dict): Mapping configuration for order fields.
            organizations_id_map (dict): Mapping of legacy to FOLIO organizations.
            instance_id_map (dict): Mapping of legacy to FOLIO instances.
            acquisition_method_map: Mapping of legacy to FOLIO acquisition methods.
            payment_status_map: Mapping of legacy to FOLIO payment statuses.
            receipt_status_map: Mapping of legacy to FOLIO receipt statuses.
            workflow_status_map: Mapping of legacy to FOLIO workflow statuses.
            location_map: Mapping of legacy to FOLIO locations.
            funds_map: Mapping of legacy to FOLIO funds.
            funds_expense_class_map: Mapping for fund expense classes.
        """
        # Get order schema, using module version from FOLIO environment if available
        release_tag = None
        try:
            release_tag = folio_client.get_module_version("mod-orders")
            logger.info(
                "Using mod-orders version from FOLIO environment: %s",
                release_tag,
            )
        except Exception:
            logger.info("Could not determine mod-orders version, falling back to latest release")

        self.composite_order_schema = resolve_acq_schema(
            "mod-orders", "composite_purchase_order", release_tag
        )

        # Detect PO lines property name from schema (compositePoLines or poLines)
        logger.debug(
            "Composite order schema:\n %s", json.dumps(self.composite_order_schema, indent=2)
        )
        self.po_lines_key = next(
            (
                p
                for p in self.composite_order_schema.get("properties", {})
                if p in ("compositePoLines", "poLines")
            ),
            "compositePoLines",
        )
        logger.info("PO lines schema property: %s", self.po_lines_key)

        # Rewrite mapping entries for backward compatibility
        legacy_key = "compositePoLines"
        if self.po_lines_key != legacy_key:
            for entry in composite_order_map.get("data", []):
                if entry.get("folio_field", "").startswith(legacy_key):
                    entry["folio_field"] = entry["folio_field"].replace(
                        legacy_key, self.po_lines_key, 1
                    )

        # customFields in orders schemas are typically open objects with
        # additionalProperties and no explicit properties; add mapped keys so
        # the generic object mapper can traverse and populate them.
        self._inject_mapped_custom_fields_into_schema(composite_order_map)

        super().__init__(
            folio_client,
            self.composite_order_schema,
            composite_order_map,
            None,
            FOLIONamespaces.orders,
            library_configuration,
            task_configuration,
        )
        logger.info("Loading Instance ID map...")
        self.instance_id_map = instance_id_map
        self.organizations_id_map = organizations_id_map
        logger.debug("Organizations ID map: %s", json.dumps(self.organizations_id_map, indent=2))
        self.folio_organization_cache = {}

        self.acquisitions_methods_mapping = RefDataMapping(
            self.folio_client,
            "/orders/acquisition-methods",
            "acquisitionMethods",
            acquisition_method_map,
            "value",
            "AcquisitionMethodMapping",
        )
        logger.info("Init done")
        self.location_mapping = RefDataMapping(
            self.folio_client,
            "/locations",
            "locations",
            location_map,
            "code",
            "OrderLineLocationMapping",
        )
        self.funds_mapping = RefDataMapping(
            self.folio_client,
            "/finance/funds",
            "funds",
            funds_map,
            "code",
            "FundsMapping",
        )
        self._order_addresses_by_name = self._load_order_addresses_by_name()
        self._validate_hardcoded_order_address_values()

        self.folio_client: FolioClient = folio_client
        self._validate_no_po_level_mod_notes_mappings(composite_order_map)
        self.notes_mapper: NotesMapper = NotesMapper(
            library_configuration,
            None,
            self.folio_client,
            self._build_pol_notes_map(composite_order_map),
            FOLIONamespaces.note,
            True,
        )
        self.notes_mapper.migration_report = self.migration_report

    @staticmethod
    def _build_pol_notes_map(composite_order_map: dict) -> dict:
        """Build the mapping config for the NotesMapper from the order mapping.

        Only POL-level notes (<pol key>[n].notes[m].x) are mod-notes records. The PO's own
        top-level "notes" is an array of strings handled by this mapper, so it is excluded.
        Each legacy row yields one POL, so the POL prefix is stripped, leaving notes[m].x.
        """
        return {
            **composite_order_map,
            "data": [
                {**p, "folio_field": POL_NOTE_PREFIX.sub("", p["folio_field"])}
                for p in composite_order_map["data"]
                if POL_NOTE_PREFIX.match(p["folio_field"])
            ],
        }

    @staticmethod
    def _validate_no_po_level_mod_notes_mappings(composite_order_map: dict) -> None:
        """Halt if mod-notes note fields are mapped at the PO level.

        The PO's own top-level "notes" is an array of strings (map it as notes[n]). Notes
        records (typeId, domain, title, content, ...) can only be linked to POLs, and must be
        mapped under the POL, e.g. compositePoLines[0].notes[0].content.
        """
        offending = [
            entry["folio_field"]
            for entry in composite_order_map.get("data", [])
            if re.match(r"notes\[\d+\]\.", entry.get("folio_field", ""))
        ]
        if offending:
            raise TransformationProcessError(
                "",
                "Notes records cannot be mapped at the PO level: the PO's top-level 'notes' "
                "is an array of strings (map it as notes[n] with no sub-property). To create "
                "mod-notes notes, map them under the PO line, for example "
                "compositePoLines[0].notes[0].content. Offending folio_field entries",
                ", ".join(offending),
            )

    @staticmethod
    def _ensure_custom_fields_object_schema(
        parent_schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not isinstance(parent_schema, dict):
            return None
        properties = parent_schema.get("properties")
        if not isinstance(properties, dict):
            return None
        custom_fields_schema = properties.get("customFields")
        if custom_fields_schema is None:
            custom_fields_schema = {
                "type": "object",
                "description": "dynamically added custom fields",
                "additionalProperties": True,
                "properties": {},
            }
            properties["customFields"] = custom_fields_schema
        elif "properties" not in custom_fields_schema or not isinstance(
            custom_fields_schema.get("properties"), dict
        ):
            custom_fields_schema["properties"] = {}

        return custom_fields_schema

    @staticmethod
    def _collect_order_custom_field_name(folio_field: str) -> str:
        if not folio_field.startswith("customFields."):
            return ""
        field_name = folio_field.split(".", 1)[1]
        return field_name if field_name and "." not in field_name else ""

    def _collect_pol_custom_field_name(self, folio_field: str) -> str:
        pol_pattern = re.compile(rf"{re.escape(self.po_lines_key)}\[\d+\]\.customFields\.([^.]+)$")
        pol_match = pol_pattern.match(folio_field)
        return pol_match.group(1) if pol_match else ""

    @staticmethod
    def _inject_custom_fields_properties(
        schema_root: dict[str, Any], custom_field_names: set[str]
    ) -> None:
        if not custom_field_names:
            return
        custom_fields_schema = CompositeOrderMapper._ensure_custom_fields_object_schema(
            schema_root
        )
        if custom_fields_schema is None:
            return
        for field_name in custom_field_names:
            custom_fields_schema["properties"][field_name] = {
                "type": "string",
                "description": "dynamically added custom prop",
            }

    def _inject_mapped_custom_fields_into_schema(self, composite_order_map: dict) -> None:
        order_custom_fields = set()
        pol_custom_fields = set()

        for map_entry in composite_order_map.get("data", []):
            folio_field = map_entry.get("folio_field", "")
            if order_field := self._collect_order_custom_field_name(folio_field):
                order_custom_fields.add(order_field)
            if pol_field := self._collect_pol_custom_field_name(folio_field):
                pol_custom_fields.add(pol_field)

        self._inject_custom_fields_properties(self.composite_order_schema, order_custom_fields)

        po_lines_schema = (
            self.composite_order_schema.get("properties", {})
            .get(self.po_lines_key, {})
            .get("items", {})
        )
        self._inject_custom_fields_properties(po_lines_schema, pol_custom_fields)

    def get_prop(self, legacy_object, folio_prop_name: str, index_or_id, schema_default_value):
        if folio_prop_name.endswith(".acquisitionMethod"):
            return self.get_mapped_ref_data_value(
                self.acquisitions_methods_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                False,
            )

        elif re.compile(rf"{re.escape(self.po_lines_key)}\[(\d+)\]\.id").fullmatch(
            folio_prop_name
        ):
            return str(uuid.uuid4())

        elif re.compile(rf"{re.escape(self.po_lines_key)}\[\d+\]\.notes\[\d+\]\.").match(
            folio_prop_name
        ):
            return ""

        if folio_prop_name.endswith(".locationId"):
            return self.get_mapped_ref_data_value(
                self.location_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                False,
            )

        if folio_prop_name.endswith(".fundId"):
            return self.get_mapped_ref_data_value(
                self.funds_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                False,
            )

        if folio_prop_name in ("billTo", "shipTo"):
            raw = super().get_prop(
                legacy_object,
                folio_prop_name,
                index_or_id,
                schema_default_value,
            )
            return self._resolve_order_address_id(raw, folio_prop_name, index_or_id)

        mapped_value = super().get_prop(
            legacy_object, folio_prop_name, index_or_id, schema_default_value
        )

        return mapped_value

    @staticmethod
    def _extract_address_name(address: dict[str, Any]) -> str:
        for key in ("name", "addressName", "label"):
            val = address.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return ""

    @staticmethod
    def _address_from_config_value(raw_value: Any) -> dict[str, Any] | None:
        value = raw_value
        if isinstance(raw_value, str):
            try:
                value = json.loads(raw_value)
            except json.JSONDecodeError:
                return None

        return value if isinstance(value, dict) else None

    def _load_order_addresses_by_name(self) -> dict[str, str]:
        entries = list(
            self.folio_client.folio_get_all(
                "/configurations/entries",
                "configs",
                self.TENANT_ADDRESSES_QUERY,
                1000,
            )
        )
        addresses_by_name: dict[str, str] = {}
        for entry in entries:
            entry_id = entry.get("id", "")
            if not isinstance(entry_id, str) or not entry_id.strip():
                continue
            entry_id = entry_id.strip()
            address = self._address_from_config_value(entry.get("value"))
            if not address:
                continue
            address_name = self._extract_address_name(address)
            if address_name:
                addresses_by_name[normalize_for_compare(address_name)] = entry_id

        if addresses_by_name:
            logger.info(
                "Loaded %s order addresses from tenant settings",
                len(addresses_by_name),
            )
        else:
            logger.warning(
                "No order addresses found in tenant settings (%s)",
                self.TENANT_ADDRESSES_QUERY,
            )
        return addresses_by_name

    def _resolve_order_address_id(self, value: str, folio_prop_name: str, index_or_id: str) -> str:
        if not value:
            return value
        if self.is_uuid(value):
            return value

        resolved = self._order_addresses_by_name.get(normalize_for_compare(value))
        if resolved:
            self.migration_report.add("OrderAddressMapping", f"{value} -> {resolved}")
            return resolved

        raise TransformationRecordFailedError(
            index_or_id,
            (f"Order address name '{value}' not found in tenant settings for {folio_prop_name}"),
            f"Available addresses: {', '.join(sorted(self._order_addresses_by_name.keys()))}",
        )

    def _check_order_address_value(
        self,
        value: Any,
        address_names_by_name: dict[str, str],
        address_ids: set[str],
        folio_field: str,
        field_type: str,
    ) -> str | None:
        if isinstance(value, str):
            value = value.strip()
        if not value:
            return None

        if self.is_uuid(value):
            if value not in address_ids:
                return (
                    f"  - '{value}' (in field: {folio_field}, {field_type}) "
                    "- UUID not found in tenant settings"
                )
            return None

        if normalize_for_compare(value) not in address_names_by_name:
            return (
                f"  - '{value}' (in field: {folio_field}, {field_type}) "
                "- name not found in tenant settings"
            )

        return None

    def _validate_hardcoded_order_address_values(self) -> None:
        address_ids = set(self._order_addresses_by_name.values())
        invalid_values: list[str] = []

        for entry in self.record_map.get("data", []):
            folio_field = entry.get("folio_field", "")
            if folio_field not in {"billTo", "shipTo"}:
                continue

            for value_key in ("value", "fallback_value"):
                mapped_value = entry.get(value_key)
                invalid = self._check_order_address_value(
                    mapped_value,
                    self._order_addresses_by_name,
                    address_ids,
                    folio_field,
                    value_key,
                )
                if invalid:
                    invalid_values.append(invalid)

        if invalid_values:
            available = ", ".join(sorted(self._order_addresses_by_name.keys()))
            raise TransformationProcessError(
                "",
                "Invalid order address values found in field mapping:",
                "\n".join(
                    [
                        *invalid_values,
                        f"Available addresses: {available}",
                        "Please update your mapping file with valid address names or UUIDs.",
                    ]
                ),
            )

    def map_order_with_notes(self, legacy_order: dict, index_or_id: str):
        """Map one legacy order row to a composite order and write its POL notes.

        This is the entry point for processing a full order row. Use it instead of
        do_map whenever the POL notes extradata is needed; do_map alone maps the composite
        order only and silently produces no notes.

        Returns:
            tuple: (composite order, legacy id)
        """
        folio_rec, legacy_id = self.do_map(legacy_order, index_or_id, FOLIONamespaces.orders, True)
        self.perform_additional_mapping(legacy_id, folio_rec)
        self.report_folio_mapping(folio_rec, self.composite_order_schema)
        self.notes_mapper.map_notes(
            legacy_order,
            legacy_id,
            folio_rec[self.po_lines_key][0]["id"],
            FOLIONamespaces.orders,
        )
        return folio_rec, legacy_id

    def perform_additional_mapping(self, index_or_id, composite_order):
        self.validate_po_number(index_or_id, composite_order.get("poNumber"))

        # Get organization UUID from FOLIO
        composite_order["vendor"] = self.get_folio_organization_uuid(
            index_or_id, composite_order.get("vendor")
        )

        # Replace legacy bib ID with instance UUID from map
        logger.debug(
            "Looking up instance UUID for order %s: %s",
            index_or_id,
            json.dumps(composite_order, indent=2),
        )
        bib_id = composite_order.get(self.po_lines_key, [{}])[0].pop("instanceId", "")
        if matching_instance := self.get_folio_instance_uuid(index_or_id, bib_id):
            composite_order[self.po_lines_key][0]["instanceId"] = matching_instance

        return composite_order

    def validate_po_number(
        self,
        index_or_id: str,
        po_number: str,
    ):
        if not self.is_valid_po_number(po_number):
            self.migration_report.add(
                "PurchaseOrderVendorLinking",
                i18n_t("RECORD FAILED: PO number has invalid character(s)"),
            )
            raise TransformationRecordFailedError(
                index_or_id,
                "Purchase Order number has invalid character(s)",
                po_number,
            )

    @staticmethod
    def is_valid_po_number(po_number: str) -> bool:
        valid_po_number_characters = r"[A-Za-z0-9]"
        return re.sub(valid_po_number_characters, "", po_number) == ""

    def get_matching_record_from_folio(
        self,
        index_or_id,
        cache: dict,
        path: str,
        match_property: str,
        match_value: str,
        result_type: str,
    ):
        if match_value in cache:
            return cache[match_value]
        else:
            query = f'?query=({match_property}=="{match_value}")'
            if matching_record := next(
                self.folio_client.folio_get_all(path, result_type, query), None
            ):
                cache[match_value] = matching_record
                return matching_record

    def get_folio_organization_uuid(self, index_or_id, org_code):
        if self.organizations_id_map:
            logger.debug(f"Looking up organization code {org_code} in organizations_id_map")
            matching_org = self.organizations_id_map.get(org_code, None)
            logger.debug(f"Mapped value for organization code {org_code}: {matching_org}")
            if matching_org:
                self.migration_report.add(
                    "PurchaseOrderVendorLinking",
                    i18n_t("Organizations linked using organizations_id_map"),
                )
                return matching_org[1]

        logger.debug(
            f"Organization code {org_code} not found in organizations_id_map, looking up in FOLIO"
        )
        matching_org = self.get_matching_record_from_folio(
            index_or_id,
            self.folio_organization_cache,
            "/organizations-storage/organizations",
            "code",
            org_code,
            "organizations",
        )
        logger.debug(
            f"FOLIO lookup result for organization code {org_code}:"
            f" {json.dumps(matching_org, indent=2)}"
        )
        if matching_org:
            self.migration_report.add(
                "PurchaseOrderVendorLinking",
                i18n_t("Organizations not in ID map, linked using FOLIO lookup"),
            )
            return matching_org["id"]
        else:
            self.migration_report.add(
                "PurchaseOrderVendorLinking",
                i18n_t("RECORD FAILED Organization identifier not in ID map/FOLIO"),
            )
            raise TransformationRecordFailedError(
                index_or_id,
                "No matching Organization for organization identifier",
                org_code,
            )

    def get_folio_instance_uuid(self, index_or_id, bib_id):
        if matching_instance := self.instance_id_map.get(bib_id):
            self.migration_report.add(
                "PurchaseOrderInstanceLinking",
                i18n_t("Instances linked using instances_id_map"),
            )
            return matching_instance[1]
        else:
            self.migration_report.add(
                "PurchaseOrderInstanceLinking",
                i18n_t("Bib identifier not in instances_id_map, no instance linked"),
            )
            Helper.log_data_issue(
                index_or_id,
                "No matching instance for bib identifier",
                bib_id,
            )
