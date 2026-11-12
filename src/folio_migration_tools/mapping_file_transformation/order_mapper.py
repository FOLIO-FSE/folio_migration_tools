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

import i18n
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
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
PRODUCT_ID_TYPE_FIELD = re.compile(r"productIds\[\d+\]\.productIdType$")
POL_NOTE_PREFIX = re.compile(r"^(?:compositePoLines|poLines)\[\d+\]\.(?=notes\[)")


class CompositeOrderMapper(MappingFileMapperBase):
    TENANT_ADDRESSES_QUERY = '?query=(module=="TENANT" and configName=="tenant.addresses")'

    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        task_configuration,
        composite_order_map: dict,
        organizations_code_map: list | None,
        instance_id_map: dict,
        acquisition_method_map,
        payment_status_map,
        receipt_status_map,
        workflow_status_map,
        location_map,
        funds_map,
        funds_expense_class_map=None,
        material_type_map=None,
    ):
        """Initialize CompositeOrderMapper for purchase order transformations.

        Args:
            folio_client (FolioClient): FOLIO API client.
            library_configuration (LibraryConfiguration): Library configuration.
            task_configuration: Task configuration for orders migration.
            composite_order_map (dict): Mapping configuration for order fields.
            organizations_code_map (list | None): Optional rows mapping legacy vendor values
                to FOLIO organization codes (legacy column plus folio_code). If None, the
                vendor value is assumed to be a FOLIO organization code.
            instance_id_map (dict): Mapping of legacy to FOLIO instances.
            acquisition_method_map: Mapping of legacy to FOLIO acquisition methods.
            payment_status_map: Mapping of legacy to FOLIO payment statuses.
            receipt_status_map: Mapping of legacy to FOLIO receipt statuses.
            workflow_status_map: Mapping of legacy to FOLIO workflow statuses.
            location_map: Mapping of legacy to FOLIO locations.
            funds_map: Mapping of legacy to FOLIO funds.
            funds_expense_class_map: Mapping for fund expense classes.
            material_type_map: Optional mapping of legacy values to FOLIO material types
                (by name), used for physical and e-resource material types on order lines.
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
        self.folio_organizations_by_code = self._load_folio_organizations_by_code()
        self.organizations_code_map = self._build_organizations_code_map(
            organizations_code_map, self.folio_organizations_by_code
        )

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
        self.expense_class_mapping = (
            RefDataMapping(
                self.folio_client,
                "/finance/expense-classes",
                "expenseClasses",
                funds_expense_class_map,
                "code",
                "ExpenseClassMapping",
            )
            if funds_expense_class_map
            else None
        )
        self.material_type_mapping = (
            RefDataMapping(
                self.folio_client,
                "/material-types",
                "mtypes",
                material_type_map,
                "name",
                "MaterialTypeMapping",
            )
            if material_type_map
            else None
        )
        self._identifier_types_by_name = self._load_identifier_types_by_name()
        self._validate_hardcoded_product_id_types()
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
                index_or_id,
                folio_prop_name,
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
            return self._get_delimited_ref_data_value(
                self.location_mapping, legacy_object, folio_prop_name, index_or_id
            )

        if folio_prop_name.endswith(".fundId"):
            return self._get_delimited_ref_data_value(
                self.funds_mapping, legacy_object, folio_prop_name, index_or_id
            )

        if folio_prop_name.endswith(".expenseClassId") and self.expense_class_mapping:
            return self._get_delimited_ref_data_value(
                self.expense_class_mapping,
                legacy_object,
                folio_prop_name,
                index_or_id,
                prevent_default=True,
            )

        if folio_prop_name.endswith((".physical.materialType", ".eresource.materialType")) and (
            self.material_type_mapping
        ):
            return self.get_mapped_ref_data_value(
                self.material_type_mapping,
                legacy_object,
                index_or_id,
                folio_prop_name,
                False,
            )

        if folio_prop_name.endswith(".productIdType"):
            raw = super().get_prop(
                legacy_object, folio_prop_name, index_or_id, schema_default_value
            )
            return self._resolve_product_id_type(str(raw or ""), folio_prop_name, index_or_id)

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

    def _get_delimited_ref_data_value(
        self,
        ref_data_mapping,
        legacy_object,
        folio_prop_name,
        index_or_id,
        prevent_default=False,
    ):
        """Look up a ref data value, one lookup per multi-field-delimited piece.

        The pieces' FOLIO ids are rejoined with the delimiter so the array mapper
        can split them into separate objects.
        """
        delimiter = self.library_configuration.multi_field_delimiter
        keys = ref_data_mapping.mapped_legacy_keys
        if not delimiter or not any(
            isinstance(legacy_object.get(k), str) and delimiter in legacy_object[k] for k in keys
        ):
            return self.get_mapped_ref_data_value(
                ref_data_mapping, legacy_object, index_or_id, folio_prop_name, prevent_default
            )
        pieces = {k: str(legacy_object.get(k, "")).split(delimiter) for k in keys}
        results = []
        for i in range(max(len(v) for v in pieces.values())):
            piece_object = dict(legacy_object)
            for k, values in pieces.items():
                piece_object[k] = values[i].strip() if i < len(values) else ""
            results.append(
                self.get_mapped_ref_data_value(
                    ref_data_mapping, piece_object, index_or_id, folio_prop_name, prevent_default
                )
            )
        return delimiter.join(results)

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

    def _load_identifier_types_by_name(self) -> dict[str, str]:
        """Build normalized FOLIO identifier type name -> id, for productIdType lookups."""
        by_name = {
            normalize_for_compare(t["name"]): t["id"]
            for t in self.folio_client.folio_get_all(
                "/identifier-types", "identifierTypes", "", 1000
            )
            if t.get("name") and t.get("id")
        }
        logger.info("Loaded %s identifier types from FOLIO", len(by_name))
        return by_name

    def _resolve_product_id_type(self, value: str, folio_prop_name: str, index_or_id: str) -> str:
        """Accept an identifier type UUID as-is, or resolve a name (case/space-insensitive).

        An unrecognized value does not fail the record. It resolves to an empty type, and
        the product id it belongs to is dropped in _drop_incomplete_product_ids.
        """
        if not value or self.is_uuid(value):
            return value
        if resolved := self._identifier_types_by_name.get(normalize_for_compare(value)):
            self.migration_report.add("ProductIdTypeMapping", f"{value} -> {resolved}")
            return resolved
        self.migration_report.add(
            "ProductIdTypeMapping", i18n.t("Unmapped (Product ID dropped)") + f" -- {value}"
        )
        return ""

    def _validate_hardcoded_product_id_types(self) -> None:
        """Halt if a hardcoded productIdType in the field mapping is not a FOLIO type."""
        known_ids = set(self._identifier_types_by_name.values())
        invalid_values: list[str] = []
        for entry in self.record_map.get("data", []):
            folio_field = entry.get("folio_field", "")
            if not PRODUCT_ID_TYPE_FIELD.search(folio_field):
                continue
            for value_key in ("value", "fallback_value"):
                value = entry.get(value_key)
                value = value.strip() if isinstance(value, str) else value
                if not value:
                    continue
                if self.is_uuid(value):
                    if value not in known_ids:
                        invalid_values.append(
                            f"  - '{value}' (in field: {folio_field}, {value_key}) "
                            "- UUID not found in FOLIO identifier types"
                        )
                elif normalize_for_compare(value) not in self._identifier_types_by_name:
                    invalid_values.append(
                        f"  - '{value}' (in field: {folio_field}, {value_key}) "
                        "- name not found in FOLIO identifier types"
                    )
        if invalid_values:
            raise TransformationProcessError(
                "",
                "Invalid productIdType values found in field mapping:",
                "\n".join(
                    [
                        *invalid_values,
                        "Available identifier types: "
                        + ", ".join(sorted(self._identifier_types_by_name)),
                        "Please update your mapping file with valid identifier type "
                        "names or UUIDs.",
                    ]
                ),
            )

    def _drop_incomplete_product_ids(self, index_or_id: str, composite_order: dict) -> None:
        """Drop product ids missing a productId or a usable productIdType.

        Each dropped object that had some content is reported as a field mapping error.
        The remaining product ids, and the record itself, are kept.
        """
        for pol in composite_order.get(self.po_lines_key, []):
            details = pol.get("details")
            if not isinstance(details, dict) or not details.get("productIds"):
                continue
            kept = []
            for product_id in details["productIds"]:
                if not isinstance(product_id, dict) or (
                    product_id.get("productId") and product_id.get("productIdType")
                ):
                    kept.append(product_id)
                elif any(v for v in product_id.values()):
                    missing = [k for k in ("productId", "productIdType") if not product_id.get(k)]
                    self.handle_transformation_field_mapping_error(
                        index_or_id,
                        TransformationFieldMappingError(
                            index_or_id,
                            "Product ID removed: missing productId or productIdType "
                            "(not a FOLIO identifier type)",
                            json.dumps({"missing": missing, **product_id}),
                        ),
                    )
            if kept:
                details["productIds"] = kept
            else:
                details.pop("productIds")

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

        self._drop_incomplete_product_ids(index_or_id, composite_order)

        return composite_order

    def validate_po_number(
        self,
        index_or_id: str,
        po_number: str,
    ):
        if not self.is_valid_po_number(po_number):
            self.migration_report.add(
                "PurchaseOrderVendorLinking",
                i18n.t("RECORD FAILED: PO number has invalid character(s)"),
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

    def _load_folio_organizations_by_code(self) -> dict:
        """Build normalized FOLIO organization code -> (id, code) from FOLIO, in one pass."""
        organizations = self.folio_client.folio_get_all(
            "/organizations-storage/organizations", "organizations", "", 1000
        )
        by_code = {
            normalize_for_compare(org["code"]): (org["id"], org["code"])
            for org in organizations
            if org.get("code")
        }
        logger.info("Loaded %s organizations from FOLIO", len(by_code))
        return by_code

    @staticmethod
    def _build_organizations_code_map(rows: list | None, folio_orgs_by_code: dict) -> dict | None:
        """Turn the optional organizations code map into normalized legacy -> FOLIO code.

        The map needs a folio_code column and exactly one legacy column. A * row is allowed
        in the file (so it can be shared with a future RefDataMapping-based version) but is
        ignored: a default vendor would silently misattribute orders, so unmapped values
        fail the record. Every folio_code, including the * row's, must exist in FOLIO.
        """
        if not rows:
            return None
        legacy_columns = [k for k in rows[0].keys() if k and k.strip() != "folio_code"]
        if "folio_code" not in rows[0] or len(legacy_columns) != 1:
            raise TransformationProcessError(
                "",
                "The organizations code map needs a folio_code column and exactly one "
                f"legacy column. Found columns: {', '.join(rows[0].keys())}",
            )
        legacy_column = legacy_columns[0]
        code_map = {}
        missing = set()
        for row in rows:
            folio_code = row["folio_code"]
            if normalize_for_compare(folio_code) not in folio_orgs_by_code:
                missing.add(folio_code)
            if normalize_for_compare(row[legacy_column]) == "*":
                logger.info("Ignoring the * row in the organizations code map")
                continue
            code_map[normalize_for_compare(row[legacy_column])] = folio_code
        if missing:
            raise TransformationProcessError(
                "",
                "Codes (folio_code) from the organizations code map are not in FOLIO",
                ", ".join(sorted(missing)),
            )
        logger.info("Loaded %s rows from the organizations code map", len(code_map))
        return code_map

    def get_folio_organization_uuid(self, index_or_id, legacy_value):
        """Resolve the vendor to a FOLIO organization id.

        With an organizations code map, the legacy value is mapped to a FOLIO code first
        (the map is then authoritative). Without one, the value is used as the FOLIO code.
        """
        folio_code = legacy_value
        if self.organizations_code_map is not None:
            folio_code = self.organizations_code_map.get(normalize_for_compare(legacy_value))
            if folio_code:
                self.migration_report.add(
                    "PurchaseOrderVendorLinking",
                    i18n.t("Organizations linked using organizations code map"),
                )
        if folio_code and (
            matching_org := self.folio_organizations_by_code.get(normalize_for_compare(folio_code))
        ):
            if self.organizations_code_map is None:
                self.migration_report.add(
                    "PurchaseOrderVendorLinking",
                    i18n.t("Organizations linked using FOLIO organization code"),
                )
            self.migration_report.add(
                "OrganizationsMapping", f"{legacy_value} -> {matching_org[1]}"
            )
            return matching_org[0]
        self.migration_report.add(
            "PurchaseOrderVendorLinking",
            i18n.t("RECORD FAILED Organization identifier not in code map/FOLIO"),
        )
        self.migration_report.add(
            "OrganizationsMapping",
            i18n.t("Unmapped (Record failed)") + f" -- {legacy_value} -> {folio_code or ''}",
        )
        raise TransformationRecordFailedError(
            index_or_id,
            "No matching Organization for organization identifier",
            legacy_value,
        )

    def get_folio_instance_uuid(self, index_or_id, bib_id):
        if matching_instance := self.instance_id_map.get(bib_id):
            self.migration_report.add(
                "PurchaseOrderInstanceLinking",
                i18n.t("Instances linked using instances_id_map"),
            )
            return matching_instance[1]
        else:
            self.migration_report.add(
                "PurchaseOrderInstanceLinking",
                i18n.t("Bib identifier not in instances_id_map, no instance linked"),
            )
            Helper.log_data_issue(
                index_or_id,
                "No matching instance for bib identifier",
                bib_id,
            )
