"""Mapper for transforming order data to FOLIO Orders format.

Provides the OrderMapper class for mapping legacy purchase order data to FOLIO
Orders and PurchaseOrderLines. Handles composite orders with embedded lines,
acquisition units, fund distributions, and vendor references.
"""

import json
import logging
import re
import uuid
from decimal import Decimal, InvalidOperation
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
MATERIAL_TYPE_FIELD = re.compile(r"\.(?:physical|eresource)\.materialType$")
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
        create_inventory_defaults: dict | None = None,
        po_lines_limit: int | None = None,
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
            create_inventory_defaults: Optional tenant defaults for a POL's inventory
                interaction (keys: physical, other, eresource), used when validating orders
                (by name), used for physical and e-resource material types on order lines.
            po_lines_limit: Optional tenant limit on the number of PO lines per order. The
                limit is not checked when it is None.
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
        self.folio_organizations_by_id: dict = {}
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
        self.create_inventory_defaults = create_inventory_defaults or {}
        self.po_lines_limit = po_lines_limit
        self._fund_info_cache: dict = {}
        self._reference_names: dict = {}
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
        self._material_types_by_name = self._load_material_types_by_name()
        self._validate_hardcoded_material_types()
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

        if MATERIAL_TYPE_FIELD.search(folio_prop_name):
            raw = str(
                super().get_prop(legacy_object, folio_prop_name, index_or_id, schema_default_value)
                or ""
            )
            # Hardcoded values are FOLIO names or UUIDs (validated at startup), not legacy
            # values, so they are never run through the map.
            if not raw:
                # Material type is only required when inventory creates items, so an empty
                # value stays empty instead of taking the map's default.
                return ""
            if self.material_type_mapping and not self._is_hardcoded_value(
                folio_prop_name, legacy_object, raw
            ):
                # Look up this field's own value. The physical and electronic material
                # types can come from different columns, but share one map.
                lookup_object = legacy_object
                keys = self.material_type_mapping.mapped_legacy_keys
                if len(keys) == 1:
                    lookup_object = {**legacy_object, keys[0]: raw}
                return self.get_mapped_ref_data_value(
                    self.material_type_mapping,
                    lookup_object,
                    index_or_id,
                    folio_prop_name,
                    False,
                )
            return self._resolve_material_type(raw, index_or_id)

        if folio_prop_name.endswith(".productIdType"):
            raw = super().get_prop(
                legacy_object, folio_prop_name, index_or_id, schema_default_value
            )
            raw = str(raw or "")
            delimiter = self.library_configuration.multi_field_delimiter
            if delimiter and delimiter in raw:
                return delimiter.join(
                    self._resolve_product_id_type(piece.strip(), folio_prop_name, index_or_id)
                    for piece in raw.split(delimiter)
                )
            return self._resolve_product_id_type(raw, folio_prop_name, index_or_id)

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
        pieces = {
            k: [piece.strip() for piece in str(legacy_object.get(k) or "").split(delimiter)]
            for k in keys
        }
        results = []
        for i in range(max(len(v) for v in pieces.values())):
            piece_object = dict(legacy_object)
            for k, values in pieces.items():
                # A column without the delimiter applies to every piece.
                if len(values) == 1:
                    piece_object[k] = values[0]
                else:
                    piece_object[k] = values[i] if i < len(values) else ""
            if not any(piece_object[k] for k in keys):
                # Keep the piece, empty, so it stays lined up with the other delimited
                # fields. Looking it up would give it the default value.
                self.handle_transformation_field_mapping_error(
                    index_or_id,
                    TransformationFieldMappingError(
                        index_or_id,
                        f"Empty value in delimited {folio_prop_name}. Nothing mapped for it",
                        str([legacy_object.get(k) for k in keys]),
                    ),
                )
                results.append("")
                continue
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

    def _load_material_types_by_name(self) -> dict[str, str]:
        """Build normalized FOLIO material type name -> id, for lookups without a map file."""
        by_name = {
            normalize_for_compare(t["name"]): t["id"]
            for t in self.folio_client.folio_get_all("/material-types", "mtypes", "", 1000)
            if t.get("name") and t.get("id")
        }
        logger.info("Loaded %s material types from FOLIO", len(by_name))
        return by_name

    def _is_hardcoded_value(self, folio_prop_name: str, legacy_object: dict, value: str) -> bool:
        """True if the resolved value is a literal from the field mapping, not legacy data.

        A non-empty value always takes precedence and legacy fields are never read. A
        fallback_value is only used when the legacy fields are empty.
        """
        if not value:
            return False
        for entry in MappingFileMapperBase.get_map_entries_by_folio_prop_name(
            folio_prop_name, self.record_map["data"]
        ):
            if entry.get("value"):
                return True
            fallback = entry.get("fallback_value")
            if (
                isinstance(fallback, str)
                and fallback.strip() == value.strip()
                and not any(
                    legacy_object.get(entry.get(key) or "")
                    for key in ("legacy_field", "fallback_legacy_field")
                )
            ):
                return True
        return False

    def _resolve_material_type(self, value: str, index_or_id: str) -> str:
        """Accept a UUID as-is or resolve a FOLIO material type name.

        Used without a material type map, and for hardcoded values even with one.

        A name that is not a FOLIO material type fails the record.
        """
        if not value or self.is_uuid(value):
            return value
        if resolved := self._material_types_by_name.get(normalize_for_compare(value)):
            self.migration_report.add("MaterialTypeMapping", f"{value} -> {resolved}")
            return resolved
        self.migration_report.add(
            "MaterialTypeMapping", i18n.t("Unmapped (Record failed)") + f" -- {value}"
        )
        raise TransformationRecordFailedError(
            index_or_id,
            "Material type name not found in FOLIO material types",
            value,
        )

    def _validate_hardcoded_material_types(self) -> None:
        """Halt if a hardcoded material type is not a FOLIO material type name or UUID."""
        known_ids = set(self._material_types_by_name.values())
        invalid_values: list[str] = []
        for entry in self.record_map.get("data", []):
            folio_field = entry.get("folio_field", "")
            if not MATERIAL_TYPE_FIELD.search(folio_field):
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
                            "- UUID not found in FOLIO material types"
                        )
                elif normalize_for_compare(value) not in self._material_types_by_name:
                    invalid_values.append(
                        f"  - '{value}' (in field: {folio_field}, {value_key}) "
                        "- name not found in FOLIO material types"
                    )
        if invalid_values:
            raise TransformationProcessError(
                "",
                "Invalid material type values found in field mapping:",
                "\n".join(
                    [
                        *invalid_values,
                        "Available material types: "
                        + ", ".join(sorted(self._material_types_by_name)),
                        "Please update your mapping file with valid material type names or UUIDs.",
                    ]
                ),
            )

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

    LOCATION_QUANTITY_FIELDS = ("quantity", "quantityPhysical", "quantityElectronic")

    def _delimited_location_quantity_fields(self, legacy_object: dict | None) -> set[str]:
        """Return the location quantity fields whose legacy value holds one value per location.

        A legacy quantity without the delimiter is a single PO-line total that gets copied
        onto every location, so it can't be summed and each location counts as one piece.
        A delimited quantity (e.g. "2<^>3") has one value per location, so those values are
        used as-is and summed when locations merge.
        """
        delimiter = self.library_configuration.multi_field_delimiter
        if not legacy_object or not delimiter:
            return set()
        pattern = re.compile(
            rf"{re.escape(self.po_lines_key)}\[\d+\]\.locations\[\d+\]\."
            rf"({'|'.join(self.LOCATION_QUANTITY_FIELDS)})"
        )
        fields: set[str] = set()
        for folio_key in self.folio_keys:
            if not (match := pattern.fullmatch(folio_key)):
                continue
            for entry in self.get_map_entries_by_folio_prop_name(
                folio_key, self.record_map["data"]
            ):
                for key in ("legacy_field", "fallback_legacy_field"):
                    value = legacy_object.get(entry.get(key) or "")
                    if isinstance(value, str) and delimiter in value:
                        fields.add(match.group(1))
        return fields

    def _merge_duplicate_locations(
        self,
        composite_order: dict,
        delimited_fields: set[str] | frozenset = frozenset(),
        index_or_id: str = "",
    ) -> None:
        """Merge the locations of each PO line that share a locationId.

        The merged location keeps the position of the first duplicate. Locations without a
        locationId are left as they are.

        Each quantity field present on any duplicate is recomputed:
        - Not in delimited_fields: the legacy value is a PO-line total copied onto every
          location, so it is ignored and the field becomes the number of duplicates merged
          (1 for a location that occurs once).
        - In delimited_fields: the legacy value has one quantity per location, so the
          duplicates' values are summed.

        A PO line with a single location is left untouched: there is nothing to merge, so
        its mapped quantity is already the number of pieces at that location.
        """
        for pol in composite_order.get(self.po_lines_key, []):
            locations = pol.get("locations", [])
            if len(locations) < 2:
                continue
            groups: dict = {}
            merged = []
            for location in locations:
                location_id = location.get("locationId") if isinstance(location, dict) else None
                if not location_id:
                    merged.append(location)
                elif location_id in groups:
                    groups[location_id].append(location)
                else:
                    groups[location_id] = [location]
                    merged.append(location)
            for group in groups.values():
                for field in self.LOCATION_QUANTITY_FIELDS:
                    if not any(field in loc for loc in group):
                        continue
                    if field in delimited_fields:
                        group[0][field] = sum(
                            self._as_int(loc.get(field), index_or_id, f"location {field}")
                            for loc in group
                        )
                    else:
                        group[0][field] = len(group)
            pol["locations"] = merged

    def _copy_quantity_to_format_field(self, composite_order: dict) -> None:
        """Copy a location's quantity to the field its order format uses.

        mod-orders only counts quantityPhysical and quantityElectronic, and recomputes
        quantity from them. A location with only quantity gets quantityPhysical (Physical
        Resource, Other) or quantityElectronic (Electronic Resource). In a P/E Mix a
        quantity above 1 becomes one electronic and the remainder physical. A P/E Mix
        quantity of 1 is ambiguous and is left for validation to report. If one of the two
        is set explicitly it is kept and the other gets the remainder of quantity.
        """
        targets = {
            "Physical Resource": "quantityPhysical",
            "Other": "quantityPhysical",
            "Electronic Resource": "quantityElectronic",
        }
        for pol in composite_order.get(self.po_lines_key, []):
            order_format = pol.get("orderFormat")
            target = targets.get(order_format)
            if not target and order_format != "P/E Mix":
                continue
            for location in pol.get("locations") or []:
                if not isinstance(location, dict) or location.get("quantity") in (None, ""):
                    continue
                physical = location.get("quantityPhysical") not in (None, "")
                electronic = location.get("quantityElectronic") not in (None, "")
                if target:
                    if not (physical or electronic):
                        location[target] = location["quantity"]
                    continue
                if physical and electronic:
                    continue
                try:
                    quantity = self._whole_number(location["quantity"])
                    given_field = "quantityPhysical" if physical else "quantityElectronic"
                    given = (
                        self._whole_number(location[given_field]) if physical or electronic else 0
                    )
                except (InvalidOperation, ValueError):
                    continue  # validation reports a quantity that is not a whole number
                if physical or electronic:
                    # One side is set explicitly and is respected. The other gets the remainder
                    if quantity > given:
                        missing = "quantityElectronic" if physical else "quantityPhysical"
                        location[missing] = quantity - given
                elif quantity > 1:
                    location["quantityElectronic"] = 1
                    location["quantityPhysical"] = quantity - 1

    @staticmethod
    def _as_int(value, index_or_id, label: str) -> int:
        """Convert a quantity to an int. Empty is 0, 1.0 is 1, anything else fails the record."""
        if value is None or str(value).strip() == "":
            return 0
        try:
            number = Decimal(str(value).strip())
        except InvalidOperation:
            number = None
        if number is None or number != number.to_integral_value():
            raise TransformationRecordFailedError(
                index_or_id, f"Quantity is not a whole number ({label})", str(value)
            )
        return int(number)

    # Fallback when a POL leaves createInventory empty and the tenant setting is missing or
    # unreadable: assume no inventory is created, so nothing is validated against it.
    DEFAULT_CREATE_INVENTORY = {
        "physical": "None",
        "other": "None",
        "eresource": "None",
    }

    def _effective_create_inventory(self, pol: dict, kind: str) -> str:
        """The createInventory value mod-orders would use: the POL's, else the tenant default."""
        section = "eresource" if kind == "eresource" else "physical"
        value = (pol.get(section) or {}).get("createInventory")
        defaults = {**self.DEFAULT_CREATE_INVENTORY, **(self.create_inventory_defaults or {})}
        return value or defaults.get(kind, "")

    # Report heading for each validation rule, picked by a phrase in the problem text. The first
    # match wins, so more specific phrases come first.
    VALIDATION_RULE_LABELS = (
        ("tenant limit", "PO lines above the tenant limit"),
        ("vendor", "Vendor not active or not a vendor"),
        ("access provider", "Access provider missing or not active"),
        ("expense class", "Expense class not in the fund's active budget"),
        ("restricted to locations", "Fund location restriction"),
        ("estimated price", "Fund distributions and estimated price"),
        ("fund distribution", "Fund distributions and estimated price"),
        ("material type", "Material type missing"),
        ("quantity", "Quantities and locations"),
        ("location", "Quantities and locations"),
        ("price", "Cost prices and discount"),
        ("additional cost", "Cost prices and discount"),
        ("discount", "Cost prices and discount"),
        ("claiming", "Claiming"),
        ("binder", "Receiving workflow, binder active"),
        ("receipt not required", "Receiving workflow, binder active"),
        ("prepayment", "Prepayment term"),
        ("package", "Package line with an instance"),
    )

    def _report_validation_problems(self, problems: list[str]) -> None:
        """Count each failed validation rule in the migration report."""
        for problem in problems:
            text = problem.lower()
            label = next(
                (label for phrase, label in self.VALIDATION_RULE_LABELS if phrase in text),
                "Other",
            )
            self.migration_report.add("OrderValidationFailures", i18n.t(label))

    def _validate_composite_order(self, index_or_id: str, composite_order: dict) -> None:
        """Check each PO line against the rules mod-orders enforces when it is POSTed.

        Every rule is checked except the material type rule, which only applies when the
        order's workflowStatus is Open, because FOLIO only requires a material type when an
        order opens. Problems from all PO lines are collected, prefixed with the POL number,
        and raised together as one failed record so the order is not sent to FOLIO.

        The rules are listed in the appendix on mod-orders validation in the docs.
        """
        is_open = str(composite_order.get("workflowStatus") or "").lower() == "open"
        problems: list[str] = []
        pols = composite_order.get(self.po_lines_key, [])
        if self.po_lines_limit is not None and len(pols) > self.po_lines_limit:
            problems.append(
                f"the order has {len(pols)} PO lines, above the tenant limit of "
                f"{self.po_lines_limit}"
            )
        if is_open:
            problems.extend(self._vendor_problems(composite_order.get("vendor")))
        for number, pol in enumerate(pols, start=1):
            label = f"POL {number}: "
            problems.extend(label + p for p in self._pol_problems(index_or_id, pol, is_open))
        if problems:
            self._report_validation_problems(problems)
            raise TransformationRecordFailedError(
                index_or_id,
                "Order would be rejected by FOLIO: " + "; ".join(problems),
                json.dumps(problems),
            )

    @staticmethod
    def _ensure_ongoing_info(composite_order: dict) -> None:
        """Make the ongoing object agree with the order type.

        mod-orders rejects an Ongoing order without an ongoing object and a One-Time order
        with one. An Ongoing order with none mapped gets a minimal one (the schema defaults).
        A One-Time order loses any ongoing object that the field mapping created.
        """
        order_type = str(composite_order.get("orderType") or "").lower()
        if order_type == "ongoing" and not composite_order.get("ongoing"):
            composite_order["ongoing"] = {"isSubscription": False, "manualRenewal": False}
        elif order_type == "one-time":
            composite_order.pop("ongoing", None)

    def _pol_problems(self, index_or_id: str, pol: dict, is_open: bool) -> list[str]:
        problems: list[str] = []
        cost = pol.get("cost")
        if not isinstance(cost, dict):
            cost = {}
        locations = [loc for loc in pol.get("locations") or [] if isinstance(loc, dict)]
        order_format = pol.get("orderFormat")
        cost_physical = self._as_int(cost.get("quantityPhysical"), index_or_id, "cost physical")
        cost_electronic = self._as_int(
            cost.get("quantityElectronic"), index_or_id, "cost electronic"
        )

        if pol.get("isPackage") and pol.get("instanceId"):
            problems.append("a package line cannot have an instance")

        interval = self._as_int(pol.get("claimingInterval"), index_or_id, "claiming interval")
        if self._is_true(pol.get("claimingActive")) and interval <= 0:
            problems.append("a claiming interval above 0 is required when claiming is active")

        problems.extend(self._fund_reference_problems(pol, locations, is_open))
        if is_open:
            problems.extend(self._access_provider_problems(pol))

        if not (locations or cost_physical or cost_electronic):
            return problems  # mod-orders skips the remaining checks for a line with no quantities

        problems.extend(
            self._format_quantity_problems(order_format, cost_physical, cost_electronic)
        )
        problems.extend(self._location_problems(index_or_id, pol, locations, cost))
        problems.extend(self._cost_price_problems(order_format, cost))

        # FOLIO's default for checkinItems is false (Synchronized), so an unmapped value is false
        independent = self._is_true(pol.get("checkinItems"))
        if (
            str(pol.get("receiptStatus") or "").lower() == "receipt not required"
            and not independent
        ):
            problems.append("receipt not required needs the Independent receiving workflow")
        if (pol.get("details") or {}).get("isBinderyActive"):
            if order_format not in ("Physical Resource", "P/E Mix"):
                problems.append("binder active requires Physical Resource or P/E Mix")
            if self._effective_create_inventory(pol, "physical") != "Instance, Holding, Item":
                problems.append("binder active requires inventory interaction that creates items")
            if not independent:
                problems.append("binder active requires the Independent receiving workflow")

        problems.extend(self._prepayment_term_problems(pol))
        if is_open:
            problems.extend(self._material_type_problems(pol, order_format))
            problems.extend(self._fund_distribution_problems(pol))
        return problems

    @staticmethod
    def _named(name: str | None, uuid: str) -> str:
        """Return 'name (uuid)', or just the uuid when the name is not known."""
        return f"{name} ({uuid})" if name else str(uuid)

    def _reference_name(self, path: str, collection: str, uuid: str) -> str | None:
        """Look up a FOLIO reference record's name (cached, None if it cannot be read)."""
        if path not in self._reference_names:
            try:
                records = self.folio_client.folio_get_all(path, collection, "", 1000)
                self._reference_names[path] = {r["id"]: r.get("name") for r in records}
            except Exception as exception:
                logger.warning("Could not read %s for names in messages (%s)", path, exception)
                self._reference_names[path] = {}
        return self._reference_names[path].get(uuid)

    def _location_label(self, location_id: str) -> str:
        return self._named(
            self._reference_name("/locations", "locations", location_id), location_id
        )

    def _vendor_problems(self, vendor_id: str | None) -> list[str]:
        """An Open order's vendor must be an active organization that is flagged as a vendor.

        Skipped when there is no vendor or the organizations were not loaded.
        """
        if not vendor_id:
            return []
        org = self.folio_organizations_by_id.get(vendor_id)
        if not org:
            return []
        problems = []
        label = self._named(org["name"], vendor_id)
        if org["status"] != "Active":
            problems.append(f"the vendor organization {label} is not Active")
        if not org["isVendor"]:
            problems.append(f"the organization {label} is not flagged as a vendor")
        return problems

    def _access_provider_problems(self, pol: dict) -> list[str]:
        """An Open order's access provider must exist and be an active organization."""
        provider = (pol.get("eresource") or {}).get("accessProvider")
        if not provider or not self.folio_organizations_by_id:
            return []
        org = self.folio_organizations_by_id.get(provider)
        if not org:
            return [f"the access provider {provider} is not an organization in FOLIO"]
        if org["status"] != "Active":
            return [f"the access provider {self._named(org['name'], provider)} is not Active"]
        return []

    def _fund_info(self, fund_id: str) -> dict | None:
        """Read a fund's location restrictions and the expense classes of its Active budgets.

        The result is cached by fund id. It is None when FOLIO cannot be read, in which case
        the checks that need it are skipped. The dict has "restrict_by_locations", "locations"
        (a set of location ids) and "expense_classes" (expense class id to status).
        """
        if fund_id not in self._fund_info_cache:
            try:
                fund = self.folio_client.folio_get(f"/finance/funds/{fund_id}")
                fund = fund.get("fund", fund)
                budgets = self.folio_client.folio_get(
                    "/finance/budgets",
                    "budgets",
                    query=f'fundId=="{fund_id}" and budgetStatus=="Active"',
                )
                self._fund_info_cache[fund_id] = {
                    "code": fund.get("code"),
                    "restrict_by_locations": bool(fund.get("restrictByLocations")),
                    "locations": {
                        loc.get("locationId")
                        for loc in fund.get("locations") or []
                        if loc.get("locationId")
                    },
                    "expense_classes": {
                        ec["expenseClassId"]: ec.get("status")
                        for budget in budgets
                        for ec in budget.get("statusExpenseClasses") or []
                    },
                }
            except Exception as exception:
                logger.warning(
                    "Could not read fund %s (%s). Its expense classes and location "
                    "restrictions are not checked.",
                    fund_id,
                    exception,
                )
                self._fund_info_cache[fund_id] = None
        return self._fund_info_cache[fund_id]

    def _fund_reference_problems(self, pol: dict, locations: list, is_open: bool) -> list[str]:
        """Check expense classes and location restrictions against the funds in FOLIO.

        An expense class must be in an Active budget of the fund, and be active there when
        the order is Open. When the order is Open and the line has locations, a fund
        restricted by locations needs at least one of its locations among the line's
        locations. A line with a holdingId location is not checked, because the holding's
        location is not known here.
        """
        problems: list[str] = []
        distributions = [d for d in pol.get("fundDistribution") or [] if isinstance(d, dict)]
        has_holding = any(loc.get("holdingId") for loc in locations)
        line_locations = {loc.get("locationId") for loc in locations if loc.get("locationId")}
        for distribution in distributions:
            fund_id = distribution.get("fundId")
            info = self._fund_info(fund_id) if fund_id else None
            if not info:
                continue
            fund = distribution.get("code") or info.get("code") or fund_id
            expense_class = distribution.get("expenseClassId")
            if expense_class:
                class_label = self._named(
                    self._reference_name(
                        "/finance/expense-classes", "expenseClasses", expense_class
                    ),
                    expense_class,
                )
                status = info["expense_classes"].get(expense_class, "missing")
                if status == "missing":
                    problems.append(
                        f"expense class {class_label} is not in an Active budget of fund {fund}"
                    )
                elif is_open and str(status).lower() == "inactive":
                    problems.append(f"expense class {class_label} is inactive for fund {fund}")
            if (
                is_open
                and locations
                and not has_holding
                and info["restrict_by_locations"]
                and info["locations"]
                and not info["locations"] & line_locations
            ):
                allowed = ", ".join(self._location_label(i) for i in sorted(info["locations"]))
                problems.append(
                    f"fund {fund} is restricted to locations that are not among the line's "
                    f"locations ({allowed})"
                )
        return problems

    @staticmethod
    def _whole_number(value) -> int:
        """Convert a mapped quantity (a string, int or float) to an int, dropping decimals.

        The value goes through str() so that a float such as 2.0 is read exactly.
        """
        return int(Decimal(str(value)))

    @staticmethod
    def _is_true(value) -> bool:
        return value is True or str(value).strip().lower() == "true"

    @staticmethod
    def _prepayment_term_problems(pol: dict) -> list[str]:
        """A multi-year line needs one fiscal year distribution per year of its prepayment term."""
        terms = pol.get("paymentTerms")
        if not CompositeOrderMapper._is_true(pol.get("multiYearPayment")) or not isinstance(
            terms, dict
        ):
            return []
        try:
            term = int(terms.get("prepaymentTerm") or 0)
        except (TypeError, ValueError):
            return []
        distributions = [d for d in terms.get("fiscalYearDistributions") or [] if d is not None]
        if term > 0 and len(distributions) != term:
            return [
                f"prepayment term {term} does not match the number of fiscal year "
                f"distributions ({len(distributions)})"
            ]
        return []

    @staticmethod
    def _format_quantity_problems(order_format, cost_physical: int, cost_electronic: int):
        """Cost quantity rules by order format."""
        problems: list[str] = []
        if order_format in ("Physical Resource", "Other"):
            if cost_physical == 0:
                problems.append("physical cost quantity must be above 0")
            if cost_electronic > 0:
                problems.append("electronic cost quantity must be 0")
        elif order_format == "Electronic Resource":
            if cost_electronic == 0:
                problems.append("electronic cost quantity must be above 0")
            if cost_physical > 0:
                problems.append("physical cost quantity must be 0")
        elif order_format == "P/E Mix":
            if cost_physical == 0:
                problems.append("physical cost quantity must be above 0")
            if cost_electronic == 0:
                problems.append("electronic cost quantity must be above 0")
        return problems

    def _material_type_problems(self, pol: dict, order_format) -> list[str]:
        """Material type is required when the inventory interaction creates items."""
        problems: list[str] = []
        for kind, wanted in (
            ("physical", order_format != "Electronic Resource"),
            ("eresource", order_format in ("Electronic Resource", "P/E Mix")),
        ):
            if (
                wanted
                and self._effective_create_inventory(pol, kind) == "Instance, Holding, Item"
                and not (pol.get(kind) or {}).get("materialType")
            ):
                problems.append(f"{kind} material type is required to create items")
        return problems

    def _location_problems(
        self, index_or_id: str, pol: dict, locations: list, cost: dict
    ) -> list[str]:
        problems: list[str] = []
        order_format = pol.get("orderFormat")
        totals = {}
        for field in ("quantityPhysical", "quantityElectronic"):
            totals[field] = sum(
                self._as_int(loc.get(field), index_or_id, f"location {field}") for loc in locations
            )
        for field, kind, holds in (
            ("quantityPhysical", "physical", order_format != "Electronic Resource"),
            (
                "quantityElectronic",
                "eresource",
                order_format in ("Electronic Resource", "P/E Mix"),
            ),
        ):
            expected = self._as_int(cost.get(field), index_or_id, f"cost {field}")
            requires = self._effective_create_inventory(pol, kind) != "None" and holds
            if locations and (totals[field] > 0 or requires) and totals[field] != expected:
                problems.append(
                    f"location {field} total {totals[field]} does not match the cost ({expected})"
                )
        for loc in locations:
            if sum(
                self._as_int(loc.get(f), index_or_id, f"location {f}")
                for f in ("quantityPhysical", "quantityElectronic")
            ) == 0 and any(f in loc for f in ("quantityPhysical", "quantityElectronic")):
                problems.append("a location has no physical or electronic quantity")
            if bool(loc.get("locationId")) == bool(loc.get("holdingId")):
                problems.append("a location needs exactly one of locationId or holdingId")
        return problems

    @staticmethod
    def _cost_price_problems(order_format: str | None, cost: dict) -> list[str]:
        def number(key):
            try:
                return float(cost[key])
            except (KeyError, TypeError, ValueError):
                return None

        problems = []
        unit, electronic = number("listUnitPrice"), number("listUnitPriceElectronic")
        if unit is not None and (unit > 0 if order_format == "Electronic Resource" else unit < 0):
            problems.append("list unit price is not valid for the order format")
        if electronic is not None and (
            electronic < 0
            if order_format in ("Electronic Resource", "P/E Mix")
            else electronic > 0
        ):
            problems.append("electronic list unit price is not valid for the order format")
        if (number("additionalCost") or 0) < 0:
            problems.append("additional cost cannot be negative")
        discount = number("discount") or 0
        if discount < 0 or (cost.get("discountType") == "percentage" and discount > 100):
            problems.append("discount is not valid")
        elif discount > 0 and cost.get("discountType") != "percentage":
            price = CompositeOrderMapper._estimated_price(cost)
            if price is not None and price < 0:
                problems.append("discount makes the estimated price negative")
        return problems

    @staticmethod
    def _estimated_price(cost: dict) -> Decimal | None:
        """The PO line estimated price as mod-orders computes it, or None if a value is invalid.

        List prices times quantities, less the discount (an amount, or a percentage of the
        total, which is the default), plus additional cost and the FYRO adjustment.
        """

        def amount(key: str) -> Decimal:
            value = cost.get(key)
            return Decimal(str(value).strip()) if str(value or "").strip() else Decimal(0)

        try:
            total = amount("listUnitPrice") * amount("quantityPhysical") + amount(
                "listUnitPriceElectronic"
            ) * amount("quantityElectronic")
            discount = amount("discount")
            total -= (
                discount if cost.get("discountType") != "percentage" else total * discount / 100
            )
            total += amount("additionalCost") + amount("fyroAdjustmentAmount")
        except InvalidOperation:
            return None
        return total.quantize(Decimal("0.01"), rounding="ROUND_HALF_EVEN")

    @staticmethod
    def _fund_distribution_problems(pol: dict) -> list[str]:
        """Fund distributions must add up to the PO line's estimated price (checked on open)."""
        distributions = [d for d in pol.get("fundDistribution") or [] if isinstance(d, dict)]
        cost = pol.get("cost")
        if not isinstance(cost, dict):
            cost = {}
        price = CompositeOrderMapper._estimated_price(cost)
        if not distributions or price is None:
            return []
        try:
            values = [Decimal(str(d.get("value") or 0)) for d in distributions]
        except InvalidOperation:
            return []
        is_percentage = [
            str(d.get("distributionType")).lower() == "percentage" for d in distributions
        ]
        message = "fund distributions do not add up to the estimated price"
        if price == 0:
            if len(set(is_percentage)) > 1:
                return ["fund distribution types cannot be mixed when the estimated price is 0"]
            if is_percentage[0]:
                return [] if sum(values) == 100 else [message]
            return [] if all(v == 0 for v in values) else [message]
        remaining = Decimal(100)
        for value, percentage in zip(values, is_percentage, strict=True):
            remaining -= value if percentage else value / price * 100
        return [message] if abs(remaining) > Decimal("1e-10") else []

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
        # Several rows can be merged into one order, so the order's legacy id alone does not
        # say which row an issue came from. Issues are logged with both.
        self._current_row = index_or_id
        try:
            folio_rec, legacy_id = self.do_map(
                legacy_order, index_or_id, FOLIONamespaces.orders, True
            )
            self.perform_additional_mapping(legacy_id, folio_rec, legacy_order)
            self.report_folio_mapping(folio_rec, self.composite_order_schema)
            self.notes_mapper.map_notes(
                legacy_order,
                legacy_id,
                folio_rec[self.po_lines_key][0]["id"],
                FOLIONamespaces.orders,
            )
        except TransformationRecordFailedError as error:
            error.index_or_id = self._with_row(error.index_or_id)
            raise
        return folio_rec, legacy_id

    def _with_row(self, identifier):
        """Return the legacy identifier followed by the row it came from, as 'id (row n)'."""
        row = getattr(self, "_current_row", "")
        if not row or str(row) in str(identifier):
            return identifier
        return f"{identifier} ({row})"

    def handle_transformation_field_mapping_error(self, index_or_id, error):
        error.index_or_id = self._with_row(error.index_or_id or index_or_id)
        super().handle_transformation_field_mapping_error(self._with_row(index_or_id), error)

    def perform_additional_mapping(self, index_or_id, composite_order, legacy_object=None):
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
        self._merge_duplicate_locations(
            composite_order, self._delimited_location_quantity_fields(legacy_object), index_or_id
        )
        self._copy_quantity_to_format_field(composite_order)
        self._ensure_ongoing_info(composite_order)
        self._validate_composite_order(index_or_id, composite_order)

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
        self.folio_organizations_by_id = {
            org["id"]: {
                "status": org.get("status"),
                "isVendor": org.get("isVendor"),
                "name": org.get("name") or org.get("code"),
            }
            for org in organizations
            if org.get("id")
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
