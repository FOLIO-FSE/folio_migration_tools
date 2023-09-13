import json
import logging
import sys

from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationProcessError


class RefDataMapping(object):
    def __init__(
        self,
        folio_client: FolioClient,
        ref_data_path,
        array_name,
        the_map,
        key_type,
        blurb_id,
    ):
        self.name = array_name
        self.cache: dict = {}
        self.blurb_id = blurb_id
        logging.info("%s reference data mapping. Initializing", self.name)
        logging.info("Fetching %s reference data from FOLIO", self.name)
        self.ref_data = list(folio_client.folio_get_all(ref_data_path, array_name, "", 1000))
        self.map = the_map
        self.regular_mappings: list = []
        self.key_type = key_type
        self.hybrid_mappings = []
        self.mapped_legacy_keys = []
        self.default_id = ""
        self.default_name = ""
        self.cached_dict = {}
        self.setup_mappings()
        logging.info("%s reference data mapping. Done init", self.name)

    def get_ref_data_tuple(self, key_value):
        if ref_object := self.cached_dict.get(key_value.lower().strip(), ()):
            return ref_object
        self.cached_dict = {
            r[self.key_type].lower(): (r["id"], r[self.key_type]) for r in self.ref_data
        }
        return self.cached_dict.get(key_value.lower().strip(), ())

    def setup_mappings(self):
        self.pre_validate_map()
        for idx, mapping in enumerate(self.map):
            try:
                # Get the legacy keys
                if idx == 0:
                    self.mapped_legacy_keys = get_mapped_legacy_keys(mapping)
                if self.is_default_mapping(mapping):
                    if t := self.get_ref_data_tuple(mapping[f"folio_{self.key_type}"]):
                        self.default_id = t[0]
                        self.default_name = t[1]
                        logging.info(
                            "Set %s as default %s mapping",
                            mapping[f"folio_{self.key_type}"],
                            self.name,
                        )
                    else:
                        x = mapping.get(f"folio_{self.key_type}", "")
                        raise TransformationProcessError(
                            "",
                            f"No {self.name} - {x} - set up in map or tenant. Check for "
                            f"inconstencies in {self.name} naming. "
                            f"Add a row to mapping file with *:s and a valid {self.name}",
                        )
                else:
                    if self.is_hybrid_default_mapping(mapping):
                        self.hybrid_mappings.append(mapping)
                    else:
                        self.regular_mappings.append(mapping)
                    t = self.get_ref_data_tuple(mapping[f"folio_{self.key_type}"])
                    if not t:
                        raise TransformationProcessError("", f"Mapping not found for {mapping}")
                    mapping["folio_id"] = t[0]
            except TransformationProcessError as transformation_process_error:
                raise transformation_process_error from transformation_process_error
            except Exception as ee:
                logging.info(json.dumps(self.map, indent=4))
                logging.error(ee)
                raise TransformationProcessError(
                    "",
                    f'"{mapping[f"folio_{self.key_type}"]}" could not be found in FOLIO',
                ) from ee

        self.post_validate_map()
        logging.info(
            f"Loaded {len(self.regular_mappings)} mappings for {len(self.ref_data)} {self.name} "
            "in FOLIO"
        )
        logging.info(
            f"loaded {len(self.hybrid_mappings)} hybrid mappings for {len(self.ref_data)} "
            f"{self.name} in FOLIO"
        )

    def get_hybrid_mapping(self, legacy_object):
        obj_key = "_".join(legacy_object[k].strip() for k in self.mapped_legacy_keys)
        if obj_key in self.cache:
            return self.cache[obj_key]
        highest_match = None
        highest_match_number = 0

        prepped_props = {k: legacy_object[k].strip() for k in self.mapped_legacy_keys}
        for mapping in self.hybrid_mappings:
            mismatch = 0
            match_numbers = []
            for k in self.mapped_legacy_keys:
                if mapping[k] == prepped_props[k]:
                    match_numbers.append(10)
                elif mapping[k] == "*":
                    match_numbers.append(1)
                else:
                    mismatch += 1
            summa = sum(match_numbers)

            if mismatch < 1 and summa > highest_match_number and min(match_numbers) > 0:
                highest_match_number = summa
                highest_match = mapping
        self.cache[obj_key] = highest_match
        return highest_match

    def get_ref_data_mapping(self, legacy_object):
        obj_key = "_".join(legacy_object[k].strip() for k in self.mapped_legacy_keys)
        if obj_key in self.cache:
            return self.cache[obj_key]
        prepped_props = {k: legacy_object[k].strip() for k in self.mapped_legacy_keys}
        for mapping in self.regular_mappings:
            match_number = sum(prepped_props[k] == mapping[k] for k in self.mapped_legacy_keys)
            if match_number == len(self.mapped_legacy_keys):
                self.cache[obj_key] = mapping
                return mapping
        return None

    def is_hybrid_default_mapping(self, mapping):
        legacy_values = [value for key, value in mapping.items() if key in self.mapped_legacy_keys]
        return "*" in legacy_values and not self.is_default_mapping(mapping)

    def is_default_mapping(self, mapping):
        legacy_values = [value for key, value in mapping.items() if key in self.mapped_legacy_keys]
        return all(f == "*" for f in legacy_values)

    def pre_validate_map(self):
        if not any(f for f in self.map if f.get(f"folio_{self.key_type}", "")):
            raise TransformationProcessError(
                "", f"Column folio_{self.key_type} missing from {self.name} map file"
            )
        folio_values_from_map = [f[f"folio_{self.key_type}"] for f in self.map]
        folio_values_from_folio = [r[self.key_type] for r in self.ref_data]
        folio_values_not_in_map = list(
            {f for f in folio_values_from_folio if f not in folio_values_from_map}
        )
        map_values_not_in_folio = list(
            {f for f in folio_values_from_map if f not in folio_values_from_folio}
        )
        if any(folio_values_not_in_map):
            logging.info(
                "Values from %s ref data in FOLIO that are not in the map: %s",
                self.name,
                folio_values_not_in_map,
            )
        if any(map_values_not_in_folio):
            raise TransformationProcessError(
                "",
                (
                    f"Values ({self.key_type}) from {self.name} map are not in "
                    f"FOLIO: {map_values_not_in_folio}"
                ),
            )

    def post_validate_map(self):
        if not self.default_id:
            raise TransformationProcessError(
                "",
                f"No fallback {self.name} set up in map."
                f"Add a row to mapping file with *:s in all legacy "
                f"columns and a valid {self.name} value",
            )
        for mapping in self.map:
            if f"folio_{self.key_type}" not in mapping:
                logging.critical(
                    f"folio_{self.key_type} is not a column in the {self.name} mapping file. Fix."
                )
                sys.exit(1)
            elif (
                all(k not in mapping for k in self.mapped_legacy_keys)
                and "legacy_code" not in mapping
            ):
                logging.critical(
                    (
                        "field names from %s missing in map legacy_code is "
                        "not a column in the %s mapping file"
                    ),
                    self.mapped_legacy_keys,
                    self.name,
                )
                sys.exit(1)
            elif not all(mapping.values()):
                logging.critical(
                    f"empty value in mapping {mapping.values()}. Check {self.name} mapping file"
                )
                sys.exit(1)


def get_mapped_legacy_keys(mapping):
    return [
        k.strip()
        for k in mapping.keys()
        if k
        not in [
            "folio_group",
            "folio_code",
            "folio_id",
            "folio_name",
            "legacy_code",
            "folio_value",
            "folio_owner",
            "folio_feeFineType",
        ]
    ]
