import json
import logging

from folioclient import FolioClient
from marc_to_folio.custom_exceptions import (TransformationCriticalDataError,
                                             TransformationProcessError)


class RefDataMapping(object):
    def __init__(
        self, folio_client: FolioClient, ref_data_path, array_name, map, key_type
    ):
        self.name = array_name
        logging.info(f"{self.name} reference data mapping. Initializing")
        logging.info(f"Fetching {self.name} reference data from FOLIO")
        self.ref_data = list(folio_client.folio_get_all(ref_data_path, array_name))
        logging.info(json.dumps(self.ref_data, indent=4))
        self.map = map
        self.key_type = key_type
        self.keys = ""
        self.default_id = ""
        self.default_name = ""        
        self.cached_dict = {}
        self.setup_mappings()
        logging.info(f"{self.name} reference data mapping. Done init")

    def get_ref_data_tuple(self, key_value):
        ref_object = self.cached_dict.get(key_value.lower().strip(), ())
        if ref_object:
            return ref_object
        self.cached_dict = {
            r[self.key_type].lower(): (r["id"], r["name"]) for r in self.ref_data
        }
        return self.cached_dict.get(key_value.lower().strip(), ())

    def setup_mappings(self):
        for idx, mapping in enumerate(self.map):
            try:
                if idx == 0:
                    self.keys = [
                        k
                        for k in mapping.keys()
                        if k not in ["folio_code", "folio_id", "folio_name", "legacy_code"]
                    ]
                    logging.info(json.dumps(self.keys, indent=4))
                if any(m for m in mapping.values() if m == "*"):
                    # Set up default mapping if available
                    t = self.get_ref_data_tuple(mapping[f"folio_{self.key_type}"])
                    if t:
                        self.default_id = t[0]
                        self.default_name = t[1]
                        logging.info(
                            f'Set {mapping[f"folio_{self.key_type}"]} as default {self.name} mapping'
                        )
                    else:
                        x = mapping.get(f"folio_{self.key_type}", "")
                        raise TransformationProcessError(
                            f"No Default {self.name} -{x}- set up in map. "
                            f"Add a row to mapping file with *:s and a valid {self.name}"
                        )
                else:
                    t= self.get_ref_data_tuple(
                        mapping[f"folio_{self.key_type}"]
                    )
                    if not t:
                        raise TransformationProcessError(f'Mapping not found for {mapping}')
                    mapping["folio_id"] = t[0]
            except TransformationProcessError as te:
                raise te
            except Exception:
                logging.info(json.dumps(self.map, indent=4))
                raise TransformationProcessError(
                    f'{mapping[f"folio_{self.key_type}"]} could not be found in FOLIO'
                )
        if not self.default_id:
            raise TransformationProcessError(
                f"No default {self.name} set up in map."
                f"Add a row to mapping file with *:s and a valid {self.name} value"
            )
        logging.info(
            f"loaded {idx} mappings for {len(self.ref_data)} {self.name} in FOLIO"
        )
        self.check_up_on_mapping()

    def check_up_on_mapping(self):
        for mapping in self.map:
            if f"folio_{self.key_type}" not in mapping:
                logging.critical(
                    f"folio_{self.key_type} is not a column in the {self.name} mapping file. Fix."
                )
                exit()
            elif all(k not in mapping for k in self.keys) and "legacy_code" not in mapping:
                logging.critical(
                    f"field names from {self.keys} missing in map legacy_code is not a column in the {self.name} mapping file"
                )
                exit()
            elif not all(mapping.values()):
                logging.critical(
                    f"empty value in mapping {mapping.values()}. Check {self.name} mapping file"
                )
                exit()
            
