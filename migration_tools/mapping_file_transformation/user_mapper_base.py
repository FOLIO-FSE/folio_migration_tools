import collections
import json
import uuid
from abc import abstractmethod
from typing import Dict

import requests
from folioclient import FolioClient


class UserMapperBase:
    def __init__(self, folio_client: FolioClient):
        self.stats = {}
        self.legacy_id_map: Dict[str, str] = {}

        self.migration_report = {}
        self.folio_client = folio_client
        self.mapped_folio_fields = {}
        self.ref_data_dicts = {}
        self.mapped_legacy_fields = {}
        self.user_schema = self.get_user_schema()

    def print_mapping_report(self, total_records):
        print("\n## Mapped FOLIO fields")
        d_sorted = {
            k: self.mapped_folio_fields[k] for k in sorted(self.mapped_folio_fields)
        }
        print("FOLIO Field | % | Has value")
        print("--- | --- | --- :")
        for k, v in d_sorted.items():
            mp = v / total_records
            mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
            print(f"{k} | {mapped_per} | {v} ")
        print("\n## Mapped Legacy fields")
        d_sorted = {
            k: self.mapped_legacy_fields[k] for k in sorted(self.mapped_legacy_fields)
        }
        print("Legacy Field | % | Has Value")
        print("--- | --- | --- :")
        for k, v in d_sorted.items():
            mp = v / total_records
            mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
            print(f"{k} | {mapped_per} | {v}")

    def report_legacy_mapping(self, legacy_object):
        for field_name, value in legacy_object.items():
            v = 1 if value else 0
            if field_name not in self.mapped_legacy_fields:
                self.mapped_legacy_fields[field_name] = v
            else:
                self.mapped_legacy_fields[field_name] += v

    def report_folio_mapping(self, folio_object):
        flat_object = flatten(folio_object)
        for field_name, value in flat_object.items():
            v = 1 if value else 0
            if field_name not in self.mapped_folio_fields:
                self.mapped_folio_fields[field_name] = v
            else:
                self.mapped_folio_fields[field_name] += v

    def instantiate_user(self):
        user_id = str(uuid.uuid4())
        folio_user = {
            "metadata": self.folio_client.get_metadata_construct(),
            "id": user_id,
            "type": "object",
            "personal": {},
            "customFields": {},
        }
        return folio_user

    def validate(self, folio_user):
        failures = []
        self.add_to_migration_report(
            "Number of addresses per user",
            len(folio_user["personal"].get("addresses", [])),
        )
        req_fields = ["username", "email", "active"]
        for req in req_fields:
            if req not in folio_user:
                failures.append(req)
                self.add_to_migration_report(
                    "Failed records that needs to get fixed",
                    f"Required field {req} is missing from {folio_user['username']}",
                )
        if not folio_user["personal"].get("lastName", ""):
            failures.append("lastName")
            self.add_to_migration_report(
                "Failed records that needs to get fixed",
                f"Required field personal.lastName is missing from {folio_user['username']}",
            )
        if len(failures) > 0:
            self.add_to_migration_report("User validation", "Total failed users")
            for failure in failures:
                self.add_to_migration_report("User validation", f"{failure}")
            raise ValueError(
                f"Record {folio_user['username']} failed validation {failures}"
            )

    def write_migration_report(self, other_report=None):
        for a in self.migration_report:
            print("")
            print(f"## {a} - {len(self.migration_report[a])} things")
            print("Measure | Count")
            print("--- | ---:")
            b = self.migration_report[a]
            sortedlist = [(k, b[k]) for k in sorted(b, key=as_str)]
            for b in sortedlist:
                print(f"{b[0]} | {b[1]}")

    def save_migration_report_to_disk(self, file_path, total_records):
        with open(file_path, "w+") as report_file:
            for a in self.migration_report:
                report_file.write("\n")
                report_file.write(f"## {a} - {len(self.migration_report[a])} things\n")
                report_file.write("Measure | Count\n")
                report_file.write("--- | ---:\n")
                b = self.migration_report[a]
                sortedlist = [(k, b[k]) for k in sorted(b, key=as_str)]
                for b in sortedlist:
                    report_file.write(f"{b[0]} | {b[1]}\n")
            report_file.write("\n## Mapped FOLIO fields\n")
            d_sorted = {
                k: self.mapped_folio_fields[k] for k in sorted(self.mapped_folio_fields)
            }
            report_file.write("FOLIO Field | % | Has Value\n")
            report_file.write("--- | --- | --- | ---:\n")
            for k, v in d_sorted.items():
                mp = v / total_records
                mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
                report_file.write(f"{k} | {mapped_per} | {v} \n")
            report_file.write("\n## Mapped Legacy fields\n")
            d_sorted = {
                k: self.mapped_legacy_fields[k]
                for k in sorted(self.mapped_legacy_fields)
            }
            report_file.write("Legacy Field | % | Has Value\n")
            report_file.write("--- | --- | --- | ---:\n")
            for k, v in d_sorted.items():
                mp = v / total_records
                mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
                report_file.write(f"{k} | {mapped_per} | {v}\n")

    @staticmethod
    def print_dict_to_md_table(my_dict, h1="", h2=""):
        d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
        print(f"{h1} | {h2}")
        print("--- | ---:")
        for k, v in d_sorted.items():
            print(f"{k} | {v}")

    def add_to_migration_report(self, header, measure_to_add):
        if header not in self.migration_report:
            self.migration_report[header] = {}
        if measure_to_add not in self.migration_report[header]:
            self.migration_report[header][measure_to_add] = 1
        else:
            self.migration_report[header][measure_to_add] += 1

    def get_ref_data_tuple_by_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        ref_object = self.ref_data_dicts.get(dict_key, {}).get(
            key_value.lower().strip(), ()
        )
        # logging.info(f"{key_value} - {ref_object} - {dict_key}")
        if ref_object:
            return ref_object
        else:
            d = {}
            for r in ref_data:
                d[r[key_type].lower()] = (r["id"], r["name"])
            self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower().strip(), ())

    @abstractmethod
    def do_map(self, legacy_user, object_map):
        raise NotImplementedError

    @abstractmethod
    def get_users(self, source_file, file_format: str):
        raise NotImplementedError

    @staticmethod
    def get_user_schema():
        url = "https://raw.githubusercontent.com/folio-org/mod-users/master/ramls/userdata.json"
        req = requests.get(url)
        return json.loads(req.text)

    @staticmethod
    def get_latest_from_github(owner, repo, filepath):
        """[gets the a json file from Github tied to the latest release]
        Args:
            owner (): [the owner (user or organization) of the repo]
            repo (): [the name of the repository]
            filepath (): [the local path to the file you want to download]
        """
        latest_path = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
        req = requests.get(latest_path)
        req.raise_for_status()
        latest = json.loads(req.text)
        # print(json.dumps(latest, indent=4))
        latest_tag = latest["tag_name"]
        latest_path = (
            f"https://raw.githubusercontent.com/{owner}/{repo}/{latest_tag}/{filepath}"
        )
        # print(latest_path)
        req = requests.get(latest_path)
        req.raise_for_status()
        return json.loads(req.text)


def flatten(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
