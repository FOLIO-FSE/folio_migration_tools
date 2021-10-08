import json
import logging
import os

import pandas as pd
import requests
from genericpath import isfile

from migration_tools.custom_exceptions import TransformationProcessError
from migration_tools.report_blurbs import Blurbs


class Helper:
    @staticmethod
    def print_dict_to_md_table(my_dict, report_file, h1="Measure", h2="Number"):
        # TODO: Move to interface or parent class
        d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
        report_file.write(f"{h1} | {h2}   \n")
        report_file.write("--- | ---:   \n")
        for k, v in d_sorted.items():
            report_file.write(f"{k} | {v:,}   \n")

    @staticmethod
    def print_mapping_report(
        report_file, total_records: int, mapped_folio_fields, mapped_legacy_fields
    ):

        report_file.write("\n## Mapped FOLIO fields\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_folio_fields[k] for k in sorted(mapped_folio_fields)}
        report_file.write(
            "<details><summary>Click to expand field report</summary>     \n\n"
        )

        report_file.write("FOLIO Field | Mapped | Unmapped  \n")
        report_file.write("--- | --- | ---:  \n")
        for k, v in d_sorted.items():
            unmapped = total_records - v[0]
            mapped = v[0]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            report_file.write(
                f"{k} | {(mapped if mapped > 0 else 0):,} ({mapped_per}) | {unmapped:,}  \n"
            )
        report_file.write("</details>   \n")

        report_file.write("\n## Mapped Legacy fields\n")
        # report_file.write(f"{blurbs[header]}\n")

        d_sorted = {k: mapped_legacy_fields[k] for k in sorted(mapped_legacy_fields)}
        report_file.write(
            "<details><summary>Click to expand field report</summary>     \n\n"
        )

        report_file.write("Legacy Field | Present | Mapped | Unmapped  \n")
        report_file.write("--- | --- | --- | ---:  \n")
        for k, v in d_sorted.items():
            present = v[0]
            present_per = "{:.1%}".format(
                present / total_records if total_records else 0
            )
            unmapped = present - v[1]
            mapped = v[1]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            report_file.write(
                f"{k} | {(present if present > 0 else 0):,} ({present_per}) | {(mapped if mapped > 0 else 0):,} ({mapped_per}) | {unmapped:,}  \n"
            )
        report_file.write("</details>   \n")

    @staticmethod
    def write_migration_report(report_file, migration_report):
        """Writes the migration report, including section headers, section blurbs, and values."""
        report_file.write(f"{Blurbs.Introduction}\n")

        for a in migration_report:
            blurb = migration_report[a].get("blurb_tuple")
            report_file.write("   \n")
            report_file.write(f"## {blurb[0]}    \n")
            report_file.write(f"{blurb[1]}    \n")
            report_file.write(
                f"<details><summary>Click to expand all {len(migration_report[a])} things</summary>     \n"
            )
            report_file.write(f"   \n")
            report_file.write("Measure | Count   \n")
            report_file.write("--- | ---:   \n")
            b = migration_report[a]
            sortedlist = [
                (k, b[k]) for k in sorted(b, key=as_str) if k != "blurb_tuple"
            ]
            for b in sortedlist:
                report_file.write(f"{b[0]} | {b[1]}   \n")
            report_file.write("</details>   \n")

    @staticmethod
    def flatten_dict(dictionary_to_flatten):
        df = pd.json_normalize(dictionary_to_flatten, sep=".")
        return df.to_dict(orient="records")[0]

    @staticmethod
    def setup_path(path, filename):
        new_path = ""
        try:
            new_path = os.path.join(path, filename)
        except Exception:
            raise TransformationProcessError(
                f"Something went wrong when joining {path} and {filename} into a path"
            )
        if not isfile(new_path):
            raise TransformationProcessError(
                f"No file called {filename} present in {path}"
            )
        return new_path

    @staticmethod
    def log_data_issue(index_or_id, message, legacy_value):
        logging.log(26, f"DATA ISSUE\t{index_or_id}\t{message}\t{legacy_value}")

    @staticmethod
    def write_to_file(file, folio_record, pg_dump=False):
        """Writes record to file. pg_dump=true for importing directly via the
        psql copy command"""
        if pg_dump:
            file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
        else:
            file.write("{}\n".format(json.dumps(folio_record)))

    @staticmethod
    def get_latest_from_github(owner, repo, filepath):

        """[gets the a json file from Github tied to the latest release]
        Args:
            owner (): [the owner (user or organization) of the repo]
            repo (): [the name of the repository]
            filepath (): [the local path to the file you want to download]
        """
        try:
            latest_path = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
            req = requests.get(latest_path)
            req.raise_for_status()
            latest = json.loads(req.text)
            # print(json.dumps(latest, indent=4))
            latest_tag = latest["tag_name"]
            logging.info(f"Latest tag of {repo} is {latest_tag}")
            latest_path = f"https://raw.githubusercontent.com/{owner}/{repo}/{latest_tag}/{filepath}"
            logging.info(latest_path)
            req = requests.get(latest_path)
            req.raise_for_status()
            return json.loads(req.text)
        except Exception:
            logging.exception(latest_path)


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
