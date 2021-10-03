from genericpath import isfile
import json
import logging
from marc_to_folio.custom_exceptions import TransformationProcessError
from pathlib import Path
import os

import requests
import pandas as pd


class Helper:
    @staticmethod
    def flatten_dict(dictionary_to_flatten):
        df = pd.json_normalize(dictionary_to_flatten, sep=".")
        return df.to_dict(orient="records")[0]

    @staticmethod
    def setup_path(path, filename):
        new_path = ""
        try:
            new_path = os.path.join(path, filename)
        except:
            raise TransformationProcessError(
                f"Something went wrong when joining {path} and {filename} into a path"
            )
        if not isfile(new_path):
            raise TransformationProcessError(
                f"No file called {filename} present in {path}"
            )
        return new_path

    @staticmethod
    def write_to_file(file, folio_record, pg_dump=False):
        """Writes record to file. pg_dump=true for importing directly via the
        psql copy command"""
        if pg_dump:
            file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
        else:
            file.write("{}\n".format(json.dumps(folio_record)))

    @staticmethod
    def log_data_issue(index_or_id, message, legacy_value):
        logging.log(26, f"DATA ISSUE\t{index_or_id}\t{message}\t{legacy_value}")

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
        except:
            logging.exception(latest_path)
