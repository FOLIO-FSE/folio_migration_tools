from genericpath import isfile
import json
import logging
from pathlib import Path
import os

import requests

class Helper():

    @staticmethod
    def setup_path(path, filename):
        new_path = ""
        try:
            new_path = os.path.join(path, filename)
        except:
            raise Exception(
                f"Something went wrong when joining {path} and {filename} into a path"
            )
        if not isfile(new_path):
            raise Exception(f"No file called {filename} present in {path}")
        return new_path

    @staticmethod
    def write_to_file(file, folio_record, pg_dump = False):
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
            latest_path = (
                f"https://raw.githubusercontent.com/{owner}/{repo}/{latest_tag}/{filepath}"
            )
            logging.info(latest_path)
            req = requests.get(latest_path)
            req.raise_for_status()
            return json.loads(req.text)
        except Exception as ee:
            logging.exception(latest_path)
