import json
import logging
import os
from pathlib import Path
from typing import List

from folio_migration_tools.custom_exceptions import TransformationProcessError


class ExtradataWriter:
    __instance = None
    __inited = False

    def __new__(cls, path_to_file: Path) -> "ExtradataWriter":
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self, path_to_file: Path) -> None:
        if type(self).__inited:
            return
        self.cache: List[str] = []
        self.path_to_file: Path = path_to_file
        if self.path_to_file.is_file():
            os.remove(self.path_to_file)
        type(self).__inited = True

    def write(self, record_type: str, data_to_write: dict, flush=False):
        try:
            if data_to_write:
                self.cache.append(f"{record_type}\t{json.dumps(data_to_write)}\n")
            if len(self.cache) > 1000 or flush:
                with open(self.path_to_file, "a") as extradata_file:
                    extradata_file.writelines(self.cache)
                    self.cache = []
                    logging.debug("Extradata writer flushing the cache")
        except Exception as ee:
            error_message = "Something went wrong in extradata Writer"
            logging.error(error_message)
            raise TransformationProcessError("", error_message, record_type) from ee

    def flush(self):
        self.write("", {}, True)
        if self.path_to_file.is_file() and os.stat(self.path_to_file).st_size == 0:
            logging.info("Removing extradata file since it is empty")
            os.remove(self.path_to_file)
