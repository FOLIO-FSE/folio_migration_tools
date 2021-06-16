import json
import logging
import logging.handlers
from marc_to_folio.folder_structure import FolderStructure
import os
from abc import abstractmethod


class MainBase:
    @staticmethod
    def setup_logging(folder_structure:FolderStructure=None, debug=False):

        DATA_OUTPUT_LVL_NUM = 25 
        logging.addLevelName(DATA_OUTPUT_LVL_NUM, "DATA_OUTPUT")
        def data_output(self, message, *args, **kws):
            if self.isEnabledFor(DATA_OUTPUT_LVL_NUM):
                # Yes, logger takes its '*args' as 'args'.
                self._log(DATA_OUTPUT_LVL_NUM, message, args, **kws) 
        logging.Logger.data_output = data_output

        logger = logging.getLogger()
        logger.handlers = []
        formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s\t%(filename)s:%(lineno)d")
        stream_handler = logging.StreamHandler()
        stream_handler.addFilter(ExcludeLevelFilter(25))
        
        if debug:
            logger.setLevel(logging.DEBUG)
            stream_handler.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
            stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if folder_structure:
            file_formatter = logging.Formatter("%(message)s")
            file_handler = logging.FileHandler(
                filename=folder_structure.transformation_log_path, 
            )
            stream_handler.addFilter(ExcludeLevelFilter(25))
            # file_handler.addFilter(LevelFilter(0, 20))
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.ERROR)
            logging.getLogger().addHandler(file_handler)

            # Data file formatter
            data_file_formatter = logging.Formatter("%(message)s")
            data_file_handler = logging.FileHandler(
                filename=str(folder_structure.transformation_extra_data_path), 
            )
            data_file_handler.addFilter(LevelFilter(25))
            data_file_handler.setFormatter(data_file_formatter)
            data_file_handler.setLevel(25)
            logging.getLogger().addHandler(data_file_handler)
        logger.info("Logging setup")

class ExcludeLevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno != self.level

class LevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno == self.level
