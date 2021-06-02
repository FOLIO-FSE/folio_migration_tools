import json
import logging
import logging.handlers
import os
from abc import abstractmethod


class MainBase:
    @staticmethod
    def setup_logging(log_file_path=None, debug=False):

        DATA_OUTPUT_LVL_NUM = 25 
        logging.addLevelName(DATA_OUTPUT_LVL_NUM, "DATA_OUTPUT")
        def data_output(self, message, *args, **kws):
            if self.isEnabledFor(DATA_OUTPUT_LVL_NUM):
                # Yes, logger takes its '*args' as 'args'.
                self._log(DATA_OUTPUT_LVL_NUM, message, args, **kws) 
        logging.Logger.data_output = data_output

        logger = logging.getLogger()
        logger.handlers = []
        formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
        stream_handler = logging.StreamHandler()
        
        if debug:
            logger.setLevel(logging.DEBUG)
            stream_handler.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
            stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if log_file_path:
            file_formatter = logging.Formatter("%(message)s")
            file_handler = logging.FileHandler(
                filename=log_file_path, 
            )
            # file_handler.addFilter(LevelFilter(0, 20))
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.ERROR)
            logging.getLogger().addHandler(file_handler)

            # Data file formatter
            data_file_formatter = logging.Formatter("%(message)s")
            data_file_handler = logging.FileHandler(
                filename=log_file_path.replace(".log",".data"), 
            )
            data_file_handler.addFilter(LevelFilter(25))
            data_file_handler.setFormatter(data_file_formatter)
            data_file_handler.setLevel(25)
            logging.getLogger().addHandler(data_file_handler)
        logger.info("Logging setup")


class LevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno == self.level
