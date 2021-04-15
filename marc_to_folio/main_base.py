import logging
import logging.handlers
import os
from abc import abstractmethod


class MainBase:
    @staticmethod
    def setup_logging(log_file_path=None):        
        logger = logging.getLogger()
        logger.handlers = []
        formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
        logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if log_file_path:
            file_formatter = logging.Formatter("%(message)s")
            file_handler = logging.FileHandler(
                filename=log_file_path
            )
            # file_handler.addFilter(LevelFilter(0, 20))
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.ERROR)
            logging.getLogger().addHandler(file_handler)
        logger.info("Logging setup")