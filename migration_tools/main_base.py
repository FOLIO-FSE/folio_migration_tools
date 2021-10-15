import logging
import logging.handlers
import sys
import time

from migration_tools.custom_exceptions import TransformationRecordFailedError
from migration_tools.folder_structure import FolderStructure


class MainBase:
    def __init__(self) -> None:
        self.num_exeptions = 0

    def log_and_exit_if_too_many_errors(
        self, error: TransformationRecordFailedError, idx
    ):
        self.num_exeptions += 1
        error.log_it()
        if self.num_exeptions / (1 + idx) > 0.2 and self.num_exeptions > 5000:
            logging.fatal(
                f"Number of exceptions exceeded limit of "
                f"{self.num_exeptions}. and failure rate is "
                f"{self.num_exeptions / (1 + idx)} Stopping."
            )
            sys.exit()

    @staticmethod
    def print_progress(num_processed, start_time):
        if num_processed > 1 and num_processed % 10000 == 0:
            elapsed = num_processed / (time.time() - start_time)
            elapsed_formatted = "{0:.4g}".format(elapsed)
            logging.info(
                f"{num_processed:,} records processed. Recs/sec: {elapsed_formatted} "
            )

    @staticmethod
    def setup_logging(folder_structure: FolderStructure = None, debug=False):
        DATA_OUTPUT_LVL_NUM = 25
        logging.addLevelName(DATA_OUTPUT_LVL_NUM, "DATA_OUTPUT")

        def data_output(self, message, *args, **kws):
            if self.isEnabledFor(DATA_OUTPUT_LVL_NUM):
                # Yes, logger takes its '*args' as 'args'.
                self._log(DATA_OUTPUT_LVL_NUM, message, args, **kws)

        logging.Logger.data_output = data_output

        DATA_ISSUE_LVL_NUM = 26
        logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")

        def data_issues(self, message, *args, **kws):
            if self.isEnabledFor(DATA_ISSUE_LVL_NUM):
                # Yes, logger takes its '*args' as 'args'.
                self._log(DATA_ISSUE_LVL_NUM, message, args, **kws)

        logging.Logger.data_issues = data_issues

        logger = logging.getLogger()
        logger.handlers = []
        formatter = logging.Formatter(
            "%(asctime)s\t%(levelname)s\t%(message)s\t%(filename)s:%(lineno)d"
        )
        stream_handler = logging.StreamHandler()
        stream_handler.addFilter(ExcludeLevelFilter(25))
        stream_handler.addFilter(ExcludeLevelFilter(26))

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
            file_handler.addFilter(ExcludeLevelFilter(25))
            file_handler.addFilter(ExcludeLevelFilter(26))
            # file_handler.addFilter(LevelFilter(0, 20))
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.INFO)
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

            # Data issue file formatter
            data_issue_file_formatter = logging.Formatter("%(message)s")
            data_issue_file_handler = logging.FileHandler(
                filename=str(folder_structure.data_issue_file_path),
            )
            data_issue_file_handler.addFilter(LevelFilter(26))
            data_issue_file_handler.setFormatter(data_issue_file_formatter)
            data_issue_file_handler.setLevel(26)
            logging.getLogger().addHandler(data_issue_file_handler)
        logger.info("Logging setup")


class ExcludeLevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno != self.level


class LevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno == self.level
