import os
import sys
import time
import logging
from multiprocessing import current_process
from logging.handlers import RotatingFileHandler


class time_it:
    def __init__(self, _round=3):
        self.start_time = None
        self.end_time = None
        self.execution_time = None
        self._round = _round

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, v1, v2, v3):
        self.end_time = time.time()
        self.execution_time = round(self.end_time - self.start_time, self._round)


def get_logger_by_params_and_make_log_folder(log_name=None,
                                             log_dir=None,
                                             log_file_name=None,
                                             log_size=104857608,
                                             log_file_count=2,
                                             log_level=20,
                                             formatter=None,
                                             console_handler=True):
    formatter = formatter if formatter else '%(asctime)s\t%(levelname)s\t%(processName)s:%(funcName)s:%(lineno)d:\t%(message)s'
    log_formatter = logging.Formatter(formatter)

    if log_name:
        log_dir = log_dir if log_dir else os.path.join(os.path.dirname(__file__), 'LOGS')
        log_file_name = log_file_name if log_file_name else f"{current_process().name}"

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_filename = f"{os.path.join(log_dir, log_file_name)}.log"
        log_file_handler = RotatingFileHandler(filename=log_filename, maxBytes=log_size, backupCount=log_file_count)
        log_file_handler.setFormatter(log_formatter)

        logger = logging.getLogger(log_name)
        logger.addHandler(log_file_handler)
    else:
        logger = logging.getLogger('root')

    logger.setLevel(log_level)

    if console_handler:
        log_console_handler = logging.StreamHandler(sys.stdout)
        log_console_handler.setFormatter(log_formatter)
        logger.addHandler(log_console_handler)

    return logger
