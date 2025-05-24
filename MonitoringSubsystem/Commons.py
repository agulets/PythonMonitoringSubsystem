import os
import sys
import logging
import threading
from multiprocessing import current_process
from logging.handlers import RotatingFileHandler


def get_logger_by_params_and_make_log_folder(log_name=None,
                                             log_dir=None,
                                             log_file_name=None,
                                             log_size=104857608,
                                             log_file_count=2,
                                             log_level=20,
                                             formatter=None,
                                             console_handler=True
                                             ):
    log_format = formatter or ('%(asctime)s\t%(levelname)s\t%(processName)s:' '%(funcName)s:%(lineno)d:\t%(message)s')
    log_formatter = logging.Formatter(log_format)

    if log_name is None:
        process_name = current_process().name
        thread_name = threading.current_thread().name
        logger_name = f"{process_name}__{thread_name}"
    else:
        logger_name = log_name

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    if log_dir is not None or log_file_name is not None:
        final_log_dir = log_dir or os.path.join(os.getcwd(), "LOGS")
        final_log_name = log_file_name or logger_name

        os.makedirs(final_log_dir, exist_ok=True)

        log_filename = os.path.join(final_log_dir, f"{final_log_name}.log")
        file_handler = RotatingFileHandler(
            filename=log_filename,
            maxBytes=log_size,
            backupCount=log_file_count
        )
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    if console_handler:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

    return logger
