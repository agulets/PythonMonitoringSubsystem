import os
import sys
import threading
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


def get_logger_by_params_and_make_log_folder(
    log_name=None,
    log_dir=None,
    log_file_name=None,
    log_size=104857608,
    log_file_count=2,
    log_level=20,
    formatter=None,
    console_handler=True
):
    # Настройка форматера
    log_format = formatter or ('%(asctime)s\t%(levelname)s\t%(processName)s:' '%(funcName)s:%(lineno)d:\t%(message)s')
    log_formatter = logging.Formatter(log_format)

    # Определение имени логгера
    if log_name is None:
        process_name = current_process().name
        thread_name = threading.current_thread().name
        logger_name = f"{process_name}__{thread_name}"
    else:
        logger_name = log_name

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Настройка файлового обработчика при наличии параметров для записи в файл
    if log_dir is not None or log_file_name is not None:
        # Определение пути для логирования
        final_log_dir = log_dir or os.path.join(os.getcwd(), "LOGS")
        final_log_name = log_file_name or logger_name

        # Создание директории при необходимости
        os.makedirs(final_log_dir, exist_ok=True)

        # Создание и настройка файлового обработчика
        log_filename = os.path.join(final_log_dir, f"{final_log_name}.log")
        file_handler = RotatingFileHandler(
            filename=log_filename,
            maxBytes=log_size,
            backupCount=log_file_count
        )
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    # Добавление консольного обработчика по требованию
    if console_handler:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

    return logger
