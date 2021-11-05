import logging
import os
from logging.handlers import RotatingFileHandler
from os.path import expanduser

from config import LOG_LEVEL


def setup_logger(log_level=None, filename='luquinhas'):  # noqa WPS210,WPS213
    """Logger setup."""
    if not log_level:
        log_level = LOG_LEVEL
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)

    dir_path = os.path.join(expanduser('./'), 'log')
    log_file_path = os.path.join(dir_path, f'{filename}_logs.log')
    report_file_path = os.path.join(dir_path, f'{filename}_report.log')

    report_file_handler = logging.FileHandler(report_file_path, mode='w', encoding='utf-8')
    report_file_handler.setLevel(logging.DEBUG)

    log_file_handler = RotatingFileHandler(
        log_file_path,
        mode='a',
        maxBytes=5 * 1024 * 1024,
        backupCount=2,
        encoding='utf-8',
    )
    log_file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    log_format = '[%(asctime)s] %(name)s (%(filename)s:%(lineno)s) %(levelname)s: %(message)s'  # noqa WPS323
    formatter = logging.Formatter(log_format)
    log_file_handler.setFormatter(formatter)
    report_file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(log_file_handler)
    logger.addHandler(report_file_handler)
    logger.addHandler(console_handler)

    url_logger = logging.getLogger('urllib3')
    url_logger.setLevel(max(logger.level, logging.INFO))

    websockets_logger = logging.getLogger('websockets')
    websockets_logger.setLevel(max(logger.level, logging.INFO))