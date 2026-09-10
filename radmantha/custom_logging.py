"""Opinionated logging configuration, for *applications* to opt into.

Importing radmantha never configures logging: library modules take a plain
``logging.getLogger(__name__)`` and inherit whatever the host application set
up. An application that wants this format calls :func:`setup_logging` from its
own entry point -- never at import time, and never from library code, because
configuring the root logger is the application's decision to make.
"""

import logging.config
import os

LOG_FILE_ENV_VAR = "RADMANTHA_LOG_FILE"
DEFAULT_LOG_FILENAME = "local_log.log"

FORMAT = "%(levelname)s %(asctime)s %(filename)s->%(funcName)s->%(lineno)d : %(message)s"


def get_log_file_path():
    """where the file handler writes.

    resolved against the caller's cwd for the same reason as the spark warehouse
    dir: a __file__-relative default would land inside site-packages once this
    is installed from a wheel.
    """
    return os.environ.get(LOG_FILE_ENV_VAR, os.path.join(os.getcwd(), DEFAULT_LOG_FILENAME))


def build_config():
    """built per call, not a module constant: get_log_file_path() reads the
    environment, and at module-import time the consumer hasn't set it yet."""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": FORMAT,
                "datefmt": "%y/%m/%d %H:%M:%S",
            }
        },
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
            "stderr": {
                "class": "logging.StreamHandler",
                "level": "ERROR",
                "formatter": "simple",
                "stream": "ext://sys.stderr",
            },
            "file": {
                "class": "logging.FileHandler",
                "formatter": "simple",
                "filename": get_log_file_path(),
                "mode": "a",
            },
        },
        "root": {"level": "DEBUG", "handlers": ["stderr", "stdout", "file"]},
    }


def is_logging_configured():
    # Check if the root logger has any handlers explicitly assigned
    return len(logging.getLogger().handlers) >= 3


def setup_logging():
    """configure the root logger with our handlers; safe to call more than once.

    for application entry points only -- see the module docstring.
    """
    if not is_logging_configured():
        logging.config.dictConfig(build_config())
        logging.getLogger("py4j").setLevel(logging.ERROR)
    return logging
