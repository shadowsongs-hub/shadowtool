import logging
import os

import shadowtool.config as config
from shadowtool.__version__ import VERSION


# logger
def init_global_logger(level: str) -> logging.Logger:
    new_logger = logging.getLogger(__name__)
    sh = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(name)-20s] [%(levelname)-8s] -- %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh.setFormatter(formatter)
    new_logger.addHandler(sh)
    new_logger.setLevel(level)
    return new_logger


# TODO: load config


logger = init_global_logger(
    os.getenv(config.ST__LOG_LEVEL, "INFO")
)

logger.debug("Logger initialised. ")


# version announcement
logger.debug(f"Loading xenpy library. Version: v{VERSION}")

__version__ = '0.0.0'
