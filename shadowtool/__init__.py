from shadowtool.logging.manager import LoggingManager

global_logger = LoggingManager().get_logger(
    logger_name='shadowtool-logger'
)

__version__ = '0.1.2'
