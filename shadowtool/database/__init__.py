from shadowtool.logging.manager import LoggingManager

db_logger = LoggingManager().get_logger(
    logger_name='shadowtool-db-logger'
)
