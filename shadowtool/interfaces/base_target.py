from shadowtool.main.general.logging_utils import LoggingMixin
from dataclasses import dataclass


@dataclass
class BaseTarget(LoggingMixin):

    """

    Base class for a target

    A target is defined as the destination of a ETL process, typically your data lake/warehouse.

    """
    pass

