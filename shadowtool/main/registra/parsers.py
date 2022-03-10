from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional, List, Union, Any


@dataclass
class BaseRegistraParser(ABC):

    file_name: str
    raw_yaml_dict: Dict[str, Any]

    @abstractmethod
    def parse(self) -> models.BaseTableRegistraTemplate:
        """return the parsed object of table level registra"""
        ...
