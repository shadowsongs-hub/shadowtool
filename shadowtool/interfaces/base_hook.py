from dataclasses import dataclass
from abc import ABC


class BaseHook(ABC):
    """
    Base class for Hooks.

    Hooks are defined as a manager class manages the interaction of an external system.
    """
    ...
