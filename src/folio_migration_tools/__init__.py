import importlib.metadata
from typing import Protocol

__version__ = importlib.metadata.version("folio_migration_tools")

class StrCoercible(Protocol):
    def __repr__(self) -> str:
        ...

    def __str__(self) -> str:
        ...
