"""FOLIO Migration Tools package for migrating library data to FOLIO LSP."""

import importlib.metadata
from typing import Protocol

__version__ = importlib.metadata.version("folio_migration_tools")


class StrCoercible(Protocol):
    """Protocol for objects that can be coerced to string."""

    def __repr__(self) -> str:
        """Return repr(self)."""
        ...

    def __str__(self) -> str:
        """Return str(self)."""
        ...
