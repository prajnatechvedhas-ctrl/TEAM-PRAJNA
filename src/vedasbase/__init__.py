"""
Vedas Base Embedded Key-Value Database.
"""

from .engine import VedasDB, open_db
from .errors import (
    VedasError,
    InvalidInputError,
    DiskFullError,
    CorruptionError,
    LockError,
    ClosedError,
)
from .format import (
    TYPE_PUT,
    TYPE_DELETE,
    TYPE_COMMIT,
)

# Export standard API
open = open_db

__all__ = [
    "open",
    "open_db",
    "VedasDB",
    "VedasError",
    "InvalidInputError",
    "DiskFullError",
    "CorruptionError",
    "LockError",
    "ClosedError",
    "TYPE_PUT",
    "TYPE_DELETE",
    "TYPE_COMMIT",
]

