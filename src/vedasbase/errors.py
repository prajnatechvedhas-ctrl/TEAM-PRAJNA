"""
Vedas Base - Error class hierarchy.
"""

class VedasError(Exception):
    """Base exception for all Vedas Base errors."""
    pass

class InvalidInputError(VedasError, ValueError):
    """Raised when an invalid key, value, batch, or parameter is provided."""
    pass

class DiskFullError(VedasError, OSError):
    """Raised when writing an operation or batch would exceed max_bytes."""
    pass

class CorruptionError(VedasError):
    """Raised when a file header, record CRC, or metadata is corrupted."""
    pass

class LockError(VedasError):
    """Raised when the database file is already locked by another process."""
    pass

class ClosedError(VedasError):
    """Raised when an operation is attempted on a closed database instance."""
    pass

