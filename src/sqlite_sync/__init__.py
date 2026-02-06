"""
sqlite_sync - Universal SQLite Synchronization Core

A dependency-grade, local-first, offline-first SQLite synchronization primitive.
"""

from sqlite_sync.engine import SyncEngine
from sqlite_sync.errors import (
    SyncError,
    SchemaError,
    BundleError,
    ConflictError,
    InvariantViolationError,
)

__version__ = "0.1.0"
__all__ = [
    "SyncEngine",
    "SyncError",
    "SchemaError",
    "BundleError",
    "ConflictError",
    "InvariantViolationError",
]
