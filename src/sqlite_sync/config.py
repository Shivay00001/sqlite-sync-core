"""
config.py - Configuration constants for sqlite_sync.

All configuration is immutable and defined at module level.
No mutable global state is permitted.
"""

from typing import Final

# Schema version for sync tables
# Increment this when sync table schema changes
SCHEMA_VERSION: Final[int] = 1

# Supported operation types per the spec
OPERATION_TYPES: Final[frozenset[str]] = frozenset({"INSERT", "UPDATE", "DELETE"})

# Maximum bundle size (operations) - applications can enforce smaller limits
MAX_BUNDLE_OPERATIONS: Final[int] = 100_000

# SQLite PRAGMA settings for sync databases
# These ensure durability and consistency
SQLITE_PRAGMAS: Final[dict[str, str]] = {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "foreign_keys": "ON",
    "busy_timeout": "5000",
}

# Reserved table names that cannot have sync enabled
# These are the sync system's own tables
RESERVED_TABLE_NAMES: Final[frozenset[str]] = frozenset(
    {
        "sync_operations",
        "sync_metadata",
        "sync_conflicts",
        "sync_peer_state",
        "sync_import_log",
    }
)

# Metadata keys used in sync_metadata table
METADATA_KEY_DEVICE_ID: Final[str] = "device_id"
METADATA_KEY_SCHEMA_VERSION: Final[str] = "schema_version"
METADATA_KEY_VECTOR_CLOCK: Final[str] = "vector_clock"

# Bundle metadata table name
BUNDLE_METADATA_TABLE: Final[str] = "bundle_metadata"
BUNDLE_OPERATIONS_TABLE: Final[str] = "bundle_operations"
