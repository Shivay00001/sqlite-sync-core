"""
db - Database layer for sqlite_sync.
"""

from sqlite_sync.db.connection import (
    create_connection,
    execute_in_transaction,
    verify_integrity,
)
from sqlite_sync.db.migrations import (
    initialize_sync_tables,
    get_device_id,
    get_schema_version,
    get_vector_clock,
    update_vector_clock,
)
from sqlite_sync.db.triggers import (
    install_triggers_for_table,
    remove_triggers_for_table,
    has_triggers,
)

__all__ = [
    # connection
    "create_connection",
    "execute_in_transaction",
    "verify_integrity",
    # migrations
    "initialize_sync_tables",
    "get_device_id",
    "get_schema_version",
    "get_vector_clock",
    "update_vector_clock",
    # triggers
    "install_triggers_for_table",
    "remove_triggers_for_table",
    "has_triggers",
]
