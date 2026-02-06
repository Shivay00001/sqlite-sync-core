"""
change_capture.py - Change capture abstraction.

This module provides the interface for registering user tables
for sync and describes how changes are captured via triggers.

Note: The actual capture happens via SQLite triggers installed
by db/triggers.py. This module provides the high-level API.
"""

import sqlite3
from typing import Iterator

from sqlite_sync.db.triggers import (
    install_triggers_for_table,
    remove_triggers_for_table,
    has_triggers,
)
from sqlite_sync.errors import ValidationError


def enable_change_capture(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Enable change capture for a user table.
    
    Installs INSERT, UPDATE, DELETE triggers that atomically
    record all mutations to sync_operations.
    
    After calling this, any write to the table will:
    1. Execute the user's write
    2. Create a sync operation (in same transaction)
    3. Increment the vector clock
    
    Args:
        conn: SQLite connection
        table_name: Name of user table to capture
        
    Raises:
        ValidationError: If table doesn't exist or has no primary key
    """
    install_triggers_for_table(conn, table_name)


def disable_change_capture(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Disable change capture for a user table.
    
    Removes sync triggers. Future writes will not be recorded.
    Does NOT delete existing operations from sync_operations.
    
    Args:
        conn: SQLite connection
        table_name: Table to stop capturing
    """
    remove_triggers_for_table(conn, table_name)


def is_capture_enabled(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Check if change capture is enabled for a table.
    
    Args:
        conn: SQLite connection
        table_name: Table to check
        
    Returns:
        True if all three triggers exist
    """
    return has_triggers(conn, table_name)


def get_captured_tables(conn: sqlite3.Connection) -> list[str]:
    """
    Get list of tables with change capture enabled.
    
    Args:
        conn: SQLite connection
        
    Returns:
        List of table names with sync triggers installed
    """
    cursor = conn.execute(
        """
        SELECT DISTINCT REPLACE(name, '_sync_insert', '')
        FROM sqlite_master 
        WHERE type = 'trigger' 
        AND name LIKE '%_sync_insert'
        """
    )
    return [row[0] for row in cursor.fetchall()]
