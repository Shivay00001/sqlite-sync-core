"""
connection.py - SQLite database connection management.

Handles connection creation, PRAGMA configuration, and
registration of application-defined SQL functions.

All connections use WAL mode for concurrent read/write.
"""

import logging
import sqlite3
import json
import time

logger = logging.getLogger("sqlite_sync.db")

from typing import Callable, Any
from sqlite_sync.config import SQLITE_PRAGMAS
from sqlite_sync.errors import DatabaseError


def create_connection(db_path: str) -> sqlite3.Connection:
    """
    Create a new SQLite connection with proper configuration.
    
    Applies all required PRAGMA settings for sync operation.
    Registers application-defined functions for trigger use.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Configured sqlite3.Connection
        
    Raises:
        DatabaseError: If connection fails
    """
    try:
        conn = sqlite3.connect(
            db_path,
            isolation_level=None,  # Manual transaction control
            check_same_thread=False,
        )
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to connect to database: {e}",
            operation="connect",
        ) from e

    # Apply PRAGMA settings
    _apply_pragmas(conn)

    # Register custom SQL functions
    _register_functions(conn)

    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """
    Apply all required PRAGMA settings.
    
    Args:
        conn: SQLite connection
    """
    for pragma, value in SQLITE_PRAGMAS.items():
        try:
            conn.execute(f"PRAGMA {pragma} = {value}")
        except sqlite3.Error as e:
            raise DatabaseError(
                f"Failed to set PRAGMA {pragma}: {e}",
                operation="pragma",
                sql=f"PRAGMA {pragma} = {value}",
            ) from e


def _register_functions(conn: sqlite3.Connection) -> None:
    """
    Register application-defined SQL functions.
    
    These functions are used by triggers to:
    - Generate UUID v7 for operation IDs
    - Increment vector clocks
    - Serialize values to MessagePack
    
    Args:
        conn: SQLite connection
    """
    from sqlite_sync.utils.uuid7 import generate_uuid_v7
    from sqlite_sync.log.vector_clock import increment_vector_clock, merge_vector_clocks
    from sqlite_sync.utils.msgpack_codec import pack_primary_key, pack_dict
    import json
    import sys

    # sync_uuid_v7() -> BLOB(16)
    def _uuid_v7() -> bytes:
        return generate_uuid_v7()

    conn.create_function("sync_uuid_v7", 0, _uuid_v7)

    # sync_vector_clock_increment(device_id BLOB, vc_json TEXT) -> TEXT
    def _vc_increment(device_id: Any, vc_json: Any) -> str:
        return increment_vector_clock(device_id, vc_json)

    conn.create_function("sync_vector_clock_increment", 2, _vc_increment)

    # sync_vector_clock_merge(vc1_json TEXT, vc2_json TEXT) -> TEXT
    def _vc_merge(vc1_json: Any, vc2_json: Any) -> str:
        return merge_vector_clocks(vc1_json, vc2_json)

    conn.create_function("sync_vector_clock_merge", 2, _vc_merge)

    # sync_pack_pk(value) -> BLOB
    def _pack_pk(value: Any) -> bytes:
        return pack_primary_key(value)

    conn.create_function("sync_pack_pk", 1, _pack_pk)

    # sync_pack_values(json_str TEXT) -> BLOB
    def _pack_values(json_str: Any) -> bytes:
        if not json_str: return b""
        data = json.loads(json_str)
        return pack_dict(data)

    conn.create_function("sync_pack_values", 1, _pack_values)

    # sync_hlc_now(node_id TEXT) -> TEXT
    def _hlc_now_placeholder(node_id: Any) -> str:
        return f"{int(time.time() * 1000)}:0:{node_id}"

    conn.create_function("sync_hlc_now", 1, _hlc_now_placeholder)

    if not hasattr(_register_functions, "_disabled_state"):
        _register_functions._disabled_state = {}

    def _sync_is_disabled() -> int:
        return _register_functions._disabled_state.get(id(conn), 0)

    conn.create_function("sync_is_disabled", 0, _sync_is_disabled)


def set_sync_disabled(conn: sqlite3.Connection, disabled: bool) -> None:
    """Set the sync-disabled state for a specific connection."""
    val = 1 if disabled else 0
    if not hasattr(_register_functions, "_disabled_state"):
        _register_functions._disabled_state = {}
    _register_functions._disabled_state[id(conn)] = val


def execute_in_transaction(
    conn: sqlite3.Connection, 
    operation: Callable[[sqlite3.Connection], Any]
) -> Any:
    """
    Execute an operation within an EXCLUSIVE transaction.
    
    Ensures atomicity: all changes commit or all rollback.
    
    Args:
        conn: SQLite connection
        operation: Callable that performs database operations
        
    Returns:
        Result of operation
        
    Raises:
        DatabaseError: If transaction fails
    """
    try:
        conn.execute("BEGIN EXCLUSIVE")
        try:
            result = operation(conn)
            conn.execute("COMMIT")
            return result
        except Exception:
            conn.execute("ROLLBACK")
            raise
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Transaction failed: {e}",
            operation="transaction",
        ) from e


def verify_integrity(conn: sqlite3.Connection) -> bool:
    """
    Run SQLite integrity check.
    
    Args:
        conn: SQLite connection
        
    Returns:
        True if database is healthy
    """
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()
        return result is not None and result[0] == "ok"
    except sqlite3.Error:
        return False
