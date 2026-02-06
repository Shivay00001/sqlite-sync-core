"""
migrations.py - Database initialization and schema management.

Handles creation of sync tables and initialization of metadata.
"""

import sqlite3
import json
from typing import Final

from sqlite_sync.db.schema import ALL_SCHEMA_STATEMENTS
from sqlite_sync.errors import DatabaseError, SchemaError
from sqlite_sync.config import (
    SCHEMA_VERSION,
    METADATA_KEY_DEVICE_ID,
    METADATA_KEY_SCHEMA_VERSION,
    METADATA_KEY_VECTOR_CLOCK,
)
from sqlite_sync.utils.uuid7 import generate_uuid_v7


def initialize_sync_tables(conn: sqlite3.Connection) -> bytes:
    """
    Create all sync tables and initialize metadata.
    
    This is idempotent: can be called multiple times safely.
    If already initialized, returns the existing device ID.
    
    Args:
        conn: SQLite connection
        
    Returns:
        Device ID (16-byte UUID)
        
    Raises:
        DatabaseError: If schema creation fails
        SchemaError: If schema version mismatch detected
    """
    # Check if already initialized
    existing_device_id = _get_existing_device_id(conn)
    if existing_device_id is not None:
        _verify_schema_version(conn)
        return existing_device_id

    # Create tables
    try:
        for statement in ALL_SCHEMA_STATEMENTS:
            # Split multi-statement strings
            for sql in statement.strip().split(";"):
                sql = sql.strip()
                if sql:
                    conn.execute(sql)
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to create sync tables: {e}",
            operation="create_tables",
        ) from e

    # Initialize metadata
    device_id = generate_uuid_v7()
    initial_vector_clock = json.dumps({device_id.hex(): 0}, sort_keys=True)

    try:
        conn.execute(
            "INSERT INTO sync_metadata (key, value) VALUES (?, ?)",
            (METADATA_KEY_DEVICE_ID, device_id),
        )
        conn.execute(
            "INSERT INTO sync_metadata (key, value) VALUES (?, ?)",
            (METADATA_KEY_SCHEMA_VERSION, SCHEMA_VERSION.to_bytes(4, "big")),
        )
        conn.execute(
            "INSERT INTO sync_metadata (key, value) VALUES (?, ?)",
            (METADATA_KEY_VECTOR_CLOCK, initial_vector_clock.encode("utf-8")),
        )
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to initialize sync metadata: {e}",
            operation="init_metadata",
        ) from e

    return device_id


def _get_existing_device_id(conn: sqlite3.Connection) -> bytes | None:
    """
    Check if database is already initialized.
    
    Args:
        conn: SQLite connection
        
    Returns:
        Device ID if initialized, None otherwise
    """
    try:
        cursor = conn.execute(
            "SELECT value FROM sync_metadata WHERE key = ?",
            (METADATA_KEY_DEVICE_ID,),
        )
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        return None
    except sqlite3.OperationalError:
        # Table doesn't exist
        return None


def _verify_schema_version(conn: sqlite3.Connection) -> None:
    """
    Verify schema version matches expected version.
    
    Args:
        conn: SQLite connection
        
    Raises:
        SchemaError: If version mismatch
    """
    try:
        cursor = conn.execute(
            "SELECT value FROM sync_metadata WHERE key = ?",
            (METADATA_KEY_SCHEMA_VERSION,),
        )
        row = cursor.fetchone()
        if row is None:
            raise SchemaError("Schema version not found in metadata")

        stored_version = int.from_bytes(row[0], "big")
        if stored_version != SCHEMA_VERSION:
            raise SchemaError(
                "Schema version mismatch",
                expected=SCHEMA_VERSION,
                actual=stored_version,
            )
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to verify schema version: {e}",
            operation="verify_schema",
        ) from e


def get_device_id(conn: sqlite3.Connection) -> bytes:
    """
    Get the device ID for this database.
    
    Args:
        conn: SQLite connection
        
    Returns:
        16-byte device ID
        
    Raises:
        SchemaError: If not initialized
    """
    try:
        cursor = conn.execute(
            "SELECT value FROM sync_metadata WHERE key = ?",
            (METADATA_KEY_DEVICE_ID,),
        )
        row = cursor.fetchone()
        if row is None:
            raise SchemaError("Database not initialized: device_id not found")
        return row[0]
    except sqlite3.OperationalError as e:
        raise SchemaError(f"Database not initialized: {e}") from e


def get_schema_version(conn: sqlite3.Connection) -> int:
    """
    Get the schema version for this database.
    
    Args:
        conn: SQLite connection
        
    Returns:
        Schema version number
        
    Raises:
        SchemaError: If not initialized
    """
    try:
        cursor = conn.execute(
            "SELECT value FROM sync_metadata WHERE key = ?",
            (METADATA_KEY_SCHEMA_VERSION,),
        )
        row = cursor.fetchone()
        if row is None:
            raise SchemaError("Database not initialized: schema_version not found")
        return int.from_bytes(row[0], "big")
    except sqlite3.OperationalError as e:
        raise SchemaError(f"Database not initialized: {e}") from e


def get_vector_clock(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Get the current vector clock for this database.
    
    Args:
        conn: SQLite connection
        
    Returns:
        Vector clock as dict
        
    Raises:
        SchemaError: If not initialized
    """
    try:
        cursor = conn.execute(
            "SELECT value FROM sync_metadata WHERE key = ?",
            (METADATA_KEY_VECTOR_CLOCK,),
        )
        row = cursor.fetchone()
        if row is None:
            raise SchemaError("Database not initialized: vector_clock not found")
        return json.loads(row[0].decode("utf-8"))
    except sqlite3.OperationalError as e:
        raise SchemaError(f"Database not initialized: {e}") from e


def update_vector_clock(conn: sqlite3.Connection, new_vc: dict[str, int]) -> None:
    """
    Update the vector clock in metadata.
    
    Args:
        conn: SQLite connection
        new_vc: New vector clock
    """
    vc_json = json.dumps(new_vc, sort_keys=True)
    conn.execute(
        "UPDATE sync_metadata SET value = ? WHERE key = ?",
        (vc_json.encode("utf-8"), METADATA_KEY_VECTOR_CLOCK),
    )
