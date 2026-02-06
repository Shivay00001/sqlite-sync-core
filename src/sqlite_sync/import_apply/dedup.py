"""
dedup.py - Deduplication for idempotent import.

Ensures importing the same bundle N times = same result.
"""

import sqlite3

from sqlite_sync.log.operations import operation_exists
from sqlite_sync.invariants import Invariants


def is_bundle_already_imported(conn: sqlite3.Connection, bundle_hash: bytes) -> bool:
    """
    Check if a bundle has already been imported.
    
    This is the first idempotency check - if we've seen this
    exact bundle before, skip entirely.
    
    Args:
        conn: SQLite connection
        bundle_hash: SHA-256 hash of bundle content
        
    Returns:
        True if bundle was already imported
    """
    return Invariants.assert_idempotent_import(conn, bundle_hash)


def filter_duplicate_operations(
    conn: sqlite3.Connection,
    op_ids: list[bytes],
) -> tuple[list[bytes], int]:
    """
    Filter out operations that already exist in local database.
    
    Args:
        conn: SQLite connection
        op_ids: List of operation IDs from bundle
        
    Returns:
        Tuple of (new_op_ids, duplicate_count)
    """
    new_ids = []
    duplicate_count = 0
    
    for op_id in op_ids:
        if operation_exists(conn, op_id):
            duplicate_count += 1
        else:
            new_ids.append(op_id)
    
    return new_ids, duplicate_count


def operation_is_duplicate(conn: sqlite3.Connection, op_id: bytes) -> bool:
    """
    Check if a single operation already exists.
    
    Args:
        conn: SQLite connection
        op_id: Operation ID to check
        
    Returns:
        True if operation exists
    """
    return operation_exists(conn, op_id)
