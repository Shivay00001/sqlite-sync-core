"""
conflict.py - Conflict detection and recording.

Conflicts occur when two devices independently modify the same row.
They are detected during import - never hidden or auto-resolved.
"""

import sqlite3
import time
from dataclasses import dataclass

from sqlite_sync.log.operations import SyncOperation, get_operations_for_row
from sqlite_sync.log.vector_clock import are_concurrent, parse_vector_clock, vector_clock_dominates
from sqlite_sync.utils.uuid7 import generate_uuid_v7
from sqlite_sync.errors import ConflictError


@dataclass(frozen=True, slots=True)
class SyncConflict:
    """
    Immutable representation of a sync conflict.
    """
    conflict_id: bytes
    table_name: str
    row_pk: bytes
    local_op_id: bytes
    remote_op_id: bytes
    detected_at: int
    resolved_at: int | None
    resolution_op_id: bytes | None


def conflict_from_row(row: tuple) -> SyncConflict:
    """Create SyncConflict from database row."""
    return SyncConflict(
        conflict_id=row[0],
        table_name=row[1],
        row_pk=row[2],
        local_op_id=row[3],
        remote_op_id=row[4],
        detected_at=row[5],
        resolved_at=row[6],
        resolution_op_id=row[7],
    )


def detect_conflict(
    conn: sqlite3.Connection,
    incoming_op: SyncOperation,
) -> SyncOperation | None:
    """
    Detect if an incoming operation conflicts with existing operations.
    
    A conflict exists if:
    1. There's an existing operation on the same (table, row_pk)
    2. The operations are concurrent (neither vector clock dominates)
    
    Args:
        conn: SQLite connection
        incoming_op: Operation being imported
        
    Returns:
        The conflicting local operation if conflict detected, None otherwise
    """
    incoming_vc = parse_vector_clock(incoming_op.vector_clock)
    
    existing_ops = get_operations_for_row(
        conn, incoming_op.table_name, incoming_op.row_pk
    )
    
    for existing_op in existing_ops:
        existing_vc = parse_vector_clock(existing_op.vector_clock)
        
        if are_concurrent(incoming_vc, existing_vc):
            # Concurrent modification = conflict
            return existing_op
    
    return None


def is_dominated(
    conn: sqlite3.Connection,
    incoming_op: SyncOperation,
) -> bool:
    """
    Check if an incoming operation is causally dominated by existing operations.
    
    An operation is dominated (stale) if there exists an applied operation
    on the same row whose vector clock dominates the incoming operation's.
    
    Args:
        conn: SQLite connection
        incoming_op: Operation being imported
        
    Returns:
        True if operation is stale and should not be applied
    """
    incoming_vc = parse_vector_clock(incoming_op.vector_clock)
    
    existing_ops = get_operations_for_row(
        conn, incoming_op.table_name, incoming_op.row_pk
    )
    
    for existing_op in existing_ops:
        existing_vc = parse_vector_clock(existing_op.vector_clock)
        
        # If existing dominates incoming, incoming is stale
        if vector_clock_dominates(existing_vc, incoming_vc):
            return True
            
    return False


def record_conflict(
    conn: sqlite3.Connection,
    table_name: str,
    row_pk: bytes,
    local_op_id: bytes,
    remote_op_id: bytes,
) -> bytes:
    """
    Record a conflict in sync_conflicts.
    
    Args:
        conn: SQLite connection
        table_name: Table where conflict occurred
        row_pk: Primary key of conflicting row
        local_op_id: ID of local operation
        remote_op_id: ID of remote operation
        
    Returns:
        conflict_id of new conflict record
    """
    conflict_id = generate_uuid_v7()
    detected_at = int(time.time() * 1_000_000)
    
    conn.execute(
        """
        INSERT INTO sync_conflicts (
            conflict_id, table_name, row_pk,
            local_op_id, remote_op_id,
            detected_at, resolved_at, resolution_op_id
        ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)
        """,
        (conflict_id, table_name, row_pk, local_op_id, remote_op_id, detected_at),
    )
    
    return conflict_id


def get_unresolved_conflicts(conn: sqlite3.Connection) -> list[SyncConflict]:
    """
    Get all unresolved conflicts.
    
    Args:
        conn: SQLite connection
        
    Returns:
        List of unresolved conflicts
    """
    cursor = conn.execute(
        """
        SELECT * FROM sync_conflicts
        WHERE resolved_at IS NULL
        ORDER BY detected_at ASC
        """
    )
    return [conflict_from_row(row) for row in cursor.fetchall()]


def get_conflict_by_id(
    conn: sqlite3.Connection,
    conflict_id: bytes,
) -> SyncConflict | None:
    """
    Get a specific conflict by ID.
    
    Args:
        conn: SQLite connection
        conflict_id: 16-byte conflict ID
        
    Returns:
        SyncConflict if found, None otherwise
    """
    cursor = conn.execute(
        "SELECT * FROM sync_conflicts WHERE conflict_id = ?",
        (conflict_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return conflict_from_row(row)


def mark_conflict_resolved(
    conn: sqlite3.Connection,
    conflict_id: bytes,
    resolution_op_id: bytes,
) -> None:
    """
    Mark a conflict as resolved.
    
    Args:
        conn: SQLite connection
        conflict_id: ID of conflict to resolve
        resolution_op_id: ID of operation that resolves the conflict
    """
    resolved_at = int(time.time() * 1_000_000)
    
    result = conn.execute(
        """
        UPDATE sync_conflicts
        SET resolved_at = ?, resolution_op_id = ?
        WHERE conflict_id = ? AND resolved_at IS NULL
        """,
        (resolved_at, resolution_op_id, conflict_id),
    )
    
    if result.rowcount == 0:
        raise ConflictError(
            "Conflict not found or already resolved",
            conflict_id=conflict_id,
        )
