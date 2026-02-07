"""
operations.py - Sync operation data structures and queries.

Operations are the fundamental unit of change in the sync system.
Each operation represents a single INSERT, UPDATE, or DELETE on a user table.
"""

import sqlite3
from dataclasses import dataclass
from typing import Iterator

from sqlite_sync.config import OPERATION_TYPES
from sqlite_sync.errors import ValidationError, DatabaseError


@dataclass(frozen=True, slots=True)
class SyncOperation:
    """
    Immutable representation of a sync operation.
    
    This mirrors the sync_operations table schema.
    Frozen dataclass ensures operations cannot be modified after creation.
    """
    op_id: bytes
    device_id: bytes
    parent_op_id: bytes | None
    vector_clock: str  # JSON string for canonical representation
    hlc: str
    table_name: str
    op_type: str
    row_pk: bytes  # MessagePack encoded
    old_values: bytes | None  # MessagePack encoded
    new_values: bytes | None  # MessagePack encoded
    schema_version: int
    created_at: int  # Unix microseconds
    is_local: bool
    applied_at: int | None  # Unix microseconds, None if pending/conflict
    
    def __post_init__(self) -> None:
        """Validate operation after initialization."""
        if len(self.op_id) != 16:
            raise ValidationError(
                f"op_id must be 16 bytes, got {len(self.op_id)}",
                field="op_id",
            )
        if len(self.device_id) != 16:
            raise ValidationError(
                f"device_id must be 16 bytes, got {len(self.device_id)}",
                field="device_id",
            )
        if self.parent_op_id is not None and len(self.parent_op_id) != 16:
            raise ValidationError(
                f"parent_op_id must be 16 bytes, got {len(self.parent_op_id)}",
                field="parent_op_id",
            )
        if self.op_type not in OPERATION_TYPES:
            raise ValidationError(
                f"op_type must be one of {OPERATION_TYPES}, got {self.op_type}",
                field="op_type",
                value=self.op_type,
            )
        if self.op_type == "INSERT" and self.new_values is None:
            raise ValidationError(
                "INSERT operation must have new_values",
                field="new_values",
            )
        if self.op_type == "DELETE" and self.old_values is None:
            raise ValidationError(
                "DELETE operation must have old_values",
                field="old_values",
            )


def operation_from_row(row: tuple) -> SyncOperation:
    """
    Create SyncOperation from database row.
    
    Args:
        row: Tuple from SELECT * FROM sync_operations
        
    Returns:
        SyncOperation instance
    """
    return SyncOperation(
        op_id=row[0],
        device_id=row[1],
        parent_op_id=row[2],
        vector_clock=row[3],
        hlc=row[4],
        table_name=row[5],
        op_type=row[6],
        row_pk=row[7],
        old_values=row[8],
        new_values=row[9],
        schema_version=row[10],
        created_at=row[11],
        is_local=bool(row[12]),
        applied_at=row[13],
    )


def operation_to_row(op: SyncOperation) -> tuple:
    """
    Convert SyncOperation to database row tuple.
    
    Args:
        op: SyncOperation instance
        
    Returns:
        Tuple suitable for INSERT
    """
    return (
        op.op_id,
        op.device_id,
        op.parent_op_id,
        op.vector_clock,
        op.hlc,
        op.table_name,
        op.op_type,
        op.row_pk,
        op.old_values,
        op.new_values,
        op.schema_version,
        op.created_at,
        1 if op.is_local else 0,
        op.applied_at,
    )


def get_operation_by_id(conn: sqlite3.Connection, op_id: bytes) -> SyncOperation | None:
    """
    Retrieve a single operation by ID.
    
    Args:
        conn: SQLite connection
        op_id: 16-byte operation ID
        
    Returns:
        SyncOperation if found, None otherwise
    """
    cursor = conn.execute(
        "SELECT * FROM sync_operations WHERE op_id = ?",
        (op_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return operation_from_row(row)


def get_operations_for_row(
    conn: sqlite3.Connection,
    table_name: str,
    row_pk: bytes,
) -> list[SyncOperation]:
    """
    Get all operations for a specific row.
    
    Used for conflict detection.
    
    Args:
        conn: SQLite connection
        table_name: Table name
        row_pk: Primary key (MessagePack encoded)
        
    Returns:
        List of operations on this row
    """
    cursor = conn.execute(
        """
        SELECT * FROM sync_operations 
        WHERE table_name = ? AND row_pk = ?
        ORDER BY created_at ASC
        """,
        (table_name, row_pk),
    )
    return [operation_from_row(row) for row in cursor.fetchall()]


def get_last_operation_for_device(
    conn: sqlite3.Connection,
    device_id: bytes,
) -> SyncOperation | None:
    """
    Get the most recent operation from a device.
    
    Used for setting parent_op_id in new operations.
    
    Args:
        conn: SQLite connection
        device_id: 16-byte device ID
        
    Returns:
        Most recent operation or None
    """
    cursor = conn.execute(
        """
        SELECT * FROM sync_operations 
        WHERE device_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (device_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return operation_from_row(row)


def iter_all_operations(conn: sqlite3.Connection) -> Iterator[SyncOperation]:
    """
    Iterate over all operations in created_at order.
    
    Args:
        conn: SQLite connection
        
    Yields:
        SyncOperation instances
    """
    cursor = conn.execute(
        "SELECT * FROM sync_operations ORDER BY created_at ASC"
    )
    for row in cursor:
        yield operation_from_row(row)


from sqlite_sync.log.vector_clock import parse_vector_clock, serialize_vector_clock, is_dominated as vc_is_dominated

def get_operations_since(
    conn: sqlite3.Connection,
    since_vc: dict[str, int] | None = None,
) -> list[SyncOperation]:
    """
    Get all local operations that are not known to the peer's vector clock.
    
    Args:
        conn: SQLite connection
        since_vc: Peer's vector clock
        
    Returns:
        List of operations
    """
    cursor = conn.execute(
        "SELECT * FROM sync_operations WHERE is_local = 1 ORDER BY created_at ASC"
    )
    all_local = [operation_from_row(row) for row in cursor.fetchall()]
    
    if since_vc is None:
        return all_local
        
    since_vc_json = serialize_vector_clock(since_vc)
    
    new_ops = []
    for op in all_local:
        # If op's vector clock is NOT dominated by peer's VC, it means peer doesn't have it
        if not vc_is_dominated(op.vector_clock, since_vc_json):
            new_ops.append(op)
            
    return new_ops


def operation_exists(conn: sqlite3.Connection, op_id: bytes) -> bool:
    """
    Check if an operation ID exists.
    
    Args:
        conn: SQLite connection
        op_id: 16-byte operation ID
        
    Returns:
        True if operation exists
    """
    cursor = conn.execute(
        "SELECT 1 FROM sync_operations WHERE op_id = ?",
        (op_id,),
    )
    return cursor.fetchone() is not None




def insert_operation(conn: sqlite3.Connection, op: SyncOperation) -> None:
    """
    Insert an operation into sync_operations.
    
    Args:
        conn: SQLite connection
        op: Operation to insert
        
    Raises:
        DatabaseError: If insert fails
    """
    try:
        conn.execute(
            """
            INSERT INTO sync_operations (
                op_id, device_id, parent_op_id, vector_clock, hlc,
                table_name, op_type, row_pk, old_values, new_values,
                schema_version, created_at, is_local, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            operation_to_row(op),
        )
    except sqlite3.IntegrityError as e:
        # Duplicate op_id - this is expected for idempotent import
        if "UNIQUE constraint failed" in str(e):
            raise DatabaseError(
                f"Operation {op.op_id.hex()} already exists",
                operation="insert_operation",
            ) from e
        raise DatabaseError(
            f"Failed to insert operation: {e}",
            operation="insert_operation",
        ) from e
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to insert operation: {e}",
            operation="insert_operation",
        ) from e
