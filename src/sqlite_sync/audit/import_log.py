"""
import_log.py - Import audit trail.

Records every bundle import for debugging and idempotency.
"""

import sqlite3
import time
from dataclasses import dataclass

from sqlite_sync.utils.uuid7 import generate_uuid_v7
from sqlite_sync.errors import DatabaseError


@dataclass(frozen=True, slots=True)
class ImportLogEntry:
    """
    Immutable representation of an import log entry.
    """
    import_id: bytes
    bundle_id: bytes
    bundle_hash: bytes
    imported_at: int
    source_device_id: bytes
    op_count: int
    applied_count: int
    conflict_count: int
    duplicate_count: int


def import_log_from_row(row: tuple) -> ImportLogEntry:
    """Create ImportLogEntry from database row."""
    return ImportLogEntry(
        import_id=row[0],
        bundle_id=row[1],
        bundle_hash=row[2],
        imported_at=row[3],
        source_device_id=row[4],
        op_count=row[5],
        applied_count=row[6],
        conflict_count=row[7],
        duplicate_count=row[8],
    )


def record_import(
    conn: sqlite3.Connection,
    bundle_id: bytes,
    bundle_hash: bytes,
    source_device_id: bytes,
    op_count: int,
    applied_count: int,
    conflict_count: int,
    duplicate_count: int,
) -> bytes:
    """
    Record a bundle import in the audit log.
    
    Args:
        conn: SQLite connection
        bundle_id: ID of imported bundle
        bundle_hash: SHA-256 hash of bundle content
        source_device_id: Device that created the bundle
        op_count: Total operations in bundle
        applied_count: Operations successfully applied
        conflict_count: Operations that caused conflicts
        duplicate_count: Operations already known
        
    Returns:
        import_id of new log entry
        
    Raises:
        DatabaseError: If bundle was already imported (duplicate hash)
    """
    import_id = generate_uuid_v7()
    imported_at = int(time.time() * 1_000_000)
    
    try:
        conn.execute(
            """
            INSERT INTO sync_import_log (
                import_id, bundle_id, bundle_hash,
                imported_at, source_device_id,
                op_count, applied_count, conflict_count, duplicate_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_id,
                bundle_id,
                bundle_hash,
                imported_at,
                source_device_id,
                op_count,
                applied_count,
                conflict_count,
                duplicate_count,
            ),
        )
    except sqlite3.IntegrityError as e:
        if "UNIQUE constraint failed" in str(e):
            raise DatabaseError(
                "Bundle already imported (duplicate hash)",
                operation="record_import",
            ) from e
        raise DatabaseError(
            f"Failed to record import: {e}",
            operation="record_import",
        ) from e
    
    return import_id


def get_import_history(
    conn: sqlite3.Connection,
    limit: int = 100,
) -> list[ImportLogEntry]:
    """
    Get recent import history.
    
    Args:
        conn: SQLite connection
        limit: Maximum entries to return
        
    Returns:
        List of import log entries, newest first
    """
    cursor = conn.execute(
        """
        SELECT * FROM sync_import_log
        ORDER BY imported_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [import_log_from_row(row) for row in cursor.fetchall()]


def get_import_by_bundle_hash(
    conn: sqlite3.Connection,
    bundle_hash: bytes,
) -> ImportLogEntry | None:
    """
    Get import log entry by bundle hash.
    
    Args:
        conn: SQLite connection
        bundle_hash: 32-byte SHA-256 hash
        
    Returns:
        ImportLogEntry if found, None otherwise
    """
    cursor = conn.execute(
        "SELECT * FROM sync_import_log WHERE bundle_hash = ?",
        (bundle_hash,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return import_log_from_row(row)
