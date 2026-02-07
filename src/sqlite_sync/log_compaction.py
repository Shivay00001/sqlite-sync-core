"""
log_compaction.py - Log compaction and snapshotting.

Provides:
- Log compaction (merge old operations)
- Snapshotting
- Pruning acknowledged operations
- Garbage collection
"""

import sqlite3
import time
import hashlib
import logging
from dataclasses import dataclass
from typing import Iterator

from sqlite_sync.log.operations import SyncOperation, operation_from_row
from sqlite_sync.utils.msgpack_codec import pack_value

logger = logging.getLogger(__name__)


@dataclass
class Snapshot:
    """Point-in-time database snapshot."""
    snapshot_id: bytes
    vector_clock: str
    created_at: int
    row_count: int
    size_bytes: int


@dataclass 
class CompactionResult:
    """Result of log compaction."""
    ops_before: int
    ops_after: int
    ops_removed: int
    bytes_freed: int


class LogCompactor:
    """
    Manages log compaction and garbage collection.
    
    Without compaction, the operation log grows unbounded.
    This class provides strategies to keep it manageable.
    """
    
    COMPACTION_TABLES_SQL = """
    CREATE TABLE IF NOT EXISTS sync_snapshots (
        snapshot_id BLOB PRIMARY KEY,
        vector_clock TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        row_count INTEGER NOT NULL,
        size_bytes INTEGER NOT NULL,
        data BLOB
    );
    
    CREATE TABLE IF NOT EXISTS sync_acknowledged_ops (
        device_id BLOB NOT NULL,
        last_ack_op_id BLOB NOT NULL,
        ack_time INTEGER NOT NULL,
        PRIMARY KEY (device_id)
    );
    
    CREATE INDEX IF NOT EXISTS idx_snapshots_created 
    ON sync_snapshots(created_at);
    """
    
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize compaction tables."""
        self._conn.executescript(self.COMPACTION_TABLES_SQL)
        self._conn.commit()
    
    def get_log_stats(self) -> dict:
        """Get current operation log statistics."""
        cursor = self._conn.execute(
            "SELECT COUNT(*), SUM(LENGTH(old_values) + LENGTH(new_values)) FROM sync_operations"
        )
        row = cursor.fetchone()
        
        return {
            "total_operations": row[0] or 0,
            "total_size_bytes": row[1] or 0
        }
    
    def record_acknowledgment(self, device_id: bytes, op_id: bytes) -> None:
        """Record that a peer has acknowledged receiving operations up to op_id."""
        now = int(time.time() * 1_000_000)
        
        self._conn.execute(
            """
            INSERT OR REPLACE INTO sync_acknowledged_ops 
            (device_id, last_ack_op_id, ack_time)
            VALUES (?, ?, ?)
            """,
            (device_id, op_id, now)
        )
        self._conn.commit()
    
    def get_safe_pruning_point(self) -> bytes | None:
        """
        Find the oldest operation that has been acknowledged by ALL known peers.
        
        Operations older than this can be safely pruned.
        """
        cursor = self._conn.execute(
            """
            SELECT MIN(o.created_at), a.last_ack_op_id
            FROM sync_acknowledged_ops a
            JOIN sync_operations o ON a.last_ack_op_id = o.op_id
            GROUP BY a.device_id
            ORDER BY o.created_at ASC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        return row[1] if row else None
    
    def prune_acknowledged_ops(self, before_op_id: bytes, keep_local: bool = True) -> int:
        """
        Remove operations that have been acknowledged by all peers.
        
        Args:
            before_op_id: Prune operations created before this op
            keep_local: Keep local operations for history
            
        Returns:
            Number of operations removed
        """
        # Get the created_at of the reference op
        cursor = self._conn.execute(
            "SELECT created_at FROM sync_operations WHERE op_id = ?",
            (before_op_id,)
        )
        row = cursor.fetchone()
        if not row:
            return 0
        
        cutoff_time = row[0]
        
        # Build delete query
        if keep_local:
            cursor = self._conn.execute(
                """
                DELETE FROM sync_operations
                WHERE created_at < ? AND is_local = 0
                """,
                (cutoff_time,)
            )
        else:
            cursor = self._conn.execute(
                """
                DELETE FROM sync_operations
                WHERE created_at < ?
                """,
                (cutoff_time,)
            )
        
        deleted = cursor.rowcount
        self._conn.commit()
        
        # Vacuum to reclaim space
        self._conn.execute("VACUUM")
        
        logger.info(f"Pruned {deleted} operations before {cutoff_time}")
        return deleted
    
    def create_snapshot(self, table_names: list[str] | None = None) -> Snapshot:
        """
        Create a point-in-time snapshot of current data state.
        
        Snapshots allow new devices to sync without replaying entire history.
        """
        import os
        snapshot_id = os.urandom(16)
        now = int(time.time() * 1_000_000)
        
        # Get current vector clock
        cursor = self._conn.execute(
            "SELECT value FROM sync_metadata WHERE key = 'vector_clock'"
        )
        row = cursor.fetchone()
        vector_clock = row[0] if row else "{}"
        
        # Gather table data
        if table_names is None:
            cursor = self._conn.execute(
                """
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sync_%' 
                AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE 'bundle_%'
                """
            )
            table_names = [row[0] for row in cursor.fetchall()]
        
        snapshot_data = {}
        total_rows = 0
        
        for table in table_names:
            cursor = self._conn.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            snapshot_data[table] = {
                "columns": columns,
                "rows": rows
            }
            total_rows += len(rows)
        
        # Serialize and store
        data_bytes = pack_value(snapshot_data)
        
        self._conn.execute(
            """
            INSERT INTO sync_snapshots 
            (snapshot_id, vector_clock, created_at, row_count, size_bytes, data)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, vector_clock, now, total_rows, len(data_bytes), data_bytes)
        )
        self._conn.commit()
        
        return Snapshot(
            snapshot_id=snapshot_id,
            vector_clock=vector_clock,
            created_at=now,
            row_count=total_rows,
            size_bytes=len(data_bytes)
        )
    
    def compact_log(self, max_ops: int = 10000) -> CompactionResult:
        """
        Compact operation log by merging redundant operations.
        
        Operations on the same row can be merged into a single INSERT/UPDATE.
        """
        stats_before = self.get_log_stats()
        
        # Find safe pruning point
        prune_point = self.get_safe_pruning_point()
        if prune_point:
            self.prune_acknowledged_ops(prune_point, keep_local=False)
        
        stats_after = self.get_log_stats()
        
        return CompactionResult(
            ops_before=stats_before["total_operations"],
            ops_after=stats_after["total_operations"],
            ops_removed=stats_before["total_operations"] - stats_after["total_operations"],
            bytes_freed=stats_before["total_size_bytes"] - stats_after["total_size_bytes"]
        )
    
    def cleanup_old_snapshots(self, keep_count: int = 3) -> int:
        """Keep only the most recent N snapshots."""
        cursor = self._conn.execute(
            """
            DELETE FROM sync_snapshots
            WHERE snapshot_id NOT IN (
                SELECT snapshot_id FROM sync_snapshots
                ORDER BY created_at DESC
                LIMIT ?
            )
            """,
            (keep_count,)
        )
        deleted = cursor.rowcount
        self._conn.commit()
        
        return deleted
