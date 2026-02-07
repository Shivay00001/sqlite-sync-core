"""
crash_safety.py - Crash-safe transaction handling.

Provides:
- Atomic transaction replay
- Rollback on failure
- Checkpoint/resume support
- WAL mode optimization
"""

import sqlite3
import time
import logging
from dataclasses import dataclass
from typing import Callable, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """Sync checkpoint for resume-on-crash."""
    checkpoint_id: bytes
    last_applied_op_id: bytes | None
    vector_clock: str
    created_at: int
    completed: bool = False


class CrashSafeExecutor:
    """
    Execute sync operations with crash safety guarantees.
    
    Features:
    - Atomic multi-operation transactions
    - Savepoint-based rollback
    - Checkpoint/resume after crash
    - WAL mode for concurrent access
    """
    
    CHECKPOINT_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS sync_checkpoints (
        checkpoint_id BLOB PRIMARY KEY,
        last_applied_op_id BLOB,
        vector_clock TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        completed INTEGER DEFAULT 0
    );
    
    CREATE INDEX IF NOT EXISTS idx_checkpoints_completed 
    ON sync_checkpoints(completed);
    """
    
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize checkpoint tables and WAL mode."""
        # Enable WAL mode for better crash safety
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        
        self._conn.executescript(self.CHECKPOINT_TABLE_SQL)
        self._conn.commit()
    
    def create_checkpoint(self, vector_clock: str) -> Checkpoint:
        """Create a new sync checkpoint."""
        import os
        checkpoint_id = os.urandom(16)
        now = int(time.time() * 1_000_000)
        
        self._conn.execute(
            """
            INSERT INTO sync_checkpoints 
            (checkpoint_id, vector_clock, created_at)
            VALUES (?, ?, ?)
            """,
            (checkpoint_id, vector_clock, now)
        )
        self._conn.commit()
        
        return Checkpoint(
            checkpoint_id=checkpoint_id,
            last_applied_op_id=None,
            vector_clock=vector_clock,
            created_at=now
        )
    
    def update_checkpoint(self, checkpoint_id: bytes, last_op_id: bytes) -> None:
        """Update checkpoint with last applied operation."""
        self._conn.execute(
            """
            UPDATE sync_checkpoints 
            SET last_applied_op_id = ?
            WHERE checkpoint_id = ?
            """,
            (last_op_id, checkpoint_id)
        )
        # Don't commit - will be committed with transaction
    
    def complete_checkpoint(self, checkpoint_id: bytes) -> None:
        """Mark checkpoint as completed."""
        self._conn.execute(
            """
            UPDATE sync_checkpoints 
            SET completed = 1
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,)
        )
        self._conn.commit()
    
    def get_incomplete_checkpoint(self) -> Checkpoint | None:
        """Get any incomplete checkpoint for resume."""
        cursor = self._conn.execute(
            """
            SELECT checkpoint_id, last_applied_op_id, vector_clock, created_at
            FROM sync_checkpoints
            WHERE completed = 0
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None
            
        return Checkpoint(
            checkpoint_id=row[0],
            last_applied_op_id=row[1],
            vector_clock=row[2],
            created_at=row[3],
            completed=False
        )
    
    @contextmanager
    def atomic_operation(self, operations_count: int = 1):
        """
        Execute operations atomically with rollback on failure.
        
        Usage:
            with executor.atomic_operation():
                # All operations here are atomic
                engine.apply_operation(op1)
                engine.apply_operation(op2)
        """
        # Create savepoint
        savepoint_name = f"sp_{int(time.time() * 1000)}"
        self._conn.execute(f"SAVEPOINT {savepoint_name}")
        
        try:
            yield
            # Success - release savepoint
            self._conn.execute(f"RELEASE {savepoint_name}")
        except Exception as e:
            # Failure - rollback to savepoint
            logger.error(f"Atomic operation failed, rolling back: {e}")
            self._conn.execute(f"ROLLBACK TO {savepoint_name}")
            self._conn.execute(f"RELEASE {savepoint_name}")
            raise
    
    def execute_with_retry(
        self, 
        operation: Callable[[], Any],
        max_retries: int = 3,
        retry_delay: float = 0.1
    ) -> Any:
        """
        Execute operation with automatic retry on transient failures.
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with self.atomic_operation():
                    return operation()
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    last_error = e
                    time.sleep(retry_delay * (2 ** attempt))
                    continue
                raise
            except Exception as e:
                raise
        
        raise last_error or RuntimeError("Operation failed after retries")
    
    def cleanup_old_checkpoints(self, max_age_seconds: int = 86400) -> int:
        """Remove completed checkpoints older than max_age."""
        cutoff = int((time.time() - max_age_seconds) * 1_000_000)
        
        cursor = self._conn.execute(
            """
            DELETE FROM sync_checkpoints
            WHERE completed = 1 AND created_at < ?
            """,
            (cutoff,)
        )
        self._conn.commit()
        
        return cursor.rowcount
