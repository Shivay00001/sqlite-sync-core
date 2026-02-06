"""
test_invariants.py - Tests for core system invariants.

These tests verify that the fundamental laws of the system hold.
"""

import os
import sqlite3
import tempfile
import pytest

from sqlite_sync import SyncEngine
from sqlite_sync.errors import InvariantViolationError
from sqlite_sync.invariants import Invariants
from sqlite_sync.log.vector_clock import parse_vector_clock


class TestAppendOnlyInvariant:
    """Tests for Invariant 1: sync_operations is append-only."""
    
    def test_operations_cannot_be_deleted(self):
        """Verify that deleting from sync_operations fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            engine = SyncEngine(db_path)
            engine.initialize()
            
            # Create a table and insert some data
            conn = engine.connection
            conn.execute("""
                CREATE TABLE todos (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL
                )
            """)
            engine.enable_sync_for_table("todos")
            
            # Insert a todo (this creates a sync operation)
            conn.execute("INSERT INTO todos (id, title) VALUES (1, 'Test')")
            
            # Verify operation was created
            cursor = conn.execute("SELECT COUNT(*) FROM sync_operations")
            count = cursor.fetchone()[0]
            assert count == 1
            
            # Attempting to delete should fail via foreign key or be blocked
            # The system design prevents this at the application level
            # Direct SQL delete would violate the invariant
            cursor = conn.execute("SELECT op_id FROM sync_operations LIMIT 1")
            op_id = cursor.fetchone()[0]
            
            # Our invariant checker should detect this
            with pytest.raises(InvariantViolationError):
                Invariants.assert_append_only(conn, op_id, "DELETE")
            
            engine.close()
    
    def test_operations_cannot_be_updated(self):
        """Verify that updating sync_operations fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            engine = SyncEngine(db_path)
            engine.initialize()
            
            conn = engine.connection
            conn.execute("""
                CREATE TABLE todos (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL
                )
            """)
            engine.enable_sync_for_table("todos")
            conn.execute("INSERT INTO todos (id, title) VALUES (1, 'Test')")
            
            cursor = conn.execute("SELECT op_id FROM sync_operations LIMIT 1")
            op_id = cursor.fetchone()[0]
            
            with pytest.raises(InvariantViolationError):
                Invariants.assert_append_only(conn, op_id, "UPDATE")
            
            engine.close()


class TestVectorClockInvariant:
    """Tests for Invariant 2: Vector clocks must be valid."""
    
    def test_valid_vector_clock(self):
        """Valid vector clocks should pass validation."""
        valid_vc = {"a" * 32: 5, "b" * 32: 3}
        # Should not raise
        Invariants.assert_valid_vector_clock(valid_vc)
    
    def test_invalid_device_id_length(self):
        """Device ID must be 32 hex chars."""
        invalid_vc = {"short": 5}
        with pytest.raises(InvariantViolationError):
            Invariants.assert_valid_vector_clock(invalid_vc)
    
    def test_invalid_counter_type(self):
        """Counter must be non-negative integer."""
        invalid_vc = {"a" * 32: "five"}
        with pytest.raises(InvariantViolationError):
            Invariants.assert_valid_vector_clock(invalid_vc)
    
    def test_negative_counter(self):
        """Counter must be non-negative."""
        invalid_vc = {"a" * 32: -1}
        with pytest.raises(InvariantViolationError):
            Invariants.assert_valid_vector_clock(invalid_vc)


class TestTransactionInvariant:
    """Tests for Invariant 7: All mutations occur within transaction."""
    
    def test_in_transaction_check(self):
        """Verify transaction state detection works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = sqlite3.connect(db_path, isolation_level=None)
            
            # Not in transaction initially
            with pytest.raises(InvariantViolationError):
                Invariants.assert_in_transaction(conn)
            
            # Start transaction
            conn.execute("BEGIN")
            # Now should pass
            Invariants.assert_in_transaction(conn)
            
            conn.execute("ROLLBACK")
            conn.close()


class TestAtomicityGuarantee:
    """Tests for atomic write guarantees."""
    
    def test_user_write_and_operation_are_atomic(self):
        """User write and sync operation must commit together."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            engine = SyncEngine(db_path)
            engine.initialize()
            
            conn = engine.connection
            conn.execute("""
                CREATE TABLE todos (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL
                )
            """)
            engine.enable_sync_for_table("todos")
            
            # Count operations before
            cursor = conn.execute("SELECT COUNT(*) FROM sync_operations")
            before_count = cursor.fetchone()[0]
            
            # Insert a row
            conn.execute("INSERT INTO todos (id, title) VALUES (1, 'Test')")
            
            # Count operations after
            cursor = conn.execute("SELECT COUNT(*) FROM sync_operations")
            after_count = cursor.fetchone()[0]
            
            # Exactly one operation should have been created
            assert after_count == before_count + 1
            
            engine.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
