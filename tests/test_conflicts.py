"""
test_conflicts.py - Tests for conflict detection and handling.

These tests verify that conflicts are properly detected, recorded,
and never auto-resolved.
"""

import os
import sqlite3
import tempfile
import time
import pytest

from sqlite_sync import SyncEngine
from sqlite_sync.bundle.generate import generate_bundle
from sqlite_sync.db.migrations import get_device_id, get_vector_clock
from sqlite_sync.log.vector_clock import are_concurrent, parse_vector_clock
from sqlite_sync.import_apply.conflict import (
    detect_conflict,
    record_conflict,
    get_unresolved_conflicts,
)
from sqlite_sync.utils.uuid7 import generate_uuid_v7


class TestConflictDetection:
    """Tests for conflict detection."""
    
    def test_concurrent_updates_detected(self):
        """Concurrent updates to same row are detected as conflicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create two databases
            db_a_path = os.path.join(tmpdir, "device_a.db")
            db_b_path = os.path.join(tmpdir, "device_b.db")
            
            from sqlite_sync.resolution.strategies import NoOpResolver
            no_op = NoOpResolver()
            
            engine_a = SyncEngine(db_a_path)
            engine_a.initialize()
            
            engine_b = SyncEngine(db_b_path, conflict_resolver=no_op)
            engine_b.initialize()
            
            # Create same table on both
            for engine in [engine_a, engine_b]:
                conn = engine.connection
                conn.execute("""
                    CREATE TABLE notes (
                        id INTEGER PRIMARY KEY,
                        content TEXT
                    )
                """)
                engine.enable_sync_for_table("notes")
            
            # Initial sync: A creates a note, syncs to B
            engine_a.connection.execute(
                "INSERT INTO notes (id, content) VALUES (1, 'Initial')"
            )
            
            bundle1_path = os.path.join(tmpdir, "bundle1.db")
            device_b_id = get_device_id(engine_b.connection)
            generate_bundle(engine_a.connection, device_b_id, bundle1_path)
            engine_b.import_bundle(bundle1_path)
            
            # Both have the note now
            cursor_a = engine_a.connection.execute("SELECT content FROM notes WHERE id = 1")
            cursor_b = engine_b.connection.execute("SELECT content FROM notes WHERE id = 1")
            assert cursor_a.fetchone()[0] == "Initial"
            assert cursor_b.fetchone()[0] == "Initial"
            
            # Now both update OFFLINE (without syncing)
            engine_a.connection.execute(
                "UPDATE notes SET content = 'Updated by A' WHERE id = 1"
            )
            engine_b.connection.execute(
                "UPDATE notes SET content = 'Updated by B' WHERE id = 1"
            )
            
            # A syncs to B - should create conflict
            bundle2_path = os.path.join(tmpdir, "bundle2.db")
            generate_bundle(engine_a.connection, device_b_id, bundle2_path)
            result = engine_b.import_bundle(bundle2_path)
            
            # Conflict should be detected
            assert result.conflict_count == 1
            
            # Conflict should be recorded
            conflicts = engine_b.get_unresolved_conflicts()
            assert len(conflicts) == 1
            assert conflicts[0].table_name == "notes"
            
            engine_a.close()
            engine_b.close()
    
    def test_concurrent_insert_delete_detected(self):
        """Concurrent INSERT and DELETE on same row detected."""
        # This test simulates:
        # A: UPDATE row 1
        # B: DELETE row 1 (concurrently)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_a_path = os.path.join(tmpdir, "device_a.db")
            db_b_path = os.path.join(tmpdir, "device_b.db")
            
            from sqlite_sync.resolution.strategies import NoOpResolver
            no_op = NoOpResolver()
            
            engine_a = SyncEngine(db_a_path)
            engine_a.initialize()
            
            engine_b = SyncEngine(db_b_path, conflict_resolver=no_op)
            engine_b.initialize()
            
            for engine in [engine_a, engine_b]:
                conn = engine.connection
                conn.execute("""
                    CREATE TABLE tasks (
                        id INTEGER PRIMARY KEY,
                        title TEXT
                    )
                """)
                engine.enable_sync_for_table("tasks")
            
            # A creates task, syncs to B
            engine_a.connection.execute(
                "INSERT INTO tasks (id, title) VALUES (1, 'Task')"
            )
            
            bundle1_path = os.path.join(tmpdir, "sync1.db")
            device_b_id = get_device_id(engine_b.connection)
            generate_bundle(engine_a.connection, device_b_id, bundle1_path)
            engine_b.import_bundle(bundle1_path)
            
            # A updates, B deletes (offline)
            engine_a.connection.execute(
                "UPDATE tasks SET title = 'Updated Task' WHERE id = 1"
            )
            engine_b.connection.execute("DELETE FROM tasks WHERE id = 1")
            
            # Sync A to B
            bundle2_path = os.path.join(tmpdir, "sync2.db")
            generate_bundle(engine_a.connection, device_b_id, bundle2_path)
            result = engine_b.import_bundle(bundle2_path)
            
            # Should have conflict
            assert result.conflict_count == 1
            
            engine_a.close()
            engine_b.close()


class TestNoAutoMerge:
    """Tests that verify conflicts are NEVER auto-resolved."""
    
    def test_conflicts_remain_unresolved(self):
        """Conflicts remain unresolved until explicitly handled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            engine = SyncEngine(db_path)
            engine.initialize()
            
            conn = engine.connection
            conn.execute("""
                CREATE TABLE data (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )
            """)
            engine.enable_sync_for_table("data")
            conn.execute("INSERT INTO data (id, value) VALUES (1, 'original')")
            
            # Manually create a conflict record
            local_op_id = conn.execute(
                "SELECT op_id FROM sync_operations LIMIT 1"
            ).fetchone()[0]
            remote_op_id = generate_uuid_v7()
            conflict_id = generate_uuid_v7()
            
            # Create dummy remote operation to satisfy FK
            conn.execute(
                """
                INSERT INTO sync_operations (
                    op_id, device_id, parent_op_id, vector_clock,
                    table_name, op_type, row_pk,
                    schema_version, created_at, is_local, applied_at
                ) VALUES (?, ?, NULL, '{}', 'data', 'INSERT', X'02', 1, 0, 0, NULL)
                """,
                (remote_op_id, generate_uuid_v7())
            )

            conn.execute(
                """
                INSERT INTO sync_conflicts (
                    conflict_id, table_name, row_pk,
                    local_op_id, remote_op_id,
                    detected_at, resolved_at, resolution_op_id
                ) VALUES (?, 'data', X'01', ?, ?, ?, NULL, NULL)
                """,
                (conflict_id, local_op_id, remote_op_id, int(time.time() * 1_000_000)),
            )
            
            # Conflict should be queryable
            conflicts = engine.get_unresolved_conflicts()
            assert len(conflicts) == 1
            
            # resolved_at should be NULL
            assert conflicts[0].resolved_at is None
            
            # System did NOT auto-resolve
            assert conflicts[0].resolution_op_id is None
            
            engine.close()


class TestVectorClockConcurrency:
    """Tests for vector clock concurrency detection."""
    
    def test_concurrent_detection(self):
        """Two vector clocks with incomparable values are concurrent."""
        # A: {A: 5, B: 3}
        # B: {A: 3, B: 5}
        # Neither dominates the other -> concurrent
        vc_a = {"a" * 32: 5, "b" * 32: 3}
        vc_b = {"a" * 32: 3, "b" * 32: 5}
        
        assert are_concurrent(vc_a, vc_b) is True
    
    def test_not_concurrent_if_dominated(self):
        """If one VC dominates another, they are not concurrent."""
        # A: {A: 5, B: 5}
        # B: {A: 3, B: 3}
        # A dominates B -> not concurrent
        vc_a = {"a" * 32: 5, "b" * 32: 5}
        vc_b = {"a" * 32: 3, "b" * 32: 3}
        
        assert are_concurrent(vc_a, vc_b) is False
    
    def test_equal_not_concurrent(self):
        """Equal vector clocks are not concurrent."""
        vc = {"a" * 32: 5}
        
        assert are_concurrent(vc, vc.copy()) is False


class TestConflictPreservation:
    """Tests that both versions are preserved during conflict."""
    
    def test_both_operations_in_log(self):
        """Both conflicting operations are preserved in sync_operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_a_path = os.path.join(tmpdir, "device_a.db")
            db_b_path = os.path.join(tmpdir, "device_b.db")
            
            engine_a = SyncEngine(db_a_path)
            engine_a.initialize()
            device_a_id = engine_a.device_id
            
            engine_b = SyncEngine(db_b_path)
            engine_b.initialize()
            device_b_id = engine_b.device_id
            
            for engine in [engine_a, engine_b]:
                conn = engine.connection
                conn.execute("""
                    CREATE TABLE items (
                        id INTEGER PRIMARY KEY,
                        val TEXT
                    )
                """)
                engine.enable_sync_for_table("items")
            
            # A creates, syncs to B
            engine_a.connection.execute("INSERT INTO items (id, val) VALUES (1, 'v1')")
            bundle1 = os.path.join(tmpdir, "b1.db")
            generate_bundle(engine_a.connection, device_b_id, bundle1)
            engine_b.import_bundle(bundle1)
            
            # Both update offline
            engine_a.connection.execute("UPDATE items SET val = 'A-update' WHERE id = 1")
            engine_b.connection.execute("UPDATE items SET val = 'B-update' WHERE id = 1")
            
            # Count B's operations before sync
            cursor = engine_b.connection.execute("SELECT COUNT(*) FROM sync_operations")
            before_count = cursor.fetchone()[0]
            
            # Sync A to B (creates conflict)
            bundle2 = os.path.join(tmpdir, "b2.db")
            generate_bundle(engine_a.connection, device_b_id, bundle2)
            engine_b.import_bundle(bundle2)
            
            # Count B's operations after sync
            cursor = engine_b.connection.execute("SELECT COUNT(*) FROM sync_operations")
            after_count = cursor.fetchone()[0]
            
            # Should have one more operation (the conflicting one from A)
            assert after_count == before_count + 1
            
            # Both A's and B's updates should be in the log
            cursor = engine_b.connection.execute(
                "SELECT device_id, op_type FROM sync_operations WHERE table_name = 'items'"
            )
            ops = cursor.fetchall()
            
            device_ids = {op[0] for op in ops}
            assert device_a_id in device_ids
            assert device_b_id in device_ids
            
            engine_a.close()
            engine_b.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
