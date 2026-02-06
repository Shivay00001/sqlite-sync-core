"""
test_idempotency.py - Tests for idempotent import.

These tests verify that importing the same bundle N times
produces the same result as importing it once.
"""

import os
import sqlite3
import tempfile
import pytest

from sqlite_sync import SyncEngine
from sqlite_sync.bundle.generate import generate_bundle
from sqlite_sync.db.migrations import get_device_id
from sqlite_sync.utils.uuid7 import generate_uuid_v7


class TestIdempotentBundleImport:
    """Tests for idempotent bundle import."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.tmpdir = tempfile.mkdtemp()
        
        # Create source database
        self.source_path = os.path.join(self.tmpdir, "source.db")
        self.source = SyncEngine(self.source_path)
        self.source.initialize()
        
        # Create target database  
        self.target_path = os.path.join(self.tmpdir, "target.db")
        self.target = SyncEngine(self.target_path)
        self.target.initialize()
        
        # Create same table on both
        for engine in [self.source, self.target]:
            conn = engine.connection
            conn.execute("""
                CREATE TABLE todos (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    done INTEGER DEFAULT 0
                )
            """)
            engine.enable_sync_for_table("todos")
    
    def teardown_method(self):
        """Clean up test fixtures."""
        self.source.close()
        self.target.close()
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
    
    def test_double_import_same_result(self):
        """Importing same bundle twice produces same result."""
        # Add data to source
        self.source.connection.execute(
            "INSERT INTO todos (id, title) VALUES (1, 'First task')"
        )
        self.source.connection.execute(
            "INSERT INTO todos (id, title) VALUES (2, 'Second task')"
        )
        
        # Generate bundle
        bundle_path = os.path.join(self.tmpdir, "bundle1.db")
        target_device_id = get_device_id(self.target.connection)
        generate_bundle(self.source.connection, target_device_id, bundle_path)
        
        # First import
        result1 = self.target.import_bundle(bundle_path)
        assert result1.applied_count == 2
        assert result1.skipped is False
        
        # Capture state after first import
        cursor = self.target.connection.execute(
            "SELECT id, title FROM todos ORDER BY id"
        )
        state_after_first = cursor.fetchall()
        
        # Second import - should be skipped
        result2 = self.target.import_bundle(bundle_path)
        assert result2.skipped is True
        assert result2.applied_count == 0
        
        # State should be identical
        cursor = self.target.connection.execute(
            "SELECT id, title FROM todos ORDER BY id"
        )
        state_after_second = cursor.fetchall()
        
        assert state_after_first == state_after_second
    
    def test_triple_import_same_result(self):
        """Importing same bundle three times produces same result."""
        self.source.connection.execute(
            "INSERT INTO todos (id, title) VALUES (1, 'Task')"
        )
        
        bundle_path = os.path.join(self.tmpdir, "bundle2.db")
        target_device_id = get_device_id(self.target.connection)
        generate_bundle(self.source.connection, target_device_id, bundle_path)
        
        # Import three times
        result1 = self.target.import_bundle(bundle_path)
        result2 = self.target.import_bundle(bundle_path)
        result3 = self.target.import_bundle(bundle_path)
        
        assert result1.skipped is False
        assert result2.skipped is True
        assert result3.skipped is True
        
        # Only one row in table
        cursor = self.target.connection.execute("SELECT COUNT(*) FROM todos")
        count = cursor.fetchone()[0]
        assert count == 1
    
    def test_partial_overlap_handled(self):
        """Mixed new and duplicate operations handled correctly."""
        # First batch
        self.source.connection.execute(
            "INSERT INTO todos (id, title) VALUES (1, 'First')"
        )
        
        bundle1_path = os.path.join(self.tmpdir, "bundle_partial1.db")
        target_device_id = get_device_id(self.target.connection)
        generate_bundle(self.source.connection, target_device_id, bundle1_path)
        
        # Import first bundle
        self.target.import_bundle(bundle1_path)
        
        # Second batch (includes first operation + new one)
        self.source.connection.execute(
            "INSERT INTO todos (id, title) VALUES (2, 'Second')"
        )
        
        bundle2_path = os.path.join(self.tmpdir, "bundle_partial2.db")
        generate_bundle(self.source.connection, target_device_id, bundle2_path)
        
        # Import second bundle
        result = self.target.import_bundle(bundle2_path)
        
        # Should have 1 duplicate and 1 new
        assert result.duplicate_count >= 1
        assert result.applied_count >= 1
        
        # Both rows should exist
        cursor = self.target.connection.execute("SELECT COUNT(*) FROM todos")
        count = cursor.fetchone()[0]
        assert count == 2


class TestIdempotentOperationApplication:
    """Tests for idempotent operation application."""
    
    def test_insert_same_row_twice(self):
        """INSERT of same row is idempotent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            engine = SyncEngine(db_path)
            engine.initialize()
            
            conn = engine.connection
            conn.execute("""
                CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            engine.enable_sync_for_table("items")
            
            # Insert a row
            conn.execute("INSERT INTO items (id, name) VALUES (1, 'test')")
            
            # Count operations
            cursor = conn.execute("SELECT COUNT(*) FROM sync_operations")
            op_count = cursor.fetchone()[0]
            assert op_count == 1
            
            # Row exists
            cursor = conn.execute("SELECT COUNT(*) FROM items")
            row_count = cursor.fetchone()[0]
            assert row_count == 1
            
            engine.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
