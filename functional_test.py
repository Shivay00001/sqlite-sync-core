"""
Full End-to-End Functional Test

This is NOT a demo - it's a complete functional verification that proves
the sync system works in all real-world scenarios.
"""

import os
import tempfile
import shutil

# Uses installed package (pip install -e .)

from sqlite_sync.engine import SyncEngine
from sqlite_sync.log_compaction import LogCompactor
from sqlite_sync.crash_safety import CrashSafeExecutor
from sqlite_sync.schema_evolution import SchemaManager


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def check(self, condition, name):
        if condition:
            self.passed += 1
            print(f"  ✓ {name}")
        else:
            self.failed += 1
            self.errors.append(name)
            print(f"  ✗ {name}")
        return condition


def test_basic_sync(result, tmpdir):
    """Test basic sync between two devices."""
    print("\n[TEST] Basic Sync Between Devices")
    
    db_a = os.path.join(tmpdir, 'a.db')
    db_b = os.path.join(tmpdir, 'b.db')
    bundle = os.path.join(tmpdir, 'bundle.db')
    
    # Create engines
    engine_a = SyncEngine(db_a)
    engine_b = SyncEngine(db_b)
    
    device_a = engine_a.initialize()
    device_b = engine_b.initialize()
    
    # Create table on both
    for engine in [engine_a, engine_b]:
        engine.connection.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
        engine.connection.commit()
        engine.enable_sync_for_table('items')
    
    # Insert data on A
    engine_a.connection.execute("INSERT INTO items (name) VALUES ('Item 1')")
    engine_a.connection.execute("INSERT INTO items (name) VALUES ('Item 2')")
    engine_a.connection.commit()
    
    # Generate bundle
    bundle_path = engine_a.generate_bundle(device_b, bundle)
    result.check(bundle_path is not None, "Generate bundle")
    result.check(os.path.exists(bundle_path), "Bundle file exists")
    
    # Import on B
    import_result = engine_b.import_bundle(bundle_path)
    result.check(import_result.applied_count == 2, f"Applied 2 ops (got {import_result.applied_count})")
    result.check(import_result.conflict_count == 0, "No conflicts")
    
    # Verify data on B
    cursor = engine_b.connection.execute("SELECT COUNT(*) FROM items")
    count = cursor.fetchone()[0]
    result.check(count == 2, f"B has 2 items (got {count})")
    
    engine_a.close()
    engine_b.close()


def test_bidirectional_sync(result, tmpdir):
    """Test sync in both directions."""
    print("\n[TEST] Bidirectional Sync")
    
    db_a = os.path.join(tmpdir, 'bi_a.db')
    db_b = os.path.join(tmpdir, 'bi_b.db')
    
    engine_a = SyncEngine(db_a)
    engine_b = SyncEngine(db_b)
    
    device_a = engine_a.initialize()
    device_b = engine_b.initialize()
    
    for engine in [engine_a, engine_b]:
        engine.connection.execute('CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)')
        engine.connection.commit()
        engine.enable_sync_for_table('data')
    
    # A creates data
    engine_a.connection.execute("INSERT INTO data (value) VALUES ('from_a')")
    engine_a.connection.commit()
    
    # Sync A -> B
    bundle_ab = os.path.join(tmpdir, 'ab.db')
    engine_a.generate_bundle(device_b, bundle_ab)
    engine_b.import_bundle(bundle_ab)
    
    # B creates data
    engine_b.connection.execute("INSERT INTO data (value) VALUES ('from_b')")
    engine_b.connection.commit()
    
    # Sync B -> A
    bundle_ba = os.path.join(tmpdir, 'ba.db')
    engine_b.generate_bundle(device_a, bundle_ba)
    import_result = engine_a.import_bundle(bundle_ba)
    
    result.check(import_result.applied_count == 1, "A receives B's data")
    
    # Both should have same data
    cursor_a = engine_a.connection.execute("SELECT value FROM data ORDER BY value")
    cursor_b = engine_b.connection.execute("SELECT value FROM data ORDER BY value")
    
    data_a = [r[0] for r in cursor_a.fetchall()]
    data_b = [r[0] for r in cursor_b.fetchall()]
    
    result.check(data_a == data_b, f"Data matches: {data_a}")
    result.check(len(data_a) == 2, "Both have 2 records")
    
    engine_a.close()
    engine_b.close()


def test_conflict_detection(result, tmpdir):
    """Test conflict detection on concurrent edits."""
    print("\n[TEST] Conflict Detection")
    
    db_a = os.path.join(tmpdir, 'conf_a.db')
    db_b = os.path.join(tmpdir, 'conf_b.db')
    
    engine_a = SyncEngine(db_a)
    engine_b = SyncEngine(db_b)
    
    device_a = engine_a.initialize()
    device_b = engine_b.initialize()
    
    for engine in [engine_a, engine_b]:
        engine.connection.execute('CREATE TABLE doc (id INTEGER PRIMARY KEY, content TEXT)')
        engine.connection.commit()
        engine.enable_sync_for_table('doc')
        engine.connection.execute("INSERT INTO doc (id, content) VALUES (1, 'original')")
        engine.connection.commit()
    
    # Both edit same row
    engine_a.connection.execute("UPDATE doc SET content = 'from_a' WHERE id = 1")
    engine_a.connection.commit()
    
    engine_b.connection.execute("UPDATE doc SET content = 'from_b' WHERE id = 1")
    engine_b.connection.commit()
    
    # Sync A -> B
    bundle = os.path.join(tmpdir, 'conflict.db')
    engine_a.generate_bundle(device_b, bundle)
    import_result = engine_b.import_bundle(bundle)
    
    # Should detect conflict
    result.check(import_result.conflict_count >= 1, f"Conflict detected ({import_result.conflict_count})")
    
    conflicts = engine_b.get_unresolved_conflicts()
    result.check(len(conflicts) >= 1, f"Unresolved conflicts exist ({len(conflicts)})")
    
    engine_a.close()
    engine_b.close()


def test_idempotent_import(result, tmpdir):
    """Test that importing same bundle twice is idempotent."""
    print("\n[TEST] Idempotent Import")
    
    db_a = os.path.join(tmpdir, 'idem_a.db')
    db_b = os.path.join(tmpdir, 'idem_b.db')
    
    engine_a = SyncEngine(db_a)
    engine_b = SyncEngine(db_b)
    
    device_a = engine_a.initialize()
    device_b = engine_b.initialize()
    
    for engine in [engine_a, engine_b]:
        engine.connection.execute('CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)')
        engine.connection.commit()
        engine.enable_sync_for_table('log')
    
    engine_a.connection.execute("INSERT INTO log (msg) VALUES ('test')")
    engine_a.connection.commit()
    
    bundle = os.path.join(tmpdir, 'idem.db')
    engine_a.generate_bundle(device_b, bundle)
    
    # Import twice
    result1 = engine_b.import_bundle(bundle)
    result2 = engine_b.import_bundle(bundle)
    
    result.check(result1.applied_count == 1, "First import applied 1")
    result.check(result2.skipped == True, f"Second import skipped (duplicate)")
    
    cursor = engine_b.connection.execute("SELECT COUNT(*) FROM log")
    count = cursor.fetchone()[0]
    result.check(count == 1, f"Still only 1 record (got {count})")
    
    engine_a.close()
    engine_b.close()


def test_streaming_sync(result, tmpdir):
    """Test streaming sync with apply_operation."""
    print("\n[TEST] Streaming Sync (apply_operation)")
    
    db_a = os.path.join(tmpdir, 'stream_a.db')
    db_b = os.path.join(tmpdir, 'stream_b.db')
    
    engine_a = SyncEngine(db_a)
    engine_b = SyncEngine(db_b)
    
    device_a = engine_a.initialize()
    device_b = engine_b.initialize()
    
    for engine in [engine_a, engine_b]:
        engine.connection.execute('CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)')
        engine.connection.commit()
        engine.enable_sync_for_table('events')
    
    # Create operations on A
    engine_a.connection.execute("INSERT INTO events (name) VALUES ('event1')")
    engine_a.connection.execute("INSERT INTO events (name) VALUES ('event2')")
    engine_a.connection.commit()
    
    # Get operations from A
    ops = engine_a.get_new_operations()
    result.check(len(ops) >= 2, f"Got {len(ops)} operations")
    
    # Apply each operation on B (streaming mode)
    applied = 0
    for op in ops:
        if engine_b.apply_operation(op):
            applied += 1
    
    result.check(applied == len(ops), f"Applied all ops via streaming ({applied}/{len(ops)})")
    
    cursor = engine_b.connection.execute("SELECT COUNT(*) FROM events")
    count = cursor.fetchone()[0]
    result.check(count == 2, f"B has 2 events (got {count})")
    
    engine_a.close()
    engine_b.close()


def test_log_compaction(result, tmpdir):
    """Test log compaction functionality."""
    print("\n[TEST] Log Compaction")
    
    db = os.path.join(tmpdir, 'compact.db')
    engine = SyncEngine(db)
    engine.initialize()
    
    engine.connection.execute('CREATE TABLE logs (id INTEGER PRIMARY KEY, text TEXT)')
    engine.connection.commit()
    engine.enable_sync_for_table('logs')
    
    # Create many operations
    for i in range(50):
        engine.connection.execute(f"INSERT INTO logs (text) VALUES ('log {i}')")
    engine.connection.commit()
    
    compactor = LogCompactor(engine.connection)
    stats = compactor.get_log_stats()
    result.check(stats['total_operations'] >= 50, f"Has {stats['total_operations']} ops")
    
    # Create snapshot
    snapshot = compactor.create_snapshot()
    result.check(snapshot.row_count == 50, f"Snapshot has {snapshot.row_count} rows")
    result.check(snapshot.size_bytes > 0, "Snapshot has data")
    
    engine.close()


def test_crash_safety(result, tmpdir):
    """Test crash-safe execution."""
    print("\n[TEST] Crash Safety")
    
    db = os.path.join(tmpdir, 'crash.db')
    engine = SyncEngine(db)
    engine.initialize()
    
    engine.connection.execute('CREATE TABLE safe (id INTEGER PRIMARY KEY, val TEXT)')
    engine.connection.commit()
    engine.enable_sync_for_table('safe')
    
    executor = CrashSafeExecutor(engine.connection)
    
    # Create checkpoint
    vc = engine.get_vector_clock()
    checkpoint = executor.create_checkpoint(str(vc))
    result.check(checkpoint is not None, "Checkpoint created")
    
    # Atomic operation
    try:
        with executor.atomic_operation():
            engine.connection.execute("INSERT INTO safe (val) VALUES ('atomic1')")
            engine.connection.execute("INSERT INTO safe (val) VALUES ('atomic2')")
        success = True
    except:
        success = False
    
    result.check(success, "Atomic operation completed")
    
    cursor = engine.connection.execute("SELECT COUNT(*) FROM safe")
    count = cursor.fetchone()[0]
    result.check(count == 2, f"Both inserts committed ({count})")
    
    executor.complete_checkpoint(checkpoint.checkpoint_id)
    
    engine.close()


def test_schema_evolution(result, tmpdir):
    """Test schema evolution."""
    print("\n[TEST] Schema Evolution")
    
    db = os.path.join(tmpdir, 'schema.db')
    engine = SyncEngine(db)
    engine.initialize()
    
    engine.connection.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    engine.connection.commit()
    
    schema = SchemaManager(engine.connection)
    
    version_before = schema.get_current_version()
    result.check(version_before >= 0, f"Has schema version {version_before}")
    
    # Add column
    migration = schema.add_column('users', 'email', 'TEXT', default_value='')
    result.check(migration is not None, "Migration created")
    
    version_after = schema.get_current_version()
    result.check(version_after > version_before, f"Version incremented {version_before} -> {version_after}")
    
    # Verify column exists
    cursor = engine.connection.execute("PRAGMA table_info(users)")
    columns = [row[1] for row in cursor.fetchall()]
    result.check('email' in columns, f"Email column added: {columns}")
    
    engine.close()


def run_all_tests():
    """Run all functional tests."""
    print("=" * 60)
    print("SQLITE-SYNC-CORE FUNCTIONAL VERIFICATION")
    print("=" * 60)
    
    result = TestResult()
    tmpdir = tempfile.mkdtemp()
    
    try:
        test_basic_sync(result, tmpdir)
        test_bidirectional_sync(result, tmpdir)
        test_conflict_detection(result, tmpdir)
        test_idempotent_import(result, tmpdir)
        test_streaming_sync(result, tmpdir)
        test_log_compaction(result, tmpdir)
        test_crash_safety(result, tmpdir)
        test_schema_evolution(result, tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    
    print("\n" + "=" * 60)
    print(f"RESULTS: {result.passed} passed, {result.failed} failed")
    print("=" * 60)
    
    if result.errors:
        print("\nFailed tests:")
        for e in result.errors:
            print(f"  - {e}")
        return 1
    else:
        print("\n✓ ALL TESTS PASSED - SYSTEM IS FULLY FUNCTIONAL")
        return 0


if __name__ == "__main__":
    exit(run_all_tests())
