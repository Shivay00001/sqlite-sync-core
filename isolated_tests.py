"""Run tests one by one with error handling."""
import os
import tempfile
import shutil
import traceback

from sqlite_sync.engine import SyncEngine
from sqlite_sync.log_compaction import LogCompactor
from sqlite_sync.crash_safety import CrashSafeExecutor
from sqlite_sync.schema_evolution import SchemaManager


def test_basic_sync(tmpdir):
    print("\n[TEST] Basic Sync Between Devices")
    
    db_a = os.path.join(tmpdir, 'a.db')
    db_b = os.path.join(tmpdir, 'b.db')
    bundle = os.path.join(tmpdir, 'bundle.db')
    
    engine_a = SyncEngine(db_a)
    engine_b = SyncEngine(db_b)
    
    device_a = engine_a.initialize()
    device_b = engine_b.initialize()
    
    for engine in [engine_a, engine_b]:
        engine.connection.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
        engine.connection.commit()
        engine.enable_sync_for_table('items')
    
    engine_a.connection.execute("INSERT INTO items (name) VALUES ('Item 1')")
    engine_a.connection.execute("INSERT INTO items (name) VALUES ('Item 2')")
    engine_a.connection.commit()
    
    bundle_path = engine_a.generate_bundle(device_b, bundle)
    import_result = engine_b.import_bundle(bundle_path)
    
    cursor = engine_b.connection.execute("SELECT COUNT(*) FROM items")
    count = cursor.fetchone()[0]
    
    engine_a.close()
    engine_b.close()
    
    assert count == 2, f"Expected 2 items, got {count}"
    print("  PASS")


def test_bidirectional_sync(tmpdir):
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
    
    engine_a.connection.execute("INSERT INTO data (value) VALUES ('from_a')")
    engine_a.connection.commit()
    
    bundle_ab = os.path.join(tmpdir, 'ab.db')
    engine_a.generate_bundle(device_b, bundle_ab)
    engine_b.import_bundle(bundle_ab)
    
    engine_b.connection.execute("INSERT INTO data (value) VALUES ('from_b')")
    engine_b.connection.commit()
    
    bundle_ba = os.path.join(tmpdir, 'ba.db')
    engine_b.generate_bundle(device_a, bundle_ba)
    import_result = engine_a.import_bundle(bundle_ba)
    
    cursor_a = engine_a.connection.execute("SELECT value FROM data ORDER BY value")
    cursor_b = engine_b.connection.execute("SELECT value FROM data ORDER BY value")
    
    data_a = [r[0] for r in cursor_a.fetchall()]
    data_b = [r[0] for r in cursor_b.fetchall()]
    
    engine_a.close()
    engine_b.close()
    
    assert data_a == data_b, f"Data mismatch: {data_a} vs {data_b}"
    print("  PASS")


def test_idempotent_import(tmpdir):
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
    
    result1 = engine_b.import_bundle(bundle)
    result2 = engine_b.import_bundle(bundle)
    
    cursor = engine_b.connection.execute("SELECT COUNT(*) FROM log")
    count = cursor.fetchone()[0]
    
    engine_a.close()
    engine_b.close()
    
    assert result1.applied_count == 1, "First import should apply 1"
    assert result2.skipped == True, "Second import should skip"
    assert count == 1, "Should have only 1 record"
    print("  PASS")


def test_streaming_sync(tmpdir):
    print("\n[TEST] Streaming Sync")
    
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
    
    engine_a.connection.execute("INSERT INTO events (name) VALUES ('event1')")
    engine_a.connection.execute("INSERT INTO events (name) VALUES ('event2')")
    engine_a.connection.commit()
    
    ops = engine_a.get_new_operations()
    assert len(ops) >= 2, f"Expected at least 2 ops, got {len(ops)}"
    
    applied = 0
    for op in ops:
        if engine_b.apply_operation(op):
            applied += 1
    
    cursor = engine_b.connection.execute("SELECT COUNT(*) FROM events")
    count = cursor.fetchone()[0]
    
    engine_a.close()
    engine_b.close()
    
    assert count == 2, f"Expected 2 events, got {count}"
    print("  PASS")


def test_log_compaction(tmpdir):
    print("\n[TEST] Log Compaction")
    
    db = os.path.join(tmpdir, 'compact.db')
    engine = SyncEngine(db)
    engine.initialize()
    
    engine.connection.execute('CREATE TABLE logs (id INTEGER PRIMARY KEY, text TEXT)')
    engine.connection.commit()
    engine.enable_sync_for_table('logs')
    
    for i in range(50):
        engine.connection.execute(f"INSERT INTO logs (text) VALUES ('log {i}')")
    engine.connection.commit()
    
    compactor = LogCompactor(engine.connection)
    stats = compactor.get_log_stats()
    
    snapshot = compactor.create_snapshot()
    
    engine.close()
    
    assert stats['total_operations'] >= 50, f"Expected 50+ ops, got {stats['total_operations']}"
    assert snapshot.row_count == 50, f"Expected 50 rows, got {snapshot.row_count}"
    print("  PASS")


def test_crash_safety(tmpdir):
    print("\n[TEST] Crash Safety")
    
    db = os.path.join(tmpdir, 'crash.db')
    engine = SyncEngine(db)
    engine.initialize()
    
    engine.connection.execute('CREATE TABLE safe (id INTEGER PRIMARY KEY, val TEXT)')
    engine.connection.commit()
    engine.enable_sync_for_table('safe')
    
    executor = CrashSafeExecutor(engine.connection)
    
    vc = engine.get_vector_clock()
    checkpoint = executor.create_checkpoint(str(vc))
    
    with executor.atomic_operation():
        engine.connection.execute("INSERT INTO safe (val) VALUES ('atomic1')")
        engine.connection.execute("INSERT INTO safe (val) VALUES ('atomic2')")
    
    cursor = engine.connection.execute("SELECT COUNT(*) FROM safe")
    count = cursor.fetchone()[0]
    
    executor.complete_checkpoint(checkpoint.checkpoint_id)
    engine.close()
    
    assert count == 2, f"Expected 2 records, got {count}"
    print("  PASS")


def test_schema_evolution(tmpdir):
    print("\n[TEST] Schema Evolution")
    
    db = os.path.join(tmpdir, 'schema.db')
    engine = SyncEngine(db)
    engine.initialize()
    
    engine.connection.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    engine.connection.commit()
    
    schema = SchemaManager(engine.connection)
    
    version_before = schema.get_current_version()
    migration = schema.add_column('users', 'email', 'TEXT', default_value='')
    version_after = schema.get_current_version()
    
    cursor = engine.connection.execute("PRAGMA table_info(users)")
    columns = [row[1] for row in cursor.fetchall()]
    
    engine.close()
    
    assert migration is not None, "Migration should be created"
    assert version_after > version_before, f"Version should increment: {version_before} -> {version_after}"
    assert 'email' in columns, f"Email column should exist: {columns}"
    print("  PASS")


def main():
    print("=" * 60)
    print("SQLITE-SYNC-CORE - FUNCTIONAL TESTS (ISOLATED)")
    print("=" * 60)
    
    tests = [
        test_basic_sync,
        test_bidirectional_sync,
        test_idempotent_import,
        test_streaming_sync,
        test_log_compaction,
        test_crash_safety,
        test_schema_evolution,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        # Each test gets its own temp directory
        tmpdir = tempfile.mkdtemp()
        try:
            test(tmpdir)
            passed += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            traceback.print_exc()
            failed += 1
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
    
    print("\n" + "=" * 60)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit(main())
