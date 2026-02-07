"""Simpler demo to verify core sync works."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from sqlite_sync.engine import SyncEngine

def cleanup_files(*files):
    for f in files:
        if os.path.exists(f):
            os.remove(f)

print("=" * 50)
print("SIMPLE SYNC DEMO")
print("=" * 50)

# Cleanup first
cleanup_files('device_a.db', 'device_b.db', 'bundle.db')

# Create Device A
print("\n[1] Creating Device A...")
engine_a = SyncEngine('device_a.db')
device_a_id = engine_a.initialize()
print(f"    Device A ID: {device_a_id.hex()[:8]}")

# Create table and enable sync
engine_a.connection.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT)')
engine_a.connection.commit()
engine_a.enable_sync_for_table('notes')
print("    [OK] Table created, sync enabled")

# Insert data
engine_a.connection.execute("INSERT INTO notes (title) VALUES ('Note from A')")
engine_a.connection.commit()
print("    [OK] Note inserted")

# Create Device B
print("\n[2] Creating Device B...")
engine_b = SyncEngine('device_b.db')
device_b_id = engine_b.initialize()
print(f"    Device B ID: {device_b_id.hex()[:8]}")

# Create same table
engine_b.connection.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT)')
engine_b.connection.commit()
engine_b.enable_sync_for_table('notes')
print("    [OK] Table created, sync enabled")

# Generate bundle from A to B
print("\n[3] Generating bundle from A for B...")
try:
    bundle_path = engine_a.generate_bundle(device_b_id, 'bundle.db')
    if bundle_path:
        print(f"    [OK] Bundle created at {bundle_path}")
    else:
        print("    [INFO] No ops to send")
except Exception as e:
    print(f"    [FAIL] {e}")
    import traceback
    traceback.print_exc()

# Import bundle on B
if os.path.exists('bundle.db'):
    print("\n[4] Importing bundle on B...")
    try:
        result = engine_b.import_bundle('bundle.db')
        print(f"    [OK] Applied: {result.applied_count}, Conflicts: {result.conflict_count}")
    except Exception as e:
        print(f"    [FAIL] {e}")
        import traceback
        traceback.print_exc()

# Verify data
print("\n[5] Verifying data on B...")
cursor = engine_b.connection.execute("SELECT * FROM notes")
rows = cursor.fetchall()
print(f"    Notes on B: {rows}")

# Cleanup
engine_a.close()
engine_b.close()
cleanup_files('device_a.db', 'device_b.db', 'bundle.db')

print("\n" + "=" * 50)
print("DEMO COMPLETED!")
print("=" * 50)
