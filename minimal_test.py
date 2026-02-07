"""Minimal test to find the exact error."""
import os
import tempfile

from sqlite_sync.engine import SyncEngine

tmpdir = tempfile.mkdtemp()
db_a = os.path.join(tmpdir, 'a.db')
db_b = os.path.join(tmpdir, 'b.db')
bundle = os.path.join(tmpdir, 'bundle.db')

print("Creating engine A...")
engine_a = SyncEngine(db_a)
device_a = engine_a.initialize()
print(f"  Device A: {device_a.hex()[:8]}")

print("Creating table...")
engine_a.connection.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
engine_a.connection.commit()

print("Enabling sync...")
engine_a.enable_sync_for_table('items')

print("Inserting data...")
engine_a.connection.execute("INSERT INTO items (name) VALUES ('test')")
engine_a.connection.commit()

print("Creating engine B...")
engine_b = SyncEngine(db_b)
device_b = engine_b.initialize()
print(f"  Device B: {device_b.hex()[:8]}")

engine_b.connection.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
engine_b.connection.commit()
engine_b.enable_sync_for_table('items')

print("Generating bundle...")
try:
    bundle_path = engine_a.generate_bundle(device_b, bundle)
    print(f"  Bundle: {bundle_path}")
except Exception as e:
    print(f"  ERROR in generate_bundle: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("Importing bundle...")
try:
    result = engine_b.import_bundle(bundle_path)
    print(f"  Applied: {result.applied_count}")
except Exception as e:
    print(f"  ERROR in import_bundle: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("Verifying data...")
cursor = engine_b.connection.execute("SELECT * FROM items")
rows = cursor.fetchall()
print(f"  Data: {rows}")

engine_a.close()
engine_b.close()

import shutil
shutil.rmtree(tmpdir)

print("\nSUCCESS!")
