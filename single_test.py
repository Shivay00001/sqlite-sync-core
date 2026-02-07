"""Run single test for debugging."""
import os
import tempfile
import shutil
from sqlite_sync.engine import SyncEngine

tmpdir = tempfile.mkdtemp()

print("[TEST] Conflict Detection")

db_a = os.path.join(tmpdir, 'conf_a.db')
db_b = os.path.join(tmpdir, 'conf_b.db')

engine_a = SyncEngine(db_a)
engine_b = SyncEngine(db_b)

device_a = engine_a.initialize()
device_b = engine_b.initialize()

print(f"Device A: {device_a.hex()[:8]}")
print(f"Device B: {device_b.hex()[:8]}")

for engine in [engine_a, engine_b]:
    engine.connection.execute('CREATE TABLE doc (id INTEGER PRIMARY KEY, content TEXT)')
    engine.connection.commit()
    engine.enable_sync_for_table('doc')
    engine.connection.execute("INSERT INTO doc (id, content) VALUES (1, 'original')")
    engine.connection.commit()

print("Both devices have initial data")

# Both edit same row
engine_a.connection.execute("UPDATE doc SET content = 'from_a' WHERE id = 1")
engine_a.connection.commit()

engine_b.connection.execute("UPDATE doc SET content = 'from_b' WHERE id = 1")
engine_b.connection.commit()

print("Both devices edited the same row")

# Sync A -> B
bundle = os.path.join(tmpdir, 'conflict.db')

try:
    print("Generating bundle...")
    result = engine_a.generate_bundle(device_b, bundle)
    print(f"Bundle: {result}")
    
    if result:
        print("Importing bundle...")
        import_result = engine_b.import_bundle(result)
        print(f"Applied: {import_result.applied_count}, Conflicts: {import_result.conflict_count}")
        
        conflicts = engine_b.get_unresolved_conflicts()
        print(f"Unresolved conflicts: {len(conflicts)}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

engine_a.close()
engine_b.close()
shutil.rmtree(tmpdir)

print("DONE")
