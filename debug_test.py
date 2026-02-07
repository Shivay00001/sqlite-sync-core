"""Debug script to test bundle generation."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from sqlite_sync.engine import SyncEngine

db_path = 'test_debug.db'
if os.path.exists(db_path):
    os.remove(db_path)

engine = SyncEngine(db_path)
device_id = engine.initialize()
print(f"Device ID: {device_id.hex()[:8]}")

engine.connection.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT)')
engine.connection.commit()
engine.enable_sync_for_table('notes')

engine.connection.execute("INSERT INTO notes (title) VALUES ('Test')")
engine.connection.commit()

try:
    result = engine.generate_bundle(b'0123456789abcdef', 'test.bundle')
    print(f'Bundle generated: {result}')
except Exception as e:
    import traceback
    traceback.print_exc()

engine.close()

# Cleanup
for f in [db_path, 'test.bundle']:
    if os.path.exists(f):
        os.remove(f)
