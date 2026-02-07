"""
Desktop App Demo - SQLite Sync Core

A simple demo showing sync between two local databases.
Run this script to see sync in action without a network.
"""

import sys
import os
import tempfile

# Use installed package (after pip install -e .)
from sqlite_sync.engine import SyncEngine


def demo_basic_sync():
    """Demonstrate basic sync between two databases."""
    print("=" * 50)
    print("DEMO: Sync Between Two Databases")
    print("=" * 50)
    
    # Use temp directory for all files
    tmpdir = tempfile.mkdtemp()
    db_a = os.path.join(tmpdir, 'device_a.db')
    db_b = os.path.join(tmpdir, 'device_b.db')
    bundle_ab = os.path.join(tmpdir, 'bundle_ab.db')
    bundle_ba = os.path.join(tmpdir, 'bundle_ba.db')
    
    try:
        # Create Device A
        print("\n[1] Creating Device A...")
        engine_a = SyncEngine(db_a)
        device_a_id = engine_a.initialize()
        print(f"    Device A ID: {device_a_id.hex()[:8]}")
        
        engine_a.connection.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL)')
        engine_a.connection.commit()
        engine_a.enable_sync_for_table('notes')
        print("    Table 'notes' created and sync enabled")
        
        # Device A inserts data
        engine_a.connection.execute("INSERT INTO notes (title) VALUES ('Note 1 from A')")
        engine_a.connection.execute("INSERT INTO notes (title) VALUES ('Note 2 from A')")
        engine_a.connection.commit()
        print("    2 notes inserted")
        
        # Create Device B
        print("\n[2] Creating Device B...")
        engine_b = SyncEngine(db_b)
        device_b_id = engine_b.initialize()
        print(f"    Device B ID: {device_b_id.hex()[:8]}")
        
        engine_b.connection.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL)')
        engine_b.connection.commit()
        engine_b.enable_sync_for_table('notes')
        print("    Table 'notes' created and sync enabled")
        
        # Generate bundle from A for B
        print("\n[3] Generating bundle: A -> B")
        bundle_path = engine_a.generate_bundle(device_b_id, bundle_ab)
        if bundle_path:
            print(f"    Bundle created at {os.path.basename(bundle_path)}")
            
            # Import on B
            print("\n[4] Device B importing bundle...")
            result = engine_b.import_bundle(bundle_path)
            print(f"    Applied: {result.applied_count}, Conflicts: {result.conflict_count}")
            
            # Verify
            cursor = engine_b.connection.execute("SELECT * FROM notes ORDER BY id")
            rows = cursor.fetchall()
            print(f"    Notes on B: {rows}")
        else:
            print("    (No operations to send)")
        
        # Device B creates a note
        print("\n[5] Device B creating a note...")
        engine_b.connection.execute("INSERT INTO notes (title) VALUES ('Note from B')")
        engine_b.connection.commit()
        print("    Note inserted")
        
        # Sync back: B -> A
        print("\n[6] Syncing back: B -> A")
        bundle_path = engine_b.generate_bundle(device_a_id, bundle_ba)
        if bundle_path:
            result = engine_a.import_bundle(bundle_path)
            print(f"    Applied: {result.applied_count}, Conflicts: {result.conflict_count}")
            
            cursor = engine_a.connection.execute("SELECT * FROM notes ORDER BY id")
            rows = cursor.fetchall()
            print(f"    Notes on A: {rows}")
        
        # Cleanup
        engine_a.close()
        engine_b.close()
        
    finally:
        # Clean up temp files
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)
    
    print("\n" + "=" * 50)
    print("DEMO COMPLETED SUCCESSFULLY!")
    print("=" * 50)


if __name__ == "__main__":
    demo_basic_sync()
