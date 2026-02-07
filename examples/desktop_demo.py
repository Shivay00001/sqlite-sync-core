"""
Desktop App Demo - SQLite Sync Core

A simple desktop application demonstrating sync between two local databases.
Run this script to see sync in action without network.
"""

import os
import sys
import time
import tempfile

# Add src to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sqlite_sync.engine import SyncEngine
from sqlite_sync.resolution import ResolutionStrategy, get_resolver, ConflictContext
from sqlite_sync.log_compaction import LogCompactor


def create_demo_database(name: str, db_path: str) -> SyncEngine:
    """Create and initialize a demo database."""
    if os.path.exists(db_path):
        os.remove(db_path)
    
    engine = SyncEngine(db_path)
    engine.initialize()
    
    # Create demo table
    engine.connection.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
            priority INTEGER DEFAULT 1,
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
    """)
    engine.connection.commit()
    
    # Enable sync
    engine.enable_sync_for_table("notes")
    
    print(f"[{name}] Initialized: {engine.device_id.hex()[:8]}...")
    return engine


def demo_basic_sync():
    """Demonstrate basic sync between two databases."""
    print("\n" + "="*60)
    print("DEMO: Basic Sync Between Two Databases")
    print("="*60 + "\n")
    
    # Create two "devices"
    device_a = create_demo_database("Device A", "demo_device_a.db")
    device_b = create_demo_database("Device B", "demo_device_b.db")
    
    # Device A creates a note
    print("\n[Device A] Creating note...")
    device_a.connection.execute(
        "INSERT INTO notes (title, content, priority) VALUES (?, ?, ?)",
        ("Meeting Notes", "Discuss sync protocol", 3)
    )
    device_a.connection.commit()
    
    # Generate bundle from A
    print("[Device A] Generating bundle...")
    bundle_path = device_a.generate_bundle(device_b.device_id, "sync_a_to_b.bundle")
    
    if bundle_path:
        # Device B imports bundle
        print("[Device B] Importing bundle...")
        result = device_b.import_bundle(bundle_path)
        print(f"[Device B] Applied {result.applied_count} operations, {result.conflict_count} conflicts")
        
        # Verify data
        cursor = device_b.connection.execute("SELECT title, content FROM notes")
        rows = cursor.fetchall()
        print(f"[Device B] Notes: {rows}")
    
    # Device B creates a note
    print("\n[Device B] Creating another note...")
    device_b.connection.execute(
        "INSERT INTO notes (title, content, priority) VALUES (?, ?, ?)",
        ("Todo List", "Implement encryption", 2)
    )
    device_b.connection.commit()
    
    # Sync back to A
    print("[Device B] Generating bundle for A...")
    bundle_path = device_b.generate_bundle(device_a.device_id, "sync_b_to_a.bundle")
    
    if bundle_path:
        print("[Device A] Importing bundle...")
        result = device_a.import_bundle(bundle_path)
        print(f"[Device A] Applied {result.applied_count} operations")
        
        # Both should have same data
        cursor = device_a.connection.execute("SELECT title FROM notes ORDER BY id")
        rows = cursor.fetchall()
        print(f"[Device A] Notes: {rows}")
    
    # Cleanup
    device_a.close()
    device_b.close()
    
    for f in ["demo_device_a.db", "demo_device_b.db", "sync_a_to_b.bundle", "sync_b_to_a.bundle"]:
        if os.path.exists(f):
            os.remove(f)
    
    print("\n✅ Basic sync demo completed!")


def demo_conflict_resolution():
    """Demonstrate conflict detection and resolution."""
    print("\n" + "="*60)
    print("DEMO: Conflict Detection and Resolution")
    print("="*60 + "\n")
    
    device_a = create_demo_database("Device A", "demo_conflict_a.db")
    device_b = create_demo_database("Device B", "demo_conflict_b.db")
    
    # Both devices create notes with same data initially
    for dev, name in [(device_a, "Device A"), (device_b, "Device B")]:
        dev.connection.execute(
            "INSERT INTO notes (id, title, content) VALUES (1, 'Shared Note', 'Initial content')"
        )
        dev.connection.commit()
        print(f"[{name}] Created initial note")
    
    # Sync to establish baseline
    time.sleep(0.1)  # Ensure different timestamps
    
    # Device A modifies the note
    device_a.connection.execute(
        "UPDATE notes SET content = 'Modified by Device A' WHERE id = 1"
    )
    device_a.connection.commit()
    print("[Device A] Modified note")
    
    time.sleep(0.1)
    
    # Device B also modifies the note (conflict!)
    device_b.connection.execute(
        "UPDATE notes SET content = 'Modified by Device B' WHERE id = 1"
    )
    device_b.connection.commit()
    print("[Device B] Modified note (creates conflict)")
    
    # Generate and import bundles
    bundle_ab = device_a.generate_bundle(device_b.device_id, "conflict_a_to_b.bundle")
    if bundle_ab:
        result = device_b.import_bundle(bundle_ab)
        print(f"\n[Device B] Import result: {result.applied_count} applied, {result.conflict_count} conflicts")
        
        # Check conflicts
        conflicts = device_b.get_unresolved_conflicts()
        print(f"[Device B] Unresolved conflicts: {len(conflicts)}")
        for c in conflicts:
            print(f"  - Table: {c.table_name}, Row: {c.row_pk.hex()[:16]}...")
    
    # Cleanup
    device_a.close()
    device_b.close()
    
    for f in ["demo_conflict_a.db", "demo_conflict_b.db", "conflict_a_to_b.bundle"]:
        if os.path.exists(f):
            os.remove(f)
    
    print("\n✅ Conflict resolution demo completed!")


def demo_log_compaction():
    """Demonstrate log compaction."""
    print("\n" + "="*60)
    print("DEMO: Log Compaction")
    print("="*60 + "\n")
    
    engine = create_demo_database("Main", "demo_compaction.db")
    
    # Create many operations
    print("[Main] Creating 100 operations...")
    for i in range(100):
        engine.connection.execute(
            "INSERT INTO notes (title) VALUES (?)",
            (f"Note #{i}",)
        )
    engine.connection.commit()
    
    # Check log size
    compactor = LogCompactor(engine.connection)
    stats_before = compactor.get_log_stats()
    print(f"[Main] Operations before: {stats_before['total_operations']}")
    
    # Create snapshot
    print("[Main] Creating snapshot...")
    snapshot = compactor.create_snapshot()
    print(f"[Main] Snapshot created: {snapshot.row_count} rows, {snapshot.size_bytes} bytes")
    
    # Simulate acknowledgment and compaction
    ops = engine.get_new_operations()
    if ops:
        compactor.record_acknowledgment(os.urandom(16), ops[-1].op_id)
    
    result = compactor.compact_log()
    print(f"[Main] Compaction: {result.ops_removed} ops removed")
    
    # Cleanup
    engine.close()
    os.remove("demo_compaction.db")
    
    print("\n✅ Log compaction demo completed!")


if __name__ == "__main__":
    print("="*60)
    print("SQLite Sync Core - Desktop Demo")
    print("="*60)
    
    demo_basic_sync()
    demo_conflict_resolution()
    demo_log_compaction()
    
    print("\n" + "="*60)
    print("All demos completed successfully!")
    print("="*60)
