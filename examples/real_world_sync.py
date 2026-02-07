"""
real_world_sync.py - Demonstrating SyncNode for simple, powerful synchronization.

This example shows how to set up two nodes and sync them with minimal code.
"""

import asyncio
import logging
import os
import shutil
from sqlite_sync import SyncNode
from sqlite_sync.resolution import ResolutionStrategy, get_resolver

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(name)s: %(message)s')

async def main():
    # Cleanup old DBs
    for db in ["node1.db", "node2.db", "node1_server.db", "node2_server.db"]:
        if os.path.exists(db):
            os.remove(db)

    print("--- 1. Initialize SyncNodes ---")
    # Node 1 uses Last-Write-Wins (LWW)
    node1 = SyncNode("node1.db", "Node-A", port=8081)
    
    # Node 2 uses Field-Level Merge
    merge_resolver = get_resolver(ResolutionStrategy.FIELD_MERGE, prefer_local=True)
    node2 = SyncNode("node2.db", "Node-B", port=8082, conflict_resolver=merge_resolver)

    await node1.start()
    await node2.start()

    print("\n--- 2. Enable Sync for Tables ---")
    node1.enable_sync_for_table("tasks")
    node2.enable_sync_for_table("tasks")

    # Create table on both (manually for demo, in real apps you'd handle this)
    for node in [node1, node2]:
        node.engine.connection.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, status TEXT)")
        node.engine.connection.commit()

    print("\n--- 3. Make Local Changes ---")
    node1.engine.connection.execute("INSERT INTO tasks (id, title, status) VALUES (1, 'Buy Milk', 'pending')")
    node1.engine.connection.commit()

    node2.engine.connection.execute("INSERT INTO tasks (id, title, status) VALUES (2, 'Clean Room', 'pending')")
    node2.engine.connection.commit()

    print("\n--- 4. Simulate a Conflict ---")
    # Both nodes update the same row differently
    node1.engine.connection.execute("UPDATE tasks SET status = 'DONE' WHERE id = 1")
    node1.engine.connection.commit()
    
    # Wait a tiny bit to ensure timestamp order
    await asyncio.sleep(0.1)
    
    node2.engine.connection.execute("UPDATE tasks SET status = 'CANCELLED' WHERE id = 1")
    node2.engine.connection.commit()

    print("\n--- 5. Trigger Sync ---")
    # Normally Discovery would handle this, but we'll do it manually for immediate feedback
    from sqlite_sync.network.peer_discovery import Peer
    peer_b = Peer(device_id=node2.device_id.hex(), device_name="Node-B", host="127.0.0.1", port=8082)
    
    await node1.sync_with_peer(peer_b)

    print("\n--- 6. Verify Results ---")
    cursor = node1.engine.connection.execute("SELECT * FROM tasks WHERE id = 1")
    row = cursor.fetchone()
    print(f"Node-A Task 1 Status: {row[2]} (Expected: CANCELLED due to LWW or Merge)")

    print("\n--- 7. Schema Migration Test ---")
    print("Adding 'priority' column to Node-A...")
    node1.engine.migrate_schema("tasks", "priority", "INTEGER", default_value=1)
    
    print("\n--- Shutting Down ---")
    await node1.stop()
    await node2.stop()

if __name__ == "__main__":
    asyncio.run(main())
