
import asyncio
import os
import threading
import time
import requests
import json
import logging
from sqlite_sync.ext.node import SyncNode
from sqlite_sync.log.operations import SyncOperation

# Configure logging
logging.basicConfig(level=logging.INFO)
# Silence noisy libraries
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# Enable debug for our package
logging.getLogger("sqlite_sync").setLevel(logging.DEBUG)

logger = logging.getLogger("layer2_test")

DB_A = "node_a_l2.db"
DB_B = "node_b_l2.db"
PORT_A = 8081
PORT_B = 8082

def cleanup():
    for f in [DB_A, DB_B, f"NodeA_server_registry.db", f"NodeB_server_registry.db"]:
        if os.path.exists(f):
            try: os.remove(f)
            except: pass

async def run_test():
    cleanup()
    
    print("\n--- STARTING LAYER 2 INTEGRATION TEST ---\n")
    
    # 1. Start Two Nodes
    print("1. Initializing Nodes...")
    node_a = SyncNode(DB_A, "NodeA", port=PORT_A, enable_discovery=False, sync_interval=2)
    node_b = SyncNode(DB_B, "NodeB", port=PORT_B, enable_discovery=False, sync_interval=2)
    
    await node_a.start()
    await node_b.start()
    
    # Create tables first!
    conn_a = node_a.engine.connection
    conn_a.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, city TEXT)")
    conn_a.commit()
    
    conn_b = node_b.engine.connection
    conn_b.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, city TEXT)")
    conn_b.commit()
    
    node_a.enable_sync_for_table("users")
    node_b.enable_sync_for_table("users")
    
    # 2. Manual Peering (Bypass UDP)
    print("\n2. Peering Nodes manually...")
    # Inject B as peer for A
    from sqlite_sync.network.peer_discovery import Peer, PeerStatus
    peer_b = Peer(node_b.engine.device_id.hex(), "NodeB", "127.0.0.1", PORT_B, "http", PeerStatus.AVAILABLE, time.time())
    await node_a.sync_manager._on_peer_discovered(peer_b)
    
    # Inject A as peer for B
    peer_a = Peer(node_a.engine.device_id.hex(), "NodeA", "127.0.0.1", PORT_A, "http", PeerStatus.AVAILABLE, time.time())
    await node_b.sync_manager._on_peer_discovered(peer_a)
    
    # 3. Create Conflict Scenario
    print("\n3. Creating Concurrent Edits...")
    
    # A inserts
    conn_a = node_a.engine.connection
    conn_a.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, city TEXT)")
    conn_a.execute("INSERT INTO users (id, name, city) VALUES ('user1', 'Alice', 'New York')")
    conn_a.commit()
    
    # Verify sync_operations
    ops_count = conn_a.execute("SELECT count(*) FROM sync_operations").fetchone()[0]
    print(f"DEBUG: Node A sync_operations count: {ops_count}")
    
    # Wait for sync
    print("   Waiting for A -> B sync...")
    await asyncio.sleep(4)
    
    # Verify B has it
    conn_b = node_b.engine.connection
    conn_b.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, city TEXT)")
    res = conn_b.execute("SELECT * FROM users WHERE id='user1'").fetchone()
    
    import pprint
    print("\n--- SYNC STATS ---")
    if node_a.sync_manager:
        print("Node A Stats:")
        pprint.pprint(node_a.sync_manager.get_status())
    if node_b.sync_manager:
        print("Node B Stats:")
        pprint.pprint(node_b.sync_manager.get_status())
    print("------------------\n")

    if not res:
        print("FAIL: Node B did not receive initial insert")
        return
    print(f"   Node B received: {res}")
    
    # Concurrent Update
    print("   Performing concurrent updates...")
    conn_a.execute("UPDATE users SET name='Alice-Updated' WHERE id='user1'") # A changes name
    conn_b.execute("UPDATE users SET city='Paris' WHERE id='user1'")         # B changes city
    conn_a.commit()
    conn_b.commit()
    
    # Wait for sync/merge
    print("   Waiting for bidirectional sync...")
    await asyncio.sleep(10)
    
    # 4. Verify Convergence
    print("\n4. Verifying Convergence...")
    
    row_a = conn_a.execute("SELECT * FROM users WHERE id='user1'").fetchone()
    row_b = conn_b.execute("SELECT * FROM users WHERE id='user1'").fetchone()
    
    print(f"   Node A Final: {row_a}")
    print(f"   Node B Final: {row_b}")
    
    success = True
    # Tuple indices: 0=id, 1=name, 2=city
    if row_a[1] != 'Alice-Updated':
        print("FAIL: Name update lost on A")
        success = False
    if row_a[2] != 'Paris':
        print("FAIL: City update lost on A")
        success = False
    if row_a != row_b:
        print("FAIL: Nodes Diverged!")
        success = False
        
    if success:
        print("\nSUCCESS: Full Field-Level Convergence via HTTP/Network Layer!")
    
    await node_a.stop()
    await node_b.stop()
    cleanup()

if __name__ == "__main__":
    asyncio.run(run_test())
