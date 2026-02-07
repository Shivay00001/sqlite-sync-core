import asyncio
import os
import threading
import time
from sqlite_sync.engine import SyncEngine
from sqlite_sync.network.server import SyncServer
from sqlite_sync.network.client import SyncClient

async def run_peer(db_path, name):
    if os.path.exists(db_path):
        os.remove(db_path)
        
    engine = SyncEngine(db_path)
    engine.initialize()
    engine.connection.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, content TEXT)")
    engine.enable_sync_for_table("notes")
    
    client = SyncClient(engine, "ws://localhost:8765")
    await client.connect()
    
    # Start listening for operations in the background
    asyncio.create_task(client.listen())
    
    # Add a local change
    print(f"[{name}] Inserting note...")
    engine.connection.execute(f"INSERT INTO notes (content) VALUES ('Hello from {name}')")
    engine.connection.commit()
    
    # Send all new operations to the peer
    ops = engine.get_new_operations()
    for op in ops:
        await client.send_operation(op)
    
    # Wait a bit to receive updates
    await asyncio.sleep(2)
    
    cursor = engine.connection.execute("SELECT * FROM notes")
    print(f"[{name}] Final notes: {cursor.fetchall()}")
    
    engine.close()

async def main():
    # Start the server in the background
    server = SyncServer()
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(1) # Wait for server to start
    
    # Run two peers
    await asyncio.gather(
        run_peer("peer_a.db", "Peer A"),
        run_peer("peer_b.db", "Peer B")
    )
    
    server_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
