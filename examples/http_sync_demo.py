"""
HTTP Sync Demo - Client/Server Sync

Demonstrates real network synchronization using HTTP transport.

Usage:
1. In terminal 1: python examples/http_sync_demo.py --server
2. In terminal 2: python examples/http_sync_demo.py --client device_a
3. In terminal 3: python examples/http_sync_demo.py --client device_b
"""

import os
import sys
import time
import argparse
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sqlite_sync.engine import SyncEngine
from sqlite_sync.transport.http_transport import HTTPTransport
from sqlite_sync.sync_loop import SyncLoop, SyncLoopConfig, SyncStatus


def run_server():
    """Run the sync server."""
    try:
        from sqlite_sync.server.http_server import run_server
        print("Starting HTTP sync server on http://localhost:8080")
        print("Press Ctrl+C to stop")
        run_server(host="localhost", port=8080, debug=True)
    except ImportError:
        print("Flask required: pip install flask")
        print("Or: pip install sqlite-sync-core[server]")
        sys.exit(1)


async def run_client(device_name: str):
    """Run a sync client."""
    db_path = f"http_demo_{device_name}.db"
    
    # Initialize engine
    if os.path.exists(db_path):
        os.remove(db_path)
    
    engine = SyncEngine(db_path)
    device_id = engine.initialize()
    
    print(f"[{device_name}] Device ID: {device_id.hex()[:8]}...")
    
    # Create demo table
    engine.connection.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            sender TEXT NOT NULL,
            content TEXT NOT NULL,
            sent_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
    """)
    engine.connection.commit()
    engine.enable_sync_for_table("messages")
    
    # Create transport
    transport = HTTPTransport(
        base_url="http://localhost:8080",
        device_id=device_id
    )
    
    # Create sync loop
    def on_status(status: SyncStatus):
        print(f"[{device_name}] Sync status: {status.value}")
    
    loop = SyncLoop(
        engine=engine,
        transport=transport,
        config=SyncLoopConfig(interval_seconds=5),
        on_status_change=on_status
    )
    
    # Connect
    print(f"[{device_name}] Connecting to server...")
    if await transport.connect():
        print(f"[{device_name}] Connected!")
    else:
        print(f"[{device_name}] Failed to connect. Is server running?")
        engine.close()
        return
    
    # Start sync loop
    await loop.start()
    
    # Interactive mode
    print(f"\n[{device_name}] Commands:")
    print("  send <message>  - Send a message")
    print("  list            - List all messages")
    print("  sync            - Force sync now")
    print("  quit            - Exit")
    print()
    
    try:
        while True:
            cmd = await asyncio.get_event_loop().run_in_executor(
                None, lambda: input(f"[{device_name}] > ")
            )
            
            if cmd.startswith("send "):
                message = cmd[5:]
                engine.connection.execute(
                    "INSERT INTO messages (sender, content) VALUES (?, ?)",
                    (device_name, message)
                )
                engine.connection.commit()
                print(f"Message sent: {message}")
                
            elif cmd == "list":
                cursor = engine.connection.execute(
                    "SELECT sender, content, sent_at FROM messages ORDER BY sent_at"
                )
                for sender, content, sent_at in cursor:
                    print(f"  [{sender}] {content}")
                    
            elif cmd == "sync":
                await loop.sync_now()
                print("Sync completed")
                
            elif cmd == "quit":
                break
                
    except KeyboardInterrupt:
        pass
    
    # Cleanup
    await loop.stop()
    await transport.disconnect()
    engine.close()
    
    print(f"[{device_name}] Goodbye!")


def main():
    parser = argparse.ArgumentParser(description="HTTP Sync Demo")
    parser.add_argument("--server", action="store_true", help="Run as server")
    parser.add_argument("--client", type=str, help="Run as client with given name")
    args = parser.parse_args()
    
    if args.server:
        run_server()
    elif args.client:
        asyncio.run(run_client(args.client))
    else:
        print("Usage:")
        print("  Server: python http_sync_demo.py --server")
        print("  Client: python http_sync_demo.py --client <name>")


if __name__ == "__main__":
    main()
