"""
cli/main.py - Turn-Key CLI for sqlite-sync-core.

Provides a production-ready interface to start nodes, manage schemas,
and monitor sync status.
"""

import asyncio
import argparse
import logging
import sys
import signal
from typing import Optional

from sqlite_sync.node import SyncNode
from sqlite_sync.resolution import ResolutionStrategy, get_resolver

# Configure production logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("sqlite-sync-cli")

class CLIManager:
    def __init__(self):
        self.node: Optional[SyncNode] = None
        self._stop_event = asyncio.Event()

    def _handle_signals(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop = asyncio.get_running_loop()
                loop.add_signal_handler(sig, lambda: self._stop_event.set())
            except (NotImplementedError, RuntimeError):
                # Signal handlers not supported on all platforms/thread configs
                pass

    async def start_node(self, args):
        """Launches a full sync node."""
        strategy = ResolutionStrategy(args.resolution.lower())
        resolver = get_resolver(strategy)
        
        self.node = SyncNode(
            db_path=args.db,
            device_name=args.name,
            port=args.port,
            sync_interval=args.interval,
            conflict_resolver=resolver
        )
        
        # Enable sync for tables
        if args.tables:
            for table in args.tables:
                self.node.enable_sync_for_table(table)
        
        await self.node.start()
        
        print(f"\n🚀 SQLITE-SYNC NODE ACTIVE")
        print(f"   Name:     {args.name}")
        print(f"   Database: {args.db}")
        print(f"   Port:     {args.port}")
        print(f"   Syncing:  {', '.join(args.tables) if args.tables else 'None'}")
        print(f"   Press Ctrl+C to stop\n")

        self._handle_signals()
        await self._stop_event.wait()
        
        print("\nStopping node...")
        await self.node.stop()

def main():
    parser = argparse.ArgumentParser(description="sqlite-sync-core CLI: Turn-key synchronization")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # 'start' command
    start_parser = subparsers.add_parser("start", help="Start a synchronization node")
    start_parser.add_argument("--db", required=True, help="Path to SQLite database")
    start_parser.add_argument("--name", required=True, help="Display name for this device")
    start_parser.add_argument("--port", type=int, default=8080, help="Listen port (default: 8080)")
    start_parser.add_argument("--interval", type=float, default=30.0, help="Sync interval in seconds")
    start_parser.add_argument("--tables", nargs="+", help="Tables to synchronize")
    start_parser.add_argument("--resolution", default="lww", choices=["lww", "merge", "manual"], 
                             help="Conflict resolution strategy")

    # 'migrate' command
    migrate_parser = subparsers.add_parser("migrate", help="Evolve database schema")
    migrate_parser.add_argument("--db", required=True, help="Path to SQLite database")
    migrate_parser.add_argument("--table", required=True, help="Table to modify")
    migrate_parser.add_argument("--add-column", required=True, help="Column name to add")
    migrate_parser.add_argument("--type", default="TEXT", help="Column type")

    args = parser.parse_args()

    if args.command == "start":
        manager = CLIManager()
        asyncio.run(manager.start_node(args))
    elif args.command == "migrate":
        from sqlite_sync.engine import SyncEngine
        engine = SyncEngine(args.db)
        engine.migrate_schema(args.table, args.add_column, args.type)
        print(f"✅ Schema migration applied: Added '{args.add_column}' to '{args.table}'")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
