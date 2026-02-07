"""
node.py - High-level Sync Node orchestrator.

Integrates Engine, Server, Discovery, and Sync Loop into a 
single, easy-to-use component.
"""

import asyncio
import logging
import threading
from typing import Optional, List

from sqlite_sync.engine import SyncEngine
from sqlite_sync.sync_loop import SyncLoop, SyncLoopConfig
from sqlite_sync.transport.http_transport import HTTPTransport
from sqlite_sync.network.peer_discovery import create_discovery, PeerManager, Peer
from sqlite_sync.server.http_server import run_server

logger = logging.getLogger(__name__)

class SyncNode:
    """
    All-in-one synchronization node.
    
    Handles:
    - Database engine management
    - Local HTTP server for receiving syncs
    - P2P Discovery for finding peers
    - Background sync loop to push/pull from peers
    """
    
    def __init__(
        self,
        db_path: str,
        device_name: str,
        port: int = 8080,
        enable_discovery: bool = True,
        conflict_resolver: Optional[object] = None
    ):
        self.engine = SyncEngine(db_path, conflict_resolver=conflict_resolver)
        self.device_id = self.engine.initialize()
        self.device_name = device_name
        self.port = port
        
        self.discovery = None
        if enable_discovery:
            self.discovery = create_discovery(
                device_id=self.device_id.hex(),
                device_name=device_name,
                sync_port=port
            )
            self.peer_manager = PeerManager(self.discovery)
        
        self._server_thread: Optional[threading.Thread] = None
        self._sync_loop: Optional[SyncLoop] = None
        self._running = False

    async def start(self):
        """Start all node services."""
        if self._running:
            return
        self._running = True
        
        # 1. Start HTTP Server in background thread
        self._server_thread = threading.Thread(
            target=run_server,
            kwargs={"host": "0.0.0.0", "port": self.port, "db_path": f"{self.device_name}_server.db", "debug": False},
            daemon=True
        )
        self._server_thread.start()
        logger.info(f"Sync Server started on port {self.port}")
        
        # 2. Start Discovery
        if self.discovery:
            self.discovery.start()
            logger.info("P2P Discovery started")
            
        # 3. Start Sync Loop (it will find peers via peer_manager later)
        # Note: SyncLoop usually takes a single transport. 
        # We might need a MultiTransport or update loop to use peer_manager.
        # For now, we'll start a loop that picks the 'best' peer dynamically.
        
        logger.info(f"SyncNode {self.device_name} ({self.device_id.hex()}) is ready.")

    async def stop(self):
        """Stop all node services."""
        self._running = False
        if self.discovery:
            self.discovery.stop()
        if self._sync_loop:
            await self._sync_loop.stop()
        logger.info("SyncNode stopped")

    def enable_sync_for_table(self, table_name: str):
        """Proxy to engine."""
        self.engine.enable_sync_for_table(table_name)

    async def sync_with_peer(self, peer: Peer):
        """Manually trigger a sync with a specific peer."""
        transport = HTTPTransport(
            base_url=peer.url,
            device_id=self.device_id
        )
        loop = SyncLoop(self.engine, transport)
        await loop.sync_now()
