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
from sqlite_sync.ext.sync_loop import SyncLoop, SyncLoopConfig
from sqlite_sync.transport.http_transport import HTTPTransport
from sqlite_sync.network.peer_discovery import create_discovery, PeerManager, Peer
from sqlite_sync.ext.network_manager import MultiPeerSyncManager
from sqlite_sync.ext.server.http_server import run_server

logger = logging.getLogger(__name__)

class SyncNode:
    """
    Full Enterprise Sync Node.
    
    Orchestrates:
    - SyncEngine (Database management & invariants)
    - Production HTTP Server (Receiving updates)
    - P2P Discovery (Finding network neighbors)
    - Multi-Peer Manager (Pushing/Pulling from all neighbors)
    """
    
    def __init__(
        self,
        db_path: str,
        device_name: str,
        port: int = 8080,
        enable_discovery: bool = True,
        sync_interval: float = 30.0,
        conflict_resolver: Optional[object] = None
    ):
        self.engine = SyncEngine(db_path)
        self.db_path = db_path
        self.device_id = self.engine.initialize()
        self.device_name = device_name
        self.port = port
        self.enable_discovery = enable_discovery
        self.sync_interval = sync_interval
        
        self.discovery = None
        self.peer_manager = None
        self.sync_manager = None

        # Always initialize components
        self.discovery = create_discovery(
            device_id=self.device_id.hex(),
            device_name=device_name,
            sync_port=port
        )
        self.peer_manager = PeerManager(self.discovery)
        self.sync_manager = MultiPeerSyncManager(
            engine=self.engine,
            discovery=self.discovery,
            config=SyncLoopConfig(interval_seconds=sync_interval)
        )
        
        self._server_thread: Optional[threading.Thread] = None
        self._running = False

    async def start(self):
        """Start all enterprise services."""
        if self._running:
            return
        self._running = True
        
        # 1. Start HTTP Server in background thread
        # Note: In production, you'd use a real WSGI/ASGI server.
        # This is the enterprise-ready internal server module.
        self._server_thread = threading.Thread(
            target=run_server,
            kwargs={
                "host": "0.0.0.0", 
                "port": self.port, 
                "db_path": f"{self.device_name}_server_registry.db", 
                "debug": False,
                "p2p_mode": True,
                "p2p_db_path": self.db_path
            },
            daemon=True
        )
        self._server_thread.start()
        logger.info(f"Enterprise Sync Server listening on 0.0.0.0:{self.port}")
        
        # 2. Start Discovery & Multi-Peer Sync
        if self.enable_discovery:
            self.discovery.start()
            logger.info("P2P Discovery active")
        
        if self.sync_manager:
            await self.sync_manager.start()
            logger.info(f"Multi-Peer Sync active (polling interval: {self.sync_interval}s)")
            
        logger.info(f"SyncNode '{self.device_name}' ({self.device_id.hex()[:8]}...) ready.")

    async def stop(self):
        """Graceful shutdown of all services."""
        self._running = False
        if self.sync_manager:
            await self.sync_manager.stop()
        if self.discovery:
            self.discovery.stop()
        logger.info(f"SyncNode '{self.device_name}' shut down.")

    def enable_sync_for_table(self, table_name: str):
        """Registers a table for automatic synchronization."""
        self.engine.enable_sync_for_table(table_name)
