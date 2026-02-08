"""
manager.py - Multi-Peer Sync Coordination.

Manages a pool of SyncLoops, one for each discovered or manual peer.
Ensures that the node stays in sync with the entire network automatically.
"""

import asyncio
import logging
from typing import Dict, Optional, List

from sqlite_sync.engine import SyncEngine
from sqlite_sync.ext.sync_loop import SyncLoop, SyncLoopConfig
from sqlite_sync.transport.http_transport import HTTPTransport
from sqlite_sync.network.peer_discovery import UDPDiscovery, Peer, PeerStatus

logger = logging.getLogger(__name__)

class MultiPeerSyncManager:
    """
    Orchestrates multiple SyncLoops for a set of peers.
    
    Listens to discovery events and maintains active sync sessions
    with all available peers.
    """
    
    def __init__(
        self,
        engine: SyncEngine,
        discovery: UDPDiscovery,
        config: Optional[SyncLoopConfig] = None
    ):
        self._engine = engine
        self._discovery = discovery
        self._config = config or SyncLoopConfig()
        
        self._active_loops: Dict[str, SyncLoop] = {} # device_id -> SyncLoop
        self._lock = asyncio.Lock()
        self._running = False

    async def start(self):
        """Start the manager and hook into discovery."""
        if self._running:
            return
        self._running = True
        
        # Hook into discovery events
        self._discovery.on_peer_discovered(self._on_peer_discovered)
        self._discovery.on_peer_lost(self._on_peer_lost)
        
        # Initial check for already discovered peers
        for peer in self._discovery.get_available_peers():
            await self._on_peer_discovered(peer)
            
        logger.info("Multi-Peer Sync Manager started")

    async def stop(self):
        """Stop all active sync loops."""
        self._running = False
        async with self._lock:
            stop_tasks = []
            for loop in self._active_loops.values():
                stop_tasks.append(loop.stop())
            
            if stop_tasks:
                await asyncio.gather(*stop_tasks)
            self._active_loops.clear()
        
        logger.info("Multi-Peer Sync Manager stopped")

    async def _on_peer_discovered(self, peer: Peer):
        """Callback for discovery of a new peer."""
        if not self._running:
            return
            
        async with self._lock:
            if peer.device_id in self._active_loops:
                # Already have a loop for this peer
                return
            
            logger.info(f"Starting sync loop for newly discovered peer: {peer.device_name} ({peer.url})")
            
            # Create transport for this peer
            transport = HTTPTransport(
                base_url=peer.url,
                device_id=self._engine.device_id
            )
            
            # Create and start loop
            loop = SyncLoop(
                engine=self._engine,
                transport=transport,
                config=self._config
            )
            self._active_loops[peer.device_id] = loop
            await loop.start()

    async def _on_peer_lost(self, peer: Peer):
        """Callback for removal of a peer."""
        async with self._lock:
            if peer.device_id in self._active_loops:
                logger.info(f"Stopping sync loop for lost peer: {peer.device_name}")
                loop = self._active_loops.pop(peer.device_id)
                await loop.stop()

    def get_status(self) -> Dict:
        """Get summary of all active sync sessions."""
        return {
            "active_peers": len(self._active_loops),
            "sessions": {
                dev_id: {
                    "status": loop.status.value,
                    "stats": loop.stats.__dict__
                }
                for dev_id, loop in self._active_loops.items()
            }
        }
