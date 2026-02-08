"""
sync_loop.py - Background sync loop with automatic scheduling.

Provides:
- Automatic background sync
- Retry logic with exponential backoff
- Sync status tracking
- Partial/delta sync
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from sqlite_sync.transport.base import TransportAdapter
from sqlite_sync.engine import SyncEngine
from sqlite_sync.resolution import ConflictResolver, ResolutionStrategy, get_resolver

logger = logging.getLogger(__name__)


class SyncStatus(Enum):
    """Current sync status."""
    IDLE = "idle"
    SYNCING = "syncing"
    WAITING_RETRY = "waiting_retry"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass
class SyncStats:
    """Statistics for sync operations."""
    last_sync_time: float = 0
    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    ops_sent: int = 0
    ops_received: int = 0
    conflicts_resolved: int = 0
    conflicts_pending: int = 0


@dataclass
class SyncLoopConfig:
    """Configuration for sync loop."""
    interval_seconds: float = 30.0
    retry_base_seconds: float = 5.0
    retry_max_seconds: float = 300.0
    max_retries: int = 10
    batch_size: int = 100
    resolution_strategy: ResolutionStrategy = ResolutionStrategy.LAST_WRITE_WINS


class SyncLoop:
    """
    Background sync loop manager.
    
    Handles automatic periodic synchronization with:
    - Configurable intervals
    - Exponential backoff on failures
    - Automatic conflict resolution
    - Status callbacks
    """
    
    def __init__(
        self,
        engine: SyncEngine,
        transport: TransportAdapter,
        config: SyncLoopConfig | None = None,
        on_status_change: Callable[[SyncStatus], None] | None = None,
        on_sync_complete: Callable[[SyncStats], None] | None = None
    ):
        self._engine = engine
        self._transport = transport
        self._config = config or SyncLoopConfig()
        self._on_status_change = on_status_change
        self._on_sync_complete = on_sync_complete
        
        self._status = SyncStatus.STOPPED
        self._stats = SyncStats()
        self._retry_count = 0
        self._running = False
        self._task: asyncio.Task | None = None
        
        self._resolver = get_resolver(self._config.resolution_strategy)
    
    @property
    def status(self) -> SyncStatus:
        return self._status
    
    @property
    def stats(self) -> SyncStats:
        return self._stats
    
    async def start(self) -> None:
        """Start the background sync loop."""
        if self._running:
            return
            
        self._running = True
        self._set_status(SyncStatus.IDLE)
        self._task = asyncio.create_task(self._loop())
        logger.info("Sync loop started")
    
    async def stop(self) -> None:
        """Stop the background sync loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._set_status(SyncStatus.STOPPED)
        logger.info("Sync loop stopped")
    
    async def sync_now(self) -> bool:
        """Trigger immediate sync."""
        return await self._do_sync()
    
    async def _loop(self) -> None:
        """Main sync loop."""
        sync_count = 0
        while self._running:
            try:
                await self._do_sync()
                self._retry_count = 0
                
                # Auto-compaction every 10 syncs
                sync_count += 1
                if sync_count >= 10:
                    logger.info("Triggering auto-compaction")
                    self._engine.compact_log()
                    sync_count = 0
                    
                await asyncio.sleep(self._config.interval_seconds)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sync error: {e}")
                self._stats.failed_syncs += 1
                await self._handle_retry()
    
    async def _do_sync(self) -> bool:
        """Perform one sync cycle."""
        self._set_status(SyncStatus.SYNCING)
        
        try:
            # Ensure connected
            if not self._transport.is_connected():
                if not await self._transport.connect():
                    raise ConnectionError("Failed to connect to remote")
            
            # Exchange vector clocks
            local_vc = self._engine.get_vector_clock()
            remote_vc = await self._transport.exchange_vector_clock(local_vc)
            
            # Get and send local ops
            local_ops = self._engine.get_new_operations(remote_vc)
            if local_ops:
                sent = await self._transport.send_operations(local_ops)
                self._stats.ops_sent += sent
            
            # Receive and apply remote ops
            remote_ops = await self._transport.receive_operations()
            if remote_ops:
                # Use apply_batch to handle conflicts/merging
                # We need source_device_id. SyncOperation has device_id, let's use the first one's ID
                # or derive from transport if possible.
                # Assuming all ops in a batch come from the same source for now.
                source_id = remote_ops[0].device_id 
                
                result = self._engine.apply_batch(remote_ops, source_id)
                self._stats.ops_received += result.applied_count
                self._stats.conflicts_resolved += result.conflict_count
            
            # Update stats
            self._stats.last_sync_time = time.time()
            self._stats.total_syncs += 1
            self._stats.successful_syncs += 1
            
            self._set_status(SyncStatus.IDLE)
            
            if self._on_sync_complete:
                self._on_sync_complete(self._stats)
            
            return True
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            self._set_status(SyncStatus.ERROR)
            return False
    
    async def _handle_retry(self) -> None:
        """Handle retry with exponential backoff."""
        if self._retry_count >= self._config.max_retries:
            logger.error("Max retries exceeded, stopping sync loop")
            self._set_status(SyncStatus.ERROR)
            self._running = False
            return
        
        self._retry_count += 1
        wait_time = min(
            self._config.retry_base_seconds * (2 ** self._retry_count),
            self._config.retry_max_seconds
        )
        
        self._set_status(SyncStatus.WAITING_RETRY)
        logger.info(f"Retry {self._retry_count}/{self._config.max_retries} in {wait_time}s")
        await asyncio.sleep(wait_time)
    
    def _set_status(self, status: SyncStatus) -> None:
        """Update status and notify callback."""
        self._status = status
        if self._on_status_change:
            self._on_status_change(status)
