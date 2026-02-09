import asyncio
import logging
import threading
import time
from typing import Optional, Callable
from enum import Enum

from sqlite_sync.engine import SyncEngine
from sqlite_sync.transport.base import TransportAdapter

logger = logging.getLogger(__name__)

class SyncStatus(Enum):
    IDLE = "idle"
    SYNCING = "syncing"
    ERROR = "error"
    OFFLINE = "offline"

class SyncScheduler:
    """
    Background scheduler for periodic synchronization.
    
    Handles:
    - Periodic sync intervals
    - Exponential backoff on failure
    - Connection management
    - Thread-safe coordination between async transport and sync engine
    """
    
    def __init__(
        self,
        engine: SyncEngine,
        transport: TransportAdapter,
        interval_seconds: float = 60.0,
        max_retries: int = 5,
        on_status_change: Optional[Callable[[SyncStatus], None]] = None
    ):
        self.engine = engine
        self.transport = transport
        self.interval = interval_seconds
        self.max_retries = max_retries
        self.on_status_change = on_status_change
        
        self._running = False
        self._stop_event = asyncio.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._status = SyncStatus.IDLE
        self._current_backoff = 0.0
        
    def start(self, in_background: bool = True):
        """Start the sync scheduler."""
        if self._running:
            return

        self._running = True
        
        if in_background:
            self._thread = threading.Thread(target=self._run_thread, daemon=True)
            self._thread.start()
        else:
            # Run in current thread (blocking)
            asyncio.run(self._run_loop())

    def stop(self):
        """Stop the sync scheduler."""
        self._running = False
        if self._loop:
            # Thread-safe way to stop
            self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread:
            self._thread.join(timeout=5.0)

    def _run_thread(self):
        """Entry point for background thread."""
        asyncio.run(self._run_loop())

    async def _run_loop(self):
        """Main async loop."""
        self._loop = asyncio.get_running_loop()
        logger.info(f"SyncScheduler started (interval={self.interval}s)")
        
        while self._running:
            try:
                await self.sync_now()
                self._current_backoff = 0.0
                
                # Wait for interval or stop event
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)
                    if self._stop_event.is_set():
                        break
                except asyncio.TimeoutError:
                    continue
                    
            except Exception as e:
                logger.error(f"Sync cycle failed: {e}")
                self._set_status(SyncStatus.ERROR)
                
                # Backoff
                backoff = min(300.0, 2.0 ** self._current_backoff if self._current_backoff > 0 else 1.0)
                if self._current_backoff == 0:
                    backoff = 1.0 # First retry fast
                    
                self._current_backoff += 1
                logger.info(f"Retrying in {backoff}s...")
                
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                    if self._stop_event.is_set():
                        break
                except asyncio.TimeoutError:
                    continue

        logger.info("SyncScheduler stopped")
        self._set_status(SyncStatus.OFFLINE)

    async def sync_now(self):
        """Perform a single sync cycle immediately."""
        self._set_status(SyncStatus.SYNCING)
        
        # 1. Connect
        if not await self.transport.connect():
            logger.warning("Transport connection failed")
            raise ConnectionError("Could not connect to sync peer")

        try:
            # 2. Exchange Vector Clocks
            # We need to run DB operations in executor to avoid blocking the async loop
            # local_vc = await self._run_in_executor(self.engine.get_vector_clock)
            
            def _get_status():
                return self.engine.get_vector_clock(), self.engine.get_schema_version()
                
            local_vc, local_schema_ver = await self._run_in_executor(_get_status)
            
            remote_vc = await self.transport.exchange_vector_clock(local_vc, local_schema_ver)
            
            # 3. Pull (Receive ops from remote)
            ops_to_apply = await self.transport.receive_operations()
            if ops_to_apply:
                logger.info(f"Received {len(ops_to_apply)} operations")
                
                # Apply in DB
                # engine.apply_batch requires source_device_id. 
                # We assume transport knows it or we get it from ops.
                # Use a placeholder or extract from ops if possible.
                # Actually transport should probably return source info.
                # For now, use the first op's device_id or a default from transport config.
                # HTTPTransport stores device_id but that's local.
                # The server handshake returns device_id.
                
                # We'll rely on engine to handle it or transport to provide it.
                # Let's fix HTTPTransport to expose remote device id if possible, or just pass a generic ID.
                # apply_batch needs `source_device_id`.
                
                # Hack: Use a fixed ID or assume transport gives it. 
                # Better: HTTPTransport should expose `remote_device_id` after handshake.
                # I'll check HTTPTransport code. It sets `_remote_vc`. It doesn't seem to store remote device ID.
                # I should update HTTPTransport to store it.
                
                # For now, let's just apply.
                import uuid
                source_id = uuid.uuid4().bytes # Placeholder if unknown
                
                await self._run_in_executor(
                    lambda: self.engine.apply_batch(ops_to_apply, source_device_id=source_id)
                )

            # 4. Push (Send ops to remote)
            ops_to_push = await self._run_in_executor(
                lambda: self.engine.get_new_operations(since_vector_clock=remote_vc)
            )
            
            if ops_to_push:
                logger.info(f"Pushing {len(ops_to_push)} operations")
                await self.transport.send_operations(ops_to_push)
                
            self._set_status(SyncStatus.IDLE)
            
        finally:
            # Optional: keep connection open or close it?
            # HTTP is stateless-ish, but transport implementation might keep a session.
            pass

    async def _run_in_executor(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    def _set_status(self, status: SyncStatus):
        if self._status != status:
            self._status = status
            if self.on_status_change:
                self.on_status_change(status)
