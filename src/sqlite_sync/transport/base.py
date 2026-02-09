"""
base.py - Abstract base class for transport adapters.

All transport implementations must inherit from TransportAdapter.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any
from sqlite_sync.log.operations import SyncOperation


@dataclass
class SyncResult:
    """Result of a sync operation."""
    sent_count: int
    received_count: int
    conflict_count: int
    error: str | None = None


class TransportAdapter(ABC):
    """
    Abstract base class for sync transport adapters.
    
    Implementations must provide methods for:
    - Connecting to remote endpoint
    - Sending operations
    - Receiving operations
    - Health checking
    """
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to remote endpoint."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection."""
        pass
    
    @abstractmethod
    async def send_operations(self, operations: list[SyncOperation]) -> int:
        """
        Send operations to remote.
        
        Returns:
            Number of operations successfully sent
        """
        pass
    
    @abstractmethod
    async def receive_operations(self) -> list[SyncOperation]:
        """
        Receive pending operations from remote.
        
        Returns:
            List of received operations
        """
        pass
    
    @abstractmethod
    async def exchange_vector_clock(self, local_vc: dict[str, int], schema_version: int = 0) -> dict[str, int]:
        """
        Exchange vector clocks with remote to determine what to sync.
         Also verifies schema compatibility.
        
        Args:
            local_vc: Local vector clock
            schema_version: Local schema version
            
        Returns:
            Remote's vector clock
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connection is active."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Transport name for logging."""
        pass
