"""
http_transport.py - HTTP-based sync transport.

Uses REST API (FastAPI) for synchronization between devices.
"""

import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Any, List, Dict, Optional
import httpx

from sqlite_sync.transport.base import TransportAdapter, SyncResult
from sqlite_sync.log.operations import SyncOperation

logger = logging.getLogger(__name__)


class HTTPTransport(TransportAdapter):
    """
    HTTP REST transport for sync operations.
    
    Endpoints expected on server:
    - POST /sync/handshake - Exchange vector clocks
    - POST /sync/push - Send operations
    - POST /sync/pull - Receive operations
    """
    
    def __init__(
        self, 
        base_url: str,
        device_id: bytes,
        auth_token: str | None = None,
        timeout: float = 30.0
    ):
        self._base_url = base_url.rstrip('/')
        self._device_id = device_id
        self._auth_token = auth_token
        self._timeout = timeout
        self._connected = False
        self._remote_vc: dict[str, int] = {}
        self._client = httpx.AsyncClient(timeout=timeout)
    
    @property
    def name(self) -> str:
        return "HTTP"
    
    async def connect(self) -> bool:
        """Test connection with a handshake request."""
        try:
            response = await self._client.get(f"{self._base_url}/sync/health")
            response.raise_for_status()
            data = response.json()
            self._connected = data.get("status") == "ok"
            return self._connected
        except Exception as e:
            logger.error(f"HTTP connect failed: {e}")
            self._connected = False
            return False
    
    async def disconnect(self) -> None:
        self._connected = False
        await self._client.aclose()
    
    def is_connected(self) -> bool:
        return self._connected
    
    async def exchange_vector_clock(self, local_vc: dict[str, int], schema_version: int = 0) -> dict[str, int]:
        """Exchange vector clocks to determine sync delta."""
        try:
            response = await self._client.post(
                f"{self._base_url}/sync/handshake",
                json={
                    "device_id": self._device_id.hex(),
                    "vector_clock": local_vc,
                    "schema_version": schema_version
                }
            )
            response.raise_for_status()
            data = response.json()
            self._remote_vc = data.get("vector_clock", {})
            return self._remote_vc
        except Exception as e:
            logger.error(f"Handshake failed: {e}")
            raise
    
    async def send_operations(self, operations: list[SyncOperation]) -> int:
        """Send operations to remote server."""
        if not operations:
            return 0
            
        serialized = [self._serialize_op(op) for op in operations]
        try:
            response = await self._client.post(
                f"{self._base_url}/sync/push",
                json={
                    "device_id": self._device_id.hex(),
                    "operations": serialized
                }
            )
            response.raise_for_status()
            data = response.json()
            return data.get("accepted_count", 0)
        except Exception as e:
            logger.error(f"Push failed: {e}")
            raise
    
    async def receive_operations(self) -> list[SyncOperation]:
        """Receive new operations from remote."""
        try:
            response = await self._client.post(
                f"{self._base_url}/sync/pull",
                json={
                    "device_id": self._device_id.hex(),
                    "since_vector_clock": self._remote_vc
                }
            )
            response.raise_for_status()
            data = response.json()
            
            ops_data = data.get("operations", [])
            return [self._deserialize_op(op) for op in ops_data]
        except Exception as e:
            logger.error(f"Pull failed: {e}")
            raise
    
    def _serialize_op(self, op: SyncOperation) -> dict:
        """Serialize operation for JSON transport."""
        return {
            "op_id": op.op_id.hex(),
            "device_id": op.device_id.hex(),
            "parent_op_id": op.parent_op_id.hex() if op.parent_op_id else None,
            "vector_clock": op.vector_clock,
            "table_name": op.table_name,
            "op_type": op.op_type,
            "row_pk": op.row_pk.hex(),
            "old_values": op.old_values.hex() if op.old_values else None,
            "new_values": op.new_values.hex() if op.new_values else None,
            "schema_version": op.schema_version,
            "created_at": op.created_at,
            "hlc": op.hlc,
        }
    
    def _deserialize_op(self, data: dict) -> SyncOperation:
        """Deserialize operation from JSON."""
        return SyncOperation(
            op_id=bytes.fromhex(data["op_id"]),
            device_id=bytes.fromhex(data["device_id"]),
            parent_op_id=bytes.fromhex(data["parent_op_id"]) if data.get("parent_op_id") else None,
            vector_clock=data["vector_clock"],
            table_name=data["table_name"],
            op_type=data["op_type"],
            row_pk=bytes.fromhex(data["row_pk"]),
            old_values=bytes.fromhex(data["old_values"]) if data.get("old_values") else None,
            new_values=bytes.fromhex(data["new_values"]) if data.get("new_values") else None,
            schema_version=data["schema_version"],
            created_at=data["created_at"],
            hlc=data.get("hlc"),
            is_local=False,
            applied_at=None
        )
