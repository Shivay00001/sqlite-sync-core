"""
http_transport.py - HTTP-based sync transport.

Uses REST API for synchronization between devices.
"""

import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from sqlite_sync.transport.base import TransportAdapter, SyncResult
from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.utils.msgpack_codec import pack_value, unpack_value

logger = logging.getLogger(__name__)


class HTTPTransport(TransportAdapter):
    """
    HTTP REST transport for sync operations.
    
    Endpoints expected on server:
    - POST /sync/handshake - Exchange vector clocks
    - POST /sync/push - Send operations
    - GET /sync/pull?since=<vc> - Receive operations
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
    
    @property
    def name(self) -> str:
        return "HTTP"
    
    async def connect(self) -> bool:
        """Test connection with a handshake request."""
        try:
            response = await self._request("GET", "/sync/health")
            self._connected = response.get("status") == "ok"
            return self._connected
        except Exception as e:
            logger.error(f"HTTP connect failed: {e}")
            self._connected = False
            return False
    
    async def disconnect(self) -> None:
        self._connected = False
    
    def is_connected(self) -> bool:
        return self._connected
    
    async def exchange_vector_clock(self, local_vc: dict[str, int]) -> dict[str, int]:
        """Exchange vector clocks to determine sync delta."""
        response = await self._request("POST", "/sync/handshake", {
            "device_id": self._device_id.hex(),
            "vector_clock": local_vc
        })
        self._remote_vc = response.get("vector_clock", {})
        return self._remote_vc
    
    async def send_operations(self, operations: list[SyncOperation]) -> int:
        """Send operations to remote server."""
        if not operations:
            return 0
            
        serialized = [self._serialize_op(op) for op in operations]
        response = await self._request("POST", "/sync/push", {
            "device_id": self._device_id.hex(),
            "operations": serialized
        })
        return response.get("accepted_count", 0)
    
    async def receive_operations(self) -> list[SyncOperation]:
        """Receive new operations from remote."""
        response = await self._request("POST", "/sync/pull", {
            "device_id": self._device_id.hex(),
            "since_vector_clock": self._remote_vc
        })
        
        ops_data = response.get("operations", [])
        return [self._deserialize_op(op) for op in ops_data]
    
    async def _request(self, method: str, path: str, data: dict | None = None) -> dict:
        """Make HTTP request."""
        url = f"{self._base_url}{path}"
        headers = {"Content-Type": "application/json"}
        
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"
        
        body = json.dumps(data).encode() if data else None
        req = Request(url, data=body, headers=headers, method=method)
        
        try:
            with urlopen(req, timeout=self._timeout) as response:
                return json.loads(response.read().decode())
        except HTTPError as e:
            logger.error(f"HTTP error {e.code}: {e.reason}")
            raise
        except URLError as e:
            logger.error(f"URL error: {e}")
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
        }
    
    def _deserialize_op(self, data: dict) -> SyncOperation:
        """Deserialize operation from JSON."""
        return SyncOperation(
            op_id=bytes.fromhex(data["op_id"]),
            device_id=bytes.fromhex(data["device_id"]),
            parent_op_id=bytes.fromhex(data["parent_op_id"]) if data["parent_op_id"] else None,
            vector_clock=data["vector_clock"],
            table_name=data["table_name"],
            op_type=data["op_type"],
            row_pk=bytes.fromhex(data["row_pk"]),
            old_values=bytes.fromhex(data["old_values"]) if data["old_values"] else None,
            new_values=bytes.fromhex(data["new_values"]) if data["new_values"] else None,
            schema_version=data["schema_version"],
            created_at=data["created_at"],
            is_local=False,
            applied_at=None
        )
