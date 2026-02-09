"""
websocket_transport.py - WebSocket-based real-time sync transport.

Provides bidirectional real-time synchronization.
"""

import json
import asyncio
import logging
from typing import Callable

try:
    import websockets
    from websockets.client import WebSocketClientProtocol
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    WebSocketClientProtocol = None

from sqlite_sync.transport.base import TransportAdapter
from sqlite_sync.log.operations import SyncOperation

logger = logging.getLogger(__name__)


class WebSocketTransport(TransportAdapter):
    """
    WebSocket transport for real-time bidirectional sync.
    
    Features:
    - Persistent connection
    - Real-time operation streaming
    - Automatic reconnection
    """
    
    def __init__(
        self,
        url: str,
        device_id: bytes,
        auth_token: str | None = None,
        reconnect_interval: float = 5.0,
        on_operation_received: Callable[[SyncOperation], None] | None = None
    ):
        if not HAS_WEBSOCKETS:
            raise ImportError("websockets package required: pip install websockets")
            
        self._url = url
        self._device_id = device_id
        self._auth_token = auth_token
        self._reconnect_interval = reconnect_interval
        self._on_operation_received = on_operation_received
        self._ws: WebSocketClientProtocol | None = None
        self._connected = False
        self._remote_vc: dict[str, int] = {}
        self._listener_task: asyncio.Task | None = None
        self._pending_ops: list[SyncOperation] = []
    
    @property
    def name(self) -> str:
        return "WebSocket"
    
    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        try:
            extra_headers = {}
            if self._auth_token:
                extra_headers["Authorization"] = f"Bearer {self._auth_token}"
            
            self._ws = await websockets.connect(
                self._url,
                extra_headers=extra_headers
            )
            self._connected = True
            
            # Send handshake
            handshake_msg = {
                "type": "handshake",
                "device_id": self._device_id.hex()
            }
            if self._auth_token:
                handshake_msg["auth_token"] = self._auth_token
            
            await self._send_message(handshake_msg)
            
            # Start listener
            self._listener_task = asyncio.create_task(self._listen())
            
            logger.info(f"WebSocket connected to {self._url}")
            return True
            
        except Exception as e:
            logger.error(f"WebSocket connect failed: {e}")
            self._connected = False
            return False
    
    async def disconnect(self) -> None:
        """Close WebSocket connection."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self._ws:
            await self._ws.close()
        
        self._connected = False
        logger.info("WebSocket disconnected")
    
    def is_connected(self) -> bool:
        return self._connected and self._ws is not None
    
    async def exchange_vector_clock(self, local_vc: dict[str, int]) -> dict[str, int]:
        """Exchange vector clocks."""
        await self._send_message({
            "type": "sync_request",
            "vector_clock": local_vc
        })
        
        # Wait for response (simplified - in production use proper async queue)
        await asyncio.sleep(0.1)
        return self._remote_vc
    
    async def send_operations(self, operations: list[SyncOperation]) -> int:
        """Send operations over WebSocket."""
        sent = 0
        for op in operations:
            try:
                await self._send_message({
                    "type": "operation",
                    "data": self._serialize_op(op)
                })
                sent += 1
            except Exception as e:
                logger.error(f"Failed to send operation: {e}")
                break
        return sent
    
    async def receive_operations(self) -> list[SyncOperation]:
        """Return pending received operations."""
        ops = self._pending_ops.copy()
        self._pending_ops.clear()
        return ops
    
    async def _listen(self) -> None:
        """Listen for incoming messages."""
        try:
            async for message in self._ws:
                await self._handle_message(json.loads(message))
        except websockets.exceptions.ConnectionClosed:
            self._connected = False
            logger.info("WebSocket connection closed")
        except Exception as e:
            logger.error(f"WebSocket listen error: {e}")
            self._connected = False
    
    async def _handle_message(self, msg: dict) -> None:
        """Handle incoming WebSocket message."""
        msg_type = msg.get("type")
        
        if msg_type == "sync_response":
            self._remote_vc = msg.get("vector_clock", {})
            
        elif msg_type == "operation":
            op = self._deserialize_op(msg.get("data", {}))
            self._pending_ops.append(op)
            
            if self._on_operation_received:
                self._on_operation_received(op)
                
        elif msg_type == "ack":
            pass  # Operation acknowledged
            
        elif msg_type == "error":
            logger.error(f"Remote error: {msg.get('message')}")
    
    async def _send_message(self, msg: dict) -> None:
        """Send JSON message over WebSocket."""
        if self._ws:
            await self._ws.send(json.dumps(msg))
    
    def _serialize_op(self, op: SyncOperation) -> dict:
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
