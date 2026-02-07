import asyncio
import logging
import websockets
from typing import Optional
from sqlite_sync.engine import SyncEngine
from sqlite_sync.network.protocol import SyncMessage
from sqlite_sync.log.operations import SyncOperation

logger = logging.getLogger(__name__)

class SyncClient:
    """WebSocket client that syncs a SyncEngine with a remote server."""
    
    def __init__(self, engine: SyncEngine, server_url: str):
        self.engine = engine
        self.server_url = server_url
        self._ws = None

    async def connect(self):
        self._ws = await websockets.connect(self.server_url)
        logger.info(f"Connected to {self.server_url}")

    async def send_operation(self, op: SyncOperation):
        if not self._ws:
            raise ConnectionError("Not connected")
        
        # In a real app, op needs serialization to a dict first
        # For now, let's assume a dict-like interface or helper
        msg = SyncMessage(
            type="operation",
            source_id=self.engine.device_id.hex(),
            payload=self._operation_to_dict(op)
        )
        await self._ws.send(msg.to_json())

    async def listen(self):
        """Listen for incoming operations from other peers."""
        if not self._ws:
            raise ConnectionError("Not connected")
        
        async for data in self._ws:
            msg = SyncMessage.from_json(data)
            if msg.type == "operation":
                op = self._dict_to_operation(msg.payload)
                self.engine.apply_operation(op)
                logger.debug(f"Applied operation {op.op_id.hex()} from peer")

    def _operation_to_dict(self, op: SyncOperation) -> dict:
        # Simplified for now, in reality needs msgpack/base64 for bytes
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

    def _dict_to_operation(self, data: dict) -> SyncOperation:
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
