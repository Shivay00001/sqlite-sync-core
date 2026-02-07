import json
import asyncio
from dataclasses import dataclass, asdict
from typing import Any, Literal
import websockets
from sqlite_sync.log.operations import SyncOperation

# Protocol Types
MessageType = Literal["handshake", "sync_request", "sync_response", "operation", "ack"]

@dataclass(frozen=True)
class SyncMessage:
    type: MessageType
    source_id: str
    payload: Any

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "SyncMessage":
        return cls(**json.loads(data))
