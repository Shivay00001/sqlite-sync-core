import asyncio
import logging
import websockets
from typing import Optional
from sqlite_sync.engine import SyncEngine
from sqlite_sync.network.protocol import SyncMessage
from sqlite_sync.log.operations import SyncOperation

logger = logging.getLogger(__name__)

class SyncServer:
    """A reference sync server that routes operations between connected peers."""
    
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.clients = set()

    async def handler(self, websocket):
        self.clients.add(websocket)
        logger.info(f"New client connected. Total: {len(self.clients)}")
        try:
            async for data in websocket:
                message = SyncMessage.from_json(data)
                # Simple broadcast for now - in production, route by device_id
                for client in self.clients:
                    if client != websocket:
                        await client.send(data)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.clients.remove(websocket)
            logger.info(f"Client disconnected. Total: {len(self.clients)}")

    async def start(self):
        async with websockets.serve(self.handler, self.host, self.port):
            logger.info(f"Sync server started on ws://{self.host}:{self.port}")
            await asyncio.Future()  # run forever

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = SyncServer()
    asyncio.run(server.start())
