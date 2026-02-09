import os
import logging
import time
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends, Header, Request, status, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from sqlite_sync.engine import SyncEngine
from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.security import SecurityManager, SignedBundle

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sqlite_sync.server")

class HandshakeRequest(BaseModel):
    device_id: str
    vector_clock: Dict[str, int]
    schema_version: int = 0
    protocol_version: int = 1

class PushRequest(BaseModel):
    device_id: str
    operations: List[Dict[str, Any]]
    bundle_id: Optional[str] = None
    signature: Optional[str] = None

class PullRequest(BaseModel):
    device_id: str
    since_vector_clock: Dict[str, int]
    limit: int = 1000

# Global configuration
DB_PATH = os.environ.get("SQLITE_SYNC_DB_PATH", "sync_server.db")
NONCE_DB_PATH = os.environ.get("SQLITE_SYNC_NONCE_DB_PATH", "sync_nonces.db")
SIGNING_SECRET = os.environ.get("SQLITE_SYNC_SIGNING_SECRET", "change-me-in-production")
SERVER_DEVICE_ID = os.environ.get("SQLITE_SYNC_SERVER_ID", "server-001").encode()

app = FastAPI(title="SQLite Sync Server")

# Initialize Security Manager
security_manager = SecurityManager(
    device_id=SERVER_DEVICE_ID,
    signing_key=SIGNING_SECRET.encode(),
    nonce_db_path=NONCE_DB_PATH
)

def get_engine() -> SyncEngine:
    if not DB_PATH:
        raise HTTPException(status_code=500, detail="Database path not configured")
    try:
        engine = SyncEngine(DB_PATH)
        return engine
    except Exception as e:
        logger.error(f"Failed to initialize engine: {e}")
        raise HTTPException(status_code=500, detail="Internal Sync Error")

async def verify_request(
    request: Request,
    x_sync_device_id: str = Header(..., alias="X-Sync-Device-Id"),
    x_sync_timestamp: str = Header(..., alias="X-Sync-Timestamp"),
    x_sync_nonce: str = Header(..., alias="X-Sync-Nonce"),
    x_sync_signature: str = Header(..., alias="X-Sync-Signature"),
):
    """
    Verify request signature and replay protection.
    
    All state-changing endpoints must use this dependency.
    """
    try:
        # 1. Read raw body
        body_bytes = await request.body()
        
        # 2. Reconstruct SignedBundle
        # We assume the signature type is HMAC for now as per minimal config
        bundle = SignedBundle(
            bundle_data=body_bytes,
            signature=bytes.fromhex(x_sync_signature),
            device_id=bytes.fromhex(x_sync_device_id),
            timestamp=int(x_sync_timestamp),
            nonce=bytes.fromhex(x_sync_nonce),
            signature_type="hmac"
        )
        
        # 3. Verify
        # We reuse the global SIGNING_SECRET for all devices (shared secret model)
        # In a more advanced setup, we'd lookup per-device keys.
        if not security_manager.verify_signature(bundle):
            logger.warning(f"Invalid signature from {x_sync_device_id}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid signature or replay detected"
            )
            
        return True
        
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid header format"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Verification error: {e}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Security verification failed"
        )

# Global Exception Handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )

@app.on_event("startup")
async def startup_event():
    logger.info(f"Starting Sync Server with DB: {DB_PATH}")
    # Initialize DB if needed - optional, but good for fail-fast
    try:
        with SyncEngine(DB_PATH) as engine:
            engine.initialize()
    except Exception as e:
        logger.error(f"Startup initialization failed: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    security_manager.close()

@app.get("/sync/health")
async def health_check():
    return {"status": "ok", "service": "sqlite-sync"}

@app.post("/sync/handshake", dependencies=[Depends(verify_request)])
async def handshake(request: HandshakeRequest, engine: SyncEngine = Depends(get_engine)):
    """Exchange vector clocks and device info."""
    try:
        with engine:
            local_vc = engine.get_vector_clock()
            server_device_id = engine.device_id.hex()
            schema_info = engine.get_schema_info()
            local_schema_version = schema_info["version"]
            schema_hash = schema_info["hash"]
            
            # Get pending migrations if client is behind
            pending_migrations = []
            migrations_safe = True
            
            if request.schema_version < local_schema_version:
                pending_migrations = engine.get_pending_migrations_for(request.schema_version)
                migrations_safe = engine.are_migrations_safe(pending_migrations)
                
                if not migrations_safe:
                    logger.warning(f"Unsafe migrations detected for client version {request.schema_version}")
                    raise HTTPException(
                        status_code=409, 
                        detail=f"Schema migration required but contains unsafe operations. "
                               f"Server at version {local_schema_version}, Client at {request.schema_version}"
                    )
            
            # Check forward compatibility (client ahead of server)
            if request.schema_version > local_schema_version:
                if not engine.check_compatibility(request.schema_version):
                    logger.warning(f"Schema mismatch: Local {local_schema_version} vs Remote {request.schema_version}")
                    raise HTTPException(
                        status_code=409, 
                        detail=f"Schema incompatibility: Server is at version {local_schema_version}, Client at {request.schema_version}"
                    )
            
            return {
                "device_id": server_device_id,
                "vector_clock": local_vc,
                "schema_version": local_schema_version,
                "schema_hash": schema_hash,
                "pending_migrations": pending_migrations,
                "migrations_safe": migrations_safe,
                "protocol_version": 1
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Handshake failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/push", dependencies=[Depends(verify_request)])
async def push_operations(request: PushRequest, engine: SyncEngine = Depends(get_engine)):
    """Receive operations from a client."""
    try:
        source_device_id = bytes.fromhex(request.device_id)
        
        # Deserialize operations
        ops = []
        for op_dict in request.operations:
            op = SyncOperation(
                op_id=bytes.fromhex(op_dict["op_id"]),
                device_id=bytes.fromhex(op_dict["device_id"]),
                parent_op_id=bytes.fromhex(op_dict["parent_op_id"]) if op_dict.get("parent_op_id") else None,
                vector_clock=op_dict["vector_clock"],
                table_name=op_dict["table_name"],
                op_type=op_dict["op_type"],
                row_pk=bytes.fromhex(op_dict["row_pk"]),
                old_values=bytes.fromhex(op_dict["old_values"]) if op_dict.get("old_values") else None,
                new_values=bytes.fromhex(op_dict["new_values"]) if op_dict.get("new_values") else None,
                schema_version=op_dict["schema_version"],
                created_at=op_dict["created_at"],
                hlc=bytes.fromhex(op_dict["hlc"]) if op_dict.get("hlc") else None,
                is_local=False,
                applied_at=None
            )
            ops.append(op)

        with engine:
            result = engine.apply_batch(
                operations=ops,
                source_device_id=source_device_id,
                bundle_id=bytes.fromhex(request.bundle_id) if request.bundle_id else None
            )
            
            return {
                "accepted_count": result.applied_count,
                "conflict_count": result.conflict_count,
                "duplicate_count": result.duplicate_count
            }
            
    except Exception as e:
        logger.exception("Push failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/pull", dependencies=[Depends(verify_request)])
async def pull_operations(request: PullRequest, engine: SyncEngine = Depends(get_engine)):
    """Send operations to a client."""
    try:
        with engine:
            # Get operations since the client's vector clock
            ops = engine.get_new_operations(since_vector_clock=request.since_vector_clock)
            
            # Serialize
            serialized_ops = []
            for op in ops:
                serialized_ops.append({
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
                    "hlc": op.hlc.hex() if op.hlc else None,
                })
                
            return {
                "operations": serialized_ops,
                "count": len(serialized_ops),
                "server_vector_clock": engine.get_vector_clock()
            }
            
    except Exception as e:
        logger.exception("Pull failed")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# WebSocket Sync Endpoint
# =============================================================================

class ConnectionManager:
    """Manages active WebSocket connections."""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, device_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[device_id] = websocket
        logger.info(f"WebSocket connected: {device_id}")
    
    def disconnect(self, device_id: str):
        if device_id in self.active_connections:
            del self.active_connections[device_id]
            logger.info(f"WebSocket disconnected: {device_id}")
    
    async def send_json(self, device_id: str, data: dict):
        if device_id in self.active_connections:
            await self.active_connections[device_id].send_json(data)


ws_manager = ConnectionManager()


def serialize_operation(op: SyncOperation) -> dict:
    """Serialize operation for WebSocket transport."""
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
        "hlc": op.hlc.hex() if op.hlc else None,
    }


def deserialize_operation(data: dict) -> SyncOperation:
    """Deserialize operation from WebSocket message."""
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
        hlc=bytes.fromhex(data["hlc"]) if data.get("hlc") else None,
        is_local=False,
        applied_at=None
    )


@app.websocket("/ws/sync")
async def websocket_sync(websocket: WebSocket):
    """
    WebSocket endpoint for real-time sync.
    
    Message types:
    - handshake: Exchange device info and vector clocks
    - push: Send operations from client to server
    - pull: Request operations from server
    - conflict: Report conflict resolution
    - ack: Acknowledge receipt
    - error: Error response
    """
    device_id = None
    engine = None
    
    try:
        # Initial connection - wait for handshake
        await websocket.accept()
        
        while True:
            try:
                message = await websocket.receive_json()
            except Exception:
                break
            
            msg_type = message.get("type")
            
            # Handle handshake
            if msg_type == "handshake":
                device_id = message.get("device_id", "unknown")
                ws_manager.active_connections[device_id] = websocket
                
                # Authenticate using existing security primitives
                auth_token = message.get("auth_token")
                if auth_token:
                    # Verify token matches signing secret (shared secret model)
                    if auth_token != SIGNING_SECRET:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Authentication failed"
                        })
                        break
                
                # Get engine and prepare response
                engine = get_engine()
                with engine:
                    schema_info = engine.get_schema_info()
                    client_schema_version = message.get("schema_version", 0)
                    
                    # Get pending migrations if needed
                    pending_migrations = []
                    if client_schema_version < schema_info["version"]:
                        pending_migrations = engine.get_pending_migrations_for(client_schema_version)
                        if not engine.are_migrations_safe(pending_migrations):
                            await websocket.send_json({
                                "type": "error",
                                "message": "Unsafe schema migration required"
                            })
                            break
                    
                    await websocket.send_json({
                        "type": "handshake_response",
                        "device_id": engine.device_id.hex(),
                        "vector_clock": engine.get_vector_clock(),
                        "schema_version": schema_info["version"],
                        "schema_hash": schema_info["hash"],
                        "pending_migrations": pending_migrations
                    })
                
                logger.info(f"WebSocket handshake completed: {device_id}")
            
            # Handle push (receive operations from client)
            elif msg_type == "push":
                if not engine:
                    engine = get_engine()
                
                operations_data = message.get("operations", [])
                ops = [deserialize_operation(op) for op in operations_data]
                
                with engine:
                    source_device_id = bytes.fromhex(message.get("device_id", device_id or "0" * 32))
                    result = engine.apply_batch(ops, source_device_id)
                    
                    await websocket.send_json({
                        "type": "push_response",
                        "accepted_count": result.applied_count,
                        "conflict_count": result.conflict_count,
                        "duplicate_count": result.duplicate_count
                    })
            
            # Handle pull (send operations to client)
            elif msg_type == "pull":
                if not engine:
                    engine = get_engine()
                
                since_vc = message.get("since_vector_clock", {})
                
                with engine:
                    ops = engine.get_new_operations(since_vector_clock=since_vc)
                    serialized_ops = [serialize_operation(op) for op in ops]
                    
                    await websocket.send_json({
                        "type": "pull_response",
                        "operations": serialized_ops,
                        "count": len(serialized_ops),
                        "server_vector_clock": engine.get_vector_clock()
                    })
            
            # Handle conflict resolution
            elif msg_type == "conflict":
                if not engine:
                    engine = get_engine()
                
                conflict_id = message.get("conflict_id")
                resolution = message.get("resolution")  # "local" or "remote"
                
                try:
                    with engine:
                        engine.resolve_conflict(conflict_id, resolution)
                        await websocket.send_json({
                            "type": "conflict_response",
                            "status": "resolved",
                            "conflict_id": conflict_id
                        })
                except Exception as e:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Conflict resolution failed: {e}"
                    })
            
            # Handle acknowledgment
            elif msg_type == "ack":
                pass  # Client acknowledged, nothing to do
            
            else:
                await websocket.send_json({
                    "type": "error",
                    "message": f"Unknown message type: {msg_type}"
                })
    
    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected: {device_id}")
    except Exception as e:
        logger.exception(f"WebSocket error: {e}")
    finally:
        if device_id:
            ws_manager.disconnect(device_id)
        if engine:
            engine.close()

