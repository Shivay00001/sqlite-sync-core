import os
import logging
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends, Header, Request, status
from pydantic import BaseModel
from fastapi.responses import JSONResponse

from sqlite_sync.engine import SyncEngine
from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.security import SecurityManager

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

# Global state for the engine path, to be set by CLI or env var
DB_PATH = os.environ.get("SQLITE_SYNC_DB_PATH", "sync_server.db")

app = FastAPI(title="SQLite Sync Server")

def get_engine() -> SyncEngine:
    if not DB_PATH:
        raise HTTPException(status_code=500, detail="Database path not configured")
    try:
        engine = SyncEngine(DB_PATH)
        # We need to act as a context manager to ensure connections are closed if needed,
        # but for per-request isolation, we can rely on garbage collection or explicit close in a try/finally block
        # in the endpoint. However, SyncEngine creates connection on first access.
        return engine
    except Exception as e:
        logger.error(f"Failed to initialize engine: {e}")
        raise HTTPException(status_code=500, detail="Internal Sync Error")

@app.on_event("startup")
async def startup_event():
    logger.info(f"Starting Sync Server with DB: {DB_PATH}")
    # Initialize DB if needed - optional, but good for fail-fast
    try:
        with SyncEngine(DB_PATH) as engine:
            engine.initialize()
    except Exception as e:
        logger.error(f"Startup initialization failed: {e}")

@app.get("/sync/health")
async def health_check():
    return {"status": "ok", "service": "sqlite-sync"}

@app.post("/sync/handshake")
async def handshake(request: HandshakeRequest, engine: SyncEngine = Depends(get_engine)):
    """Exchange vector clocks and device info."""
    try:
        with engine:
            local_vc = engine.get_vector_clock()
            server_device_id = engine.device_id.hex()
            local_schema_version = engine.get_schema_version()
            
            # Check compatibility
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
                "protocol_version": 1
            }
    except Exception as e:
        logger.exception("Handshake failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/push")
async def push_operations(request: PushRequest, engine: SyncEngine = Depends(get_engine)):
    """Receive operations from a client."""
    try:
        source_device_id = bytes.fromhex(request.device_id)
        
        # Deserialize operations
        # The engine's apply_batch expects SyncOperation objects
        ops = []
        for op_dict in request.operations:
            # We need to use the deserialize logic from http_transport or similar
            # Since that logic was in the client, we need a shared util or reimplement it here.
            # Ideally, `SyncOperation` has a `from_dict` or similar, or we reuse `http_transport._deserialize_op`.
            # For now I will manually map it to avoid circular dependency or code duplication if I can't find a shared place.
            # Actually, `http_transport` had `_deserialize_op`. I should move that to `SyncOperation` or a serializer.
             
            # Mapping assuming dict structure matches `http_transport._serialize_op`
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
                hlc=op_dict.get("hlc"),
                is_local=False,
                applied_at=None
            )
            ops.append(op)

        with engine:
            # TODO: Verify signature if security enabled
            
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

@app.post("/sync/pull")
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
                    "hlc": op.hlc,
                })
                
            return {
                "operations": serialized_ops,
                "count": len(serialized_ops),
                "server_vector_clock": engine.get_vector_clock()
            }
            
    except Exception as e:
        logger.exception("Pull failed")
        raise HTTPException(status_code=500, detail=str(e))
