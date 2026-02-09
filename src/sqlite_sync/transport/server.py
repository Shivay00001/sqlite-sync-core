import os
import logging
import time
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends, Header, Request, status
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
