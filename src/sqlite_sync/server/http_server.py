"""
http_server.py - Production-Grade HTTP Sync Server

Enterprise-ready sync server with:
- SQLite-backed persistent storage
- Connection pooling
- Request validation
- Rate limiting
- Proper error handling
"""

import json
import time
import logging
import sqlite3
import threading
from dataclasses import dataclass, asdict
from typing import Any, Optional
from functools import wraps
import os

try:
    from flask import Flask, request, jsonify, g
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False

logger = logging.getLogger(__name__)


# =============================================================================
# Database Schema
# =============================================================================

SCHEMA_SQL = """
-- Device registry
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    device_name TEXT,
    vector_clock TEXT DEFAULT '{}',
    registered_at INTEGER NOT NULL,
    last_seen_at INTEGER,
    status TEXT DEFAULT 'active',
    metadata TEXT DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_devices_status ON devices(status);
CREATE INDEX IF NOT EXISTS idx_devices_last_seen ON devices(last_seen_at);

-- Pending operations queue
CREATE TABLE IF NOT EXISTS pending_operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_device_id TEXT NOT NULL,
    source_device_id TEXT NOT NULL,
    operation_data TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    delivered_at INTEGER,
    FOREIGN KEY (target_device_id) REFERENCES devices(device_id),
    FOREIGN KEY (source_device_id) REFERENCES devices(device_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_target ON pending_operations(target_device_id, delivered_at);
CREATE INDEX IF NOT EXISTS idx_pending_expires ON pending_operations(expires_at);

-- Rate limiting
CREATE TABLE IF NOT EXISTS rate_limits (
    device_id TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    request_count INTEGER DEFAULT 1,
    PRIMARY KEY (device_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_rate_window ON rate_limits(window_start);

-- Sync audit log
CREATE TABLE IF NOT EXISTS sync_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    action TEXT NOT NULL,
    details TEXT,
    timestamp INTEGER NOT NULL,
    ip_address TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_device ON sync_audit_log(device_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON sync_audit_log(timestamp);
"""


# =============================================================================
# Database Connection Pool
# =============================================================================

class ConnectionPool:
    """Thread-safe SQLite connection pool."""
    
    def __init__(self, db_path: str, pool_size: int = 10):
        self._db_path = db_path
        self._pool_size = pool_size
        self._connections: list[sqlite3.Connection] = []
        self._lock = threading.Lock()
        self._local = threading.local()
        self._initialize_db()
    
    def _initialize_db(self) -> None:
        """Initialize database schema."""
        conn = sqlite3.connect(self._db_path)
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        conn.close()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool."""
        # Use thread-local storage for connection reuse
        if hasattr(self._local, 'conn') and self._local.conn:
            return self._local.conn
        
        with self._lock:
            if self._connections:
                conn = self._connections.pop()
            else:
                conn = sqlite3.connect(self._db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
        
        self._local.conn = conn
        return conn
    
    def release_connection(self, conn: sqlite3.Connection) -> None:
        """Return connection to pool."""
        with self._lock:
            if len(self._connections) < self._pool_size:
                self._connections.append(conn)
            else:
                conn.close()
        
        if hasattr(self._local, 'conn'):
            self._local.conn = None
    
    def close_all(self) -> None:
        """Close all connections in pool."""
        with self._lock:
            for conn in self._connections:
                conn.close()
            self._connections.clear()


# =============================================================================
# Rate Limiter
# =============================================================================

class RateLimiter:
    """Token bucket rate limiter with persistent storage."""
    
    def __init__(
        self, 
        pool: ConnectionPool,
        requests_per_minute: int = 60,
        burst_size: int = 10
    ):
        self._pool = pool
        self._rpm = requests_per_minute
        self._burst = burst_size
        self._window_seconds = 60
    
    def check_rate_limit(self, device_id: str) -> tuple[bool, int]:
        """
        Check if request is allowed.
        
        Returns:
            (allowed, retry_after_seconds)
        """
        conn = self._pool.get_connection()
        now = int(time.time())
        window_start = now - (now % self._window_seconds)
        
        try:
            # Get current count
            cursor = conn.execute(
                "SELECT request_count FROM rate_limits WHERE device_id = ? AND window_start = ?",
                (device_id, window_start)
            )
            row = cursor.fetchone()
            
            if row is None:
                # First request in window
                conn.execute(
                    "INSERT INTO rate_limits (device_id, window_start, request_count) VALUES (?, ?, 1)",
                    (device_id, window_start)
                )
                conn.commit()
                return True, 0
            
            current_count = row[0]
            
            if current_count >= self._rpm:
                # Rate limited
                retry_after = self._window_seconds - (now % self._window_seconds)
                return False, retry_after
            
            # Increment count
            conn.execute(
                "UPDATE rate_limits SET request_count = request_count + 1 WHERE device_id = ? AND window_start = ?",
                (device_id, window_start)
            )
            conn.commit()
            return True, 0
            
        except Exception as e:
            logger.error(f"Rate limit check failed: {e}")
            return True, 0  # Fail open
    
    def cleanup_old_windows(self) -> int:
        """Remove expired rate limit windows."""
        conn = self._pool.get_connection()
        cutoff = int(time.time()) - (self._window_seconds * 2)
        
        cursor = conn.execute(
            "DELETE FROM rate_limits WHERE window_start < ?",
            (cutoff,)
        )
        conn.commit()
        return cursor.rowcount


# =============================================================================
# Server Implementation
# =============================================================================

class SyncServer:
    """Production-grade sync server."""
    
    def __init__(
        self,
        db_path: str = "sync_server.db",
        requests_per_minute: int = 60,
        operation_ttl_hours: int = 24
    ):
        self._pool = ConnectionPool(db_path)
        self._rate_limiter = RateLimiter(self._pool, requests_per_minute)
        self._operation_ttl = operation_ttl_hours * 3600
    
    def register_device(
        self, 
        device_id: str, 
        device_name: str = "Unknown",
        metadata: dict = None
    ) -> dict:
        """Register a new device."""
        conn = self._pool.get_connection()
        now = int(time.time())
        
        try:
            conn.execute(
                """
                INSERT INTO devices (device_id, device_name, registered_at, last_seen_at, metadata)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(device_id) DO UPDATE SET
                    device_name = excluded.device_name,
                    last_seen_at = excluded.last_seen_at,
                    metadata = excluded.metadata
                """,
                (device_id, device_name, now, now, json.dumps(metadata or {}))
            )
            conn.commit()
            
            self._audit_log(conn, device_id, "register", {"device_name": device_name})
            
            return {
                "status": "ok",
                "device_id": device_id,
                "registered_at": now
            }
        except Exception as e:
            logger.error(f"Device registration failed: {e}")
            raise
    
    def handshake(self, device_id: str, local_vc: dict) -> dict:
        """Exchange vector clocks between devices."""
        conn = self._pool.get_connection()
        now = int(time.time())
        
        try:
            # Update device's clock and last_seen
            conn.execute(
                """
                UPDATE devices 
                SET vector_clock = ?, last_seen_at = ?
                WHERE device_id = ?
                """,
                (json.dumps(local_vc), now, device_id)
            )
            
            # Get merged clock of all active devices
            cursor = conn.execute(
                """
                SELECT device_id, vector_clock FROM devices 
                WHERE status = 'active' AND last_seen_at > ?
                """,
                (now - 3600,)  # Active in last hour
            )
            
            merged_vc = {}
            known_devices = []
            
            for row in cursor:
                known_devices.append(row['device_id'])
                try:
                    vc = json.loads(row['vector_clock'])
                    for dev, counter in vc.items():
                        merged_vc[dev] = max(merged_vc.get(dev, 0), counter)
                except json.JSONDecodeError:
                    pass
            
            conn.commit()
            
            return {
                "status": "ok",
                "vector_clock": merged_vc,
                "known_devices": known_devices
            }
            
        except Exception as e:
            logger.error(f"Handshake failed: {e}")
            raise
    
    def push_operations(
        self, 
        source_device_id: str, 
        operations: list[dict]
    ) -> dict:
        """Receive operations from a device and queue for other devices."""
        conn = self._pool.get_connection()
        now = int(time.time())
        expires_at = now + self._operation_ttl
        
        try:
            # Get all active devices except source
            cursor = conn.execute(
                """
                SELECT device_id FROM devices 
                WHERE device_id != ? AND status = 'active'
                """,
                (source_device_id,)
            )
            
            target_devices = [row['device_id'] for row in cursor]
            
            # Queue operations for each target device
            queued_count = 0
            for target_device_id in target_devices:
                for op in operations:
                    conn.execute(
                        """
                        INSERT INTO pending_operations 
                        (target_device_id, source_device_id, operation_data, created_at, expires_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (target_device_id, source_device_id, json.dumps(op), now, expires_at)
                    )
                    queued_count += 1
            
            conn.commit()
            
            self._audit_log(conn, source_device_id, "push", {
                "operation_count": len(operations),
                "target_count": len(target_devices)
            })
            
            logger.info(f"Queued {queued_count} operations from {source_device_id}")
            
            return {
                "status": "ok",
                "accepted_count": len(operations),
                "queued_for_devices": len(target_devices)
            }
            
        except Exception as e:
            logger.error(f"Push operations failed: {e}")
            raise
    
    def pull_operations(self, device_id: str) -> dict:
        """Send pending operations to a device."""
        conn = self._pool.get_connection()
        now = int(time.time())
        
        try:
            # Get undelivered, non-expired operations
            cursor = conn.execute(
                """
                SELECT id, operation_data FROM pending_operations
                WHERE target_device_id = ? AND delivered_at IS NULL AND expires_at > ?
                ORDER BY created_at ASC
                LIMIT 1000
                """,
                (device_id, now)
            )
            
            operations = []
            op_ids = []
            
            for row in cursor:
                op_ids.append(row['id'])
                try:
                    operations.append(json.loads(row['operation_data']))
                except json.JSONDecodeError:
                    logger.warning(f"Invalid operation data in queue: {row['id']}")
            
            # Mark as delivered
            if op_ids:
                placeholders = ','.join(['?'] * len(op_ids))
                conn.execute(
                    f"UPDATE pending_operations SET delivered_at = ? WHERE id IN ({placeholders})",
                    [now] + op_ids
                )
            
            # Update last_seen
            conn.execute(
                "UPDATE devices SET last_seen_at = ? WHERE device_id = ?",
                (now, device_id)
            )
            
            conn.commit()
            
            return {
                "status": "ok",
                "operations": operations,
                "count": len(operations)
            }
            
        except Exception as e:
            logger.error(f"Pull operations failed: {e}")
            raise
    
    def get_device_status(self, device_id: str) -> dict | None:
        """Get device status and stats."""
        conn = self._pool.get_connection()
        
        cursor = conn.execute(
            """
            SELECT device_id, device_name, vector_clock, registered_at, 
                   last_seen_at, status, metadata
            FROM devices WHERE device_id = ?
            """,
            (device_id,)
        )
        
        row = cursor.fetchone()
        if row is None:
            return None
        
        # Get pending operation count
        cursor = conn.execute(
            "SELECT COUNT(*) FROM pending_operations WHERE target_device_id = ? AND delivered_at IS NULL",
            (device_id,)
        )
        pending_count = cursor.fetchone()[0]
        
        return {
            "device_id": row['device_id'],
            "device_name": row['device_name'],
            "vector_clock": json.loads(row['vector_clock']),
            "registered_at": row['registered_at'],
            "last_seen_at": row['last_seen_at'],
            "status": row['status'],
            "pending_operations": pending_count
        }
    
    def cleanup_expired(self) -> dict:
        """Remove expired operations and old audit logs."""
        conn = self._pool.get_connection()
        now = int(time.time())
        
        # Remove expired operations
        cursor = conn.execute(
            "DELETE FROM pending_operations WHERE expires_at < ?",
            (now,)
        )
        ops_deleted = cursor.rowcount
        
        # Remove delivered operations older than 1 hour
        cursor = conn.execute(
            "DELETE FROM pending_operations WHERE delivered_at IS NOT NULL AND delivered_at < ?",
            (now - 3600,)
        )
        delivered_deleted = cursor.rowcount
        
        # Remove old audit logs (keep 7 days)
        cursor = conn.execute(
            "DELETE FROM sync_audit_log WHERE timestamp < ?",
            (now - 7 * 86400,)
        )
        audit_deleted = cursor.rowcount
        
        # Cleanup rate limit windows
        rate_cleaned = self._rate_limiter.cleanup_old_windows()
        
        conn.commit()
        
        return {
            "expired_ops_deleted": ops_deleted,
            "delivered_ops_deleted": delivered_deleted,
            "audit_logs_deleted": audit_deleted,
            "rate_windows_deleted": rate_cleaned
        }
    
    def _audit_log(
        self, 
        conn: sqlite3.Connection, 
        device_id: str, 
        action: str, 
        details: dict = None,
        ip_address: str = None
    ) -> None:
        """Log sync action for audit."""
        conn.execute(
            """
            INSERT INTO sync_audit_log (device_id, action, details, timestamp, ip_address)
            VALUES (?, ?, ?, ?, ?)
            """,
            (device_id, action, json.dumps(details or {}), int(time.time()), ip_address)
        )
    
    def close(self) -> None:
        """Close all connections."""
        self._pool.close_all()


# =============================================================================
# Flask Application Factory
# =============================================================================

def create_sync_server(
    db_path: str = "sync_server.db",
    requests_per_minute: int = 60,
    require_auth: bool = False,
    api_keys: list[str] = None
):
    """
    Create a production-ready Flask sync server.
    
    Args:
        db_path: Path to SQLite database
        requests_per_minute: Rate limit per device
        require_auth: Whether to require API key authentication
        api_keys: List of valid API keys
        
    Returns:
        Flask app instance
    """
    if not HAS_FLASK:
        raise ImportError("Flask required: pip install flask")
    
    app = Flask(__name__)
    server = SyncServer(db_path, requests_per_minute)
    rate_limiter = server._rate_limiter
    valid_api_keys = set(api_keys or [])
    
    # Request validation decorator
    def validate_request(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Check API key if required
            if require_auth:
                api_key = request.headers.get('X-API-Key')
                if not api_key or api_key not in valid_api_keys:
                    return jsonify({"error": "Unauthorized", "code": "AUTH_REQUIRED"}), 401
            
            # Check rate limit
            device_id = None
            if request.is_json and request.json:
                device_id = request.json.get('device_id')
            
            if device_id:
                allowed, retry_after = rate_limiter.check_rate_limit(device_id)
                if not allowed:
                    return jsonify({
                        "error": "Rate limit exceeded",
                        "code": "RATE_LIMITED",
                        "retry_after": retry_after
                    }), 429
            
            return f(*args, **kwargs)
        return wrapper
    
    @app.route("/sync/health", methods=["GET"])
    def health():
        """Health check endpoint."""
        return jsonify({
            "status": "ok",
            "timestamp": int(time.time()),
            "version": "1.0.0"
        })
    
    @app.route("/sync/register", methods=["POST"])
    @validate_request
    def register_device():
        """Register a new device."""
        try:
            data = request.json
            result = server.register_device(
                device_id=data.get("device_id"),
                device_name=data.get("device_name", "Unknown"),
                metadata=data.get("metadata")
            )
            return jsonify(result)
        except Exception as e:
            logger.error(f"Register error: {e}")
            return jsonify({"error": str(e), "code": "REGISTER_FAILED"}), 500
    
    @app.route("/sync/handshake", methods=["POST"])
    @validate_request
    def handshake():
        """Exchange vector clocks."""
        try:
            data = request.json
            result = server.handshake(
                device_id=data.get("device_id"),
                local_vc=data.get("vector_clock", {})
            )
            return jsonify(result)
        except Exception as e:
            logger.error(f"Handshake error: {e}")
            return jsonify({"error": str(e), "code": "HANDSHAKE_FAILED"}), 500
    
    @app.route("/sync/push", methods=["POST"])
    @validate_request
    def push_operations():
        """Receive operations from a device."""
        try:
            data = request.json
            result = server.push_operations(
                source_device_id=data.get("device_id"),
                operations=data.get("operations", [])
            )
            return jsonify(result)
        except Exception as e:
            logger.error(f"Push error: {e}")
            return jsonify({"error": str(e), "code": "PUSH_FAILED"}), 500
    
    @app.route("/sync/pull", methods=["POST"])
    @validate_request
    def pull_operations():
        """Send pending operations to a device."""
        try:
            data = request.json
            result = server.pull_operations(device_id=data.get("device_id"))
            return jsonify(result)
        except Exception as e:
            logger.error(f"Pull error: {e}")
            return jsonify({"error": str(e), "code": "PULL_FAILED"}), 500
    
    @app.route("/sync/device/<device_id>", methods=["GET"])
    @validate_request  
    def get_device(device_id: str):
        """Get device status."""
        result = server.get_device_status(device_id)
        if result is None:
            return jsonify({"error": "Device not found", "code": "NOT_FOUND"}), 404
        return jsonify(result)
    
    @app.route("/sync/admin/cleanup", methods=["POST"])
    @validate_request
    def admin_cleanup():
        """Cleanup expired data (admin endpoint)."""
        result = server.cleanup_expired()
        return jsonify({"status": "ok", **result})
    
    @app.route("/sync/admin/stats", methods=["GET"])
    @validate_request
    def admin_stats():
        """Get server statistics."""
        conn = server._pool.get_connection()
        
        device_count = conn.execute("SELECT COUNT(*) FROM devices").fetchone()[0]
        active_devices = conn.execute(
            "SELECT COUNT(*) FROM devices WHERE last_seen_at > ?",
            (int(time.time()) - 3600,)
        ).fetchone()[0]
        pending_ops = conn.execute(
            "SELECT COUNT(*) FROM pending_operations WHERE delivered_at IS NULL"
        ).fetchone()[0]
        
        return jsonify({
            "status": "ok",
            "total_devices": device_count,
            "active_devices": active_devices,
            "pending_operations": pending_ops,
            "timestamp": int(time.time())
        })
    
    @app.teardown_appcontext
    def close_connection(exception):
        """Release connection back to pool."""
        pass  # Connection pool handles this via thread-local
    
    return app


def run_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    db_path: str = "sync_server.db",
    debug: bool = False
):
    """Run the production sync server."""
    app = create_sync_server(db_path=db_path)
    print(f"Starting production sync server on http://{host}:{port}")
    print(f"Database: {db_path}")
    app.run(host=host, port=port, debug=debug, threaded=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    run_server(debug=True)
