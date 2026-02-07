"""
auth.py - Enterprise Authentication & Authorization

Provides:
- API key authentication
- JWT token support  
- Device registration with approval workflow
- Role-based access control
- Session management with revocation
"""

import os
import time
import json
import hmac
import hashlib
import base64
import secrets
import sqlite3
import logging
from dataclasses import dataclass
from typing import Optional, Callable
from enum import Enum
from functools import wraps

logger = logging.getLogger(__name__)


# =============================================================================
# Data Types
# =============================================================================

class DeviceRole(Enum):
    """Device access roles."""
    PENDING = "pending"     # Awaiting approval
    READ_ONLY = "read_only" # Can pull but not push
    READ_WRITE = "read_write"  # Full sync access
    ADMIN = "admin"         # Can manage other devices


class TokenType(Enum):
    """Token types."""
    API_KEY = "api_key"
    JWT = "jwt"
    SESSION = "session"


@dataclass
class AuthResult:
    """Authentication result."""
    authenticated: bool
    device_id: Optional[str] = None
    role: Optional[DeviceRole] = None
    error: Optional[str] = None
    token_type: Optional[TokenType] = None


@dataclass
class DeviceCredentials:
    """Device authentication credentials."""
    device_id: str
    api_key: str
    api_secret: str
    role: DeviceRole
    created_at: int
    expires_at: Optional[int] = None
    revoked: bool = False


# =============================================================================
# Database Schema
# =============================================================================

AUTH_SCHEMA_SQL = """
-- API Keys
CREATE TABLE IF NOT EXISTS api_keys (
    key_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'read_write',
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    last_used_at INTEGER,
    revoked INTEGER DEFAULT 0,
    revoked_at INTEGER,
    metadata TEXT DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_api_keys_device ON api_keys(device_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);

-- Sessions (for JWT refresh tokens)
CREATE TABLE IF NOT EXISTS auth_sessions (
    session_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    refresh_token_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    last_used_at INTEGER,
    revoked INTEGER DEFAULT 0,
    ip_address TEXT,
    user_agent TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_device ON auth_sessions(device_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON auth_sessions(expires_at);

-- Device approval queue
CREATE TABLE IF NOT EXISTS device_approvals (
    device_id TEXT PRIMARY KEY,
    device_name TEXT,
    requested_role TEXT DEFAULT 'read_write',
    request_time INTEGER NOT NULL,
    approved INTEGER DEFAULT 0,
    approved_by TEXT,
    approved_at INTEGER,
    rejected INTEGER DEFAULT 0,
    rejection_reason TEXT
);

-- Auth audit log
CREATE TABLE IF NOT EXISTS auth_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT,
    action TEXT NOT NULL,
    success INTEGER NOT NULL,
    details TEXT,
    ip_address TEXT,
    timestamp INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auth_audit_device ON auth_audit_log(device_id);
CREATE INDEX IF NOT EXISTS idx_auth_audit_timestamp ON auth_audit_log(timestamp);
"""


# =============================================================================
# JWT Implementation (No external dependencies)
# =============================================================================

class JWTManager:
    """
    Minimal JWT implementation using HMAC-SHA256.
    
    For production, consider using PyJWT with RS256.
    """
    
    def __init__(self, secret_key: bytes, issuer: str = "sqlite-sync"):
        self._secret = secret_key
        self._issuer = issuer
        self._access_ttl = 3600  # 1 hour
        self._refresh_ttl = 86400 * 30  # 30 days
    
    def create_access_token(
        self, 
        device_id: str, 
        role: DeviceRole,
        additional_claims: dict = None
    ) -> str:
        """Create a short-lived access token."""
        now = int(time.time())
        
        payload = {
            "iss": self._issuer,
            "sub": device_id,
            "role": role.value,
            "iat": now,
            "exp": now + self._access_ttl,
            "type": "access"
        }
        
        if additional_claims:
            payload.update(additional_claims)
        
        return self._encode(payload)
    
    def create_refresh_token(self, device_id: str) -> str:
        """Create a long-lived refresh token."""
        now = int(time.time())
        
        payload = {
            "iss": self._issuer,
            "sub": device_id,
            "iat": now,
            "exp": now + self._refresh_ttl,
            "type": "refresh",
            "jti": secrets.token_hex(16)  # Unique token ID
        }
        
        return self._encode(payload)
    
    def verify_token(self, token: str) -> tuple[bool, dict | str]:
        """
        Verify and decode a token.
        
        Returns:
            (valid, payload_or_error)
        """
        try:
            payload = self._decode(token)
            
            if payload is None:
                return False, "Invalid signature"
            
            # Check expiration
            if payload.get("exp", 0) < time.time():
                return False, "Token expired"
            
            # Check issuer
            if payload.get("iss") != self._issuer:
                return False, "Invalid issuer"
            
            return True, payload
            
        except Exception as e:
            return False, str(e)
    
    def _encode(self, payload: dict) -> str:
        """Encode payload to JWT."""
        header = {"alg": "HS256", "typ": "JWT"}
        
        header_b64 = self._base64url_encode(json.dumps(header).encode())
        payload_b64 = self._base64url_encode(json.dumps(payload).encode())
        
        message = f"{header_b64}.{payload_b64}"
        signature = hmac.new(
            self._secret,
            message.encode(),
            hashlib.sha256
        ).digest()
        signature_b64 = self._base64url_encode(signature)
        
        return f"{message}.{signature_b64}"
    
    def _decode(self, token: str) -> dict | None:
        """Decode and verify JWT."""
        parts = token.split('.')
        if len(parts) != 3:
            return None
        
        header_b64, payload_b64, signature_b64 = parts
        
        # Verify signature
        message = f"{header_b64}.{payload_b64}"
        expected_sig = hmac.new(
            self._secret,
            message.encode(),
            hashlib.sha256
        ).digest()
        
        actual_sig = self._base64url_decode(signature_b64)
        
        if not hmac.compare_digest(expected_sig, actual_sig):
            return None
        
        # Decode payload
        payload_json = self._base64url_decode(payload_b64)
        return json.loads(payload_json)
    
    def _base64url_encode(self, data: bytes) -> str:
        """Base64url encode without padding."""
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode()
    
    def _base64url_decode(self, data: str) -> bytes:
        """Base64url decode with padding."""
        padding = 4 - len(data) % 4
        if padding != 4:
            data += '=' * padding
        return base64.urlsafe_b64decode(data)


# =============================================================================
# Authentication Manager
# =============================================================================

class AuthManager:
    """
    Enterprise authentication manager.
    
    Supports:
    - API key authentication
    - JWT tokens with refresh
    - Device registration workflow
    - Role-based access control
    """
    
    def __init__(
        self,
        conn: sqlite3.Connection,
        jwt_secret: bytes = None,
        require_approval: bool = False
    ):
        self._conn = conn
        self._jwt_secret = jwt_secret or os.urandom(32)
        self._jwt = JWTManager(self._jwt_secret)
        self._require_approval = require_approval
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize auth tables."""
        self._conn.executescript(AUTH_SCHEMA_SQL)
        self._conn.commit()
    
    # -------------------------------------------------------------------------
    # API Key Management
    # -------------------------------------------------------------------------
    
    def create_api_key(
        self,
        device_id: str,
        role: DeviceRole = DeviceRole.READ_WRITE,
        expires_in_days: int = None
    ) -> DeviceCredentials:
        """
        Create a new API key for a device.
        
        Returns:
            DeviceCredentials with plain-text API key (store securely!)
        """
        key_id = secrets.token_hex(8)
        api_key = f"ssk_{secrets.token_urlsafe(32)}"  # sqlite-sync-key
        api_secret = secrets.token_urlsafe(32)
        
        # Hash the key for storage
        key_hash = self._hash_key(api_key)
        
        now = int(time.time())
        expires_at = None
        if expires_in_days:
            expires_at = now + (expires_in_days * 86400)
        
        self._conn.execute(
            """
            INSERT INTO api_keys 
            (key_id, device_id, key_hash, role, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (key_id, device_id, key_hash, role.value, now, expires_at)
        )
        self._conn.commit()
        
        self._audit_log(device_id, "api_key_created", True, {"key_id": key_id})
        
        return DeviceCredentials(
            device_id=device_id,
            api_key=api_key,
            api_secret=api_secret,
            role=role,
            created_at=now,
            expires_at=expires_at
        )
    
    def authenticate_api_key(self, api_key: str) -> AuthResult:
        """Authenticate using API key."""
        key_hash = self._hash_key(api_key)
        now = int(time.time())
        
        cursor = self._conn.execute(
            """
            SELECT device_id, role, expires_at, revoked
            FROM api_keys
            WHERE key_hash = ?
            """,
            (key_hash,)
        )
        
        row = cursor.fetchone()
        
        if row is None:
            self._audit_log(None, "api_key_auth", False, {"reason": "not_found"})
            return AuthResult(authenticated=False, error="Invalid API key")
        
        device_id, role_str, expires_at, revoked = row
        
        if revoked:
            self._audit_log(device_id, "api_key_auth", False, {"reason": "revoked"})
            return AuthResult(authenticated=False, error="API key revoked")
        
        if expires_at and expires_at < now:
            self._audit_log(device_id, "api_key_auth", False, {"reason": "expired"})
            return AuthResult(authenticated=False, error="API key expired")
        
        # Update last used
        self._conn.execute(
            "UPDATE api_keys SET last_used_at = ? WHERE key_hash = ?",
            (now, key_hash)
        )
        self._conn.commit()
        
        self._audit_log(device_id, "api_key_auth", True)
        
        return AuthResult(
            authenticated=True,
            device_id=device_id,
            role=DeviceRole(role_str),
            token_type=TokenType.API_KEY
        )
    
    def revoke_api_key(self, key_id: str, device_id: str = None) -> bool:
        """Revoke an API key."""
        query = "UPDATE api_keys SET revoked = 1, revoked_at = ? WHERE key_id = ?"
        params = [int(time.time()), key_id]
        
        if device_id:
            query += " AND device_id = ?"
            params.append(device_id)
        
        cursor = self._conn.execute(query, params)
        self._conn.commit()
        
        if cursor.rowcount > 0:
            self._audit_log(device_id, "api_key_revoked", True, {"key_id": key_id})
            return True
        return False
    
    # -------------------------------------------------------------------------
    # JWT Authentication
    # -------------------------------------------------------------------------
    
    def authenticate_jwt(self, token: str) -> AuthResult:
        """Authenticate using JWT access token."""
        valid, payload_or_error = self._jwt.verify_token(token)
        
        if not valid:
            self._audit_log(None, "jwt_auth", False, {"reason": payload_or_error})
            return AuthResult(authenticated=False, error=payload_or_error)
        
        payload = payload_or_error
        
        if payload.get("type") != "access":
            return AuthResult(authenticated=False, error="Not an access token")
        
        device_id = payload.get("sub")
        role_str = payload.get("role", "read_write")
        
        self._audit_log(device_id, "jwt_auth", True)
        
        return AuthResult(
            authenticated=True,
            device_id=device_id,
            role=DeviceRole(role_str),
            token_type=TokenType.JWT
        )
    
    def create_token_pair(
        self,
        device_id: str,
        role: DeviceRole
    ) -> tuple[str, str]:
        """
        Create access and refresh token pair.
        
        Returns:
            (access_token, refresh_token)
        """
        access_token = self._jwt.create_access_token(device_id, role)
        refresh_token = self._jwt.create_refresh_token(device_id)
        
        # Store refresh token hash for revocation
        session_id = secrets.token_hex(16)
        refresh_hash = self._hash_key(refresh_token)
        now = int(time.time())
        
        self._conn.execute(
            """
            INSERT INTO auth_sessions 
            (session_id, device_id, refresh_token_hash, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (session_id, device_id, refresh_hash, now, now + 86400 * 30)
        )
        self._conn.commit()
        
        self._audit_log(device_id, "token_pair_created", True, {"session_id": session_id})
        
        return access_token, refresh_token
    
    def refresh_access_token(self, refresh_token: str) -> AuthResult | str:
        """
        Use refresh token to get new access token.
        
        Returns:
            New access token or AuthResult with error
        """
        valid, payload_or_error = self._jwt.verify_token(refresh_token)
        
        if not valid:
            return AuthResult(authenticated=False, error=payload_or_error)
        
        payload = payload_or_error
        
        if payload.get("type") != "refresh":
            return AuthResult(authenticated=False, error="Not a refresh token")
        
        device_id = payload.get("sub")
        refresh_hash = self._hash_key(refresh_token)
        now = int(time.time())
        
        # Check if refresh token is valid and not revoked
        cursor = self._conn.execute(
            """
            SELECT session_id, revoked FROM auth_sessions
            WHERE device_id = ? AND refresh_token_hash = ? AND expires_at > ?
            """,
            (device_id, refresh_hash, now)
        )
        
        row = cursor.fetchone()
        
        if row is None:
            return AuthResult(authenticated=False, error="Session not found")
        
        session_id, revoked = row
        
        if revoked:
            return AuthResult(authenticated=False, error="Session revoked")
        
        # Update last used
        self._conn.execute(
            "UPDATE auth_sessions SET last_used_at = ? WHERE session_id = ?",
            (now, session_id)
        )
        self._conn.commit()
        
        # Get device role
        cursor = self._conn.execute(
            "SELECT role FROM api_keys WHERE device_id = ? AND revoked = 0 LIMIT 1",
            (device_id,)
        )
        row = cursor.fetchone()
        role = DeviceRole(row[0]) if row else DeviceRole.READ_WRITE
        
        # Create new access token
        new_access_token = self._jwt.create_access_token(device_id, role)
        
        self._audit_log(device_id, "token_refreshed", True, {"session_id": session_id})
        
        return new_access_token
    
    def revoke_all_sessions(self, device_id: str) -> int:
        """Revoke all sessions for a device."""
        cursor = self._conn.execute(
            "UPDATE auth_sessions SET revoked = 1 WHERE device_id = ? AND revoked = 0",
            (device_id,)
        )
        self._conn.commit()
        
        count = cursor.rowcount
        self._audit_log(device_id, "all_sessions_revoked", True, {"count": count})
        
        return count
    
    # -------------------------------------------------------------------------
    # Device Registration Workflow
    # -------------------------------------------------------------------------
    
    def request_device_registration(
        self,
        device_id: str,
        device_name: str,
        requested_role: DeviceRole = DeviceRole.READ_WRITE
    ) -> dict:
        """
        Request device registration (for approval workflow).
        
        Returns:
            Registration status
        """
        now = int(time.time())
        
        self._conn.execute(
            """
            INSERT INTO device_approvals 
            (device_id, device_name, requested_role, request_time)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                device_name = excluded.device_name,
                requested_role = excluded.requested_role,
                request_time = excluded.request_time,
                approved = 0,
                rejected = 0
            """,
            (device_id, device_name, requested_role.value, now)
        )
        self._conn.commit()
        
        self._audit_log(device_id, "registration_requested", True, {"role": requested_role.value})
        
        if not self._require_approval:
            # Auto-approve
            return self.approve_device(device_id, "auto")
        
        return {
            "status": "pending",
            "device_id": device_id,
            "message": "Registration pending approval"
        }
    
    def approve_device(
        self,
        device_id: str,
        approved_by: str,
        role: DeviceRole = None
    ) -> dict:
        """Approve a pending device registration."""
        now = int(time.time())
        
        cursor = self._conn.execute(
            "SELECT requested_role FROM device_approvals WHERE device_id = ? AND approved = 0",
            (device_id,)
        )
        row = cursor.fetchone()
        
        if row is None:
            return {"error": "No pending registration found"}
        
        final_role = role or DeviceRole(row[0])
        
        # Update approval
        self._conn.execute(
            """
            UPDATE device_approvals 
            SET approved = 1, approved_by = ?, approved_at = ?
            WHERE device_id = ?
            """,
            (approved_by, now, device_id)
        )
        
        # Create API key
        credentials = self.create_api_key(device_id, final_role)
        
        self._audit_log(device_id, "device_approved", True, {"by": approved_by})
        
        return {
            "status": "approved",
            "device_id": device_id,
            "api_key": credentials.api_key,
            "role": final_role.value
        }
    
    def reject_device(self, device_id: str, reason: str) -> bool:
        """Reject a pending device registration."""
        cursor = self._conn.execute(
            """
            UPDATE device_approvals 
            SET rejected = 1, rejection_reason = ?
            WHERE device_id = ? AND approved = 0
            """,
            (reason, device_id)
        )
        self._conn.commit()
        
        if cursor.rowcount > 0:
            self._audit_log(device_id, "device_rejected", True, {"reason": reason})
            return True
        return False
    
    def get_pending_approvals(self) -> list[dict]:
        """Get all pending device registrations."""
        cursor = self._conn.execute(
            """
            SELECT device_id, device_name, requested_role, request_time
            FROM device_approvals
            WHERE approved = 0 AND rejected = 0
            ORDER BY request_time DESC
            """
        )
        
        return [
            {
                "device_id": row[0],
                "device_name": row[1],
                "requested_role": row[2],
                "request_time": row[3]
            }
            for row in cursor
        ]
    
    # -------------------------------------------------------------------------
    # Authorization
    # -------------------------------------------------------------------------
    
    def check_permission(
        self,
        role: DeviceRole,
        action: str
    ) -> bool:
        """
        Check if role has permission for action.
        
        Actions:
        - read: Pull operations
        - write: Push operations
        - admin: Manage devices, view audit logs
        """
        permissions = {
            DeviceRole.PENDING: set(),
            DeviceRole.READ_ONLY: {"read"},
            DeviceRole.READ_WRITE: {"read", "write"},
            DeviceRole.ADMIN: {"read", "write", "admin"}
        }
        
        return action in permissions.get(role, set())
    
    def require_permission(self, action: str) -> Callable:
        """Decorator to require permission for an action."""
        def decorator(f):
            @wraps(f)
            def wrapper(*args, auth_result: AuthResult = None, **kwargs):
                if not auth_result or not auth_result.authenticated:
                    raise PermissionError("Not authenticated")
                
                if not self.check_permission(auth_result.role, action):
                    raise PermissionError(f"Permission denied: {action}")
                
                return f(*args, auth_result=auth_result, **kwargs)
            return wrapper
        return decorator
    
    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------
    
    def _hash_key(self, key: str) -> str:
        """Hash an API key or token for storage."""
        return hashlib.sha256(key.encode()).hexdigest()
    
    def _audit_log(
        self,
        device_id: str | None,
        action: str,
        success: bool,
        details: dict = None,
        ip_address: str = None
    ) -> None:
        """Log authentication action."""
        self._conn.execute(
            """
            INSERT INTO auth_audit_log 
            (device_id, action, success, details, ip_address, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (device_id, action, int(success), json.dumps(details or {}), ip_address, int(time.time()))
        )
        # Don't commit here - let caller handle transaction
    
    def cleanup_expired(self) -> dict:
        """Remove expired sessions and old audit logs."""
        now = int(time.time())
        
        # Expired sessions
        cursor = self._conn.execute(
            "DELETE FROM auth_sessions WHERE expires_at < ?",
            (now,)
        )
        sessions_deleted = cursor.rowcount
        
        # Old audit logs (keep 30 days)
        cursor = self._conn.execute(
            "DELETE FROM auth_audit_log WHERE timestamp < ?",
            (now - 30 * 86400,)
        )
        audit_deleted = cursor.rowcount
        
        self._conn.commit()
        
        return {
            "sessions_deleted": sessions_deleted,
            "audit_logs_deleted": audit_deleted
        }
