"""
security.py - Enterprise Security Layer

Provides:
- HMAC-SHA256 bundle signing
- Ed25519 asymmetric signing for device identity
- AES-256-GCM encryption
- Persistent nonce storage (SQLite-backed)
- Device identity verification
- Replay protection with persistent storage
- Security audit logging
"""

import hmac
import hashlib
import time
import os
import json
import sqlite3
import threading
import logging
from dataclasses import dataclass
from typing import Any, Optional, Dict

logger = logging.getLogger(__name__)

# Try to import cryptography for advanced features
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey, Ed25519PublicKey
    )
    from cryptography.exceptions import InvalidSignature
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


# =============================================================================
# Data Types
# =============================================================================

@dataclass
class SignedBundle:
    """Bundle with cryptographic signature."""
    bundle_data: bytes
    signature: bytes
    device_id: bytes
    timestamp: int
    nonce: bytes
    signature_type: str = "hmac"  # "hmac" or "ed25519"


@dataclass  
class EncryptedBundle:
    """Encrypted bundle data."""
    ciphertext: bytes
    nonce: bytes
    salt: bytes
    device_id: bytes
    timestamp: int


@dataclass
class DeviceIdentity:
    """Device cryptographic identity."""
    device_id: bytes
    public_key: bytes
    private_key: Optional[bytes] = None
    created_at: int = 0
    key_type: str = "ed25519"


# =============================================================================
# Persistent Nonce Storage
# =============================================================================

class NonceStore:
    """
    Persistent nonce storage to survive restarts.
    
    Prevents replay attacks across application restarts.
    """
    
    SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS used_nonces (
        nonce_hash TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
    );
    
    CREATE INDEX IF NOT EXISTS idx_nonces_expires ON used_nonces(expires_at);
    CREATE INDEX IF NOT EXISTS idx_nonces_device ON used_nonces(device_id);
    """
    
    def __init__(self, db_path: str = ":memory:", ttl_seconds: int = 3600):
        self._db_path = db_path
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize database."""
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.executescript(self.SCHEMA_SQL)
        self._conn.commit()
    
    def add_nonce(self, nonce: bytes, device_id: str) -> bool:
        """
        Add a nonce and check if it was already used.
        
        Returns:
            True if nonce was new (not a replay)
        """
        nonce_hash = hashlib.sha256(nonce).hexdigest()
        now = int(time.time())
        expires_at = now + self._ttl
        
        with self._lock:
            # Check if exists
            cursor = self._conn.execute(
                "SELECT 1 FROM used_nonces WHERE nonce_hash = ? AND expires_at > ?",
                (nonce_hash, now)
            )
            
            if cursor.fetchone():
                logger.warning(f"Replay detected: nonce from {device_id}")
                return False
            
            # Add new nonce
            try:
                self._conn.execute(
                    """
                    INSERT INTO used_nonces (nonce_hash, device_id, timestamp, expires_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (nonce_hash, device_id, now, expires_at)
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
    
    def is_nonce_used(self, nonce: bytes) -> bool:
        """Check if nonce has been used."""
        nonce_hash = hashlib.sha256(nonce).hexdigest()
        now = int(time.time())
        
        cursor = self._conn.execute(
            "SELECT 1 FROM used_nonces WHERE nonce_hash = ? AND expires_at > ?",
            (nonce_hash, now)
        )
        return cursor.fetchone() is not None
    
    def cleanup_expired(self) -> int:
        """Remove expired nonces."""
        now = int(time.time())
        
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM used_nonces WHERE expires_at < ?",
                (now,)
            )
            self._conn.commit()
            return cursor.rowcount
    
    def get_stats(self) -> Dict:
        """Get nonce store statistics."""
        now = int(time.time())
        
        cursor = self._conn.execute("SELECT COUNT(*) FROM used_nonces")
        total = cursor.fetchone()[0]
        
        cursor = self._conn.execute(
            "SELECT COUNT(*) FROM used_nonces WHERE expires_at > ?",
            (now,)
        )
        active = cursor.fetchone()[0]
        
        return {
            "total_nonces": total,
            "active_nonces": active,
            "expired_nonces": total - active
        }
    
    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()


# =============================================================================
# Device Key Store
# =============================================================================

class DeviceKeyStore:
    """
    Persistent storage for device public keys.
    
    Used for Ed25519 signature verification.
    """
    
    SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS device_keys (
        device_id TEXT PRIMARY KEY,
        public_key BLOB NOT NULL,
        key_type TEXT DEFAULT 'ed25519',
        created_at INTEGER NOT NULL,
        revoked INTEGER DEFAULT 0,
        revoked_at INTEGER,
        metadata TEXT DEFAULT '{}'
    );
    
    CREATE TABLE IF NOT EXISTS key_trust_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        timestamp INTEGER NOT NULL
    );
    """
    
    def __init__(self, db_path: str = ":memory:"):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize database."""
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.executescript(self.SCHEMA_SQL)
        self._conn.commit()
    
    def register_device(
        self, 
        device_id: str, 
        public_key: bytes,
        key_type: str = "ed25519"
    ) -> bool:
        """Register a device's public key."""
        now = int(time.time())
        
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO device_keys (device_id, public_key, key_type, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(device_id) DO UPDATE SET
                        public_key = excluded.public_key,
                        key_type = excluded.key_type
                    """,
                    (device_id, public_key, key_type, now)
                )
                
                self._log_action(device_id, "register", {"key_type": key_type})
                self._conn.commit()
                return True
                
            except Exception as e:
                logger.error(f"Failed to register device key: {e}")
                return False
    
    def get_public_key(self, device_id: str) -> Optional[bytes]:
        """Get device's public key."""
        cursor = self._conn.execute(
            "SELECT public_key FROM device_keys WHERE device_id = ? AND revoked = 0",
            (device_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None
    
    def revoke_device(self, device_id: str, reason: str = None) -> bool:
        """Revoke a device's key."""
        now = int(time.time())
        
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE device_keys SET revoked = 1, revoked_at = ? WHERE device_id = ?",
                (now, device_id)
            )
            
            if cursor.rowcount > 0:
                self._log_action(device_id, "revoke", {"reason": reason})
                self._conn.commit()
                return True
        return False
    
    def is_device_trusted(self, device_id: str) -> bool:
        """Check if device is trusted (has valid key)."""
        cursor = self._conn.execute(
            "SELECT 1 FROM device_keys WHERE device_id = ? AND revoked = 0",
            (device_id,)
        )
        return cursor.fetchone() is not None
    
    def _log_action(self, device_id: str, action: str, details: dict = None) -> None:
        """Log key trust action."""
        self._conn.execute(
            """
            INSERT INTO key_trust_log (device_id, action, details, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (device_id, action, json.dumps(details or {}), int(time.time()))
        )
    
    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()


# =============================================================================
# Ed25519 Asymmetric Signing
# =============================================================================

class AsymmetricSigner:
    """
    Ed25519 asymmetric signing for device identity.
    
    Provides unforgeable device signatures without shared secrets.
    """
    
    def __init__(self, key_store: DeviceKeyStore = None):
        if not HAS_CRYPTO:
            raise ImportError(
                "cryptography package required for Ed25519: pip install cryptography"
            )
        self._key_store = key_store
    
    def generate_keypair(self) -> DeviceIdentity:
        """Generate a new Ed25519 keypair."""
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        
        # Serialize keys
        private_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        public_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        
        device_id = hashlib.sha256(public_bytes).digest()[:16]
        
        return DeviceIdentity(
            device_id=device_id,
            public_key=public_bytes,
            private_key=private_bytes,
            created_at=int(time.time()),
            key_type="ed25519"
        )
    
    def sign_data(self, data: bytes, private_key: bytes) -> bytes:
        """Sign data with Ed25519 private key."""
        key = Ed25519PrivateKey.from_private_bytes(private_key)
        return key.sign(data)
    
    def verify_signature(
        self, 
        data: bytes, 
        signature: bytes, 
        public_key: bytes
    ) -> bool:
        """Verify Ed25519 signature."""
        try:
            key = Ed25519PublicKey.from_public_bytes(public_key)
            key.verify(signature, data)
            return True
        except InvalidSignature:
            return False
        except Exception as e:
            logger.error(f"Signature verification error: {e}")
            return False
    
    def sign_bundle(
        self, 
        bundle_data: bytes, 
        identity: DeviceIdentity
    ) -> SignedBundle:
        """Sign a bundle with device identity."""
        if not identity.private_key:
            raise ValueError("Private key required for signing")
        
        timestamp = int(time.time())
        nonce = os.urandom(16)
        
        # Create message to sign
        message = self._create_sign_message(bundle_data, timestamp, nonce)
        signature = self.sign_data(message, identity.private_key)
        
        return SignedBundle(
            bundle_data=bundle_data,
            signature=signature,
            device_id=identity.device_id,
            timestamp=timestamp,
            nonce=nonce,
            signature_type="ed25519"
        )
    
    def verify_bundle(
        self, 
        signed_bundle: SignedBundle,
        public_key: bytes = None
    ) -> bool:
        """Verify a signed bundle."""
        # Get public key from store if not provided
        if public_key is None and self._key_store:
            device_id_str = signed_bundle.device_id.hex()
            public_key = self._key_store.get_public_key(device_id_str)
        
        if public_key is None:
            logger.error("Public key not found for device")
            return False
        
        message = self._create_sign_message(
            signed_bundle.bundle_data,
            signed_bundle.timestamp,
            signed_bundle.nonce
        )
        
        return self.verify_signature(message, signed_bundle.signature, public_key)
    
    def _create_sign_message(
        self, 
        data: bytes, 
        timestamp: int, 
        nonce: bytes
    ) -> bytes:
        """Create message for signing."""
        return b"".join([
            len(data).to_bytes(4, "big"),
            data,
            timestamp.to_bytes(8, "big"),
            nonce
        ])


# =============================================================================
# Security Manager (Enhanced)
# =============================================================================

class SecurityManager:
    """
    Enterprise security manager for sync operations.
    
    Features:
    - HMAC-SHA256 symmetric signing
    - Ed25519 asymmetric signing (optional)
    - AES-256-GCM encryption
    - Persistent nonce-based replay protection
    - Device identity and key management
    - Security audit logging
    """
    
    def __init__(
        self, 
        device_id: bytes,
        signing_key: bytes = None,
        encryption_key: bytes = None,
        nonce_db_path: str = None,
        key_db_path: str = None
    ):
        self._device_id = device_id
        self._signing_key = signing_key or os.urandom(32)
        self._encryption_key = encryption_key
        self._nonce_expiry_seconds = 3600
        
        # Persistent stores
        self._nonce_store = NonceStore(
            db_path=nonce_db_path or ":memory:",
            ttl_seconds=self._nonce_expiry_seconds
        )
        
        self._key_store = DeviceKeyStore(
            db_path=key_db_path or ":memory:"
        )
        
        # Asymmetric signer (optional)
        self._asymmetric_signer: Optional[AsymmetricSigner] = None
        self._device_identity: Optional[DeviceIdentity] = None
        
        if HAS_CRYPTO:
            self._asymmetric_signer = AsymmetricSigner(self._key_store)
    
    # -------------------------------------------------------------------------
    # HMAC Signing (Symmetric)
    # -------------------------------------------------------------------------
    
    def sign_bundle(self, bundle_data: bytes) -> SignedBundle:
        """Sign bundle with HMAC-SHA256."""
        timestamp = int(time.time())
        nonce = os.urandom(16)
        
        message = self._create_sign_message(bundle_data, timestamp, nonce)
        
        signature = hmac.new(
            self._signing_key,
            message,
            hashlib.sha256
        ).digest()
        
        return SignedBundle(
            bundle_data=bundle_data,
            signature=signature,
            device_id=self._device_id,
            timestamp=timestamp,
            nonce=nonce,
            signature_type="hmac"
        )
    
    def verify_signature(
        self, 
        signed_bundle: SignedBundle, 
        signing_key: bytes = None
    ) -> bool:
        """Verify bundle signature with replay protection."""
        key = signing_key or self._signing_key
        
        # Check timestamp freshness
        now = int(time.time())
        if abs(now - signed_bundle.timestamp) > self._nonce_expiry_seconds:
            logger.warning("Bundle timestamp too old")
            return False
        
        # Check nonce for replay (persistent)
        device_id_str = signed_bundle.device_id.hex()
        if not self._nonce_store.add_nonce(signed_bundle.nonce, device_id_str):
            logger.warning("Replay attack detected")
            return False
        
        # Verify signature
        message = self._create_sign_message(
            signed_bundle.bundle_data,
            signed_bundle.timestamp,
            signed_bundle.nonce
        )
        
        if signed_bundle.signature_type == "ed25519":
            return self._verify_asymmetric(signed_bundle, message)
        
        expected = hmac.new(key, message, hashlib.sha256).digest()
        return hmac.compare_digest(expected, signed_bundle.signature)
    
    def _verify_asymmetric(
        self, 
        signed_bundle: SignedBundle, 
        message: bytes
    ) -> bool:
        """Verify Ed25519 signature."""
        if not self._asymmetric_signer:
            logger.error("Asymmetric signing not available")
            return False
        
        device_id_str = signed_bundle.device_id.hex()
        public_key = self._key_store.get_public_key(device_id_str)
        
        if not public_key:
            logger.error(f"Unknown device: {device_id_str}")
            return False
        
        return self._asymmetric_signer.verify_signature(
            message, 
            signed_bundle.signature, 
            public_key
        )
    
    # -------------------------------------------------------------------------
    # Asymmetric Signing (Ed25519)
    # -------------------------------------------------------------------------
    
    def generate_device_identity(self) -> DeviceIdentity:
        """Generate Ed25519 device identity."""
        if not self._asymmetric_signer:
            raise ImportError("cryptography package required for Ed25519")
        
        identity = self._asymmetric_signer.generate_keypair()
        self._device_identity = identity
        
        # Register public key
        self._key_store.register_device(
            identity.device_id.hex(),
            identity.public_key,
            identity.key_type
        )
        
        return identity
    
    def sign_bundle_asymmetric(
        self, 
        bundle_data: bytes,
        identity: DeviceIdentity = None
    ) -> SignedBundle:
        """Sign bundle with Ed25519."""
        if not self._asymmetric_signer:
            raise ImportError("cryptography package required for Ed25519")
        
        identity = identity or self._device_identity
        if not identity:
            raise ValueError("Device identity required")
        
        return self._asymmetric_signer.sign_bundle(bundle_data, identity)
    
    def register_trusted_device(
        self, 
        device_id: str, 
        public_key: bytes
    ) -> bool:
        """Register a trusted device's public key."""
        return self._key_store.register_device(device_id, public_key)
    
    def revoke_device(self, device_id: str, reason: str = None) -> bool:
        """Revoke a device's trust."""
        return self._key_store.revoke_device(device_id, reason)
    
    def is_device_trusted(self, device_id: str) -> bool:
        """Check if device is trusted."""
        return self._key_store.is_device_trusted(device_id)
    
    # -------------------------------------------------------------------------
    # Encryption
    # -------------------------------------------------------------------------
    
    def encrypt_bundle(self, bundle_data: bytes, password: str) -> EncryptedBundle:
        """Encrypt bundle with AES-256-GCM."""
        if not HAS_CRYPTO:
            raise ImportError("cryptography package required")
        
        salt = os.urandom(16)
        key = self._derive_key(password, salt)
        
        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(nonce, bundle_data, None)
        
        return EncryptedBundle(
            ciphertext=ciphertext,
            nonce=nonce,
            salt=salt,
            device_id=self._device_id,
            timestamp=int(time.time())
        )
    
    def decrypt_bundle(self, encrypted: EncryptedBundle, password: str) -> bytes:
        """Decrypt bundle."""
        if not HAS_CRYPTO:
            raise ImportError("cryptography package required")
        
        key = self._derive_key(password, encrypted.salt)
        aesgcm = AESGCM(key)
        return aesgcm.decrypt(encrypted.nonce, encrypted.ciphertext, None)
    
    # -------------------------------------------------------------------------
    # Device Identity
    # -------------------------------------------------------------------------
    
    def create_challenge(self) -> bytes:
        """Create a random challenge for device verification."""
        return os.urandom(32)
    
    def respond_to_challenge(
        self, 
        challenge: bytes,
        identity: DeviceIdentity = None
    ) -> bytes:
        """Create response to authentication challenge."""
        identity = identity or self._device_identity
        
        if identity and identity.private_key and self._asymmetric_signer:
            # Ed25519 signature
            return self._asymmetric_signer.sign_data(challenge, identity.private_key)
        else:
            # HMAC fallback
            return hmac.new(
                self._device_id, 
                challenge, 
                hashlib.sha256
            ).digest()
    
    def verify_challenge_response(
        self,
        challenge: bytes,
        response: bytes,
        device_id: str
    ) -> bool:
        """Verify challenge response."""
        # Try Ed25519 verification first
        if self._key_store:
            public_key = self._key_store.get_public_key(device_id)
            if public_key and self._asymmetric_signer:
                return self._asymmetric_signer.verify_signature(
                    challenge, response, public_key
                )
        
        # HMAC fallback
        expected = hmac.new(
            bytes.fromhex(device_id), 
            challenge, 
            hashlib.sha256
        ).digest()
        return hmac.compare_digest(expected, response)
    
    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------
    
    def _create_sign_message(
        self, 
        data: bytes, 
        timestamp: int, 
        nonce: bytes
    ) -> bytes:
        """Create message for signing."""
        return b"".join([
            len(data).to_bytes(4, "big"),
            data,
            timestamp.to_bytes(8, "big"),
            nonce
        ])
    
    def _derive_key(self, password: str, salt: bytes) -> bytes:
        """Derive encryption key from password."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(password.encode())
    
    def cleanup_expired_nonces(self) -> int:
        """Remove expired nonces."""
        return self._nonce_store.cleanup_expired()
    
    def get_security_stats(self) -> Dict:
        """Get security statistics."""
        nonce_stats = self._nonce_store.get_stats()
        
        return {
            "device_id": self._device_id.hex(),
            "has_identity": self._device_identity is not None,
            "asymmetric_available": self._asymmetric_signer is not None,
            **nonce_stats
        }
    
    def close(self) -> None:
        """Close all resources."""
        self._nonce_store.close()
        self._key_store.close()
