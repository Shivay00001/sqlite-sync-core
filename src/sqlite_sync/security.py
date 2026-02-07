"""
security.py - Security layer for sync operations.

Provides:
- Bundle signing (HMAC)
- Optional encryption (AES-256-GCM)
- Device identity verification
- Replay protection
"""

import hmac
import hashlib
import time
import os
import json
from dataclasses import dataclass
from typing import Any

# Try to import cryptography for AES
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


@dataclass
class SignedBundle:
    """Bundle with cryptographic signature."""
    bundle_data: bytes
    signature: bytes
    device_id: bytes
    timestamp: int
    nonce: bytes


@dataclass  
class EncryptedBundle:
    """Encrypted bundle data."""
    ciphertext: bytes
    nonce: bytes
    salt: bytes
    device_id: bytes
    timestamp: int


class SecurityManager:
    """
    Manages security operations for sync.
    
    Features:
    - HMAC-SHA256 bundle signing
    - AES-256-GCM encryption (optional)
    - Nonce-based replay protection
    - Device identity binding
    """
    
    def __init__(
        self, 
        device_id: bytes,
        signing_key: bytes | None = None,
        encryption_key: bytes | None = None
    ):
        self._device_id = device_id
        self._signing_key = signing_key or os.urandom(32)
        self._encryption_key = encryption_key
        self._used_nonces: set[bytes] = set()
        self._nonce_expiry_seconds = 3600  # 1 hour
    
    def sign_bundle(self, bundle_data: bytes) -> SignedBundle:
        """
        Sign bundle data with HMAC-SHA256.
        
        Args:
            bundle_data: Raw bundle bytes
            
        Returns:
            SignedBundle with signature
        """
        timestamp = int(time.time())
        nonce = os.urandom(16)
        
        # Create message to sign
        message = self._create_sign_message(bundle_data, timestamp, nonce)
        
        # Generate HMAC signature
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
            nonce=nonce
        )
    
    def verify_signature(self, signed_bundle: SignedBundle, signing_key: bytes) -> bool:
        """
        Verify bundle signature.
        
        Args:
            signed_bundle: Bundle with signature
            signing_key: Expected signing key
            
        Returns:
            True if signature is valid
        """
        # Check timestamp freshness (prevent replay of old bundles)
        now = int(time.time())
        if abs(now - signed_bundle.timestamp) > self._nonce_expiry_seconds:
            return False
        
        # Check nonce hasn't been used (prevent replay)
        if signed_bundle.nonce in self._used_nonces:
            return False
        
        # Recreate expected signature
        message = self._create_sign_message(
            signed_bundle.bundle_data,
            signed_bundle.timestamp,
            signed_bundle.nonce
        )
        
        expected_signature = hmac.new(
            signing_key,
            message,
            hashlib.sha256
        ).digest()
        
        # Constant-time comparison
        if hmac.compare_digest(expected_signature, signed_bundle.signature):
            self._used_nonces.add(signed_bundle.nonce)
            return True
        
        return False
    
    def encrypt_bundle(self, bundle_data: bytes, password: str) -> EncryptedBundle:
        """
        Encrypt bundle with AES-256-GCM.
        
        Args:
            bundle_data: Raw bundle bytes
            password: Encryption password
            
        Returns:
            EncryptedBundle
        """
        if not HAS_CRYPTO:
            raise ImportError("cryptography package required: pip install cryptography")
        
        # Derive key from password
        salt = os.urandom(16)
        key = self._derive_key(password, salt)
        
        # Encrypt with AES-GCM
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
        """
        Decrypt bundle.
        
        Args:
            encrypted: Encrypted bundle
            password: Decryption password
            
        Returns:
            Decrypted bundle data
        """
        if not HAS_CRYPTO:
            raise ImportError("cryptography package required: pip install cryptography")
        
        # Derive key
        key = self._derive_key(password, encrypted.salt)
        
        # Decrypt
        aesgcm = AESGCM(key)
        return aesgcm.decrypt(encrypted.nonce, encrypted.ciphertext, None)
    
    def generate_device_keypair(self) -> tuple[bytes, bytes]:
        """Generate a new device signing keypair."""
        private_key = os.urandom(32)
        # For HMAC, we just use the same key - for asymmetric, use proper crypto
        public_key = hashlib.sha256(private_key).digest()
        return private_key, public_key
    
    def verify_device_identity(self, device_id: bytes, proof: bytes, challenge: bytes) -> bool:
        """
        Verify device identity using challenge-response.
        
        Args:
            device_id: Claimed device ID
            proof: HMAC proof of challenge
            challenge: Random challenge bytes
            
        Returns:
            True if identity verified
        """
        # In production, lookup the device's public key
        # For now, just verify format
        expected = hmac.new(device_id, challenge, hashlib.sha256).digest()
        return hmac.compare_digest(expected, proof)
    
    def _create_sign_message(self, data: bytes, timestamp: int, nonce: bytes) -> bytes:
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
        """Remove expired nonces from memory."""
        # In production, nonces should be stored with timestamps
        # This is a simplified implementation
        old_count = len(self._used_nonces)
        if old_count > 10000:
            self._used_nonces.clear()
        return old_count
