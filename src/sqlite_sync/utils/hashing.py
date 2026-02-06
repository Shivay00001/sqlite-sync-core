"""
hashing.py - Cryptographic hashing utilities.

SHA-256 is used for:
- Bundle content hashes (integrity verification)
- Idempotency checks (same bundle = same hash)

All hashing is deterministic: same input = same output.
"""

import hashlib
from typing import Iterable

from sqlite_sync.errors import ValidationError


def sha256_bytes(data: bytes) -> bytes:
    """
    Compute SHA-256 hash of bytes.
    
    Args:
        data: Bytes to hash
        
    Returns:
        32-byte SHA-256 digest
    """
    return hashlib.sha256(data).digest()


def sha256_hex(data: bytes) -> str:
    """
    Compute SHA-256 hash and return as hex string.
    
    Args:
        data: Bytes to hash
        
    Returns:
        64-character lowercase hex string
    """
    return hashlib.sha256(data).hexdigest()


def sha256_operations(op_ids: Iterable[bytes]) -> bytes:
    """
    Compute SHA-256 hash of a sequence of operation IDs.
    
    Used for bundle content verification.
    Operations are hashed in order - caller must ensure
    deterministic ordering.
    
    Args:
        op_ids: Iterable of operation IDs (16-byte UUIDs)
        
    Returns:
        32-byte SHA-256 digest
        
    Raises:
        ValidationError: If any op_id is not 16 bytes
    """
    hasher = hashlib.sha256()
    count = 0

    for op_id in op_ids:
        if len(op_id) != 16:
            raise ValidationError(
                f"Operation ID must be 16 bytes, got {len(op_id)} at index {count}",
                field="op_id",
                value=op_id.hex() if isinstance(op_id, bytes) else op_id,
            )
        hasher.update(op_id)
        count += 1

    return hasher.digest()


def verify_hash(data: bytes, expected_hash: bytes) -> bool:
    """
    Verify that data matches expected SHA-256 hash.
    
    Uses constant-time comparison to prevent timing attacks.
    
    Args:
        data: Data to hash
        expected_hash: Expected 32-byte hash
        
    Returns:
        True if hash matches, False otherwise
    """
    actual_hash = sha256_bytes(data)
    return hmac_compare(actual_hash, expected_hash)


def hmac_compare(a: bytes, b: bytes) -> bool:
    """
    Constant-time comparison of two byte strings.
    
    Prevents timing attacks by ensuring comparison time
    is independent of where strings differ.
    
    Args:
        a: First byte string
        b: Second byte string
        
    Returns:
        True if equal, False otherwise
    """
    if len(a) != len(b):
        return False

    result = 0
    for x, y in zip(a, b):
        result |= x ^ y

    return result == 0
