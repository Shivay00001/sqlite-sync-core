"""
msgpack_codec.py - Canonical MessagePack serialization.

MessagePack is used for serializing:
- Primary key values (row_pk)
- Column values (old_values, new_values)

Canonicalization ensures identical data produces identical bytes,
which is critical for deterministic hashing and replay.
"""

import msgpack
from typing import Any

from sqlite_sync.errors import ValidationError


def pack_value(value: Any) -> bytes:
    """
    Serialize a single value to MessagePack.
    
    Used for primary key serialization.
    
    Args:
        value: Value to serialize (must be msgpack-compatible)
        
    Returns:
        MessagePack bytes
        
    Raises:
        ValidationError: If value cannot be serialized
    """
    try:
        return msgpack.packb(value, use_bin_type=True)
    except (TypeError, ValueError) as e:
        raise ValidationError(
            f"Cannot serialize value to MessagePack: {e}",
            field="value",
            value=value,
        ) from e


def unpack_value(data: bytes) -> Any:
    """
    Deserialize a single value from MessagePack.
    
    Args:
        data: MessagePack bytes
        
    Returns:
        Deserialized value
        
    Raises:
        ValidationError: If data cannot be deserialized
    """
    try:
        return msgpack.unpackb(data, raw=False)
    except (msgpack.UnpackException, ValueError) as e:
        raise ValidationError(
            f"Cannot deserialize MessagePack: {e}",
            field="data",
            value=data[:50] if len(data) > 50 else data,
        ) from e


def pack_dict(data: dict[str, Any]) -> bytes:
    """
    Serialize a dictionary to canonical MessagePack.
    
    Keys are sorted alphabetically to ensure canonical representation.
    This is critical for deterministic hashing.
    
    Args:
        data: Dictionary to serialize
        
    Returns:
        MessagePack bytes with keys in sorted order
        
    Raises:
        ValidationError: If data cannot be serialized
    """
    if not isinstance(data, dict):
        raise ValidationError(
            f"Expected dict, got {type(data).__name__}",
            field="data",
            value=data,
        )

    try:
        # Sort keys for canonical representation
        sorted_data = {k: data[k] for k in sorted(data.keys())}
        return msgpack.packb(sorted_data, use_bin_type=True)
    except (TypeError, ValueError) as e:
        raise ValidationError(
            f"Cannot serialize dict to MessagePack: {e}",
            field="data",
            value=str(data)[:100],
        ) from e


def unpack_dict(data: bytes) -> dict[str, Any]:
    """
    Deserialize a dictionary from MessagePack.
    
    Args:
        data: MessagePack bytes
        
    Returns:
        Deserialized dictionary
        
    Raises:
        ValidationError: If data cannot be deserialized or is not a dict
    """
    try:
        result = msgpack.unpackb(data, raw=False)
    except (msgpack.UnpackException, ValueError) as e:
        raise ValidationError(
            f"Cannot deserialize MessagePack: {e}",
            field="data",
            value=data[:50] if len(data) > 50 else data,
        ) from e

    if not isinstance(result, dict):
        raise ValidationError(
            f"Expected dict, got {type(result).__name__}",
            field="data",
            value=result,
        )

    return result


def pack_primary_key(pk_values: Any) -> bytes:
    """
    Serialize primary key value(s) to MessagePack.
    
    Handles both single-column and composite primary keys.
    
    Args:
        pk_values: Single value or tuple of values for composite keys
        
    Returns:
        MessagePack bytes
    """
    return pack_value(pk_values)


def unpack_primary_key(data: bytes) -> Any:
    """
    Deserialize primary key value(s) from MessagePack.
    
    Args:
        data: MessagePack bytes
        
    Returns:
        Single value or tuple for composite keys
    """
    return unpack_value(data)
