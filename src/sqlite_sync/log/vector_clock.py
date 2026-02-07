"""
vector_clock.py - Vector clock operations for causal ordering.

Vector clocks track the happens-before relationship across devices.
They are essential for detecting concurrent operations (conflicts).

Format: JSON object mapping device_id (hex string) -> counter (int)
Example: {"a1b2c3...": 5, "d4e5f6...": 3}

All operations return canonical JSON (sorted keys) for determinism.
"""

import json
from typing import Final

from sqlite_sync.errors import ValidationError
from sqlite_sync.invariants import Invariants


# Empty vector clock (canonical representation)
EMPTY_VECTOR_CLOCK: Final[str] = "{}"


def increment_vector_clock(device_id: bytes, vc_json: str) -> str:
    """
    Increment the counter for a device in the vector clock.
    
    Used when this device creates a new operation.
    
    Args:
        device_id: 16-byte device UUID
        vc_json: Current vector clock as JSON string
        
    Returns:
        New vector clock as canonical JSON string
        
    Raises:
        ValidationError: If inputs are invalid
    """
    if not isinstance(device_id, bytes) or len(device_id) != 16:
        raise ValidationError(
            f"device_id must be 16 bytes, got {type(device_id).__name__}",
            field="device_id",
        )
    
    try:
        vc = json.loads(vc_json)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"Invalid vector clock JSON: {e}",
            field="vc_json",
            value=vc_json[:100] if len(vc_json) > 100 else vc_json,
        ) from e
    
    device_hex = device_id.hex()
    vc[device_hex] = vc.get(device_hex, 0) + 1
    
    # Return canonical JSON (sorted keys)
    return json.dumps(vc, sort_keys=True)


def merge_vector_clocks(vc1_json: str, vc2_json: str) -> str:
    """
    Merge two vector clocks (take max for each device).
    
    Used when importing operations to update local vector clock.
    
    Args:
        vc1_json: First vector clock as JSON
        vc2_json: Second vector clock as JSON
        
    Returns:
        Merged vector clock as canonical JSON
    """
    try:
        vc1 = json.loads(vc1_json)
        vc2 = json.loads(vc2_json)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"Invalid vector clock JSON: {e}",
            field="vector_clock",
        ) from e
    
    merged: dict[str, int] = {}
    all_devices = set(vc1.keys()) | set(vc2.keys())
    
    for device in all_devices:
        merged[device] = max(vc1.get(device, 0), vc2.get(device, 0))
    
    return json.dumps(merged, sort_keys=True)


def vector_clock_dominates(vc1: dict[str, int], vc2: dict[str, int]) -> bool:
    """
    Check if vc1 >= vc2 (vc1 has seen everything vc2 has seen).
    
    vc1 dominates vc2 if for every device d:
        vc1[d] >= vc2[d]
    
    Args:
        vc1: First vector clock as dict
        vc2: Second vector clock as dict
        
    Returns:
        True if vc1 dominates vc2
    """
    Invariants.assert_valid_vector_clock(vc1)
    Invariants.assert_valid_vector_clock(vc2)
    
    for device, counter in vc2.items():
        if vc1.get(device, 0) < counter:
            return False
    return True


def are_concurrent(vc1: dict[str, int], vc2: dict[str, int]) -> bool:
    """
    Check if two vector clocks are concurrent (neither dominates).
    
    Two operations are concurrent if:
    - vc1 does NOT dominate vc2, AND
    - vc2 does NOT dominate vc1
    
    Concurrent operations on the same row indicate a conflict.
    
    Args:
        vc1: First vector clock as dict
        vc2: Second vector clock as dict
        
    Returns:
        True if vector clocks are concurrent
    """
    return (
        not vector_clock_dominates(vc1, vc2) and
        not vector_clock_dominates(vc2, vc1)
    )


def vector_clock_to_sort_key(vc: dict[str, int]) -> tuple[int, ...]:
    """
    Convert vector clock to a deterministic sort key.
    
    Enables total ordering of operations for deterministic replay.
    Operations are sorted by their vector clocks first.
    
    Args:
        vc: Vector clock as dict
        
    Returns:
        Tuple of counters in deterministic order (by device ID)
    """
    Invariants.assert_valid_vector_clock(vc)
    
    # Sort by device ID (alphabetically) for determinism
    sorted_devices = sorted(vc.keys())
    return tuple(vc.get(d, 0) for d in sorted_devices)


def parse_vector_clock(vc_json: str) -> dict[str, int]:
    """
    Parse vector clock JSON to dict.
    
    Args:
        vc_json: Vector clock as JSON string
        
    Returns:
        Vector clock as dict
        
    Raises:
        ValidationError: If JSON is invalid
    """
    try:
        vc = json.loads(vc_json)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"Invalid vector clock JSON: {e}",
            field="vector_clock",
        ) from e
    
    if not isinstance(vc, dict):
        raise ValidationError(
            f"Vector clock must be object, got {type(vc).__name__}",
            field="vector_clock",
        )
    
    Invariants.assert_valid_vector_clock(vc)
    return vc


def serialize_vector_clock(vc: dict[str, int]) -> str:
    """
    Serialize vector clock dict to canonical JSON.
    
    Args:
        vc: Vector clock as dict
        
    Returns:
        Canonical JSON string
    """
    Invariants.assert_valid_vector_clock(vc)
    return json.dumps(vc, sort_keys=True)


def is_dominated(vc1_json: str, vc2_json: str) -> bool:
    """
    Check if vc1 dominates vc2 using JSON strings.
    
    Args:
        vc1_json: First vector clock JSON
        vc2_json: Second vector clock JSON
        
    Returns:
        True if vc1 dominates vc2
    """
    vc1 = parse_vector_clock(vc1_json)
    vc2 = parse_vector_clock(vc2_json)
    return vector_clock_dominates(vc1, vc2)

