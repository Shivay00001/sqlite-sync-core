"""
ordering.py - Deterministic operation ordering.

Ensures same operations always apply in the same order
regardless of arrival order or device.
"""

from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.log.vector_clock import (
    parse_vector_clock,
    vector_clock_to_sort_key,
)
from sqlite_sync.invariants import Invariants


def sort_operations_deterministically(
    operations: list[SyncOperation],
) -> list[SyncOperation]:
    """
    Sort operations in deterministic order.
    
    Order is determined by:
    1. Vector clock (causal ordering)
    2. Operation ID (tie-breaker for concurrent ops)
    
    This ensures that given the same set of operations,
    they will always be applied in the same order.
    
    Args:
        operations: List of operations to sort
        
    Returns:
        Sorted list of operations
    """
    # Validate no duplicates (would break determinism)
    op_id_vc_pairs = [(op.op_id, op.vector_clock) for op in operations]
    Invariants.assert_deterministic_ordering(op_id_vc_pairs)
    
    def sort_key(op: SyncOperation) -> tuple:
        vc = parse_vector_clock(op.vector_clock)
        vc_key = vector_clock_to_sort_key(vc)
        # Use op_id as tie-breaker (bytes are comparable)
        return (vc_key, op.op_id)
    
    return sorted(operations, key=sort_key)


def compare_operations(op1: SyncOperation, op2: SyncOperation) -> int:
    """
    Compare two operations for ordering.
    
    Args:
        op1: First operation
        op2: Second operation
        
    Returns:
        -1 if op1 < op2, 0 if equal, 1 if op1 > op2
    """
    vc1 = parse_vector_clock(op1.vector_clock)
    vc2 = parse_vector_clock(op2.vector_clock)
    
    key1 = (vector_clock_to_sort_key(vc1), op1.op_id)
    key2 = (vector_clock_to_sort_key(vc2), op2.op_id)
    
    if key1 < key2:
        return -1
    elif key1 > key2:
        return 1
    else:
        return 0
