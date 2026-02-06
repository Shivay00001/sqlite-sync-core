"""
log - Operation log and vector clock modules.
"""

from sqlite_sync.log.vector_clock import (
    increment_vector_clock,
    merge_vector_clocks,
    vector_clock_dominates,
    are_concurrent,
    vector_clock_to_sort_key,
    parse_vector_clock,
    serialize_vector_clock,
    EMPTY_VECTOR_CLOCK,
)
from sqlite_sync.log.operations import (
    SyncOperation,
    operation_from_row,
    operation_to_row,
    get_operation_by_id,
    get_operations_for_row,
    get_last_operation_for_device,
    iter_all_operations,
    operation_exists,
    insert_operation,
)

__all__ = [
    # vector_clock
    "increment_vector_clock",
    "merge_vector_clocks",
    "vector_clock_dominates",
    "are_concurrent",
    "vector_clock_to_sort_key",
    "parse_vector_clock",
    "serialize_vector_clock",
    "EMPTY_VECTOR_CLOCK",
    # operations
    "SyncOperation",
    "operation_from_row",
    "operation_to_row",
    "get_operation_by_id",
    "get_operations_for_row",
    "get_last_operation_for_device",
    "iter_all_operations",
    "operation_exists",
    "insert_operation",
]
