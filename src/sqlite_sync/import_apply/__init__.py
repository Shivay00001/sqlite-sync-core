"""
import_apply - Import and apply pipeline.
"""

from sqlite_sync.import_apply.dedup import (
    is_bundle_already_imported,
    filter_duplicate_operations,
    operation_is_duplicate,
)
from sqlite_sync.import_apply.ordering import (
    sort_operations_deterministically,
    compare_operations,
)
from sqlite_sync.import_apply.conflict import (
    SyncConflict,
    conflict_from_row,
    detect_conflict,
    record_conflict,
    get_unresolved_conflicts,
    get_conflict_by_id,
    mark_conflict_resolved,
)
from sqlite_sync.import_apply.apply import apply_operation

__all__ = [
    # dedup
    "is_bundle_already_imported",
    "filter_duplicate_operations",
    "operation_is_duplicate",
    # ordering
    "sort_operations_deterministically",
    "compare_operations",
    # conflict
    "SyncConflict",
    "conflict_from_row",
    "detect_conflict",
    "record_conflict",
    "get_unresolved_conflicts",
    "get_conflict_by_id",
    "mark_conflict_resolved",
    # apply
    "apply_operation",
]
