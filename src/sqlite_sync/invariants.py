"""
invariants.py - Core invariant definitions and enforcement.

This module defines the fundamental invariants that MUST hold at all times.
Invariant checks are called at critical points to detect violations early.

If an invariant is violated, the system MUST halt with InvariantViolationError.
"""

from typing import TYPE_CHECKING
import sqlite3

from sqlite_sync.errors import InvariantViolationError

if TYPE_CHECKING:
    pass


class Invariants:
    """
    Core invariants that must hold for system correctness.
    
    These are the laws of the system, not guidelines.
    Violation of any invariant indicates a critical bug.
    """

    # Invariant names as constants for consistent error messages
    APPEND_ONLY = "APPEND_ONLY"
    CAUSAL_CONSISTENCY = "CAUSAL_CONSISTENCY"
    DETERMINISTIC_ORDERING = "DETERMINISTIC_ORDERING"
    EXPLICIT_CONFLICTS = "EXPLICIT_CONFLICTS"
    IDEMPOTENT_IMPORT = "IDEMPOTENT_IMPORT"
    TRANSPORT_INDEPENDENCE = "TRANSPORT_INDEPENDENCE"
    ATOMIC_TRANSACTIONS = "ATOMIC_TRANSACTIONS"

    @staticmethod
    def assert_append_only(
        conn: sqlite3.Connection, op_id: bytes, operation: str
    ) -> None:
        """
        Invariant 1: sync_operations is append-only.
        
        Once an operation is inserted, it MUST NOT be modified or deleted.
        
        Args:
            conn: Database connection
            op_id: Operation ID being checked
            operation: The operation being attempted (UPDATE/DELETE)
            
        Raises:
            InvariantViolationError: If attempting to modify existing operations
        """
        if operation in ("UPDATE", "DELETE"):
            cursor = conn.execute(
                "SELECT 1 FROM sync_operations WHERE op_id = ?", (op_id,)
            )
            if cursor.fetchone() is not None:
                raise InvariantViolationError(
                    Invariants.APPEND_ONLY,
                    f"Attempted to {operation} existing operation {op_id.hex()}. "
                    "sync_operations is append-only.",
                )

    @staticmethod
    def assert_valid_vector_clock(vc: dict[str, int]) -> None:
        """
        Invariant 2: Vector clocks must be valid.
        
        All counters must be non-negative integers.
        All device IDs must be valid hex strings (from UUIDs).
        
        Args:
            vc: Vector clock dictionary
            
        Raises:
            InvariantViolationError: If vector clock is malformed
        """
        if not isinstance(vc, dict):
            raise InvariantViolationError(
                Invariants.CAUSAL_CONSISTENCY,
                f"Vector clock must be a dict, got {type(vc).__name__}",
            )

        for device_id, counter in vc.items():
            if not isinstance(device_id, str):
                raise InvariantViolationError(
                    Invariants.CAUSAL_CONSISTENCY,
                    f"Device ID must be string, got {type(device_id).__name__}",
                )
            if len(device_id) != 32:  # 16 bytes as hex
                raise InvariantViolationError(
                    Invariants.CAUSAL_CONSISTENCY,
                    f"Device ID must be 32 hex chars, got {len(device_id)}",
                )
            if not isinstance(counter, int) or counter < 0:
                raise InvariantViolationError(
                    Invariants.CAUSAL_CONSISTENCY,
                    f"Counter must be non-negative int, got {counter!r}",
                )

    @staticmethod
    def assert_deterministic_ordering(ops: list[tuple[bytes, str]]) -> None:
        """
        Invariant 3: Operations must have a total deterministic order.
        
        Given the same set of operations, they must always sort identically.
        This is achieved by sorting by (vector_clock_sort_key, op_id).
        
        Args:
            ops: List of (op_id, vector_clock_json) tuples
            
        Raises:
            InvariantViolationError: If ordering would be non-deterministic
        """
        # Check for duplicate op_ids (would break deterministic ordering)
        op_ids = [op[0] for op in ops]
        if len(op_ids) != len(set(op_ids)):
            duplicates = [oid for oid in op_ids if op_ids.count(oid) > 1]
            raise InvariantViolationError(
                Invariants.DETERMINISTIC_ORDERING,
                f"Duplicate operation IDs found: {[d.hex() for d in set(duplicates)]}",
            )

    @staticmethod
    def assert_conflict_recorded(
        conn: sqlite3.Connection, local_op_id: bytes, remote_op_id: bytes
    ) -> None:
        """
        Invariant 4: Conflicts must be explicitly recorded.
        
        When a conflict is detected, there MUST be a corresponding
        record in sync_conflicts.
        
        Args:
            conn: Database connection
            local_op_id: Local operation ID
            remote_op_id: Remote operation ID
            
        Raises:
            InvariantViolationError: If conflict not recorded
        """
        cursor = conn.execute(
            """
            SELECT 1 FROM sync_conflicts 
            WHERE local_op_id = ? AND remote_op_id = ?
            """,
            (local_op_id, remote_op_id),
        )
        if cursor.fetchone() is None:
            raise InvariantViolationError(
                Invariants.EXPLICIT_CONFLICTS,
                f"Conflict between {local_op_id.hex()} and {remote_op_id.hex()} "
                "was detected but not recorded.",
            )

    @staticmethod
    def assert_idempotent_import(
        conn: sqlite3.Connection, bundle_hash: bytes
    ) -> bool:
        """
        Invariant 5: Importing the same bundle N times = same result.
        
        If we've already imported a bundle, we MUST skip it entirely.
        Returns True if bundle was already imported (should skip).
        
        Args:
            conn: Database connection
            bundle_hash: SHA-256 hash of bundle content
            
        Returns:
            True if bundle was already imported, False otherwise
        """
        cursor = conn.execute(
            "SELECT 1 FROM sync_import_log WHERE bundle_hash = ?", (bundle_hash,)
        )
        return cursor.fetchone() is not None

    @staticmethod
    def assert_in_transaction(conn: sqlite3.Connection) -> None:
        """
        Invariant 7: All mutations must occur within a transaction.
        
        Args:
            conn: Database connection
            
        Raises:
            InvariantViolationError: If not in a transaction
        """
        if conn.in_transaction is False:
            raise InvariantViolationError(
                Invariants.ATOMIC_TRANSACTIONS,
                "Database mutation attempted outside of transaction.",
            )
