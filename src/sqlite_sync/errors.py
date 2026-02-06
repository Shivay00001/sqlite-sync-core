"""
errors.py - Domain-specific exceptions for sqlite_sync.

All exceptions inherit from SyncError for unified handling.
Each exception type represents a distinct failure mode.
"""

from typing import Any


class SyncError(Exception):
    """Base exception for all sqlite_sync errors."""

    def __init__(self, message: str, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def __str__(self) -> str:
        if self.context:
            context_str = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
            return f"{self.message} [{context_str}]"
        return self.message


class InvariantViolationError(SyncError):
    """
    Raised when a core system invariant is violated.
    
    This is a critical error indicating a bug in the system or
    an attempt to corrupt data. The system MUST NOT continue.
    """

    def __init__(self, invariant: str, details: str) -> None:
        super().__init__(
            f"Invariant violation: {invariant}. {details}",
            context={"invariant": invariant, "details": details},
        )
        self.invariant = invariant
        self.details = details


class SchemaError(SyncError):
    """
    Raised when schema validation fails.
    
    This includes schema version mismatches between databases,
    missing sync tables, or incompatible table structures.
    """

    def __init__(self, message: str, expected: Any = None, actual: Any = None) -> None:
        context = {}
        if expected is not None:
            context["expected"] = expected
        if actual is not None:
            context["actual"] = actual
        super().__init__(message, context=context)
        self.expected = expected
        self.actual = actual


class BundleError(SyncError):
    """
    Raised when bundle validation or processing fails.
    
    This includes integrity check failures, hash mismatches,
    malformed bundles, or missing required data.
    """

    def __init__(
        self, message: str, bundle_path: str | None = None, reason: str | None = None
    ) -> None:
        context = {}
        if bundle_path is not None:
            context["bundle_path"] = bundle_path
        if reason is not None:
            context["reason"] = reason
        super().__init__(message, context=context)
        self.bundle_path = bundle_path
        self.reason = reason


class ConflictError(SyncError):
    """
    Raised when a conflict resolution operation fails.
    
    Note: The existence of conflicts is NOT an error - conflicts are
    expected in distributed systems. This exception is raised when
    an operation on a conflict (e.g., resolution) fails.
    """

    def __init__(
        self,
        message: str,
        conflict_id: bytes | None = None,
        table_name: str | None = None,
    ) -> None:
        context = {}
        if conflict_id is not None:
            context["conflict_id"] = conflict_id.hex()
        if table_name is not None:
            context["table_name"] = table_name
        super().__init__(message, context=context)
        self.conflict_id = conflict_id
        self.table_name = table_name


class DatabaseError(SyncError):
    """
    Raised when a database operation fails unexpectedly.
    
    This wraps SQLite errors with additional context about
    what operation was being attempted.
    """

    def __init__(
        self, message: str, operation: str | None = None, sql: str | None = None
    ) -> None:
        context = {}
        if operation is not None:
            context["operation"] = operation
        if sql is not None:
            # Truncate long SQL for readability
            context["sql"] = sql[:200] + "..." if len(sql) > 200 else sql
        super().__init__(message, context=context)
        self.operation = operation
        self.sql = sql


class ValidationError(SyncError):
    """
    Raised when input validation fails.
    
    This includes invalid UUIDs, malformed data, or
    values that don't meet expected constraints.
    """

    def __init__(
        self, message: str, field: str | None = None, value: Any = None
    ) -> None:
        context = {}
        if field is not None:
            context["field"] = field
        if value is not None:
            context["value"] = repr(value)[:100]
        super().__init__(message, context=context)
        self.field = field
        self.value = value


class OperationError(SyncError):
    """
    Raised when an operation cannot be applied.
    
    This includes attempting to apply an operation to a
    non-existent table, type mismatches, or constraint violations.
    """

    def __init__(
        self,
        message: str,
        op_id: bytes | None = None,
        op_type: str | None = None,
        table_name: str | None = None,
    ) -> None:
        context = {}
        if op_id is not None:
            context["op_id"] = op_id.hex()
        if op_type is not None:
            context["op_type"] = op_type
        if table_name is not None:
            context["table_name"] = table_name
        super().__init__(message, context=context)
        self.op_id = op_id
        self.op_type = op_type
        self.table_name = table_name
