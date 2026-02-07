"""
resolution.py - Conflict resolution strategies.

Provides multiple strategies for resolving sync conflicts:
- Last-Write-Wins (LWW)
- Field-level merge
- Custom resolver callback
"""

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any
from enum import Enum

from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.utils.msgpack_codec import unpack_value


class ResolutionStrategy(Enum):
    """Available conflict resolution strategies."""
    LAST_WRITE_WINS = "lww"
    FIELD_MERGE = "field_merge"
    CUSTOM = "custom"
    MANUAL = "manual"  # Keep conflict for manual resolution


@dataclass
class ConflictContext:
    """Context provided to conflict resolvers."""
    table_name: str
    row_pk: bytes
    local_op: SyncOperation
    remote_op: SyncOperation
    local_values: dict[str, Any]
    remote_values: dict[str, Any]


@dataclass
class ResolutionResult:
    """Result of conflict resolution."""
    resolved: bool
    winning_op: SyncOperation | None
    merged_values: dict[str, Any] | None
    reason: str


class ConflictResolver(ABC):
    """Abstract base class for conflict resolvers."""
    
    @abstractmethod
    def resolve(self, context: ConflictContext) -> ResolutionResult:
        """
        Resolve a conflict between local and remote operations.
        
        Args:
            context: Conflict context with both operations
            
        Returns:
            Resolution result indicating winner or merged values
        """
        pass
    
    @property
    @abstractmethod
    def strategy(self) -> ResolutionStrategy:
        """Return the strategy type."""
        pass


class LastWriteWinsResolver(ConflictResolver):
    """
    Last-Write-Wins (LWW) conflict resolution.
    
    The operation with the latest created_at timestamp wins.
    Simple and predictable, but may lose data.
    """
    
    @property
    def strategy(self) -> ResolutionStrategy:
        return ResolutionStrategy.LAST_WRITE_WINS
    
    def resolve(self, context: ConflictContext) -> ResolutionResult:
        local_time = context.local_op.created_at
        remote_time = context.remote_op.created_at
        
        if local_time >= remote_time:
            return ResolutionResult(
                resolved=True,
                winning_op=context.local_op,
                merged_values=None,
                reason=f"Local wins (created_at: {local_time} >= {remote_time})"
            )
        else:
            return ResolutionResult(
                resolved=True,
                winning_op=context.remote_op,
                merged_values=None,
                reason=f"Remote wins (created_at: {remote_time} > {local_time})"
            )


class FieldMergeResolver(ConflictResolver):
    """
    Field-level merge conflict resolution.
    
    Merges non-conflicting fields, marks conflicting fields for review.
    Better data preservation than LWW.
    """
    
    def __init__(self, prefer_local_on_conflict: bool = True):
        self._prefer_local = prefer_local_on_conflict
    
    @property
    def strategy(self) -> ResolutionStrategy:
        return ResolutionStrategy.FIELD_MERGE
    
    def resolve(self, context: ConflictContext) -> ResolutionResult:
        local_values = context.local_values
        remote_values = context.remote_values
        
        merged = {}
        conflicts = []
        
        all_keys = set(local_values.keys()) | set(remote_values.keys())
        
        for key in all_keys:
            local_val = local_values.get(key)
            remote_val = remote_values.get(key)
            
            if local_val == remote_val:
                merged[key] = local_val
            elif key not in local_values:
                merged[key] = remote_val
            elif key not in remote_values:
                merged[key] = local_val
            else:
                # Actual conflict on this field
                conflicts.append(key)
                merged[key] = local_val if self._prefer_local else remote_val
        
        reason = f"Field merge complete. Conflicts on: {conflicts}" if conflicts else "Field merge - no conflicts"
        
        return ResolutionResult(
            resolved=True,
            winning_op=None,
            merged_values=merged,
            reason=reason
        )


class CustomResolver(ConflictResolver):
    """
    Custom conflict resolution using user-provided callback.
    
    Maximum flexibility for application-specific logic.
    """
    
    def __init__(self, resolver_fn: Callable[[ConflictContext], ResolutionResult]):
        self._resolver_fn = resolver_fn
    
    @property
    def strategy(self) -> ResolutionStrategy:
        return ResolutionStrategy.CUSTOM
    
    def resolve(self, context: ConflictContext) -> ResolutionResult:
        return self._resolver_fn(context)


class ManualResolver(ConflictResolver):
    """
    Manual conflict resolution - marks for later review.
    
    Safest option - no automatic data modification.
    """
    
    @property
    def strategy(self) -> ResolutionStrategy:
        return ResolutionStrategy.MANUAL
    
    def resolve(self, context: ConflictContext) -> ResolutionResult:
        return ResolutionResult(
            resolved=False,
            winning_op=None,
            merged_values=None,
            reason="Marked for manual resolution"
        )


def get_resolver(strategy: ResolutionStrategy, **kwargs) -> ConflictResolver:
    """
    Factory function to get a conflict resolver.
    
    Args:
        strategy: Resolution strategy to use
        **kwargs: Strategy-specific options
        
    Returns:
        Configured conflict resolver
    """
    if strategy == ResolutionStrategy.LAST_WRITE_WINS:
        return LastWriteWinsResolver()
    elif strategy == ResolutionStrategy.FIELD_MERGE:
        return FieldMergeResolver(
            prefer_local_on_conflict=kwargs.get("prefer_local", True)
        )
    elif strategy == ResolutionStrategy.CUSTOM:
        if "resolver_fn" not in kwargs:
            raise ValueError("Custom strategy requires resolver_fn")
        return CustomResolver(kwargs["resolver_fn"])
    elif strategy == ResolutionStrategy.MANUAL:
        return ManualResolver()
    else:
        raise ValueError(f"Unknown strategy: {strategy}")
