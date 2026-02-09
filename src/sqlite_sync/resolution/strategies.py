from typing import Protocol, Any, Dict, Callable, Optional
import logging
from dataclasses import dataclass

from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.hlc import HLC
from sqlite_sync.utils.msgpack_codec import unpack_dict

logger = logging.getLogger(__name__)

class ConflictResolver(Protocol):
    """Protocol for conflict resolution strategies."""
    
    def resolve(
        self, 
        local_op: SyncOperation, 
        remote_op: SyncOperation,
        base_state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Resolve conflict between local and remote operations.
        
        Returns:
            Merged dictionary of values to apply.
        """
        ...
    
    @property
    def name(self) -> str:
        """Name of the strategy."""
        ...

    @property
    def auto_resolve(self) -> bool:
        """Whether this strategy automatically resolves conflicts."""
        return True

class NoOpResolver:
    """
    Resolver that does nothing (keeps local state).
    Used for manual conflict resolution scenarios or testing.
    """
    @property
    def name(self) -> str:
        return "NO_OP"

    @property
    def auto_resolve(self) -> bool:
        return False

    def resolve(
        self, 
        local_op: SyncOperation, 
        remote_op: SyncOperation,
        base_state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Returns local new values (effectively ignoring remote update).
        """
        return unpack_dict(local_op.new_values) if local_op.new_values else {}

class LWWResolver:
    """
    Last-Write-Wins (LWW) resolution strategy.
    
    Uses Hybrid Logical Clocks (HLC) to determine the winner.
    Supports field-level merging if enabled.
    """
    
    def __init__(self, field_level: bool = True, auto_resolve: bool = True):
        self.field_level = field_level
        self._auto_resolve = auto_resolve
        
    @property
    def name(self) -> str:
        return "LWW_FIELD" if self.field_level else "LWW_ROW"

    @property
    def auto_resolve(self) -> bool:
        return self._auto_resolve

    def resolve(
        self, 
        local_op: SyncOperation, 
        remote_op: SyncOperation,
        base_state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Resolve conflict using LWW.
        """
        # Unpack values
        l_new = unpack_dict(local_op.new_values) if local_op.new_values else {}
        r_new = unpack_dict(remote_op.new_values) if remote_op.new_values else {}
        
        # Parse HLCs
        l_hlc = HLC.unpack(local_op.hlc) if local_op.hlc else HLC(0, 0, "")
        r_hlc = HLC.unpack(remote_op.hlc) if remote_op.hlc else HLC(0, 0, "")
        
        remote_wins = r_hlc > l_hlc
        
        if not self.field_level:
            # Row-level LWW: winner takes all
            return r_new if remote_wins else l_new
            
        # Field-level LWW
        # Logic:
        # 1. Start with Local values (as base)
        # 2. Identify what changed in Remote vs RemoteOld (if available)
        # 3. Identify what changed in Local vs LocalOld
        # 4. If Remote changed a field that Local didn't -> Remote wins
        # 5. If Both changed a field -> HLC wins
        
        l_old = unpack_dict(local_op.old_values) if local_op.old_values else {}
        r_old = unpack_dict(remote_op.old_values) if remote_op.old_values else {}
        
        l_changes = self._get_changes(l_new, l_old)
        r_changes = self._get_changes(r_new, r_old)
        
        merged = l_new.copy()
        
        all_keys = set(l_new.keys()) | set(r_new.keys())
        
        for k in all_keys:
            changed_by_l = k in l_changes
            changed_by_r = k in r_changes
            
            if k not in merged and k in r_new:
                 merged[k] = r_new[k]

            if changed_by_r:
                if not changed_by_l:
                    # Remote changed it, Local didn't -> Accept Remote
                    if k in r_new:
                        merged[k] = r_new[k]
                else:
                    # Both changed it -> Conflict -> HLC wins
                    if remote_wins:
                        if k in r_new:
                            merged[k] = r_new[k]
                            
        return merged

    def _get_changes(self, new_vals: Dict[str, Any], old_vals: Dict[str, Any]) -> set[str]:
        """Identify keys that have different values in new vs old."""
        changes = set()
        for k, v in new_vals.items():
            if k not in old_vals or old_vals[k] != v:
                changes.add(k)
        return changes

class CustomResolver:
    """
    Wrapper for user-defined conflict resolution callbacks.
    """
    
    def __init__(
        self, 
        callback: Callable[[Dict[str, Any], Dict[str, Any], str], Dict[str, Any]],
        name: str = "CUSTOM",
        auto_resolve: bool = True
    ):
        self._callback = callback
        self._name = name
        self._auto_resolve = auto_resolve
        
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def auto_resolve(self) -> bool:
        return self._auto_resolve

    def resolve(
        self, 
        local_op: SyncOperation, 
        remote_op: SyncOperation,
        base_state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        l_new = unpack_dict(local_op.new_values) if local_op.new_values else {}
        r_new = unpack_dict(remote_op.new_values) if remote_op.new_values else {}
        
        return self._callback(l_new, r_new, local_op.table_name)
