"""
lww_merge.py - Advanced Field-Level Last-Write-Wins (LWW) Merging.

This module provides the logic for merging concurrent operations row-by-row,
field-by-field, using HLC timestamps to decide the winner for each column.
"""

from typing import Dict, Any, Optional
from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.hlc import HLC

def _get_changes(new_vals: Dict[str, Any], old_vals: Dict[str, Any]) -> set[str]:
    """Identify keys that have different values in new vs old."""
    changes = set()
    for k, v in new_vals.items():
        if k not in old_vals or old_vals[k] != v:
            changes.add(k)
    return changes

def get_merged_state(
    local_op: SyncOperation,
    remote_op: SyncOperation
) -> Dict[str, Any]:
    """
    Smart Field-Level Merge.
    
    1. Identify changed fields in Local Op (vs Local Old).
    2. Identify changed fields in Remote Op (vs Remote Old).
    3. Construct merged state:
       - Start with Local Current values.
       - For fields changed ONLY by Remote: Apply Remote value.
       - For fields changed by BOTH: Apply winner based on HLC.
    """
    from sqlite_sync.utils.msgpack_codec import unpack_dict
    
    # 1. Unpack values
    l_new = unpack_dict(local_op.new_values) if local_op.new_values else {}
    l_old = unpack_dict(local_op.old_values) if local_op.old_values else {}
    
    r_new = unpack_dict(remote_op.new_values) if remote_op.new_values else {}
    r_old = unpack_dict(remote_op.old_values) if remote_op.old_values else {}
    
    # If INSERT (no old values), treat all present fields as changes
    # But for INSERT vs INSERT, we likely want HLC winner since PKs match
    if local_op.op_type == 'INSERT' and remote_op.op_type == 'INSERT':
        l_hlc = HLC.unpack(local_op.hlc)
        r_hlc = HLC.unpack(remote_op.hlc)
        return r_new if r_hlc > l_hlc else l_new
        
    import sys
    # 2. Identify changes
    l_changes = _get_changes(l_new, l_old)
    r_changes = _get_changes(r_new, r_old)
    
    # sys.stderr.write(f"DEBUG MERGE:\n L_changes: {l_changes}\n R_changes: {r_changes}\n")
    
    # 3. Merge
    merged = l_new.copy()
    
    l_hlc_obj = HLC.unpack(local_op.hlc)
    r_hlc_obj = HLC.unpack(remote_op.hlc)
    remote_wins_conflict = r_hlc_obj > l_hlc_obj
    
    all_keys = set(l_new.keys()) | set(r_new.keys())
    
    for k in all_keys:
        changed_by_l = k in l_changes
        changed_by_r = k in r_changes
        
        if changed_by_r:
            if not changed_by_l:
                # Remote changed it, Local didn't -> Accept Remote
                if k in r_new:
                    merged[k] = r_new[k]
                    # sys.stderr.write(f"  Field {k}: Remote change applied (Val: {r_new[k]})\n")
            else:
                # Both changed it -> Conflict -> HLC wins
                if remote_wins_conflict:
                     if k in r_new:
                        merged[k] = r_new[k]
                        # sys.stderr.write(f"  Field {k}: Conflict, Remote wins\n")
                else:
                    # sys.stderr.write(f"  Field {k}: Conflict, Local wins\n")
                    pass
        # else: Remote didn't change it, keep Local (already in merged)
    
    return merged
