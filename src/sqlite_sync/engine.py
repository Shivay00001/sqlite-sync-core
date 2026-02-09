"""
engine.py - Main sync engine.

The SyncEngine is the primary public interface for the sync system.
It coordinates all sync operations:
- Database initialization
- Table sync registration
- Bundle generation and import
- Conflict querying
"""

import sqlite3
import time
import os
from dataclasses import dataclass
from typing import Final, Any

from sqlite_sync.db.connection import create_connection, execute_in_transaction, set_sync_disabled
from sqlite_sync.db.migrations import (
    initialize_sync_tables,
    get_device_id,
    get_schema_version,
    get_vector_clock,
    update_vector_clock,
)
from sqlite_sync.import_apply.conflict import (
    SyncConflict,
    detect_conflict,
    record_conflict,
    is_dominated,
    get_unresolved_conflicts as _get_unresolved_conflicts,
)
from sqlite_sync.import_apply.apply import apply_operation as _apply_operation, apply_operation_raw
from sqlite_sync.db.triggers import install_triggers_for_table, has_triggers
from sqlite_sync.bundle.validate import validate_bundle
from sqlite_sync.bundle.generate import generate_bundle as _generate_bundle
from sqlite_sync.log.vector_clock import (
    parse_vector_clock,
    serialize_vector_clock,
    merge_vector_clocks,
    EMPTY_VECTOR_CLOCK,
)
from sqlite_sync.import_apply.dedup import is_bundle_already_imported
from sqlite_sync.audit.import_log import record_import
from sqlite_sync.import_apply.ordering import sort_operations_deterministically
from sqlite_sync.log.operations import (
    SyncOperation,
    operation_from_row,
    insert_operation,
    operation_exists,
    get_operations_since,
)

@dataclass(frozen=True)
class ImportResult:
    bundle_id: bytes
    source_device_id: bytes
    op_count: int
    applied_count: int
    conflict_count: int
    duplicate_count: int
    is_duplicate_bundle: bool

class SyncEngine:
    """
    Core engine for SQLite synchronization.
    """

    def __init__(self, db_path: str, conflict_resolver: Any = None):
        self._db_path = db_path
        self._conn = None
        self._device_id = None
        self._clock = None
        
        # Default to LWW if not provided
        if conflict_resolver is None:
            from sqlite_sync.resolution.strategies import LWWResolver
            self._resolver = LWWResolver()
        else:
            self._resolver = conflict_resolver

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = create_connection(self._db_path)
        return self._conn

    @property
    def device_id(self) -> bytes:
        if self._device_id is None:
            self._device_id = get_device_id(self.connection)
        return self._device_id

    def initialize(self) -> bytes:
        """Initialize sync tables and metadata."""
        self._device_id = initialize_sync_tables(self.connection)
        self._init_clock()
        return self._device_id

    def _init_clock(self):
        from sqlite_sync.hlc import HLClock
        import sys
        self._clock = HLClock(self.device_id.hex())
        
        def hlc_now(_node_id: Any) -> str:
            if self._clock is None: return "0:0:0"
            return self._clock.now().pack()
            
        self.connection.create_function("sync_hlc_now", 1, hlc_now)

    def enable_sync_for_table(self, table_name: str) -> None:
        """Enable synchronization for a specific table."""
        install_triggers_for_table(self.connection, table_name)

    def is_sync_enabled(self, table_name: str) -> bool:
        """Check if sync is enabled for a table."""
        return has_triggers(self.connection, table_name)

    def generate_bundle(self, peer_device_id: bytes, output_path: str) -> str | None:
        """Generate a bundle for a peer."""
        return _generate_bundle(self.connection, peer_device_id, output_path)

    def apply_batch(
        self, 
        operations: list[SyncOperation], 
        source_device_id: bytes,
        bundle_id: bytes | None = None,
        content_hash: bytes | None = None
    ) -> ImportResult:
        """
        Apply a batch of operations with full conflict detection and resolution.
        Used by both import_bundle and stream-based sync.
        """
        conn = self.connection
        import uuid
        
        # Default metadata for stream sync
        if bundle_id is None:
            bundle_id = uuid.uuid4().bytes
        if content_hash is None:
            content_hash = b"\x00" * 32
            
        def do_apply(conn: sqlite3.Connection) -> ImportResult:
            # Disable triggers for the session
            set_sync_disabled(conn, True)
            try:
                applied_count = 0
                conflict_count = 0
                duplicate_count = 0
                
                new_ops = [op for op in operations if not operation_exists(conn, op.op_id)]
                duplicate_count = len(operations) - len(new_ops)
                # print(f"DEBUG: apply_batch: {len(operations)} received, {len(new_ops)} new, {duplicate_count} dups")
                
                if not new_ops:
                    record_import(conn, bundle_id, content_hash, source_device_id, len(operations), 0, 0, duplicate_count)
                    return ImportResult(bundle_id, source_device_id, len(operations), 0, 0, duplicate_count, False)
                
                for op in sort_operations_deterministically(new_ops):
                    conflicting_op = detect_conflict(conn, op)
                    
                    if conflicting_op is not None:
                        # Use configured resolver
                        merged_values = self._resolver.resolve(conflicting_op, op)
                        
                        insert_operation(conn, op)
                        record_conflict(conn, op.table_name, op.row_pk, conflicting_op.op_id, op.op_id)
                        
                        if getattr(self._resolver, "auto_resolve", True):
                            apply_operation_raw(conn, op.table_name, op.row_pk, merged_values)
                            
                            conn.execute("UPDATE sync_conflicts SET resolved_at = ?, resolution_strategy = ? WHERE local_op_id = ? AND remote_op_id = ?",
                                        (int(time.time() * 1_000_000), self._resolver.name, conflicting_op.op_id, op.op_id))
                            
                            from sqlite_sync.hlc import HLC
                            if self._clock: self._clock.update(HLC.unpack(op.hlc))
                            applied_count += 1
                        
                        conflict_count += 1
                    else:
                        if is_dominated(conn, op):
                            insert_operation(conn, op)
                            # applied_count += 1 # Dominated ops are "applied" to log but not DB
                        else:
                            _apply_operation(conn, op)
                            insert_operation(conn, op)
                            applied_count += 1
                    
                    # Update local HLC if op is newer
                    if op.hlc:
                        from sqlite_sync.hlc import HLC
                        try:
                            if self._clock: self._clock.update(HLC.unpack(op.hlc))
                        except: pass

                    cvc = serialize_vector_clock(get_vector_clock(conn))
                    update_vector_clock(conn, parse_vector_clock(merge_vector_clocks(cvc, op.vector_clock)))
                
                now = int(time.time() * 1_000_000)
                conn.execute("INSERT INTO sync_peer_state (peer_device_id, last_sent_vector_clock, last_sent_at, last_received_vector_clock, last_received_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(peer_device_id) DO UPDATE SET last_received_vector_clock = excluded.last_received_vector_clock, last_received_at = excluded.last_received_at",
                            (source_device_id, EMPTY_VECTOR_CLOCK, 0, serialize_vector_clock(get_vector_clock(conn)), now))
                
                record_import(conn, bundle_id, content_hash, source_device_id, len(operations), applied_count, conflict_count, duplicate_count)
                return ImportResult(bundle_id, source_device_id, len(operations), applied_count, conflict_count, duplicate_count, False)
            finally:
                # Restore to default state
                set_sync_disabled(conn, False)
        
        return execute_in_transaction(conn, do_apply)

    def import_bundle(self, bundle_path: str) -> ImportResult:
        """Import a bundle from a peer."""
        conn = self.connection
        local_schema_version = get_schema_version(conn)
        metadata = validate_bundle(bundle_path, local_schema_version)
        
        if is_bundle_already_imported(conn, metadata.content_hash):
            return ImportResult(metadata.bundle_id, metadata.source_device_id, metadata.op_count, 0, 0, 0, True)
        
        bundle_conn = sqlite3.connect(bundle_path)
        bundle_cursor = bundle_conn.execute("SELECT * FROM bundle_operations ORDER BY created_at ASC")
        bundle_ops = [operation_from_row(row) for row in bundle_cursor.fetchall()]
        bundle_conn.close()
        
        if not bundle_ops:
            record_import(conn, metadata.bundle_id, metadata.content_hash, metadata.source_device_id, 0, 0, 0, 0)
            return ImportResult(metadata.bundle_id, metadata.source_device_id, 0, 0, 0, 0, False)
        
        return self.apply_batch(bundle_ops, metadata.source_device_id, metadata.bundle_id, metadata.content_hash)

    def migrate_schema(self, table_name: str, column_name: str, column_type: str, default_value: Any = None) -> Any:
        from sqlite_sync.schema_evolution import SchemaManager
        return SchemaManager(self.connection).add_column(table_name, column_name, column_type, default_value)

    def compact_log(self, max_ops: int = 10000) -> Any:
        from sqlite_sync.log.compaction import LogCompactor
        return LogCompactor(self.connection).compact_log(max_ops=max_ops)

    def create_snapshot(self) -> Any:
        from sqlite_sync.log.compaction import LogCompactor
        return LogCompactor(self.connection).create_snapshot()

    def get_schema_version(self) -> int:
        from sqlite_sync.db.migrations import get_schema_version
        return get_schema_version(self.connection)

    def check_compatibility(self, remote_version: int) -> bool:
        from sqlite_sync.schema_evolution import SchemaManager
        return SchemaManager(self.connection).check_compatibility(remote_version)

    def get_unresolved_conflicts(self) -> list[SyncConflict]:
        return _get_unresolved_conflicts(self.connection)

    def get_new_operations(self, since_vector_clock: dict[str, int] | None = None) -> list[SyncOperation]:
        return get_operations_since(self.connection, since_vector_clock)
    
    def get_vector_clock(self) -> dict[str, int]:
        return get_vector_clock(self.connection)

    def resolve_conflict(self, conflict_id: str, resolution: str) -> None:
        """
        Manually resolve a conflict.
        
        Args:
            conflict_id: Hex-encoded conflict ID
            resolution: 'local' or 'remote'
        """
        if resolution not in ("local", "remote"):
            raise ValueError("Resolution must be 'local' or 'remote'")
            
        cid_bytes = bytes.fromhex(conflict_id)
        from sqlite_sync.import_apply.conflict import get_conflict_by_id, mark_conflict_resolved
        from sqlite_sync.log.operations import get_operation_by_id
        from sqlite_sync.import_apply.apply import apply_operation_raw
        
        conn = self.connection
        with execute_in_transaction(conn, lambda c: None): # Check transaction
            conflict = get_conflict_by_id(conn, cid_bytes)
            if not conflict:
                raise ValueError(f"Conflict {conflict_id} not found")
                
            if resolution == "local":
                # Local wins, DB is already in local state (remote was not applied)
                # Just mark resolved
                mark_conflict_resolved(conn, cid_bytes, conflict.local_op_id)
                
            else: # remote
                # Remote wins, apply remote op
                remote_op = get_operation_by_id(conn, conflict.remote_op_id)
                if not remote_op:
                    raise ValueError("Remote operation not found")
                
                from sqlite_sync.utils.msgpack_codec import unpack_dict
                values = unpack_dict(remote_op.new_values) if remote_op.new_values else {}
                apply_operation_raw(conn, remote_op.table_name, remote_op.row_pk, values)
                mark_conflict_resolved(conn, cid_bytes, conflict.remote_op_id)
                
                # Update clock if possible
                if self._clock and remote_op.hlc:
                    from sqlite_sync.hlc import HLC
                    try:
                        self._clock.update(HLC.unpack(remote_op.hlc))
                    except: pass
