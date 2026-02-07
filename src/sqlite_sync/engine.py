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

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = None
        self._device_id = None
        self._clock = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
        
        def do_import(conn: sqlite3.Connection) -> ImportResult:
            # Disable triggers for the session
            set_sync_disabled(conn, True)
            try:
                applied_count = 0
                conflict_count = 0
                duplicate_count = 0
                
                new_ops = [op for op in bundle_ops if not operation_exists(conn, op.op_id)]
                duplicate_count = len(bundle_ops) - len(new_ops)
                
                if not new_ops:
                    record_import(conn, metadata.bundle_id, metadata.content_hash, metadata.source_device_id, len(bundle_ops), 0, 0, duplicate_count)
                    return ImportResult(metadata.bundle_id, metadata.source_device_id, len(bundle_ops), 0, 0, duplicate_count, False)
                
                for op in sort_operations_deterministically(new_ops):
                    conflicting_op = detect_conflict(conn, op)
                    
                    if conflicting_op is not None:
                        from sqlite_sync.resolution.lww_merge import get_merged_state
                        merged_values = get_merged_state(conflicting_op, op)
                        
                        insert_operation(conn, op)
                        record_conflict(conn, op.table_name, op.row_pk, conflicting_op.op_id, op.op_id)
                        apply_operation_raw(conn, op.table_name, op.row_pk, merged_values)
                        
                        conn.execute("UPDATE sync_conflicts SET resolved_at = ?, resolution_strategy = 'LWW_FIELD' WHERE local_op_id = ? AND remote_op_id = ?",
                                    (int(time.time() * 1_000_000), conflicting_op.op_id, op.op_id))
                        
                        from sqlite_sync.hlc import HLC
                        if self._clock: self._clock.update(HLC.unpack(op.hlc))
                        applied_count += 1
                        conflict_count += 1
                    else:
                        if is_dominated(conn, op):
                            insert_operation(conn, op)
                            applied_count += 1
                        else:
                            _apply_operation(conn, op)
                            insert_operation(conn, op)
                            applied_count += 1
                    
                    cvc = serialize_vector_clock(get_vector_clock(conn))
                    update_vector_clock(conn, parse_vector_clock(merge_vector_clocks(cvc, op.vector_clock)))
                
                now = int(time.time() * 1_000_000)
                conn.execute("INSERT INTO sync_peer_state (peer_device_id, last_sent_vector_clock, last_sent_at, last_received_vector_clock, last_received_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(peer_device_id) DO UPDATE SET last_received_vector_clock = excluded.last_received_vector_clock, last_received_at = excluded.last_received_at",
                            (metadata.source_device_id, EMPTY_VECTOR_CLOCK, 0, serialize_vector_clock(get_vector_clock(conn)), now))
                
                record_import(conn, metadata.bundle_id, metadata.content_hash, metadata.source_device_id, len(bundle_ops), applied_count, conflict_count, duplicate_count)
                return ImportResult(metadata.bundle_id, metadata.source_device_id, len(bundle_ops), applied_count, conflict_count, duplicate_count, False)
            finally:
                # Restore to default state
                set_sync_disabled(conn, False)
        
        return execute_in_transaction(conn, do_import)

    def migrate_schema(self, table_name: str, column_name: str, column_type: str, default_value: Any = None) -> Any:
        from sqlite_sync.schema_evolution import SchemaManager
        return SchemaManager(self.connection).add_column(table_name, column_name, column_type, default_value)

    def compact_log(self, max_ops: int = 10000) -> Any:
        from sqlite_sync.log.compaction import LogCompactor
        return LogCompactor(self.connection).compact_log(max_ops=max_ops)

    def get_unresolved_conflicts(self) -> list[SyncConflict]:
        return _get_unresolved_conflicts(self.connection)

    def get_new_operations(self, since_vector_clock: dict[str, int] | None = None) -> list[SyncOperation]:
        return get_operations_since(self.connection, since_vector_clock)
    
    def get_vector_clock(self) -> dict[str, int]:
        return get_vector_clock(self.connection)
