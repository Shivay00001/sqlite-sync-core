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
from dataclasses import dataclass
from typing import Final

from sqlite_sync.db.connection import create_connection, execute_in_transaction
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
from sqlite_sync.import_apply.apply import apply_operation as _apply_operation
from sqlite_sync.db.triggers import install_triggers_for_table, has_triggers
from sqlite_sync.bundle.validate import validate_bundle
from sqlite_sync.bundle.generate import generate_bundle as _generate_bundle
from sqlite_sync.bundle.format import BundleMetadata
from sqlite_sync.log.vector_clock import (
    parse_vector_clock,
    serialize_vector_clock,
    merge_vector_clocks,
    EMPTY_VECTOR_CLOCK,
)
from sqlite_sync.import_apply.dedup import is_bundle_already_imported
from sqlite_sync.import_apply.ordering import sort_operations_deterministically
from sqlite_sync.log.operations import (
    SyncOperation,
    operation_from_row,
    insert_operation,
    operation_exists,
    get_operations_since,
)
from sqlite_sync.audit.import_log import record_import
from sqlite_sync.errors import SyncError, SchemaError, BundleError, DatabaseError
from sqlite_sync.config import SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class ImportResult:
    """
    Result of a bundle import operation.
    """
    bundle_id: bytes
    source_device_id: bytes
    total_operations: int
    applied_count: int
    conflict_count: int
    duplicate_count: int
    skipped: bool  # True if bundle was already imported


class SyncEngine:
    """
    Main sync engine - the public interface for the sync system.
    
    Thread Safety: NOT thread-safe. Each thread should have its own engine.
    
    Usage:
        engine = SyncEngine("my_database.db")
        engine.initialize()
        engine.enable_sync_for_table("todos")
        
        # Generate bundle for a peer
        bundle_path = engine.generate_bundle(peer_device_id, "output.bundle.db")
        
        # Import bundle from a peer
        result = engine.import_bundle("received.bundle.db")
    """
    
    def __init__(self, db_path: str) -> None:
        """
        Create a new sync engine for a database.
        
        Args:
            db_path: Path to SQLite database file
        """
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._device_id: bytes | None = None
    
    def initialize(self) -> bytes:
        """
        Initialize the database for sync.
        
        Creates sync tables and metadata if not present.
        Safe to call multiple times (idempotent).
        
        Returns:
            Device ID (16-byte UUID)
        """
        self._conn = create_connection(self._db_path)
        self._device_id = initialize_sync_tables(self._conn)
        return self._device_id
    
    @property
    def device_id(self) -> bytes:
        """Get the device ID for this database."""
        if self._device_id is None:
            raise SchemaError("Engine not initialized. Call initialize() first.")
        return self._device_id
    
    @property
    def connection(self) -> sqlite3.Connection:
        """Get the database connection."""
        if self._conn is None:
            raise SchemaError("Engine not initialized. Call initialize() first.")
        return self._conn
    
    def enable_sync_for_table(self, table_name: str) -> None:
        """
        Enable sync for a user table.
        
        Installs triggers to capture INSERT, UPDATE, DELETE operations.
        
        Args:
            table_name: Name of table to enable sync on
        """
        install_triggers_for_table(self.connection, table_name)
    
    def is_sync_enabled(self, table_name: str) -> bool:
        """
        Check if sync is enabled for a table.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if sync triggers are installed
        """
        return has_triggers(self.connection, table_name)
    
    def generate_bundle(
        self,
        peer_device_id: bytes,
        output_path: str,
    ) -> str | None:
        """
        Generate a sync bundle for a peer.
        
        Args:
            peer_device_id: 16-byte UUID of peer device
            output_path: Where to write bundle file
            
        Returns:
            output_path if bundle created, None if nothing to send
        """
        return _generate_bundle(self.connection, peer_device_id, output_path)
    
    def import_bundle(self, bundle_path: str) -> ImportResult:
        """
        Import a sync bundle from a peer.
        
        This is the main sync import method. It:
        1. Validates the bundle
        2. Checks for duplicate import (idempotency)
        3. Deduplicates operations
        4. Detects conflicts
        5. Applies non-conflicting operations
        6. Records conflicts for later resolution
        7. Updates local vector clock
        8. Records import in audit log
        
        All changes are atomic (committed in single transaction).
        
        Args:
            bundle_path: Path to bundle file
            
        Returns:
            ImportResult with statistics
            
        Raises:
            BundleError: If bundle validation fails
            SyncError: If import fails
        """
        conn = self.connection
        local_schema_version = get_schema_version(conn)
        
        # Step 1: Validate bundle
        metadata = validate_bundle(bundle_path, local_schema_version)
        
        # Step 2: Check if already imported (idempotency)
        if is_bundle_already_imported(conn, metadata.content_hash):
            return ImportResult(
                bundle_id=metadata.bundle_id,
                source_device_id=metadata.source_device_id,
                total_operations=metadata.op_count,
                applied_count=0,
                conflict_count=0,
                duplicate_count=0,
                skipped=True,
            )
        
        # Load operations from bundle
        bundle_conn = sqlite3.connect(bundle_path)
        bundle_cursor = bundle_conn.execute(
            "SELECT * FROM bundle_operations ORDER BY created_at ASC"
        )
        bundle_ops = [operation_from_row(row) for row in bundle_cursor.fetchall()]
        bundle_conn.close()
        
        if not bundle_ops:
            # Empty bundle - just record it
            record_import(
                conn,
                metadata.bundle_id,
                metadata.content_hash,
                metadata.source_device_id,
                0, 0, 0, 0,
            )
            return ImportResult(
                bundle_id=metadata.bundle_id,
                source_device_id=metadata.source_device_id,
                total_operations=0,
                applied_count=0,
                conflict_count=0,
                duplicate_count=0,
                skipped=False,
            )
        
        # Step 3-7: Import in transaction
        def do_import(conn: sqlite3.Connection) -> ImportResult:
            applied_count = 0
            conflict_count = 0
            duplicate_count = 0
            
            # Filter duplicates and sort
            new_ops = []
            for op in bundle_ops:
                if operation_exists(conn, op.op_id):
                    duplicate_count += 1
                else:
                    new_ops.append(op)
            
            if not new_ops:
                # All duplicates
                record_import(
                    conn,
                    metadata.bundle_id,
                    metadata.content_hash,
                    metadata.source_device_id,
                    len(bundle_ops),
                    0, 0, duplicate_count,
                )
                return ImportResult(
                    bundle_id=metadata.bundle_id,
                    source_device_id=metadata.source_device_id,
                    total_operations=len(bundle_ops),
                    applied_count=0,
                    conflict_count=0,
                    duplicate_count=duplicate_count,
                    skipped=False,
                )
            
            # Sort deterministically
            sorted_ops = sort_operations_deterministically(new_ops)
            
            # Process each operation
            for op in sorted_ops:
                # Check for conflict
                conflicting_op = detect_conflict(conn, op)
                
                if conflicting_op is not None:
                    # Insert operation but mark as not applied
                    remote_op = SyncOperation(
                        op_id=op.op_id,
                        device_id=op.device_id,
                        parent_op_id=op.parent_op_id,
                        vector_clock=op.vector_clock,
                        table_name=op.table_name,
                        op_type=op.op_type,
                        row_pk=op.row_pk,
                        old_values=op.old_values,
                        new_values=op.new_values,
                        schema_version=op.schema_version,
                        created_at=op.created_at,
                        is_local=False,
                        applied_at=None,  # Not applied due to conflict
                    )
                    insert_operation(conn, remote_op)

                    # Record conflict
                    record_conflict(
                        conn,
                        op.table_name,
                        op.row_pk,
                        conflicting_op.op_id,
                        op.op_id,
                    )
                    
                    conflict_count += 1
                else:
                    # No conflict - check if stale
                    if is_dominated(conn, op):
                        # Operation is stale (dominated by existing state)
                        # We insert it for history but don't apply to user table
                        applied_op = SyncOperation(
                            op_id=op.op_id,
                            device_id=op.device_id,
                            parent_op_id=op.parent_op_id,
                            vector_clock=op.vector_clock,
                            table_name=op.table_name,
                            op_type=op.op_type,
                            row_pk=op.row_pk,
                            old_values=op.old_values,
                            new_values=op.new_values,
                            schema_version=op.schema_version,
                            created_at=op.created_at,
                            is_local=False,
                            applied_at=int(time.time() * 1_000_000), # Marked as applied (skipped)
                        )
                        insert_operation(conn, applied_op)
                        # Count as applied since we successfully handled it (by ignoring)
                        applied_count += 1
                    else:
                        # Apply to user table
                        apply_operation(conn, op)
                        
                        applied_op = SyncOperation(
                            op_id=op.op_id,
                            device_id=op.device_id,
                            parent_op_id=op.parent_op_id,
                            vector_clock=op.vector_clock,
                            table_name=op.table_name,
                            op_type=op.op_type,
                            row_pk=op.row_pk,
                            old_values=op.old_values,
                            new_values=op.new_values,
                            schema_version=op.schema_version,
                            created_at=op.created_at,
                            is_local=False,
                            applied_at=int(time.time() * 1_000_000),
                        )
                        insert_operation(conn, applied_op)
                        applied_count += 1
                
                # Merge vector clock
                current_vc_json = serialize_vector_clock(get_vector_clock(conn))
                merged_vc_json = merge_vector_clocks(current_vc_json, op.vector_clock)
                update_vector_clock(conn, parse_vector_clock(merged_vc_json))
            
            # Update peer state
            final_vc = get_vector_clock(conn)
            now = int(time.time() * 1_000_000)
            
            conn.execute(
                """
                INSERT INTO sync_peer_state (
                    peer_device_id,
                    last_sent_vector_clock, last_sent_at,
                    last_received_vector_clock, last_received_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(peer_device_id) DO UPDATE SET
                    last_received_vector_clock = excluded.last_received_vector_clock,
                    last_received_at = excluded.last_received_at
                """,
                (
                    metadata.source_device_id,
                    EMPTY_VECTOR_CLOCK,
                    0,
                    serialize_vector_clock(final_vc),
                    now,
                ),
            )
            
            # Record import
            record_import(
                conn,
                metadata.bundle_id,
                metadata.content_hash,
                metadata.source_device_id,
                len(bundle_ops),
                applied_count,
                conflict_count,
                duplicate_count,
            )
            
            return ImportResult(
                bundle_id=metadata.bundle_id,
                source_device_id=metadata.source_device_id,
                total_operations=len(bundle_ops),
                applied_count=applied_count,
                conflict_count=conflict_count,
                duplicate_count=duplicate_count,
                skipped=False,
            )
        
        return execute_in_transaction(conn, do_import)
    
    def get_unresolved_conflicts(self) -> list[SyncConflict]:
        """
        Get all unresolved conflicts.
        
        Returns:
            List of unresolved conflicts
        """
        return _get_unresolved_conflicts(self.connection)

    def apply_operation(self, op: SyncOperation) -> bool:
        """
        Apply a single operation to the database.
        
        This is used for real-time streaming sync.
        It handles conflict detection, deduplication, and vector clock updates.
        
        Args:
            op: SyncOperation to apply
            
        Returns:
            True if applied, False if skipped (duplicate/conflict)
        """
        conn = self.connection
        
        def do_apply(conn: sqlite3.Connection) -> bool:
            if operation_exists(conn, op.op_id):
                return False
                
            conflicting_op = detect_conflict(conn, op)
            if conflicting_op is not None:
                # Record remote op but mark as not applied
                remote_op = SyncOperation(
                    op_id=op.op_id,
                    device_id=op.device_id,
                    parent_op_id=op.parent_op_id,
                    vector_clock=op.vector_clock,
                    table_name=op.table_name,
                    op_type=op.op_type,
                    row_pk=op.row_pk,
                    old_values=op.old_values,
                    new_values=op.new_values,
                    schema_version=op.schema_version,
                    created_at=op.created_at,
                    is_local=False,
                    applied_at=None,
                )
                insert_operation(conn, remote_op)
                record_conflict(conn, op.table_name, op.row_pk, conflicting_op.op_id, op.op_id)
                return False
                
            if is_dominated(conn, op):
                # Stale op
                applied_op = SyncOperation(
                    op_id=op.op_id,
                    device_id=op.device_id,
                    parent_op_id=op.parent_op_id,
                    vector_clock=op.vector_clock,
                    table_name=op.table_name,
                    op_type=op.op_type,
                    row_pk=op.row_pk,
                    old_values=op.old_values,
                    new_values=op.new_values,
                    schema_version=op.schema_version,
                    created_at=op.created_at,
                    is_local=False,
                    applied_at=int(time.time() * 1_000_000),
                )
                insert_operation(conn, applied_op)
                return True

            # Apply to user table
            _apply_operation(conn, op)
            
            applied_op = SyncOperation(
                op_id=op.op_id,
                device_id=op.device_id,
                parent_op_id=op.parent_op_id,
                vector_clock=op.vector_clock,
                table_name=op.table_name,
                op_type=op.op_type,
                row_pk=op.row_pk,
                old_values=op.old_values,
                new_values=op.new_values,
                schema_version=op.schema_version,
                created_at=op.created_at,
                is_local=False,
                applied_at=int(time.time() * 1_000_000),
            )
            insert_operation(conn, applied_op)
            
            # Merge vector clock
            current_vc_json = serialize_vector_clock(get_vector_clock(conn))
            merged_vc_json = merge_vector_clocks(current_vc_json, op.vector_clock)
            update_vector_clock(conn, parse_vector_clock(merged_vc_json))
            
            return True

        return execute_in_transaction(conn, do_apply)

    def get_new_operations(self, since_vector_clock: dict[str, int] | None = None) -> list[SyncOperation]:
        """
        Get operations generated on this device that the peer might not have.
        
        Args:
            since_vector_clock: Peer's current vector clock. If None, returns all.
            
        Returns:
            List of operations to send to peer.
        """
        return get_operations_since(self.connection, since_vector_clock)
    
    def get_vector_clock(self) -> dict[str, int]:
        """
        Get the current vector clock.
        
        Returns:
            Vector clock as dict
        """
        return get_vector_clock(self.connection)
    
    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._device_id = None
    
    def __enter__(self) -> "SyncEngine":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
