"""
generate.py - Bundle generation for sync export.

Generates a bundle containing operations that a peer hasn't seen.
Bundles are self-contained SQLite files.
"""

import sqlite3
import time
from typing import Final

from sqlite_sync.bundle.format import BundleMetadata, get_bundle_schema_sql
from sqlite_sync.db.migrations import get_device_id, get_schema_version, get_vector_clock
from sqlite_sync.log.vector_clock import (
    vector_clock_dominates,
    parse_vector_clock,
    serialize_vector_clock,
    EMPTY_VECTOR_CLOCK,
)
from sqlite_sync.log.operations import operation_from_row
from sqlite_sync.utils.uuid7 import generate_uuid_v7
from sqlite_sync.utils.hashing import sha256_operations
from sqlite_sync.errors import DatabaseError


def generate_bundle(
    local_conn: sqlite3.Connection,
    peer_device_id: bytes,
    output_path: str,
) -> str | None:
    """
    Generate a sync bundle for a peer.
    
    Selects operations that the peer hasn't seen (based on vector clock)
    and packages them into a self-contained SQLite file.
    
    Args:
        local_conn: Connection to local database
        peer_device_id: UUID of peer device
        output_path: Where to write the bundle file
        
    Returns:
        output_path if bundle was created, None if nothing to send
        
    Raises:
        DatabaseError: If bundle creation fails
    """
    # Get peer's last known vector clock
    peer_vc = _get_peer_vector_clock(local_conn, peer_device_id)
    
    # Find operations peer hasn't seen
    operations = _find_unseen_operations(local_conn, peer_vc)
    
    if not operations:
        return None  # Nothing to send
    
    # Create bundle database
    try:
        bundle_conn = sqlite3.connect(output_path)
        
        # Create bundle schema
        for sql in get_bundle_schema_sql():
            bundle_conn.execute(sql)
        
        # Insert operations
        bundle_conn.executemany(
            """
            INSERT INTO bundle_operations (
                op_id, device_id, parent_op_id, vector_clock,
                table_name, op_type, row_pk, old_values, new_values,
                schema_version, created_at, is_local, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            operations,
        )
        
        # Calculate content hash
        op_ids = [op[0] for op in operations]
        content_hash = sha256_operations(op_ids)
        
        # Insert metadata
        device_id = get_device_id(local_conn)
        schema_version = get_schema_version(local_conn)
        
        metadata = BundleMetadata(
            bundle_id=generate_uuid_v7(),
            source_device_id=device_id,
            created_at=int(time.time() * 1_000_000),
            schema_version=schema_version,
            op_count=len(operations),
            content_hash=content_hash,
        )
        
        bundle_conn.execute(
            """
            INSERT INTO bundle_metadata (
                bundle_id, source_device_id, created_at,
                schema_version, op_count, content_hash
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                metadata.bundle_id,
                metadata.source_device_id,
                metadata.created_at,
                metadata.schema_version,
                metadata.op_count,
                metadata.content_hash,
            ),
        )
        
        bundle_conn.commit()
        bundle_conn.close()
        
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to create bundle: {e}",
            operation="generate_bundle",
        ) from e
    
    # Update peer state
    _update_peer_sent_state(local_conn, peer_device_id)
    
    return output_path


def _get_peer_vector_clock(
    conn: sqlite3.Connection,
    peer_device_id: bytes,
) -> dict[str, int]:
    """
    Get the last vector clock we sent to a peer.
    
    Returns empty dict for new peers.
    """
    cursor = conn.execute(
        """
        SELECT last_sent_vector_clock 
        FROM sync_peer_state 
        WHERE peer_device_id = ?
        """,
        (peer_device_id,),
    )
    row = cursor.fetchone()
    
    if row is None:
        return {}  # New peer
    
    return parse_vector_clock(row[0])


def _find_unseen_operations(
    conn: sqlite3.Connection,
    peer_vc: dict[str, int],
) -> list[tuple]:
    """
    Find operations that the peer hasn't seen.
    
    An operation is unseen if its vector clock is NOT dominated by peer_vc.
    """
    cursor = conn.execute(
        "SELECT * FROM sync_operations ORDER BY created_at ASC"
    )
    
    unseen = []
    for row in cursor:
        op_vc_json = row[3]  # vector_clock column
        op_vc = parse_vector_clock(op_vc_json)
        
        # If peer's VC doesn't dominate this op's VC, peer hasn't seen it
        if not vector_clock_dominates(peer_vc, op_vc):
            unseen.append(row)
    
    return unseen


def _update_peer_sent_state(
    conn: sqlite3.Connection,
    peer_device_id: bytes,
) -> None:
    """
    Update what we've sent to this peer.
    """
    current_vc = get_vector_clock(conn)
    current_vc_json = serialize_vector_clock(current_vc)
    now = int(time.time() * 1_000_000)
    
    conn.execute(
        """
        INSERT INTO sync_peer_state (
            peer_device_id, 
            last_sent_vector_clock, last_sent_at,
            last_received_vector_clock, last_received_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(peer_device_id) DO UPDATE SET
            last_sent_vector_clock = excluded.last_sent_vector_clock,
            last_sent_at = excluded.last_sent_at
        """,
        (peer_device_id, current_vc_json, now, EMPTY_VECTOR_CLOCK, 0),
    )
    conn.commit()
