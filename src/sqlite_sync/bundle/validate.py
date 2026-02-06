"""
validate.py - Bundle validation before import.

All bundles are treated as untrusted input.
Validation ensures integrity, schema compatibility, and well-formedness.
"""

import sqlite3

from sqlite_sync.bundle.format import BundleMetadata, metadata_from_row
from sqlite_sync.utils.hashing import sha256_operations
from sqlite_sync.errors import BundleError
from sqlite_sync.config import OPERATION_TYPES


def validate_bundle(bundle_path: str, expected_schema_version: int) -> BundleMetadata:
    """
    Validate a bundle file completely.
    
    Checks:
    1. SQLite integrity (PRAGMA integrity_check)
    2. Required tables exist
    3. Metadata is present and valid
    4. Schema version matches
    5. Content hash matches
    6. Operations are well-formed
    
    Args:
        bundle_path: Path to bundle file
        expected_schema_version: Schema version of local database
        
    Returns:
        BundleMetadata if valid
        
    Raises:
        BundleError: If validation fails
    """
    try:
        conn = sqlite3.connect(bundle_path)
    except sqlite3.Error as e:
        raise BundleError(
            f"Cannot open bundle: {e}",
            bundle_path=bundle_path,
            reason="open_failed",
        ) from e
    
    try:
        # Step 1: SQLite integrity check
        _check_integrity(conn, bundle_path)
        
        # Step 2: Check required tables exist
        _check_tables_exist(conn, bundle_path)
        
        # Step 3: Load and validate metadata
        metadata = _load_metadata(conn, bundle_path)
        
        # Step 4: Check schema version
        if metadata.schema_version != expected_schema_version:
            raise BundleError(
                f"Schema version mismatch: bundle has {metadata.schema_version}, "
                f"local has {expected_schema_version}",
                bundle_path=bundle_path,
                reason="schema_mismatch",
            )
        
        # Step 5: Verify content hash
        _verify_content_hash(conn, metadata, bundle_path)
        
        # Step 6: Validate operations are well-formed
        _validate_operations(conn, bundle_path)
        
        return metadata
        
    finally:
        conn.close()


def _check_integrity(conn: sqlite3.Connection, bundle_path: str) -> None:
    """Run SQLite integrity check."""
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()
        if result is None or result[0] != "ok":
            raise BundleError(
                "Bundle integrity check failed",
                bundle_path=bundle_path,
                reason="integrity_check_failed",
            )
    except sqlite3.Error as e:
        raise BundleError(
            f"Integrity check error: {e}",
            bundle_path=bundle_path,
            reason="integrity_check_error",
        ) from e


def _check_tables_exist(conn: sqlite3.Connection, bundle_path: str) -> None:
    """Verify required bundle tables exist."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    )
    tables = {row[0] for row in cursor.fetchall()}
    
    required = {"bundle_metadata", "bundle_operations"}
    missing = required - tables
    
    if missing:
        raise BundleError(
            f"Bundle missing required tables: {missing}",
            bundle_path=bundle_path,
            reason="missing_tables",
        )


def _load_metadata(conn: sqlite3.Connection, bundle_path: str) -> BundleMetadata:
    """Load and validate bundle metadata."""
    cursor = conn.execute("SELECT * FROM bundle_metadata")
    row = cursor.fetchone()
    
    if row is None:
        raise BundleError(
            "Bundle has no metadata",
            bundle_path=bundle_path,
            reason="no_metadata",
        )
    
    # Check for multiple metadata rows (invalid)
    if cursor.fetchone() is not None:
        raise BundleError(
            "Bundle has multiple metadata rows",
            bundle_path=bundle_path,
            reason="multiple_metadata",
        )
    
    try:
        return metadata_from_row(row)
    except (ValueError, TypeError) as e:
        raise BundleError(
            f"Invalid bundle metadata: {e}",
            bundle_path=bundle_path,
            reason="invalid_metadata",
        ) from e


def _verify_content_hash(
    conn: sqlite3.Connection,
    metadata: BundleMetadata,
    bundle_path: str,
) -> None:
    """Verify content hash matches operations."""
    cursor = conn.execute(
        "SELECT op_id FROM bundle_operations ORDER BY op_id"
    )
    op_ids = [row[0] for row in cursor.fetchall()]
    
    # Check operation count matches
    if len(op_ids) != metadata.op_count:
        raise BundleError(
            f"Operation count mismatch: metadata says {metadata.op_count}, "
            f"found {len(op_ids)}",
            bundle_path=bundle_path,
            reason="op_count_mismatch",
        )
    
    # Calculate and compare hash
    calculated_hash = sha256_operations(op_ids)
    
    if calculated_hash != metadata.content_hash:
        raise BundleError(
            "Content hash mismatch (bundle may be corrupted)",
            bundle_path=bundle_path,
            reason="hash_mismatch",
        )


def _validate_operations(conn: sqlite3.Connection, bundle_path: str) -> None:
    """Validate operations are well-formed."""
    cursor = conn.execute(
        "SELECT op_id, device_id, op_type FROM bundle_operations"
    )
    
    for row in cursor:
        op_id, device_id, op_type = row
        
        # Check UUID lengths
        if len(op_id) != 16:
            raise BundleError(
                f"Invalid op_id length: {len(op_id)} bytes",
                bundle_path=bundle_path,
                reason="invalid_op_id",
            )
        
        if len(device_id) != 16:
            raise BundleError(
                f"Invalid device_id length: {len(device_id)} bytes",
                bundle_path=bundle_path,
                reason="invalid_device_id",
            )
        
        # Check operation type
        if op_type not in OPERATION_TYPES:
            raise BundleError(
                f"Invalid operation type: {op_type}",
                bundle_path=bundle_path,
                reason="invalid_op_type",
            )
