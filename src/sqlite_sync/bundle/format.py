"""
format.py - Bundle format constants and types.

Bundles are self-contained SQLite database files containing:
1. Operations to replicate (bundle_operations table)
2. Metadata (bundle_metadata table)

This module defines the structure and provides helpers.
"""

from dataclasses import dataclass
from typing import Final

from sqlite_sync.db.schema import BUNDLE_SCHEMA_STATEMENTS


# Bundle file extension (convention, not enforced)
BUNDLE_EXTENSION: Final[str] = ".bundle.db"


@dataclass(frozen=True, slots=True)
class BundleMetadata:
    """
    Immutable bundle metadata.
    
    Stored in bundle_metadata table (single row).
    """
    bundle_id: bytes  # 16-byte UUID v7
    source_device_id: bytes  # 16-byte UUID
    created_at: int  # Unix microseconds
    schema_version: int
    op_count: int
    content_hash: bytes  # 32-byte SHA-256
    
    def __post_init__(self) -> None:
        """Validate metadata after initialization."""
        if len(self.bundle_id) != 16:
            raise ValueError(f"bundle_id must be 16 bytes, got {len(self.bundle_id)}")
        if len(self.source_device_id) != 16:
            raise ValueError(f"source_device_id must be 16 bytes")
        if len(self.content_hash) != 32:
            raise ValueError(f"content_hash must be 32 bytes")
        if self.op_count < 0:
            raise ValueError(f"op_count must be non-negative")


def metadata_from_row(row: tuple) -> BundleMetadata:
    """
    Create BundleMetadata from database row.
    
    Args:
        row: Tuple from SELECT * FROM bundle_metadata
        
    Returns:
        BundleMetadata instance
    """
    return BundleMetadata(
        bundle_id=row[0],
        source_device_id=row[1],
        created_at=row[2],
        schema_version=row[3],
        op_count=row[4],
        content_hash=row[5],
    )


def metadata_to_row(meta: BundleMetadata) -> tuple:
    """
    Convert BundleMetadata to database row tuple.
    
    Args:
        meta: BundleMetadata instance
        
    Returns:
        Tuple suitable for INSERT
    """
    return (
        meta.bundle_id,
        meta.source_device_id,
        meta.created_at,
        meta.schema_version,
        meta.op_count,
        meta.content_hash,
    )


def get_bundle_schema_sql() -> list[str]:
    """
    Get SQL statements to create bundle schema.
    
    Returns:
        List of SQL CREATE statements
    """
    statements = []
    for statement in BUNDLE_SCHEMA_STATEMENTS:
        statements.append(statement.strip())
    return statements
