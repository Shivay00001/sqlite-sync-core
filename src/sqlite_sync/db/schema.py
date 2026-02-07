"""
schema.py - Sync system table schema definitions.
"""

from typing import Final

# sync_operations table
SYNC_OPERATIONS_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS sync_operations (
    op_id BLOB PRIMARY KEY CHECK(length(op_id) = 16),
    device_id BLOB NOT NULL CHECK(length(device_id) = 16),
    parent_op_id BLOB CHECK(parent_op_id IS NULL OR length(parent_op_id) = 16),
    vector_clock TEXT NOT NULL,
    hlc TEXT NOT NULL,
    table_name TEXT NOT NULL,
    op_type TEXT NOT NULL CHECK(op_type IN ('INSERT', 'UPDATE', 'DELETE')),
    row_pk BLOB NOT NULL,
    old_values BLOB,
    new_values BLOB,
    schema_version INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    is_local INTEGER NOT NULL CHECK(is_local IN (0, 1)),
    applied_at INTEGER,
    FOREIGN KEY (parent_op_id) REFERENCES sync_operations(op_id)
) STRICT;
"""

SYNC_OPERATIONS_INDICES: Final[str] = """
CREATE INDEX IF NOT EXISTS idx_ops_device_created ON sync_operations(device_id, created_at);
CREATE INDEX IF NOT EXISTS idx_ops_table_pk ON sync_operations(table_name, row_pk);
CREATE INDEX IF NOT EXISTS idx_ops_id ON sync_operations(op_id);
"""

# sync_metadata table
SYNC_METADATA_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS sync_metadata (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
) STRICT;
"""

# sync_conflicts table
SYNC_CONFLICTS_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS sync_conflicts (
    conflict_id BLOB PRIMARY KEY CHECK(length(conflict_id) = 16),
    table_name TEXT NOT NULL,
    row_pk BLOB NOT NULL,
    local_op_id BLOB NOT NULL CHECK(length(local_op_id) = 16),
    remote_op_id BLOB NOT NULL CHECK(length(remote_op_id) = 16),
    detected_at INTEGER NOT NULL,
    resolved_at INTEGER,
    resolution_op_id BLOB CHECK(resolution_op_id IS NULL OR length(resolution_op_id) = 16),
    resolution_strategy TEXT,
    FOREIGN KEY (local_op_id) REFERENCES sync_operations(op_id),
    FOREIGN KEY (remote_op_id) REFERENCES sync_operations(op_id),
    FOREIGN KEY (resolution_op_id) REFERENCES sync_operations(op_id)
) STRICT;
"""

SYNC_CONFLICTS_INDICES: Final[str] = """
CREATE INDEX IF NOT EXISTS idx_conflicts_unresolved ON sync_conflicts(detected_at) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_conflicts_row ON sync_conflicts(table_name, row_pk);
"""

# sync_peer_state table
SYNC_PEER_STATE_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS sync_peer_state (
    peer_device_id BLOB PRIMARY KEY CHECK(length(peer_device_id) = 16),
    last_sent_vector_clock TEXT NOT NULL,
    last_sent_at INTEGER NOT NULL,
    last_received_vector_clock TEXT NOT NULL,
    last_received_at INTEGER NOT NULL
) STRICT;
"""

# sync_import_log table
SYNC_IMPORT_LOG_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS sync_import_log (
    import_id BLOB PRIMARY KEY CHECK(length(import_id) = 16),
    bundle_id BLOB NOT NULL CHECK(length(bundle_id) = 16),
    bundle_hash BLOB NOT NULL CHECK(length(bundle_hash) = 32),
    imported_at INTEGER NOT NULL,
    source_device_id BLOB NOT NULL CHECK(length(source_device_id) = 16),
    op_count INTEGER NOT NULL,
    applied_count INTEGER NOT NULL,
    conflict_count INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    UNIQUE(bundle_hash)
) STRICT;
"""

SYNC_IMPORT_LOG_INDICES: Final[str] = """
CREATE INDEX IF NOT EXISTS idx_import_log_time ON sync_import_log(imported_at);
"""

# All schema statements in order
ALL_SCHEMA_STATEMENTS: Final[tuple[str, ...]] = (
    SYNC_OPERATIONS_SCHEMA,
    SYNC_OPERATIONS_INDICES,
    SYNC_METADATA_SCHEMA,
    SYNC_CONFLICTS_SCHEMA,
    SYNC_CONFLICTS_INDICES,
    SYNC_PEER_STATE_SCHEMA,
    SYNC_IMPORT_LOG_SCHEMA,
    SYNC_IMPORT_LOG_INDICES,
)

# Bundle schema
BUNDLE_METADATA_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS bundle_metadata (
    bundle_id BLOB PRIMARY KEY CHECK(length(bundle_id) = 16),
    source_device_id BLOB NOT NULL CHECK(length(source_device_id) = 16),
    created_at INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    op_count INTEGER NOT NULL,
    content_hash BLOB NOT NULL CHECK(length(content_hash) = 32)
) STRICT;
"""

BUNDLE_OPERATIONS_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS bundle_operations (
    op_id BLOB PRIMARY KEY CHECK(length(op_id) = 16),
    device_id BLOB NOT NULL CHECK(length(device_id) = 16),
    parent_op_id BLOB CHECK(parent_op_id IS NULL OR length(parent_op_id) = 16),
    vector_clock TEXT NOT NULL,
    hlc TEXT NOT NULL,
    table_name TEXT NOT NULL,
    op_type TEXT NOT NULL CHECK(op_type IN ('INSERT', 'UPDATE', 'DELETE')),
    row_pk BLOB NOT NULL,
    old_values BLOB,
    new_values BLOB,
    schema_version INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    is_local INTEGER NOT NULL CHECK(is_local IN (0, 1)),
    applied_at INTEGER
) STRICT;
"""

BUNDLE_SCHEMA_STATEMENTS: Final[tuple[str, ...]] = (
    BUNDLE_METADATA_SCHEMA,
    BUNDLE_OPERATIONS_SCHEMA,
)
