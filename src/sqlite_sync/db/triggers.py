"""
triggers.py - SQLite trigger generation for change capture.

Triggers execute atomically within user transactions to capture
every mutation as a sync operation.
"""

import sqlite3
from typing import Final

from sqlite_sync.errors import DatabaseError, ValidationError
from sqlite_sync.config import RESERVED_TABLE_NAMES


# Template for INSERT trigger
INSERT_TRIGGER_TEMPLATE: Final[str] = """
CREATE TRIGGER IF NOT EXISTS {table_name}_sync_insert 
AFTER INSERT ON {table_name}
FOR EACH ROW
WHEN sync_is_disabled() = 0
BEGIN
    INSERT INTO sync_operations (
        op_id,
        device_id,
        parent_op_id,
        vector_clock,
        hlc,
        table_name,
        op_type,
        row_pk,
        old_values,
        new_values,
        schema_version,
        created_at,
        is_local,
        applied_at
    )
    SELECT
        sync_uuid_v7(),
        (SELECT value FROM sync_metadata WHERE key = 'device_id'),
        (SELECT op_id FROM sync_operations 
         WHERE device_id = (SELECT value FROM sync_metadata WHERE key = 'device_id')
         ORDER BY created_at DESC 
         LIMIT 1),
        sync_vector_clock_increment(
            (SELECT value FROM sync_metadata WHERE key = 'device_id'),
            (SELECT value FROM sync_metadata WHERE key = 'vector_clock')
        ),
        sync_hlc_now((SELECT value FROM sync_metadata WHERE key = 'device_id')),
        '{table_name}',
        'INSERT',
        sync_pack_pk({pk_expression_new}),
        NULL,
        sync_pack_values(json_object({column_pairs_new})),
        CAST((SELECT value FROM sync_metadata WHERE key = 'schema_version') AS INTEGER),
        CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER),
        1,
        CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)
    ;
    
    UPDATE sync_metadata 
    SET value = CAST(sync_vector_clock_increment(
        (SELECT value FROM sync_metadata WHERE key = 'device_id'),
        value
    ) AS BLOB)
    WHERE key = 'vector_clock';
END
"""

# Template for UPDATE trigger
UPDATE_TRIGGER_TEMPLATE: Final[str] = """
CREATE TRIGGER IF NOT EXISTS {table_name}_sync_update 
AFTER UPDATE ON {table_name}
FOR EACH ROW
WHEN sync_is_disabled() = 0
BEGIN
    INSERT INTO sync_operations (
        op_id,
        device_id,
        parent_op_id,
        vector_clock,
        hlc,
        table_name,
        op_type,
        row_pk,
        old_values,
        new_values,
        schema_version,
        created_at,
        is_local,
        applied_at
    )
    SELECT
        sync_uuid_v7(),
        (SELECT value FROM sync_metadata WHERE key = 'device_id'),
        (SELECT op_id FROM sync_operations 
         WHERE device_id = (SELECT value FROM sync_metadata WHERE key = 'device_id')
         ORDER BY created_at DESC 
         LIMIT 1),
        sync_vector_clock_increment(
            (SELECT value FROM sync_metadata WHERE key = 'device_id'),
            (SELECT value FROM sync_metadata WHERE key = 'vector_clock')
        ),
        sync_hlc_now((SELECT value FROM sync_metadata WHERE key = 'device_id')),
        '{table_name}',
        'UPDATE',
        sync_pack_pk({pk_expression_new}),
        sync_pack_values(json_object({column_pairs_old})),
        sync_pack_values(json_object({column_pairs_new})),
        CAST((SELECT value FROM sync_metadata WHERE key = 'schema_version') AS INTEGER),
        CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER),
        1,
        CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)
    ;
    
    UPDATE sync_metadata 
    SET value = CAST(sync_vector_clock_increment(
        (SELECT value FROM sync_metadata WHERE key = 'device_id'),
        value
    ) AS BLOB)
    WHERE key = 'vector_clock';
END
"""

# Template for DELETE trigger
DELETE_TRIGGER_TEMPLATE: Final[str] = """
CREATE TRIGGER IF NOT EXISTS {table_name}_sync_delete 
AFTER DELETE ON {table_name}
FOR EACH ROW
WHEN sync_is_disabled() = 0
BEGIN
    INSERT INTO sync_operations (
        op_id,
        device_id,
        parent_op_id,
        vector_clock,
        hlc,
        table_name,
        op_type,
        row_pk,
        old_values,
        new_values,
        schema_version,
        created_at,
        is_local,
        applied_at
    )
    SELECT
        sync_uuid_v7(),
        (SELECT value FROM sync_metadata WHERE key = 'device_id'),
        (SELECT op_id FROM sync_operations 
         WHERE device_id = (SELECT value FROM sync_metadata WHERE key = 'device_id')
         ORDER BY created_at DESC 
         LIMIT 1),
        sync_vector_clock_increment(
            (SELECT value FROM sync_metadata WHERE key = 'device_id'),
            (SELECT value FROM sync_metadata WHERE key = 'vector_clock')
        ),
        sync_hlc_now((SELECT value FROM sync_metadata WHERE key = 'device_id')),
        '{table_name}',
        'DELETE',
        sync_pack_pk({pk_expression_old}),
        sync_pack_values(json_object({column_pairs_old})),
        NULL,
        CAST((SELECT value FROM sync_metadata WHERE key = 'schema_version') AS INTEGER),
        CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER),
        1,
        CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)
    ;
    
    UPDATE sync_metadata 
    SET value = CAST(sync_vector_clock_increment(
        (SELECT value FROM sync_metadata WHERE key = 'device_id'),
        value
    ) AS BLOB)
    WHERE key = 'vector_clock';
END
"""


def install_triggers_for_table(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Install INSERT, UPDATE, DELETE triggers for a table.
    
    Args:
        conn: SQLite connection
        table_name: Name of table to enable sync on
        
    Raises:
        ValidationError: If table name is invalid
        DatabaseError: If trigger creation fails
    """
    _validate_table_name(table_name)
    
    # Get table schema
    columns, pk_columns = _get_table_info(conn, table_name)
    
    if not columns:
        raise ValidationError(
            f"Table '{table_name}' not found or has no columns",
            field="table_name",
            value=table_name,
        )
    
    if not pk_columns:
        raise ValidationError(
            f"Table '{table_name}' has no primary key. Sync requires a primary key.",
            field="table_name",
            value=table_name,
        )
    
    # Build column expressions
    column_pairs_new = ", ".join(
        f"'{col}', NEW.{col}" for col in columns
    )
    column_pairs_old = ", ".join(
        f"'{col}', OLD.{col}" for col in columns
    )
    
    # Build primary key expression
    if len(pk_columns) == 1:
        pk_expression_new = f"NEW.{pk_columns[0]}"
        pk_expression_old = f"OLD.{pk_columns[0]}"
    else:
        # Composite key - pack as JSON array
        pk_expression_new = "json_array(" + ", ".join(f"NEW.{pk}" for pk in pk_columns) + ")"
        pk_expression_old = "json_array(" + ", ".join(f"OLD.{pk}" for pk in pk_columns) + ")"
    
    # Generate and execute triggers
    try:
        insert_sql = INSERT_TRIGGER_TEMPLATE.format(
            table_name=table_name,
            pk_expression_new=pk_expression_new,
            column_pairs_new=column_pairs_new,
        )
        conn.executescript(insert_sql)
        
        update_sql = UPDATE_TRIGGER_TEMPLATE.format(
            table_name=table_name,
            pk_expression_new=pk_expression_new,
            column_pairs_old=column_pairs_old,
            column_pairs_new=column_pairs_new,
        )
        conn.executescript(update_sql)
        
        delete_sql = DELETE_TRIGGER_TEMPLATE.format(
            table_name=table_name,
            pk_expression_old=pk_expression_old,
            column_pairs_old=column_pairs_old,
        )
        conn.executescript(delete_sql)
        
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to create triggers for table '{table_name}': {e}",
            operation="create_triggers",
        ) from e


def remove_triggers_for_table(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Remove sync triggers from a table.
    
    Args:
        conn: SQLite connection
        table_name: Name of table
    """
    _validate_table_name(table_name)
    
    try:
        conn.execute(f"DROP TRIGGER IF EXISTS {table_name}_sync_insert")
        conn.execute(f"DROP TRIGGER IF EXISTS {table_name}_sync_update")
        conn.execute(f"DROP TRIGGER IF EXISTS {table_name}_sync_delete")
    except sqlite3.Error as e:
        raise DatabaseError(
            f"Failed to remove triggers for table '{table_name}': {e}",
            operation="drop_triggers",
        ) from e


def _validate_table_name(table_name: str) -> None:
    """
    Validate table name is safe and not reserved.
    
    Args:
        table_name: Table name to validate
        
    Raises:
        ValidationError: If table name is invalid
    """
    if not table_name:
        raise ValidationError("Table name cannot be empty", field="table_name")
    
    if table_name in RESERVED_TABLE_NAMES:
        raise ValidationError(
            f"Cannot enable sync on reserved table '{table_name}'",
            field="table_name",
            value=table_name,
        )
    
    # Basic SQL injection prevention
    if not table_name.replace("_", "").isalnum():
        raise ValidationError(
            f"Table name contains invalid characters: '{table_name}'",
            field="table_name",
            value=table_name,
        )


def _get_table_info(
    conn: sqlite3.Connection, table_name: str
) -> tuple[list[str], list[str]]:
    """
    Get column names and primary key columns for a table.
    
    Args:
        conn: SQLite connection
        table_name: Table name
        
    Returns:
        Tuple of (all_columns, pk_columns)
    """
    try:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()
        
        columns = []
        pk_columns = []
        
        for row in rows:
            col_name = row[1]
            is_pk = row[5] > 0  # pk column in PRAGMA table_info
            
            columns.append(col_name)
            if is_pk:
                pk_columns.append(col_name)
        
        return columns, pk_columns
        
    except sqlite3.Error:
        return [], []


def has_triggers(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Check if sync triggers exist for a table.
    
    Args:
        conn: SQLite connection
        table_name: Table name
        
    Returns:
        True if all three triggers exist
    """
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE ?",
            (f"{table_name}_sync_%",),
        )
        triggers = {row[0] for row in cursor.fetchall()}
        
        expected = {
            f"{table_name}_sync_insert",
            f"{table_name}_sync_update",
            f"{table_name}_sync_delete",
        }
        
        return expected.issubset(triggers)
        
    except sqlite3.Error:
        return False
