"""
apply.py - Operation application to user tables.

Applies sync operations to user tables.
Operations are applied inside transactions for atomicity.
"""

import sqlite3

from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.utils.msgpack_codec import unpack_dict, unpack_primary_key
from sqlite_sync.errors import OperationError, DatabaseError


def apply_operation(
    conn: sqlite3.Connection,
    op: SyncOperation,
) -> None:
    """
    Apply a sync operation to the user table.
    
    Args:
        conn: SQLite connection
        op: Operation to apply
        
    Raises:
        OperationError: If operation cannot be applied
        DatabaseError: If database error occurs
    """
    if op.op_type == "INSERT":
        _apply_insert(conn, op)
    elif op.op_type == "UPDATE":
        _apply_update(conn, op)
    elif op.op_type == "DELETE":
        _apply_delete(conn, op)
    else:
        raise OperationError(
            f"Unknown operation type: {op.op_type}",
            op_id=op.op_id,
            op_type=op.op_type,
            table_name=op.table_name,
        )


def _apply_insert(conn: sqlite3.Connection, op: SyncOperation) -> None:
    """Apply INSERT operation."""
    if op.new_values is None:
        raise OperationError(
            "INSERT operation has no new_values",
            op_id=op.op_id,
            op_type="INSERT",
            table_name=op.table_name,
        )
    
    values_dict = unpack_dict(op.new_values)
    
    if not values_dict:
        raise OperationError(
            "INSERT operation has empty new_values",
            op_id=op.op_id,
            op_type="INSERT",
            table_name=op.table_name,
        )
    
    columns = list(values_dict.keys())
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    
    sql = f"INSERT INTO {op.table_name} ({column_list}) VALUES ({placeholders})"
    
    try:
        conn.execute(sql, list(values_dict.values()))
    except sqlite3.IntegrityError as e:
        # Row may already exist (idempotent import case)
        if "UNIQUE constraint failed" in str(e):
            # This is OK for idempotent import - row already exists
            pass
        else:
            raise OperationError(
                f"INSERT failed: {e}",
                op_id=op.op_id,
                op_type="INSERT",
                table_name=op.table_name,
            ) from e
    except sqlite3.Error as e:
        raise DatabaseError(
            f"INSERT failed: {e}",
            operation="apply_insert",
            sql=sql,
        ) from e


def _apply_update(conn: sqlite3.Connection, op: SyncOperation) -> None:
    """Apply UPDATE operation."""
    if op.new_values is None:
        raise OperationError(
            "UPDATE operation has no new_values",
            op_id=op.op_id,
            op_type="UPDATE",
            table_name=op.table_name,
        )
    
    values_dict = unpack_dict(op.new_values)
    pk_value = unpack_primary_key(op.row_pk)
    
    if not values_dict:
        raise OperationError(
            "UPDATE operation has empty new_values",
            op_id=op.op_id,
            op_type="UPDATE",
            table_name=op.table_name,
        )
    
    # Build SET clause
    set_parts = []
    set_values = []
    
    for col, val in values_dict.items():
        set_parts.append(f"{col} = ?")
        set_values.append(val)
    
    set_clause = ", ".join(set_parts)
    
    # Determine primary key column(s)
    # For single-column PK, pk_value is the value directly
    # For composite PK, pk_value is a list/tuple
    if isinstance(pk_value, (list, tuple)):
        # Composite PK - need to determine columns from values_dict
        # First columns in the dict are assumed to be PK (order preserved)
        pk_columns = list(values_dict.keys())[:len(pk_value)]
        where_parts = [f"{col} = ?" for col in pk_columns]
        where_clause = " AND ".join(where_parts)
        where_values = list(pk_value)
    else:
        # Single column PK - use first column from values_dict
        pk_column = list(values_dict.keys())[0]
        where_clause = f"{pk_column} = ?"
        where_values = [pk_value]
    
    sql = f"UPDATE {op.table_name} SET {set_clause} WHERE {where_clause}"
    
    try:
        conn.execute(sql, set_values + where_values)
    except sqlite3.Error as e:
        raise DatabaseError(
            f"UPDATE failed: {e}",
            operation="apply_update",
            sql=sql,
        ) from e


def _apply_delete(conn: sqlite3.Connection, op: SyncOperation) -> None:
    """Apply DELETE operation."""
    if op.old_values is None:
        raise OperationError(
            "DELETE operation has no old_values",
            op_id=op.op_id,
            op_type="DELETE",
            table_name=op.table_name,
        )
    
    old_dict = unpack_dict(op.old_values)
    pk_value = unpack_primary_key(op.row_pk)
    
    # Determine primary key column(s)
    if isinstance(pk_value, (list, tuple)):
        pk_columns = list(old_dict.keys())[:len(pk_value)]
        where_parts = [f"{col} = ?" for col in pk_columns]
        where_clause = " AND ".join(where_parts)
        where_values = list(pk_value)
    else:
        pk_column = list(old_dict.keys())[0]
        where_clause = f"{pk_column} = ?"
        where_values = [pk_value]
    
    sql = f"DELETE FROM {op.table_name} WHERE {where_clause}"
    
    try:
        conn.execute(sql, where_values)
    except sqlite3.Error as e:
        raise DatabaseError(
            f"DELETE failed: {e}",
            operation="apply_delete",
            sql=sql,
        ) from e
