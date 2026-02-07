"""
apply.py - Operation application to user tables.

Applies sync operations to user tables.
Operations are applied inside transactions for atomicity.
"""

import sqlite3
import logging

from typing import Any
from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.utils.msgpack_codec import unpack_dict, unpack_primary_key
from sqlite_sync.errors import OperationError, DatabaseError

logger = logging.getLogger("sqlite_sync.apply")

def apply_operation(
    conn: sqlite3.Connection,
    op: SyncOperation,
) -> None:
    """
    Apply a sync operation to the user table.
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
    
    sql = f"INSERT OR IGNORE INTO {op.table_name} ({column_list}) VALUES ({placeholders})"
    
    try:
        # print(f"DEBUG: Applying INSERT to {op.table_name}: {values_dict}")
        conn.execute(sql, list(values_dict.values()))
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
    
    # Primary key handling
    if isinstance(pk_value, (list, tuple)):
        # For simplicity, we assume primary keys are the first columns in values_dict
        pk_columns = list(values_dict.keys())[:len(pk_value)]
        where_parts = [f"{col} = ?" for col in pk_columns]
        where_clause = " AND ".join(where_parts)
        where_values = list(pk_value)
    else:
        # First column is assumed to be PK
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
    
    # Primary key handling
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

def apply_operation_raw(
    conn: sqlite3.Connection,
    table_name: str,
    row_pk: bytes,
    values_dict: dict[str, Any]
) -> None:
    """
    Apply a dictionary of values directly to a user table.
    Used for merged states in advanced synchronization.
    """
    if not values_dict:
        return

    pk_value = unpack_primary_key(row_pk)
    
    # 1. Try to see if row exists
    # Build SET clause
    set_parts = []
    set_values = []
    for col, val in values_dict.items():
        set_parts.append(f"{col} = ?")
        set_values.append(val)
    set_clause = ", ".join(set_parts)

    # Primary key handling
    # Query database for PK columns
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    pk_cols = [info[1] for info in columns_info if info[5] > 0]
    pk_cols.sort(key=lambda x: [info[5] for info in columns_info if info[1] == x][0]) # Sort by pk index
    
    if not pk_cols:
         # Fallback or error? For now fallback to first column but log warning
         import sys
         sys.stderr.write(f"WARNING: No PK found for {table_name}, assuming first col\n")
         pk_cols = [list(values_dict.keys())[0]]

    # Primary key handling
    if isinstance(pk_value, (list, tuple)):
         # If composite PK, ensure we have enough columns
         if len(pk_cols) != len(pk_value):
              # This is a mismatch between schema and provided PK value
              # Fallback to guessing if schema query failed/mismatched?
              # But let's trust the schema query.
              pass 
         
         where_clause = " AND ".join(f"{col} = ?" for col in pk_cols)
         where_values = list(pk_value)
    else:
         # Single PK
         pk_column = pk_cols[0]
         where_clause = f"{pk_column} = ?"
         where_values = [pk_value]

    # Try UPDATE first
    sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
    cursor = conn.execute(sql, set_values + where_values)
    
    if cursor.rowcount == 0:
        # If no rows updated, INSERT it
        columns = list(values_dict.keys())
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        conn.execute(sql, list(values_dict.values()))
