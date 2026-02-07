"""
schema_evolution.py - Schema migration and evolution handling.

Provides:
- Schema versioning
- Migration tracking
- Forward/backward compatibility
- Safe schema changes during sync
"""

import sqlite3
import json
import time
import hashlib
from dataclasses import dataclass
from typing import Any
from enum import Enum

from sqlite_sync.errors import SchemaError


class MigrationType(Enum):
    """Types of schema migrations."""
    ADD_TABLE = "add_table"
    DROP_TABLE = "drop_table"
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    RENAME_COLUMN = "rename_column"
    MODIFY_COLUMN = "modify_column"


@dataclass
class SchemaMigration:
    """Represents a single schema migration."""
    migration_id: bytes
    version_from: int
    version_to: int
    migration_type: MigrationType
    table_name: str
    column_name: str | None
    column_definition: str | None
    created_at: int
    applied_at: int | None = None


class SchemaManager:
    """
    Manages schema evolution across sync operations.
    
    Features:
    - Track schema versions
    - Record and replay migrations
    - Validate compatibility
    - Handle missing columns gracefully
    """
    
    # SQL to create schema tracking tables
    SCHEMA_TABLES_SQL = """
    CREATE TABLE IF NOT EXISTS sync_schema_versions (
        version INTEGER PRIMARY KEY,
        schema_hash TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        description TEXT
    );
    
    CREATE TABLE IF NOT EXISTS sync_schema_migrations (
        migration_id BLOB PRIMARY KEY,
        version_from INTEGER NOT NULL,
        version_to INTEGER NOT NULL,
        migration_type TEXT NOT NULL,
        table_name TEXT NOT NULL,
        column_name TEXT,
        column_definition TEXT,
        sql_up TEXT,
        sql_down TEXT,
        created_at INTEGER NOT NULL,
        applied_at INTEGER
    );
    
    CREATE INDEX IF NOT EXISTS idx_migrations_version 
    ON sync_schema_migrations(version_from, version_to);
    """
    
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._initialize_tables()
    
    def _initialize_tables(self) -> None:
        """Create schema tracking tables if not exist."""
        self._conn.executescript(self.SCHEMA_TABLES_SQL)
        self._conn.commit()
    
    def get_current_version(self) -> int:
        """Get current schema version."""
        cursor = self._conn.execute(
            "SELECT MAX(version) FROM sync_schema_versions"
        )
        result = cursor.fetchone()[0]
        return result if result is not None else 0
    
    def compute_schema_hash(self) -> str:
        """Compute hash of current schema for comparison."""
        cursor = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        schemas = [row[0] or "" for row in cursor.fetchall()]
        combined = "\n".join(schemas)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]
    
    def record_version(self, version: int, description: str = "") -> None:
        """Record a new schema version."""
        schema_hash = self.compute_schema_hash()
        now = int(time.time() * 1_000_000)
        
        self._conn.execute(
            """
            INSERT OR REPLACE INTO sync_schema_versions 
            (version, schema_hash, created_at, description)
            VALUES (?, ?, ?, ?)
            """,
            (version, schema_hash, now, description)
        )
        self._conn.commit()
    
    def add_column(
        self, 
        table_name: str, 
        column_name: str, 
        column_type: str,
        default_value: Any = None
    ) -> SchemaMigration:
        """
        Add a column to an existing table with sync safety.
        
        This is the safe way to evolve schema during sync.
        """
        current_version = self.get_current_version()
        new_version = current_version + 1
        
        # Build SQL
        default_clause = ""
        if default_value is not None:
            if isinstance(default_value, str):
                default_clause = f" DEFAULT '{default_value}'"
            else:
                default_clause = f" DEFAULT {default_value}"
        
        sql_up = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}{default_clause}"
        sql_down = f"-- Cannot drop column in SQLite: {column_name}"
        
        # Execute migration
        self._conn.execute(sql_up)
        
        # Record migration
        migration_id = self._generate_migration_id(table_name, column_name, new_version)
        now = int(time.time() * 1_000_000)
        
        self._conn.execute(
            """
            INSERT INTO sync_schema_migrations 
            (migration_id, version_from, version_to, migration_type, 
             table_name, column_name, column_definition, sql_up, sql_down, 
             created_at, applied_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                migration_id, current_version, new_version,
                MigrationType.ADD_COLUMN.value, table_name, column_name,
                f"{column_type}{default_clause}", sql_up, sql_down,
                now, now
            )
        )
        
        # Update version
        self.record_version(new_version, f"Added {column_name} to {table_name}")
        
        return SchemaMigration(
            migration_id=migration_id,
            version_from=current_version,
            version_to=new_version,
            migration_type=MigrationType.ADD_COLUMN,
            table_name=table_name,
            column_name=column_name,
            column_definition=f"{column_type}{default_clause}",
            created_at=now,
            applied_at=now
        )
    
    def get_pending_migrations(self, from_version: int) -> list[SchemaMigration]:
        """Get migrations needed to reach current version."""
        cursor = self._conn.execute(
            """
            SELECT migration_id, version_from, version_to, migration_type,
                   table_name, column_name, column_definition, created_at, applied_at
            FROM sync_schema_migrations
            WHERE version_from >= ?
            ORDER BY version_from ASC
            """,
            (from_version,)
        )
        
        migrations = []
        for row in cursor.fetchall():
            migrations.append(SchemaMigration(
                migration_id=row[0],
                version_from=row[1],
                version_to=row[2],
                migration_type=MigrationType(row[3]),
                table_name=row[4],
                column_name=row[5],
                column_definition=row[6],
                created_at=row[7],
                applied_at=row[8]
            ))
        
        return migrations
    
    def check_compatibility(self, remote_version: int) -> bool:
        """Check if remote version is compatible with local."""
        local_version = self.get_current_version()
        
        # Same version = compatible
        if remote_version == local_version:
            return True
        
        # Remote is older - we have migrations they need
        if remote_version < local_version:
            migrations = self.get_pending_migrations(remote_version)
            # All forward-compatible migrations are OK
            return all(
                m.migration_type in (MigrationType.ADD_COLUMN, MigrationType.ADD_TABLE)
                for m in migrations
            )
        
        # Remote is newer - we need their migrations
        # Can't apply without receiving them first
        return False
    
    def _generate_migration_id(self, table: str, col: str, version: int) -> bytes:
        """Generate unique migration ID."""
        content = f"{table}:{col}:{version}:{time.time()}"
        return hashlib.sha256(content.encode()).digest()[:16]
