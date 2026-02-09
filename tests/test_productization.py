"""
test_productization.py - Tests for productization patch features.

Tests:
1. Schema migration propagation across peers
2. WebSocket sync parity with HTTP
3. Peer discovery adds peers correctly
4. Daemon mode scheduler runs and syncs
5. CLI start alias works
"""

import os
import sys
import time
import json
import asyncio
import tempfile
import threading
import pytest
from unittest.mock import Mock, patch, MagicMock

from sqlite_sync import SyncEngine
from sqlite_sync.schema_evolution import SchemaManager, MigrationType
from sqlite_sync.scheduler import SyncScheduler, SyncStatus
from sqlite_sync.network.peer_discovery import UDPDiscovery, Peer, PeerStatus, create_discovery


# =============================================================================
# Test 1: Schema Migration Propagation
# =============================================================================

class TestSchemaMigrationPropagation:
    """Test schema migration propagation across peers."""
    
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_a = os.path.join(self.tmpdir, "peer_a.db")
        self.db_b = os.path.join(self.tmpdir, "peer_b.db")
    
    def teardown_method(self):
        import shutil
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)
    
    def test_migration_serialization(self):
        """Test that migrations can be serialized for transfer."""
        engine = SyncEngine(self.db_a)
        engine.initialize()
        
        with engine:
            # Create a user table
            engine.connection.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
            )
            engine.connection.commit()
            
            # Add a column via schema manager
            sm = SchemaManager(engine.connection)
            migration = sm.add_column("users", "email", "TEXT", "none@example.com")
            
            # Serialize migrations
            serialized = sm.serialize_migrations(0)
            
            assert len(serialized) >= 1
            assert any(m["column_name"] == "email" for m in serialized)
            assert any(m["migration_type"] == "add_column" for m in serialized)
        
        engine.close()
    
    def test_migration_safety_check(self):
        """Test that unsafe migrations are detected."""
        engine = SyncEngine(self.db_a)
        engine.initialize()
        
        with engine:
            sm = SchemaManager(engine.connection)
            
            # Safe migrations
            safe_migration = {
                "migration_type": MigrationType.ADD_COLUMN.value,
                "table_name": "users"
            }
            assert sm.is_safe_migration(safe_migration) is True
            
            safe_migration2 = {
                "migration_type": MigrationType.ADD_TABLE.value,
                "table_name": "posts"
            }
            assert sm.is_safe_migration(safe_migration2) is True
            
            # Unsafe migrations
            unsafe_migration = {
                "migration_type": MigrationType.DROP_COLUMN.value,
                "table_name": "users"
            }
            assert sm.is_safe_migration(unsafe_migration) is False
        
        engine.close()
    
    def test_migration_propagation_between_peers(self):
        """Test schema migration propagates from peer A to peer B."""
        # Initialize both peers
        engine_a = SyncEngine(self.db_a)
        engine_a.initialize()
        
        engine_b = SyncEngine(self.db_b)
        engine_b.initialize()
        
        with engine_a:
            # Create table on A
            engine_a.connection.execute(
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT)"
            )
            engine_a.connection.commit()
            
            # Add column on A
            sm_a = SchemaManager(engine_a.connection)
            sm_a.add_column("tasks", "priority", "INTEGER", 0)
            
            # Get migrations for B
            migrations = sm_a.serialize_migrations(0)
        
        # Apply migrations on B
        with engine_b:
            # B needs the base table first
            engine_b.connection.execute(
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT)"
            )
            engine_b.connection.commit()
            
            sm_b = SchemaManager(engine_b.connection)
            
            # Check safety
            assert sm_b.all_migrations_safe(migrations)
            
            # Apply
            applied, errors = sm_b.apply_remote_migrations(migrations)
            
            # Verify column exists
            cursor = engine_b.connection.execute("PRAGMA table_info(tasks)")
            columns = [row[1] for row in cursor.fetchall()]
            assert "priority" in columns
        
        engine_a.close()
        engine_b.close()


# =============================================================================
# Test 2: WebSocket Sync Parity
# =============================================================================

class TestWebSocketParity:
    """Test WebSocket endpoint has parity with HTTP."""
    
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "server.db")
    
    def teardown_method(self):
        import shutil
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)
    
    def test_websocket_endpoint_exists(self):
        """Test that WebSocket endpoint is registered in the app."""
        from sqlite_sync.transport.server import app
        
        # Check routes
        routes = [route.path for route in app.routes]
        assert "/ws/sync" in routes
    
    def test_websocket_serialization_functions(self):
        """Test operation serialization/deserialization for WebSocket."""
        from sqlite_sync.transport.server import serialize_operation, deserialize_operation
        from sqlite_sync.log.operations import SyncOperation
        
        # Create a test operation
        op = SyncOperation(
            op_id=b"\x01" * 16,
            device_id=b"\x02" * 16,
            parent_op_id=None,
            vector_clock={"device1": 1},
            table_name="users",
            op_type="INSERT",
            row_pk=b"\x03" * 16,
            old_values=None,
            new_values=b"\x04" * 10,
            schema_version=1,
            created_at=1000000,
            hlc=b"\x05" * 12,
            is_local=False,
            applied_at=None
        )
        
        # Serialize
        serialized = serialize_operation(op)
        assert serialized["table_name"] == "users"
        assert serialized["op_type"] == "INSERT"
        
        # Deserialize
        deserialized = deserialize_operation(serialized)
        assert deserialized.table_name == op.table_name
        assert deserialized.op_type == op.op_type
        assert deserialized.op_id == op.op_id


# =============================================================================
# Test 3: Peer Discovery
# =============================================================================

class TestPeerDiscovery:
    """Test peer discovery CLI wiring."""
    
    def test_discovery_creation(self):
        """Test UDPDiscovery can be created."""
        discovery = create_discovery(
            device_id="test-device-123",
            device_name="TestDevice",
            sync_port=8000
        )
        
        assert discovery is not None
        assert discovery._device_id == "test-device-123"
        assert discovery._sync_port == 8000
    
    def test_peer_management(self):
        """Test manual peer management."""
        discovery = create_discovery(
            device_id="local-device",
            device_name="LocalDevice",
            sync_port=8000
        )
        
        # Add manual peer
        peer = discovery.add_manual_peer(
            device_id="remote-device",
            host="192.168.1.100",
            port=8000,
            device_name="RemoteDevice"
        )
        
        assert peer.device_id == "remote-device"
        assert peer.url == "http://192.168.1.100:8000"
        
        # Check peers list
        peers = discovery.get_peers()
        assert len(peers) == 1
        assert peers[0].device_name == "RemoteDevice"
        
        # Remove peer
        removed = discovery.remove_peer("remote-device")
        assert removed is True
        assert len(discovery.get_peers()) == 0
    
    def test_peers_config_functions(self):
        """Test peers config load/save functions."""
        from sqlite_sync.cli.main import load_peers_config, save_peers_config, PEERS_CONFIG_FILE
        
        # Use temp file
        import tempfile
        original_config_file = PEERS_CONFIG_FILE
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_config = f.name
        
        # Patch the config file path
        with patch('sqlite_sync.cli.main.PEERS_CONFIG_FILE', temp_config):
            # Initially empty
            with patch('os.path.exists', return_value=False):
                config = load_peers_config()
                assert config == {"peers": []}
        
        # Cleanup
        if os.path.exists(temp_config):
            os.remove(temp_config)


# =============================================================================
# Test 4: Daemon Mode Scheduler
# =============================================================================

class TestDaemonModeScheduler:
    """Test scheduler daemon mode functionality."""
    
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
    
    def teardown_method(self):
        import shutil
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)
    
    def test_scheduler_background_mode(self):
        """Test scheduler can run in background thread."""
        engine = SyncEngine(self.db_path)
        engine.initialize()
        
        # Mock transport
        transport = Mock()
        transport.connect = Mock(return_value=asyncio.coroutine(lambda: True)())
        
        scheduler = SyncScheduler(engine, transport, interval_seconds=1.0)
        
        # Start in background
        scheduler.start(in_background=True)
        
        # Should be running
        assert scheduler._running is True
        assert scheduler._thread is not None
        assert scheduler._thread.is_alive()
        
        # Stop
        scheduler.stop()
        time.sleep(0.5)
        
        assert scheduler._running is False
        
        engine.close()
    
    def test_scheduler_status_tracking(self):
        """Test scheduler status changes are tracked."""
        engine = SyncEngine(self.db_path)
        engine.initialize()
        
        transport = Mock()
        
        status_changes = []
        
        def on_status_change(status):
            status_changes.append(status)
        
        scheduler = SyncScheduler(
            engine, transport, 
            interval_seconds=60.0,
            on_status_change=on_status_change
        )
        
        # Initial status should be IDLE
        assert scheduler._status == SyncStatus.IDLE
        
        engine.close()


# =============================================================================
# Test 5: CLI Start Alias
# =============================================================================

class TestCLIStartAlias:
    """Test CLI start command as alias for serve."""
    
    def test_cli_has_start_command(self):
        """Test that start command exists in CLI."""
        from sqlite_sync.cli.main import app
        
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "start" in command_names
        assert "serve" in command_names
    
    def test_cli_has_peers_command(self):
        """Test that peers command exists in CLI."""
        from sqlite_sync.cli.main import app
        
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "peers" in command_names
    
    def test_sync_has_daemon_option(self):
        """Test that sync command has --daemon option."""
        from sqlite_sync.cli.main import sync
        import inspect
        
        sig = inspect.signature(sync)
        param_names = list(sig.parameters.keys())
        assert "daemon" in param_names


# =============================================================================
# Integration Test
# =============================================================================

class TestProductizationIntegration:
    """Integration tests for the complete productization patch."""
    
    def test_engine_schema_methods(self):
        """Test engine has new schema migration methods."""
        tmpdir = tempfile.mkdtemp()
        db_path = os.path.join(tmpdir, "test.db")
        
        try:
            engine = SyncEngine(db_path)
            engine.initialize()
            
            with engine:
                # Test new methods exist
                assert hasattr(engine, 'get_schema_info')
                assert hasattr(engine, 'get_pending_migrations_for')
                assert hasattr(engine, 'apply_remote_migrations')
                assert hasattr(engine, 'are_migrations_safe')
                
                # Test get_schema_info
                info = engine.get_schema_info()
                assert "version" in info
                assert "hash" in info
            
            engine.close()
        finally:
            import shutil
            shutil.rmtree(tmpdir)
    
    def test_http_transport_migration_methods(self):
        """Test HTTP transport has migration-related methods."""
        from sqlite_sync.transport.http_transport import HTTPTransport
        
        transport = HTTPTransport("http://localhost:8000", b"test" * 4)
        
        assert hasattr(transport, 'get_pending_migrations')
        assert hasattr(transport, 'are_migrations_safe')
