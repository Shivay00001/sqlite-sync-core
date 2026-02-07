"""
Enterprise Module Verification Test

Tests all new enterprise features:
- Server with SQLite persistence
- Authentication (API keys, JWT)
- Metrics and observability
- Peer discovery
- Enhanced security
"""

import os
import sys
import tempfile
import time
import sqlite3

# Ensure we can import from source
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))


def test_metrics():
    """Test Prometheus-compatible metrics."""
    print("Testing metrics...", end=" ")
    
    from sqlite_sync.metrics import (
        get_registry, Counter, Gauge, Histogram,
        sync_operations_total, configure_logging
    )
    
    registry = get_registry()
    
    # Test counter
    counter = registry.counter("test_counter", "Test counter", labels=["status"])
    counter.inc(status="success")
    counter.inc(5, status="error")
    
    assert counter.get(status="success") == 1
    assert counter.get(status="error") == 5
    
    # Test gauge
    gauge = registry.gauge("test_gauge", "Test gauge")
    gauge.set(42.0)
    assert gauge.get() == 42.0
    gauge.inc(8)
    assert gauge.get() == 50.0
    
    # Test histogram
    histogram = registry.histogram("test_latency", "Test latency")
    histogram.observe(0.1)
    histogram.observe(0.5)
    histogram.observe(1.5)
    
    # Test export
    prometheus_output = registry.export_prometheus()
    assert "test_counter" in prometheus_output
    
    json_output = registry.export_json()
    assert "metrics" in json_output
    
    print("PASS")


def test_auth():
    """Test authentication module."""
    print("Testing auth...", end=" ")
    
    from sqlite_sync.server.auth import (
        AuthManager, DeviceRole, AuthResult, JWTManager
    )
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    try:
        conn = sqlite3.connect(db_path)
        auth = AuthManager(conn, require_approval=False)
        
        # Test API key creation
        credentials = auth.create_api_key("device-001", DeviceRole.READ_WRITE)
        assert credentials.api_key.startswith("ssk_")
        
        # Test API key authentication
        result = auth.authenticate_api_key(credentials.api_key)
        assert result.authenticated
        assert result.device_id == "device-001"
        assert result.role == DeviceRole.READ_WRITE
        
        # Test invalid API key
        bad_result = auth.authenticate_api_key("ssk_invalid_key")
        assert not bad_result.authenticated
        
        # Test JWT
        access, refresh = auth.create_token_pair("device-001", DeviceRole.READ_WRITE)
        jwt_result = auth.authenticate_jwt(access)
        assert jwt_result.authenticated
        
        # Test device registration
        reg_result = auth.request_device_registration("new-device", "New Device")
        assert reg_result["status"] == "approved"  # auto-approved
        
        conn.close()
    finally:
        try:
            os.unlink(db_path)
        except:
            pass  # Ignore Windows file locking
    
    print("PASS")


def test_security():
    """Test enhanced security module."""
    print("Testing security...", end=" ")
    
    from sqlite_sync.security import (
        SecurityManager, NonceStore, DeviceKeyStore
    )
    
    import shutil
    tmpdir = tempfile.mkdtemp()
    
    try:
        # Use separate database files for each test component
        security_nonce_db = os.path.join(tmpdir, "security_nonces.db")
        security_key_db = os.path.join(tmpdir, "security_keys.db")
        standalone_nonce_db = os.path.join(tmpdir, "standalone_nonces.db")
        standalone_key_db = os.path.join(tmpdir, "standalone_keys.db")
        
        device_id = os.urandom(16)
        manager = SecurityManager(
            device_id=device_id,
            nonce_db_path=security_nonce_db,
            key_db_path=security_key_db
        )
        
        # Test HMAC signing
        data = b"test bundle data"
        signed = manager.sign_bundle(data)
        assert signed.signature_type == "hmac"
        
        # Test verification (should pass)
        assert manager.verify_signature(signed)
        
        # Close manager before testing standalone stores
        manager.close()
        
        # Test persistent nonce store (separate database)
        nonce_store = NonceStore(standalone_nonce_db, ttl_seconds=3600)
        nonce = os.urandom(16)
        assert nonce_store.add_nonce(nonce, "device-001")  # First use OK
        assert not nonce_store.add_nonce(nonce, "device-001")  # Replay blocked
        
        stats = nonce_store.get_stats()
        assert stats["active_nonces"] >= 1
        nonce_store.close()
        
        # Test device key store (separate database)
        key_store = DeviceKeyStore(standalone_key_db)
        assert key_store.register_device("dev-001", b"fake_public_key")
        assert key_store.get_public_key("dev-001") == b"fake_public_key"
        assert key_store.is_device_trusted("dev-001")
        assert key_store.revoke_device("dev-001")
        assert not key_store.is_device_trusted("dev-001")
        key_store.close()
        
    finally:
        # Manual cleanup - ignore errors on Windows
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except:
            pass
    
    print("PASS")


def test_security_ed25519():
    """Test Ed25519 asymmetric signing (if available)."""
    print("Testing Ed25519 signing...", end=" ")
    
    try:
        from sqlite_sync.security import AsymmetricSigner, DeviceKeyStore
    except ImportError:
        print("SKIP (cryptography not installed)")
        return
    
    with tempfile.TemporaryDirectory() as tmpdir:
        key_db = os.path.join(tmpdir, "keys.db")
        key_store = DeviceKeyStore(key_db)
        signer = AsymmetricSigner(key_store)
        
        # Generate keypair
        identity = signer.generate_keypair()
        assert len(identity.public_key) == 32
        assert len(identity.private_key) == 32
        
        # Sign and verify
        data = b"test data for signing"
        signature = signer.sign_data(data, identity.private_key)
        assert signer.verify_signature(data, signature, identity.public_key)
        
        # Wrong signature should fail
        bad_sig = os.urandom(64)
        assert not signer.verify_signature(data, bad_sig, identity.public_key)
        
        # Sign bundle
        signed_bundle = signer.sign_bundle(b"bundle data", identity)
        assert signed_bundle.signature_type == "ed25519"
        
        key_store.close()
    
    print("PASS")


def test_peer_discovery():
    """Test peer discovery module."""
    print("Testing peer discovery...", end=" ")
    
    from sqlite_sync.network.peer_discovery import (
        UDPDiscovery, PeerManager, Peer, PeerStatus, DiscoveryConfig
    )
    
    # Test peer creation
    peer = Peer(
        device_id="abc123",
        device_name="Test Device",
        host="192.168.1.100",
        port=8080,
        transport="http"
    )
    assert peer.url == "http://192.168.1.100:8080"
    assert not peer.is_stale(timeout_seconds=60)
    
    # Test discovery config
    config = DiscoveryConfig(
        announce_interval=10.0,
        peer_timeout=30.0
    )
    assert config.max_peers == 100
    
    # Test peer manager
    manager = PeerManager()
    assert manager.get_best_peer() is None  # No peers
    
    # Test manual peer addition
    discovery = UDPDiscovery(
        device_id="test-device",
        device_name="Test Device",
        sync_port=8080
    )
    
    manual_peer = discovery.add_manual_peer(
        device_id="manual-001",
        host="10.0.0.1",
        port=9090,
        device_name="Manual Peer"
    )
    assert manual_peer.metadata.get("manual") is True
    
    peers = discovery.get_peers()
    assert len(peers) == 1
    
    print("PASS")


def test_server_persistence():
    """Test production server with SQLite persistence."""
    print("Testing server persistence...", end=" ")
    
    from sqlite_sync.server.http_server import SyncServer, ConnectionPool
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    try:
        server = SyncServer(db_path=db_path, requests_per_minute=100)
        
        # Test device registration
        result = server.register_device(
            device_id="device-001",
            device_name="Test Device",
            metadata={"version": "1.0"}
        )
        assert result["status"] == "ok"
        
        # Test handshake
        vc_result = server.handshake(
            device_id="device-001",
            local_vc={"device-001": 5}
        )
        assert vc_result["status"] == "ok"
        assert "vector_clock" in vc_result
        
        # Register second device
        server.register_device("device-002", "Second Device")
        
        # Test push operations
        ops = [
            {"op_id": "op1", "table": "users", "data": {"name": "Alice"}},
            {"op_id": "op2", "table": "users", "data": {"name": "Bob"}}
        ]
        push_result = server.push_operations("device-001", ops)
        assert push_result["accepted_count"] == 2
        
        # Test pull operations (device-002 should get the ops)
        pull_result = server.pull_operations("device-002")
        assert pull_result["count"] == 2
        
        # Second pull should be empty (already delivered)
        pull_result2 = server.pull_operations("device-002")
        assert pull_result2["count"] == 0
        
        # Test device status
        status = server.get_device_status("device-001")
        assert status is not None
        assert status["device_name"] == "Test Device"
        
        # Test cleanup
        cleanup_result = server.cleanup_expired()
        assert "expired_ops_deleted" in cleanup_result
        
        server.close()
        
    finally:
        try:
            os.unlink(db_path)
        except:
            pass  # Ignore Windows file locking
    
    print("PASS")


def test_rate_limiting():
    """Test rate limiting."""
    print("Testing rate limiting...", end=" ")
    
    from sqlite_sync.server.http_server import RateLimiter, ConnectionPool
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    try:
        pool = ConnectionPool(db_path)
        limiter = RateLimiter(pool, requests_per_minute=5)
        
        # First 5 requests should pass
        for i in range(5):
            allowed, retry = limiter.check_rate_limit("device-test")
            assert allowed, f"Request {i+1} should be allowed"
        
        # 6th request should be rate limited
        allowed, retry = limiter.check_rate_limit("device-test")
        assert not allowed, "6th request should be rate limited"
        assert retry > 0, "Retry-after should be positive"
        
        pool.close_all()
        
    finally:
        try:
            os.unlink(db_path)
        except:
            pass  # Ignore Windows file locking
    
    print("PASS")


def test_health_checks():
    """Test health check system."""
    print("Testing health checks...", end=" ")
    
    from sqlite_sync.metrics import HealthChecker, get_health_checker
    
    checker = HealthChecker()
    
    # Register a passing check
    checker.register_check("test_pass", lambda: {"healthy": True, "message": "OK"})
    
    # Register a failing check
    checker.register_check("test_fail", lambda: {"healthy": False, "message": "Error"})
    
    status = checker.check_all()
    assert not status.healthy  # One check failed
    assert status.checks["test_pass"]["healthy"]
    assert not status.checks["test_fail"]["healthy"]
    
    # Test disk check
    disk_result = checker.check_disk(".")
    assert "used_percent" in disk_result
    
    print("PASS")


def main():
    """Run all enterprise module tests."""
    print("=" * 60)
    print("Enterprise Module Verification Tests")
    print("=" * 60)
    print()
    
    tests = [
        test_metrics,
        test_auth,
        test_security,
        test_security_ed25519,
        test_peer_discovery,
        test_server_persistence,
        test_rate_limiting,
        test_health_checks,
    ]
    
    passed = 0
    failed = 0
    
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            print(f"FAIL: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
