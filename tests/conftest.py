"""
conftest.py - pytest fixtures for sqlite_sync tests.
"""

import os
import tempfile
import pytest

from sqlite_sync import SyncEngine


@pytest.fixture(autouse=True)
def _no_proxy_env(monkeypatch):
    """Neutralize ambient proxy env vars.

    httpx.AsyncClient() reads proxy env vars at construction; in sandboxes
    where the proxy URL is unparseable by the installed httpx (e.g. special
    characters in the embedded credentials), client construction raises
    httpx.InvalidURL. The sync tests talk to localhost/mocks and never need
    an egress proxy, so strip these vars for the whole suite.
    """
    for var in (
        "http_proxy", "https_proxy", "all_proxy", "no_proxy",
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test databases."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def engine(temp_dir):
    """Create an initialized SyncEngine in a temp directory."""
    db_path = os.path.join(temp_dir, "test.db")
    engine = SyncEngine(db_path)
    engine.initialize()
    yield engine
    engine.close()


@pytest.fixture
def two_engines(temp_dir):
    """Create two initialized SyncEngines for sync testing."""
    db_a_path = os.path.join(temp_dir, "device_a.db")
    db_b_path = os.path.join(temp_dir, "device_b.db")
    
    engine_a = SyncEngine(db_a_path)
    engine_a.initialize()
    
    engine_b = SyncEngine(db_b_path)
    engine_b.initialize()
    
    yield engine_a, engine_b
    
    engine_a.close()
    engine_b.close()
