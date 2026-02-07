"""
sqlite_sync - Universal SQLite Synchronization Core

A dependency-grade, local-first, offline-first SQLite synchronization primitive.
Enterprise-ready with authentication, metrics, peer discovery, and enhanced security.
"""

from sqlite_sync.engine import SyncEngine
from sqlite_sync.errors import (
    SyncError,
    SchemaError,
    BundleError,
    ConflictError,
    InvariantViolationError,
)

# Core
__version__ = "0.2.0"
__all__ = [
    # Core
    "SyncEngine",
    # Errors
    "SyncError",
    "SchemaError",
    "BundleError",
    "ConflictError",
    "InvariantViolationError",
    "SyncNode",
]

from sqlite_sync.node import SyncNode

# Optional enterprise imports (fail gracefully if deps missing)
try:
    from sqlite_sync.metrics import (
        get_registry,
        sync_operations_total,
        sync_conflicts_total,
        sync_latency_seconds,
        configure_logging,
        SyncLogger,
        HealthChecker,
        get_health_checker,
    )
    __all__.extend([
        "get_registry",
        "sync_operations_total", 
        "sync_conflicts_total",
        "sync_latency_seconds",
        "configure_logging",
        "SyncLogger",
        "HealthChecker",
        "get_health_checker",
    ])
except ImportError:
    pass

try:
    from sqlite_sync.security import SecurityManager, DeviceIdentity
    __all__.extend(["SecurityManager", "DeviceIdentity"])
except ImportError:
    pass
