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
__version__ = "0.7.0"
__all__ = [
    # Core
    "SyncEngine",
    # Errors
    "SyncError",
    "SchemaError",
    "BundleError",
    "ConflictError",
    "InvariantViolationError",
]

# Enterprise: SyncNode (full orchestration)
try:
    from sqlite_sync.ext.node import SyncNode
    __all__.append("SyncNode")
except ImportError:
    pass

# Enterprise: P2P Discovery
try:
    from sqlite_sync.network.peer_discovery import (
        UDPDiscovery,
        PeerManager,
        Peer,
        PeerStatus,
        DiscoveryConfig,
        create_discovery,
    )
    __all__.extend([
        "UDPDiscovery",
        "PeerManager", 
        "Peer",
        "PeerStatus",
        "DiscoveryConfig",
        "create_discovery",
    ])
except ImportError:
    pass

# Enterprise: Conflict Resolution
try:
    from sqlite_sync.resolution import (
        ResolutionStrategy,
        ConflictResolver,
        ConflictContext,
        ResolutionResult,
        get_resolver,
        LastWriteWinsResolver,
        FieldMergeResolver,
    )
    __all__.extend([
        "ResolutionStrategy",
        "ConflictResolver",
        "ConflictContext",
        "ResolutionResult",
        "get_resolver",
        "LastWriteWinsResolver",
        "FieldMergeResolver",
    ])
except ImportError:
    pass

# Enterprise: Schema Evolution
try:
    from sqlite_sync.schema_evolution import SchemaManager, SchemaMigration
    __all__.extend(["SchemaManager", "SchemaMigration"])
except ImportError:
    pass

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
