# API Reference

Complete API documentation for sqlite-sync-core.

## Core Classes

### SyncEngine

The main interface for sync operations.

```python
from sqlite_sync import SyncEngine

engine = SyncEngine("database.db")
```

#### Methods

| Method | Description |
|--------|-------------|
| `initialize()` | Initialize sync tables, returns device ID |
| `enable_sync_for_table(name)` | Enable change capture for table |
| `generate_bundle(peer_id, path)` | Create bundle for peer |
| `import_bundle(path)` | Import bundle, returns ImportResult |
| `apply_operation(op)` | Apply single operation (streaming sync) |
| `get_new_operations(since_vc)` | Get ops newer than vector clock |
| `get_unresolved_conflicts()` | List pending conflicts |
| `get_vector_clock()` | Get current vector clock |
| `close()` | Close database connection |

### ImportResult

```python
@dataclass
class ImportResult:
    bundle_id: bytes
    source_device_id: bytes
    total_operations: int
    applied_count: int
    conflict_count: int
    duplicate_count: int
    skipped: bool
```

### SyncOperation

```python
@dataclass
class SyncOperation:
    op_id: bytes           # 16-byte UUID
    device_id: bytes       # Origin device
    parent_op_id: bytes    # Previous op (causal chain)
    vector_clock: str      # JSON vector clock
    table_name: str
    op_type: str           # INSERT, UPDATE, DELETE
    row_pk: bytes          # MessagePack encoded PK
    old_values: bytes      # For UPDATE/DELETE
    new_values: bytes      # For INSERT/UPDATE
    schema_version: int
    created_at: int        # Unix microseconds
    is_local: bool
    applied_at: int | None
```

---

## Transport Layer

### TransportAdapter (Abstract)

Base class for transport implementations.

```python
from sqlite_sync.transport import TransportAdapter

class MyTransport(TransportAdapter):
    async def connect(self) -> bool: ...
    async def disconnect(self) -> None: ...
    async def send_operations(self, ops) -> int: ...
    async def receive_operations(self) -> list: ...
    async def exchange_vector_clock(self, vc) -> dict: ...
```

### HTTPTransport

```python
from sqlite_sync.transport import HTTPTransport

transport = HTTPTransport(
    base_url="http://localhost:8080",
    device_id=device_id,
    auth_token="optional-token"
)
```

### WebSocketTransport

```python
from sqlite_sync.transport import WebSocketTransport

transport = WebSocketTransport(
    url="ws://localhost:8765",
    device_id=device_id,
    on_operation_received=callback
)
```

---

## Conflict Resolution

### Strategies

```python
from sqlite_sync.resolution import (
    ResolutionStrategy,
    get_resolver,
    ConflictContext,
    ResolutionResult
)

# Available strategies
ResolutionStrategy.LAST_WRITE_WINS
ResolutionStrategy.FIELD_MERGE
ResolutionStrategy.CUSTOM
ResolutionStrategy.MANUAL
```

### ConflictContext

```python
@dataclass
class ConflictContext:
    table_name: str
    row_pk: bytes
    local_op: SyncOperation
    remote_op: SyncOperation
    local_values: dict
    remote_values: dict
```

### ResolutionResult

```python
@dataclass
class ResolutionResult:
    resolved: bool
    winning_op: SyncOperation | None
    merged_values: dict | None
    reason: str
```

---

## Sync Loop

### SyncLoop

```python
from sqlite_sync.sync_loop import SyncLoop, SyncLoopConfig, SyncStatus

config = SyncLoopConfig(
    interval_seconds=30,
    retry_base_seconds=5,
    max_retries=10,
    resolution_strategy=ResolutionStrategy.LAST_WRITE_WINS
)

loop = SyncLoop(engine, transport, config)
await loop.start()
await loop.sync_now()
await loop.stop()
```

### SyncStatus

```python
class SyncStatus(Enum):
    IDLE = "idle"
    SYNCING = "syncing"
    WAITING_RETRY = "waiting_retry"
    ERROR = "error"
    STOPPED = "stopped"
```

---

## Schema Evolution

### SchemaManager

```python
from sqlite_sync.schema_evolution import SchemaManager

schema = SchemaManager(connection)
version = schema.get_current_version()
migration = schema.add_column("table", "column", "TEXT", default="")
compatible = schema.check_compatibility(remote_version)
```

---

## Log Compaction

### LogCompactor

```python
from sqlite_sync.log_compaction import LogCompactor

compactor = LogCompactor(connection)
stats = compactor.get_log_stats()
snapshot = compactor.create_snapshot()
result = compactor.compact_log()
```

---

## Security

### SecurityManager

```python
from sqlite_sync.security import SecurityManager

security = SecurityManager(device_id, signing_key)
signed = security.sign_bundle(data)
verified = security.verify_signature(signed, key)
encrypted = security.encrypt_bundle(data, password)
decrypted = security.decrypt_bundle(encrypted, password)
```

---

## Crash Safety

### CrashSafeExecutor

```python
from sqlite_sync.crash_safety import CrashSafeExecutor

executor = CrashSafeExecutor(connection)
checkpoint = executor.create_checkpoint(vector_clock)

with executor.atomic_operation():
    # All operations atomic
    pass

incomplete = executor.get_incomplete_checkpoint()
```

---

## Errors

```python
from sqlite_sync.errors import (
    SyncError,       # Base exception
    SchemaError,     # Schema issues
    BundleError,     # Bundle validation failed
    DatabaseError,   # SQLite errors
    ValidationError  # Data validation errors
)
```
