# sqlite-sync-core

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![PyPI](https://img.shields.io/pypi/v/sqlite-sync-core.svg)](https://pypi.org/project/sqlite-sync-core/)
[![Status: Production Ready](https://img.shields.io/badge/status-production--ready-brightgreen.svg)](https://github.com/shivay00001/sqlite-sync-core)

**A deterministic, infrastructure-free, local-first SQLite synchronization engine designed for offline-critical and privacy-sensitive systems.**

Built by [VisionQuantech](https://github.com/shivay00001), India 🇮🇳

---

## Why sqlite-sync-core?

| Feature | sqlite-sync-core | Firebase | Supabase |
|---------|------------------|----------|----------|
| **No server required** | ✅ | ❌ | ❌ |
| **Works offline** | ✅ | ⚠️ Limited | ⚠️ Limited |
| **Transport agnostic** | ✅ USB, Email, HTTP, WS | Proprietary | WebSocket only |
| **Explicit conflicts** | ✅ Never auto-overwrites | ❌ LWW | ❌ LWW |
| **Deterministic replay** | ✅ Git-like correctness | ❌ | ❌ |
| **No telemetry** | ✅ | ❌ | ❌ |
| **Self-hosted** | ✅ | ❌ | ✅ |

---

## Installation

```bash
# Basic installation
pip install sqlite-sync-core

# With HTTP server support
pip install sqlite-sync-core[server]

# With encryption support
pip install sqlite-sync-core[crypto]

# Everything
pip install sqlite-sync-core[all]
```

---

## Quick Start

### Initialize a Sync-Enabled Database

```python
from sqlite_sync import SyncEngine

engine = SyncEngine("my_app.db")
device_id = engine.initialize()

# Create your table
engine.connection.execute("""
    CREATE TABLE todos (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        done INTEGER DEFAULT 0
    )
""")

# Enable sync for this table
engine.enable_sync_for_table("todos")

# All changes are now automatically captured!
engine.connection.execute("INSERT INTO todos (title) VALUES ('Buy milk')")
engine.connection.commit()
```

### Sync via File Transfer (USB, Email, Cloud Drive)

```python
# Device A: Generate bundle
bundle_path = engine_a.generate_bundle(
    peer_device_id=device_b_id,
    output_path="sync_bundle.db"
)
# Send bundle_path via any method: USB, email, Dropbox, etc.

# Device B: Import bundle
result = engine_b.import_bundle("sync_bundle.db")
print(f"Applied: {result.applied_count}, Conflicts: {result.conflict_count}")
```

### Sync via HTTP (Real-time)

```python
from sqlite_sync.transport import HTTPTransport
from sqlite_sync.sync_loop import SyncLoop, SyncLoopConfig

# Create transport
transport = HTTPTransport(
    base_url="http://localhost:8080",
    device_id=engine.device_id
)

# Create background sync loop
sync_loop = SyncLoop(
    engine=engine,
    transport=transport,
    config=SyncLoopConfig(interval_seconds=30)
)

await sync_loop.start()  # Syncs automatically in background
```

### Sync via WebSocket (Real-time Bidirectional)

```python
from sqlite_sync.transport import WebSocketTransport

transport = WebSocketTransport(
    url="ws://localhost:8765",
    device_id=engine.device_id,
    on_operation_received=lambda op: print(f"Received: {op.op_type}")
)

await transport.connect()
await transport.send_operations(engine.get_new_operations())
```

---

## Conflict Resolution

sqlite-sync-core detects conflicts but gives YOU control over resolution.

### Built-in Strategies

```python
from sqlite_sync.resolution import ResolutionStrategy, get_resolver

# Last-Write-Wins (simple but may lose data)
resolver = get_resolver(ResolutionStrategy.LAST_WRITE_WINS)

# Field-level merge (preserves non-conflicting fields)
resolver = get_resolver(ResolutionStrategy.FIELD_MERGE, prefer_local=True)

# Manual (keep for user review)
resolver = get_resolver(ResolutionStrategy.MANUAL)

# Custom (your business logic)
def my_resolver(context):
    # Your logic here
    return ResolutionResult(resolved=True, winning_op=context.local_op, ...)

resolver = get_resolver(ResolutionStrategy.CUSTOM, resolver_fn=my_resolver)
```

### Handle Conflicts

```python
conflicts = engine.get_unresolved_conflicts()
for conflict in conflicts:
    print(f"Conflict on {conflict.table_name}, row {conflict.row_pk.hex()}")
    print(f"Local op: {conflict.local_op_id.hex()}")
    print(f"Remote op: {conflict.remote_op_id.hex()}")
```

---

## Advanced Features

### Log Compaction

```python
from sqlite_sync.log_compaction import LogCompactor

compactor = LogCompactor(engine.connection)

# Create snapshot for new devices
snapshot = compactor.create_snapshot()

# Prune old operations
compactor.prune_acknowledged_ops(safe_op_id)

# Full compaction
result = compactor.compact_log()
print(f"Removed {result.ops_removed} operations")
```

### Schema Evolution

```python
from sqlite_sync.schema_evolution import SchemaManager

schema = SchemaManager(engine.connection)

# Safe column addition (syncs across devices)
migration = schema.add_column(
    table_name="todos",
    column_name="priority",
    column_type="INTEGER",
    default_value=1
)

# Check compatibility
if schema.check_compatibility(remote_version=1):
    print("Compatible!")
```

### Security

```python
from sqlite_sync.security import SecurityManager

security = SecurityManager(
    device_id=engine.device_id,
    signing_key=my_secret_key
)

# Sign bundles
signed = security.sign_bundle(bundle_data)

# Encrypt bundles (requires cryptography package)
encrypted = security.encrypt_bundle(bundle_data, password="secret")
decrypted = security.decrypt_bundle(encrypted, password="secret")
```

### Crash Safety

```python
from sqlite_sync.crash_safety import CrashSafeExecutor

executor = CrashSafeExecutor(engine.connection)

# Resume after crash
checkpoint = executor.get_incomplete_checkpoint()
if checkpoint:
    print(f"Resuming from {checkpoint.last_applied_op_id.hex()}")

# Atomic operations
with executor.atomic_operation():
    engine.apply_operation(op1)
    engine.apply_operation(op2)  # Both or neither
```

---

## Run the Sync Server

```bash
# Start HTTP sync server
python -m sqlite_sync.server.http_server

# Or in code
from sqlite_sync.server import run_server
run_server(host="0.0.0.0", port=8080)
```

---

## Examples

See the `examples/` directory:

| Example | Description |
|---------|-------------|
| `basic_usage.py` | Simple CLI sync demo |
| `desktop_demo.py` | Local sync between databases |
| `http_sync_demo.py` | Client/server network sync |
| `network_sync.py` | WebSocket real-time sync |

---

## Core Invariants

| # | Invariant | Description |
|---|-----------|-------------|
| 1 | **Append-only** | Operations never modified |
| 2 | **Causal consistency** | Vector clocks ensure ordering |
| 3 | **Deterministic** | Same ops = same result everywhere |
| 4 | **Explicit conflicts** | Never silently overwrites |
| 5 | **Idempotent** | Import same bundle N times = same result |
| 6 | **Transport agnostic** | Bundles work anywhere |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Your Application                      │
├─────────────────────────────────────────────────────────┤
│                     SyncEngine                           │
│  ┌─────────┐ ┌─────────────┐ ┌──────────────────────┐   │
│  │ Capture │ │ Resolution  │ │   Schema Evolution   │   │
│  └─────────┘ └─────────────┘ └──────────────────────┘   │
│  ┌─────────┐ ┌─────────────┐ ┌──────────────────────┐   │
│  │ Bundle  │ │ Compaction  │ │      Security        │   │
│  └─────────┘ └─────────────┘ └──────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│              Transport Layer (Pluggable)                 │
│  ┌──────┐  ┌───────────┐  ┌───────┐  ┌───────────────┐  │
│  │ HTTP │  │ WebSocket │  │ File  │  │ Custom/P2P    │  │
│  └──────┘  └───────────┘  └───────┘  └───────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## Use Cases

- **Offline-first mobile apps** - Works without internet
- **Air-gapped systems** - Defense, government, NGOs
- **Privacy-sensitive applications** - Medical, legal, finance
- **Field operations** - Works with USB/SD card transfer
- **Embedded systems** - IoT with intermittent connectivity
- **Multi-device personal apps** - Notes, todos, journals

---

## License

**Dual License:**

| Use Case | License | Cost |
|----------|---------|------|
| Personal/Open Source | AGPL-3.0 | **Free** |
| Commercial/Proprietary | Commercial | **Paid** |

Contact for commercial licensing:

- <shivaysinghrajput@proton.me>
- <shivaysinghrajput@outlook.com>

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `pytest tests/ -v`
4. Submit a pull request

---

**Built with ❤️ for offline-first applications**
