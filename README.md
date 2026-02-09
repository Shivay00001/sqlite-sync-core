# sqlite-sync-core

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![PyPI](https://img.shields.io/pypi/v/sqlite-sync-core.svg)](https://pypi.org/project/sqlite-sync-core/)
[![Status: Production Ready](https://img.shields.io/badge/status-production--ready-brightgreen.svg)](https://github.com/shivay00001/sqlite-sync-core)

**A production-ready SQLite synchronization library for distributed systems.**

`sqlite-sync-core` provides a powerful, local-first synchronization engine that works seamlessly across multi-peer networks. It handles the "hard parts" of sync (HLC clocks, causality, delta bundles, and conflict resolution) while providing a clean API for developers.

---

## 🚀 Quick Start

### Installation

```bash
pip install sqlite-sync-core
```

### CLI Usage

```bash
# Initialize a database for sync
sqlite-sync init myapp.db --table tasks

# Start a sync server
sqlite-sync start myapp.db --port 8000

# Sync with a remote peer
sqlite-sync sync myapp.db http://peer:8000

# Run as daemon (background sync)
sqlite-sync sync myapp.db http://peer:8000 --daemon

# Discover peers on LAN
sqlite-sync peers --discover --auto-add

# Add a column migration (syncs to peers)
sqlite-sync migrate myapp.db --table tasks --add-column priority --type INTEGER
```

---

## 🏗️ Features

- **Hybrid Logical Clocks**: Causal ordering with wall-clock correlation
- **Conflict Resolution**: LWW, Field-Level Merge, or Custom strategies
- **Multi-Transport**: HTTP and WebSocket sync
- **P2P Discovery**: Zero-config peer discovery on LAN
- **Schema Evolution**: Migrations that sync across the network
- **Background Sync**: Scheduler with daemon mode

---

## Library Usage

### Initialize and Sync

```python
from sqlite_sync import SyncEngine
from sqlite_sync.transport.http_transport import HTTPTransport
from sqlite_sync.scheduler import SyncScheduler

# Initialize the engine
engine = SyncEngine("app.db")
engine.initialize()

# Enable sync for a table
with engine:
    engine.enable_sync_for_table("tasks")

# Set up transport and scheduler
transport = HTTPTransport("http://peer:8000", engine.device_id)
scheduler = SyncScheduler(engine, transport, interval_seconds=10)

# Start background sync
scheduler.start(in_background=True)
```

### Schema Migrations

```python
from sqlite_sync.schema_evolution import SchemaManager

with engine:
    sm = SchemaManager(engine.connection)
    
    # Add a column - will sync to all peers
    migration = sm.add_column("tasks", "priority", "INTEGER", default_value="0")
    print(f"Migration created: {migration.migration_id.hex()}")
```

### Generate and Import Bundles

```python
# Generate a delta bundle for a peer
bundle_path = engine.generate_bundle(
    peer_device_id=peer_b_id,
    output_path="delta.db"
)

# Import bundle on receiving peer
result = engine.import_bundle("delta.db")
print(f"Applied: {result.applied_count}, Conflicts: {result.conflict_count}")
```

---

## Core Invariants

| # | Invariant | Description |
|---|-----------|-------------|
| 1 | **Causal Consistency** | HLC ensures correct partial ordering of operations |
| 2 | **Deterministic Replay** | Same operations = same state across all replicas |
| 3 | **Conflict Tolerance** | Detects and resolves conflicts explicitly |
| 4 | **Offline-First** | Works entirely without cloud or internet |

---

## Architecture

```
┌─────────────────────────────────┐
│     Your Application            │
└───────────────┬─────────────────┘
                │ Uses
┌───────────────▼─────────────────┐
│       sqlite-sync-core          │
│  (Engine, Transport, Scheduler) │
└───────────────┬─────────────────┘
                │ Persists to
┌───────────────▼─────────────────┐
│         SQLite Database         │
└─────────────────────────────────┘
```

---

## CLI Commands

| Command | Description |
|---------|-------------|
| `init` | Initialize database for sync |
| `start` / `serve` | Start HTTP sync server |
| `sync` | Run sync (one-off or daemon) |
| `status` | Show sync status and conflicts |
| `resolve` | Resolve conflicts interactively |
| `migrate` | Manage schema migrations |
| `peers` | Discover and manage peers |
| `snapshot` | Create database snapshot |

---

## License

**AGPL-3.0** for Open Source.
Contact <shivaysinghrajput@proton.me> for commercial licensing.

---

**Built for developers who need reliable sync infrastructure.**
