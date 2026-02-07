# sqlite-sync-core

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![PyPI](https://img.shields.io/pypi/v/sqlite-sync-core.svg)](https://pypi.org/project/sqlite-sync-core/)
[![Status: Production Grade](https://img.shields.io/badge/status-production--grade-brightgreen.svg)](https://github.com/shivay00001/sqlite-sync-core)

**A production-grade, turn-key synchronization system for SQLite.**

`sqlite-sync-core` provides a powerful, local-first synchronization engine that works seamlessly across multi-peer networks. It handles the "hard parts" of sync (vector clocks, causality, delta bundles, and conflict resolution) while providing a simple, turn-key interface for developers.

---

## 🚀 Turn-Key Synchronization

You can launch a full synchronization node in one command. No infrastructure required.

### 16-Second Setup (CLI)

```bash
# Install the package
pip install sqlite-sync-core

# Start a node and sync the 'tasks' table automatically
sqlite-sync start --db app.db --name Device-A --tables tasks
```

### Automatic Multi-Peer Sync

Nodes automatically discover each other on the local network (P2P) and synchronize state in the background without any manual peer configuration.

---

## 🏗️ Enterprise Features

- **Multi-Peer Orchestration**: Automatically scales sync across N devices.
- **P2P Discovery**: Zero-config peer-to-peer discovery on LAN.
- **Automatic Resolution**: Configurable strategies like Last-Write-Wins and Field-Level Merge.
- **Schema Evolution**: Built-in migrations that sync across the network.
- **Transport Agnostic**: Works over HTTP, WebSockets, or file transfer.

---

## Technical Usage (Library)

### Initialize a Node in Code

```python
from sqlite_sync import SyncNode

node = SyncNode(
    db_path="app.db",
    device_name="MobileApp",
    sync_interval=10  # Sync every 10 seconds
)

await node.start()
node.enable_sync_for_table("users")
```

### Safe Schema Migrations

```bash
# Safely add a column that will sync to all other peers
sqlite-sync migrate --db app.db --table tasks --add-column priority --type INTEGER
```

---

## Core Invariants

| # | Invariant | Description |
|---|-----------|-------------|
| 1 | **Causal consistency** | Vector clocks ensure the correct order of operations. |
| 2 | **Deterministic Replay** | Identical sets of operations always result in identical state. |
| 3 | **Conflict Tolerance** | Detects and resolves conflicts explicitly and safely. |
| 4 | **Offline-First** | Entirely local-first design; works without cloud or internet. |

### 2. Generate a Delta Bundle

```python
# To be sent to Peer B
bundle_path = engine.generate_bundle(
    peer_device_id=peer_b_id,
    output_path="delta.db"
)
```

### 3. Import and Detect Conflicts

```python
# On Peer B
result = engine.import_bundle("delta.db")

print(f"Ops Applied: {result.applied_count}")
print(f"Conflicts Detected: {result.conflict_count}")
```

---

## Core Invariants

| # | Invariant | Description |
|---|-----------|-------------|
| 1 | **Append-only** | Operation history is immutable. |
| 2 | **Causal consistency** | Vector clocks ensure correct partial ordering. |
| 3 | **Deterministic** | Replay results are identical across all replicas. |
| 4 | **Idempotent** | Processing the same bundle twice is a no-op. |

---

## Architecture

```
┌─────────────────────────────────┐
│     Your Sync System / App      │
└───────────────┬─────────────────┘
                │ Uses
┌───────────────▼─────────────────┐
│       sqlite-sync-core          │
│  (Logging, Bundling, Clocks)    │
└───────────────┬─────────────────┘
                │ Persists to
┌───────────────▼─────────────────┐
│         SQLite Database         │
└─────────────────────────────────┘
```

---

## License

**AGPL-3.0** for Open Source.
Contact <shivaysinghrajput@proton.me> for commercial licensing.

---

**Built for developers who need a reliable sync foundation.**
