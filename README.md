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
sqlite-sync start app.db --name Device-A --table tasks
```

### Automatic Multi-Peer Sync

Nodes automatically discover each other on the local network (P2P) and synchronize state in the background without any manual peer configuration.

```bash
# On Device A
sqlite-sync start app.db --name Device-A --table tasks --port 8000

# On Device B (auto-discovers Device A)
sqlite-sync start app.db --name Device-B --table tasks --port 8001
```

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
sqlite-sync migrate app.db --table tasks --add-column priority --type INTEGER
```

---

## Core Invariants

| # | Invariant | Description |
|---|-----------|-------------|
| 1 | **Causal consistency** | Vector clocks ensure the correct order of operations. |
| 2 | **Deterministic Replay** | Identical sets of operations always result in identical state. |
| 3 | **Conflict Tolerance** | Detects and resolves conflicts explicitly and safely. |
| 4 | **Offline-First** | Entirely local-first design; works without cloud or internet. |

---

## CLI Commands

| Command | Description |
|---------|-------------|
| `start` | Start full sync node with P2P discovery |
| `init` | Initialize database for sync |
| `serve` | Start HTTP sync server (simple mode) |
| `sync` | Run sync with a specific peer |
| `status` | Show sync status and conflicts |
| `resolve` | Resolve conflicts interactively |
| `migrate` | Manage schema migrations |
| `peers` | Discover and manage peers |
| `snapshot` | Create database snapshot |

### Start Command Options

```bash
sqlite-sync start <db_path> [OPTIONS]

Options:
  --name, -n        Device name for network discovery
  --table, -t       Tables to enable sync for (can repeat)
  --port, -p        Port to bind to (default: 8000)
  --interval, -i    Sync interval in seconds (default: 30)
  --auto-discover   Enable P2P discovery (default: enabled)
  --daemon, -d      Run as background daemon
```

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
**Commercial License** available for enterprise use.

Contact <shivaysinghrajput@proton.me> for commercial licensing.

---

**Built for developers who need a reliable sync foundation.**
