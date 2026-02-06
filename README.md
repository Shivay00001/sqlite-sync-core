# Universal SQLite Synchronization Core

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Status: Production Ready](https://img.shields.io/badge/status-production--ready-brightgreen.svg)]()

**A dependency-grade, local-first, offline-first SQLite synchronization primitive.**

Captures database changes as structured operations, packages them into self-contained bundles, and applies them deterministically across disconnected devices.

---

## Features

- 🔒 **Append-only log** – Operations are immutable history
- 🕐 **Vector clocks** – Causality tracking across devices  
- ⚔️ **Conflict detection** – Never auto-merges, preserves conflicts
- 🔄 **Deterministic replay** – Same operations = same result everywhere
- 📦 **Transport agnostic** – Bundles work over USB, email, Bluetooth, anything
- 🚫 **Zero infrastructure** – No servers, no cloud, no network required

---

## Installation

### From GitHub

```bash
git clone https://github.com/shivay00001/sqlite-sync-core.git
cd sqlite-sync-core
pip install -e .
```

### From PyPI (coming soon)

```bash
pip install sqlite-sync-core
```

### Requirements

- Python 3.11+
- `msgpack` (auto-installed)

---

## Quick Start

```python
from sqlite_sync import SyncEngine

# Initialize sync-enabled database
engine = SyncEngine("my_database.db")
engine.initialize()

# Create a user table
engine.connection.execute("""
    CREATE TABLE todos (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        done INTEGER DEFAULT 0
    )
""")

# Enable sync for the table
engine.enable_sync_for_table("todos")

# Now any INSERT/UPDATE/DELETE is automatically captured!
engine.connection.execute("INSERT INTO todos (title) VALUES ('Buy milk')")
```

---

## Syncing Between Devices

### Device A: Generate a bundle

```python
from sqlite_sync import SyncEngine

engine_a = SyncEngine("device_a.db")
engine_a.initialize()

# Generate bundle for Device B
bundle_path = engine_a.generate_bundle(
    peer_device_id=device_b_id,  # 16-byte UUID
    output_path="sync_bundle.db"
)
# Send bundle_path to Device B (USB, email, cloud, etc.)
```

### Device B: Import the bundle

```python
engine_b = SyncEngine("device_b.db")
engine_b.initialize()

# Import received bundle
result = engine_b.import_bundle("sync_bundle.db")

print(f"Applied: {result.applied_count}")
print(f"Conflicts: {result.conflict_count}")
print(f"Duplicates: {result.duplicate_count}")
```

---

## Handling Conflicts

Conflicts occur when two devices modify the same row independently.

```python
# Get all unresolved conflicts
conflicts = engine.get_unresolved_conflicts()

for conflict in conflicts:
    print(f"Table: {conflict.table_name}")
    print(f"Row PK: {conflict.row_pk}")
    print(f"Local op: {conflict.local_op_id.hex()}")
    print(f"Remote op: {conflict.remote_op_id.hex()}")
```

> **Note:** This library intentionally does NOT auto-resolve conflicts.
> You must implement your own resolution strategy.

---

## Core Invariants

| # | Invariant | Description |
|---|-----------|-------------|
| 1 | **Append-only** | `sync_operations` only grows, never modified |
| 2 | **Causal consistency** | Vector clocks ensure happens-before |
| 3 | **Deterministic ordering** | Same operations always sort identically |
| 4 | **Explicit conflicts** | Concurrent writes preserved as records |
| 5 | **Idempotent import** | Same bundle × N imports = same result |
| 6 | **Transport independence** | Bundles are self-contained SQLite files |

---

## API Reference

### `SyncEngine`

| Method | Description |
|--------|-------------|
| `initialize()` | Initialize sync tables, returns device ID |
| `enable_sync_for_table(name)` | Install triggers for a table |
| `generate_bundle(peer_id, path)` | Create bundle for peer |
| `import_bundle(path)` | Import bundle, returns `ImportResult` |
| `get_unresolved_conflicts()` | Get all pending conflicts |
| `get_vector_clock()` | Get current vector clock |
| `close()` | Close database connection |

### `ImportResult`

| Field | Type | Description |
|-------|------|-------------|
| `bundle_id` | bytes | UUID of imported bundle |
| `source_device_id` | bytes | Device that created bundle |
| `total_operations` | int | Total ops in bundle |
| `applied_count` | int | Successfully applied |
| `conflict_count` | int | Conflicts detected |
| `duplicate_count` | int | Already had these ops |
| `skipped` | bool | True if bundle already imported |

---

## Project Structure

```
sqlite_sync_core/
├── src/sqlite_sync/
│   ├── engine.py          # Main SyncEngine class
│   ├── config.py          # Configuration constants
│   ├── errors.py          # Exception classes
│   ├── invariants.py      # Core invariant enforcement
│   ├── db/                # Database layer
│   │   ├── schema.py      # Table definitions
│   │   ├── migrations.py  # Initialization
│   │   ├── triggers.py    # Change capture triggers
│   │   └── connection.py  # Connection management
│   ├── bundle/            # Bundle operations
│   │   ├── generate.py    # Bundle creation
│   │   ├── validate.py    # Bundle validation
│   │   └── format.py      # Bundle metadata
│   ├── import_apply/      # Import pipeline
│   │   ├── apply.py       # Apply operations
│   │   ├── conflict.py    # Conflict detection
│   │   ├── ordering.py    # Deterministic sort
│   │   └── dedup.py       # Deduplication
│   ├── log/               # Operation log
│   │   ├── operations.py  # SyncOperation dataclass
│   │   └── vector_clock.py# Vector clock logic
│   └── utils/             # Utilities
│       ├── uuid7.py       # UUID v7 generation
│       ├── hashing.py     # SHA-256 utilities
│       └── msgpack_codec.py# Serialization
├── tests/                 # Test suite
├── pyproject.toml         # Package config
└── README.md
```

---

## Running Tests

```bash
# Using the custom test runner (no pytest required)
python run_verification.py

# Or with pytest
pip install pytest
pytest tests/ -v
```

---

## License

MIT License - Use freely in commercial and open-source projects.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run the test suite
5. Submit a pull request

---

**Built with ❤️ for offline-first applications**
