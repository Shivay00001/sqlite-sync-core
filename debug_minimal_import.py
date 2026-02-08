
import sys
import os

# Mimic adding src to path
sys.path.insert(0, os.path.abspath('src'))

try:
    from sqlite_sync import SyncEngine
    print("SyncEngine imported")
    from sqlite_sync.ext.node import SyncNode
    print("SyncNode imported")
    from sqlite_sync.network.peer_discovery import UDPDiscovery
    print("UDPDiscovery imported")
except ImportError as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
