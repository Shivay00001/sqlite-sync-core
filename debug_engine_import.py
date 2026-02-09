import sys
import traceback

try:
    print("Importing SyncEngine...")
    from sqlite_sync import SyncEngine
    print("SyncEngine imported successfully.")
    
    print("Importing HTTPTransport...")
    from sqlite_sync.transport.http_transport import HTTPTransport
    print("HTTPTransport imported successfully.")
    
except ImportError:
    traceback.print_exc()
except Exception:
    traceback.print_exc()
