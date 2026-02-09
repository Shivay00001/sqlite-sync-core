import sys
import traceback

try:
    print("Importing server app...")
    from sqlite_sync.transport.server import app
    print("Server app imported successfully.")
    
except ImportError:
    traceback.print_exc()
except Exception:
    traceback.print_exc()
