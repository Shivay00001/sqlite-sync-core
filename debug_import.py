
import sys
import traceback

try:
    import sqlite_sync
    print("Import successful")
except ImportError:
    traceback.print_exc()
except Exception:
    traceback.print_exc()
