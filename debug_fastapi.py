import sys
import traceback

print("Current Python:", sys.version)

try:
    print("Importing FastAPI...")
    import fastapi
    print(f"FastAPI version: {fastapi.__version__}")
    
    print("Importing Starlette...")
    import starlette
    print(f"Starlette version: {starlette.__version__}")
    
    print("Importing TestClient...")
    from fastapi.testclient import TestClient
    print("TestClient imported successfully.")
    
except ImportError:
    traceback.print_exc()
except Exception:
    traceback.print_exc()
