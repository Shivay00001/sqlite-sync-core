import os
import sys
import shutil
import asyncio
from httpx import AsyncClient, ASGITransport
from sqlite_sync import SyncEngine
from sqlite_sync.transport.server import app, get_engine
from sqlite_sync.transport.http_transport import HTTPTransport

# Mock get_engine dependency
mock_engine = None

def override_get_engine():
    return mock_engine

app.dependency_overrides[get_engine] = override_get_engine

async def run_verification():
    print("Starting Transport Verification (Async)...")
    
    # Setup
    tmpdir = "verify_transport_tmp"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    
    db_path = os.path.join(tmpdir, "server.db")
    global mock_engine
    mock_engine = SyncEngine(db_path)
    mock_engine.initialize()
    
    # Use AsyncClient with ASGITransport
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        
        try:
            # Test 1: Successful Handshake
            print("\nTest 1: Successful Handshake")
            response = await client.post(
                "/sync/handshake",
                json={
                    "device_id": "client1",
                    "vector_clock": {},
                    "schema_version": 1
                }
            )
            if response.status_code == 200:
                print("PASS: Status code 200")
                data = response.json()
                if "device_id" in data and "schema_version" in data:
                    print("PASS: Response structure valid")
                else:
                    print(f"FAIL: Invalid response structure: {data}")
            else:
                print(f"FAIL: Status code {response.status_code}")
                print(f"Response: {response.text}")

            # Test 2: Schema Mismatch
            print("\nTest 2: Schema Mismatch")
            # Local version is 1 (default). Client sends 999.
            response = await client.post(
                "/sync/handshake",
                json={
                    "device_id": "client1",
                    "vector_clock": {},
                    "schema_version": 999 
                }
            )
            if response.status_code == 409:
                print("PASS: Status code 409 (Conflict)")
                if "Schema incompatibility" in response.json()["detail"]:
                    print("PASS: Error detail correct")
                else:
                    print(f"FAIL: Unexpected error detail: {response.text}")
            else:
                print(f"FAIL: Expected 409, got {response.status_code}")
                print(f"Response: {response.text}")

        except Exception as e:
            print(f"\nCRITICAL ERROR: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Teardown
            if mock_engine:
                mock_engine.close()
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
            print("\nVerification Complete.")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_verification())
