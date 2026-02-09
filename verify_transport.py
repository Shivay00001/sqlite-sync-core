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
            else:
                print(f"FAIL: Expected 409, got {response.status_code}")

            # Test 3: Push Operations (Verify HLC Serialization)
            print("\nTest 3: Push Operations")
            import time
            from sqlite_sync.log.operations import SyncOperation
            
            # Create a dummy operation with HLC bytes
            op = {
                "op_id": b"1234567890123456",
                "device_id": b"1234567890123456",
                "vector_clock": {"1234567890123456": 1},
                "table_name": "test_table",
                "op_type": "INSERT",
                "row_pk": b"row1",
                "new_values": b"{}",
                "schema_version": 1,
                "created_at": time.time(),
                "hlc": b"\x00\x00\x00\x00\x00\x00\x00\x01" # Bytes!
            }
            
            # Manually serialize like http_transport does (to test server handling)
            # Actually, we should test if http_transport.send_operations works
            # But here we are using client.post directly. 
            # Let's use the code from http_transport._serialize_op effectively
            
            op_serialized = {
                "op_id": op["op_id"].hex(),
                "device_id": op["device_id"].hex(),
                "vector_clock": op["vector_clock"],
                "table_name": op["table_name"],
                "op_type": op["op_type"],
                "row_pk": op["row_pk"].hex(),
                "new_values": op["new_values"].hex(),
                "schema_version": op["schema_version"],
                "created_at": op["created_at"],
                "hlc": op["hlc"].hex() # This is what we fixed!
            }
            
            # We need to mock apply_batch in the mock_engine
            # But mock_engine is a real SyncEngine instance on a specific DB
            # So it will try to write to DB. That's fine.
            # We need to ensure the table exists or mock apply_batch.
            # Let's mock apply_batch to avoid DB constraints
            
            original_apply = mock_engine.apply_batch
            
            class MockResult:
                applied_count = 1
                conflict_count = 0
                duplicate_count = 0
                
            def mock_apply_batch(*args, **kwargs):
                return MockResult()
                
            mock_engine.apply_batch = mock_apply_batch
            
            print(f"DEBUG: Serialized Op: {op_serialized}")
            
            try:
                response = await client.post(
                    "/sync/push",
                    json={
                        "device_id": b"1234567890123456".hex(),
                        "operations": [op_serialized]
                    }
                )
                
                if response.status_code == 200:
                    print("PASS: Push successful (200 OK)")
                else:
                    print(f"FAIL: Push failed {response.status_code}")
                    with open("error.txt", "w") as f:
                        f.write(response.text)
                    print("Error written to error.txt")
            finally:
                mock_engine.apply_batch = original_apply


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
