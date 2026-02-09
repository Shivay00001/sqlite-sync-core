
import sqlite3
import os
import subprocess
import time
import sys
import asyncio
from sqlite_sync import SyncNode

import uuid

DB_PATH = f"verify_readme_{uuid.uuid4().hex}.db"

def cleanup():
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except Exception as e:
            print(f"Warning: Failed to cleanup {DB_PATH}: {e}")

def setup_db():
    cleanup()
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT)")
    conn.commit()
    conn.close()
    print(f"Created {DB_PATH} with table 'tasks'")

def run_command(cmd):
    print(f"Running: {cmd}")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise Exception(f"Command failed: {cmd}")
    print("Success")
    return result.stdout

def test_migrate():
    print("\n--- Testing Migrate CLI ---")
    run_command(f"python -m sqlite_sync.cli.main migrate {DB_PATH} --table tasks --add-column priority --type INTEGER")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("PRAGMA table_info(tasks)")
    columns = [row[1] for row in cursor.fetchall()]
    conn.close()
    
    if "priority" in columns:
        print("Verification: Column 'priority' exists.")
    else:
        raise Exception("Verification Failed: Column 'priority' not found.")

def test_start_cli():
    print("\n--- Testing Start CLI (Timeout 5s) ---")
    cmd = f"python -m sqlite_sync.cli.main start {DB_PATH} --name Device-Test --table tasks --port 8090"
    print(f"Starting: {cmd}")
    
    process = subprocess.Popen(
        cmd, 
        shell=True,
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
    )
    
    try:
        time.sleep(5)
        
        if process.poll() is None:
            print("Process is still running (Good)")
        else:
            stdout, stderr = process.communicate()
            print(f"Process exited early!")
            print(f"STDOUT: {stdout}")
            print(f"STDERR: {stderr}")
            raise Exception("Start CLI failed to keep running")
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
            print("Terminated background process")

async def test_python_api():
    print("\n--- Testing Python API SyncNode ---")
    node = SyncNode(
        db_path=DB_PATH,
        device_name="PythonAPITest",
        sync_interval=1,
        port=8091,
        enable_discovery=False # Avoid port conflict with concurrent runs if any
    )
    
    print("Starting node...")
    start_task = asyncio.create_task(node.start())
    
    await asyncio.sleep(2)
    
    print("Enabling sync for table...")
    node.enable_sync_for_table("tasks")
    
    print("Stopping node...")
    await node.stop()
    await start_task
    print("Node stopped.")

def main():
    try:
        setup_db()
        test_migrate()
        test_start_cli()
        asyncio.run(test_python_api())
        print("\n[OK] All README verifications passed!")
    except Exception as e:
        print(f"\n[FAIL] Verification failed: {e}")
        sys.exit(1)
    finally:
        if os.path.exists(DB_PATH):
            try:
                os.remove(DB_PATH)
            except:
                pass

if __name__ == "__main__":
    main()
