import os
import sqlite3
from sqlite_sync.engine import SyncEngine

def run_example():
    db_path = "example_basic.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    print("--- SQLite Sync: Basic Example ---")
    
    # 1. Initialize engine
    with SyncEngine(db_path) as engine:
        device_id = engine.initialize()
        print(f"Device ID: {device_id.hex()}")
        
        # 2. Create a table
        engine.connection.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER DEFAULT 0
            )
        """)
        
        # 3. Enable sync for the table
        engine.enable_sync_for_table("tasks")
        print("Sync enabled for 'tasks' table.")
        
        # 4. Perform some operations
        print("Inserting a task...")
        engine.connection.execute("INSERT INTO tasks (title) VALUES ('Build production-ready sync')")
        engine.connection.commit()
        
        # 5. Check captured operations
        ops = engine.get_new_operations()
        print(f"Captured {len(ops)} operations.")
        for op in ops:
            print(f"  Op ID: {op.op_id.hex()}, Type: {op.op_type}, Table: {op.table_name}")
            
    print("\nExample finished. Database saved to", db_path)

if __name__ == "__main__":
    run_example()
