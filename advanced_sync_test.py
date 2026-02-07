"""
advanced_sync_test.py - Final verification script with extreme logging.
"""

import sys
import os
import sqlite3
import asyncio
import json
from sqlite_sync import SyncEngine

def setup_db(db_path, table_name):
    print(f"DEBUG: Setting up {db_path}...")
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            print(f"DEBUG: Removed existing {db_path}")
        except Exception as e:
            print(f"DEBUG: Failed to remove {db_path}: {e}")
    conn = sqlite3.connect(db_path)
    conn.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)")
    conn.close()
    print(f"DEBUG: Created {db_path} with table {table_name}")

async def run_test():
    print("STARTING: Advanced Field-Level Convergence Test")
    try:
        db_a = "test_device_a.db"
        db_b = "test_device_b.db"
        table = "users"
        
        setup_db(db_a, table)
        setup_db(db_b, table)
        
        # 1. Initialize Node A
        print("\n--- NODE A: Initialize ---")
        with SyncEngine(db_a) as engine_a:
            engine_a.initialize()
            engine_a.enable_sync_for_table(table)
            print("DEBUG: Node A initialized and sync enabled")
            
            # Initial state
            engine_a.connection.execute(f"INSERT INTO {table} (id, name, age, city) VALUES (1, 'Initial', 20, 'London')")
            engine_a.connection.commit()
            print("DEBUG: Node A inserted row 1")
            
            # 2. Initialize Node B and Sync Initial State
            print("\n--- NODE B: Initialize & Sync Initial ---")
            with SyncEngine(db_b) as engine_b:
                id_b = engine_b.initialize()
                engine_b.enable_sync_for_table(table)
                print("DEBUG: Node B initialized and sync enabled")
                
                bundle_path = "init.bundle"
                print(f"DEBUG: Generating bundle on Node A for Node B...")
                engine_a.generate_bundle(id_b, bundle_path)
                
                # Check bundle contents
                bconn = sqlite3.connect(bundle_path)
                ops = bconn.execute("SELECT * FROM bundle_operations").fetchall()
                print(f"DEBUG: Bundle contains {len(ops)} operations")
                for op in ops:
                    print(f"DEBUG: Op: {op[6]} on {op[5]} at {op[10]}")
                bconn.close()
                
                print(f"DEBUG: Checking Node B triggers...")
                triggers = engine_b.connection.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
                print(f"DEBUG: Node B Triggers: {triggers}")
                
                print(f"DEBUG: Checking Node B state before import...")
                count = engine_b.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"DEBUG: Node B {table} count: {count}")
                if count > 0:
                    rows = engine_b.connection.execute(f"SELECT * FROM {table}").fetchall()
                    print(f"DEBUG: Node B Rows: {rows}")
                
                print(f"DEBUG: Importing bundle on Node B...")
                res = engine_b.import_bundle(bundle_path)
                print(f"Success: Initial state synced. Applied: {res.applied_count}")

        # 3. Concurrent Updates (Both Offline)
        print("\n--- CONCURRENT UPDATES ---")
        # Node A updates 'name'
        with SyncEngine(db_a) as engine_a:
            engine_a.connection.execute(f"UPDATE {table} SET name = 'Updated-By-A' WHERE id = 1")
            engine_a.connection.commit()
            print("DEBUG: Node A updated name")
        
        # Node B updates 'city'
        with SyncEngine(db_b) as engine_b:
            engine_b.connection.execute(f"UPDATE {table} SET city = 'Paris' WHERE id = 1")
            engine_b.connection.commit()
            print("DEBUG: Node B updated city")
            id_b = engine_b.device_id
        
        # 4. Final Merge (A -> B)
        print("\n--- FINAL MERGE (A -> B) ---")
        with SyncEngine(db_a) as engine_a:
            bundle_a = "a.bundle"
            engine_a.generate_bundle(id_b, bundle_a)
            
            with SyncEngine(db_b) as engine_b:
                print(f"DEBUG: Importing Node A bundle on Node B...")
                res = engine_b.import_bundle(bundle_a)
                print(f"DEBUG: Import Result: {res}")
                
                # Verify Node B state
                row = engine_b.connection.execute(f"SELECT name, age, city FROM {table} WHERE id = 1").fetchone()
                print(f"Node B Final: name='{row[0]}', age={row[1]}, city='{row[2]}'")
                
                if row[0] == 'Updated-By-A' and row[2] == 'Paris':
                    print("\nSUCCESS: Field-level LWW Convergence achieved! 🚀")
                else:
                    print("\nFAILURE: State did not converge correctly.")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
