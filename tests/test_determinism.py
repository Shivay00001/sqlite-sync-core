"""
test_determinism.py - Tests for deterministic behavior.

These tests verify that given the same operations, the system
always produces the same results regardless of order or timing.
"""

import os
import sqlite3
import tempfile
import pytest

from sqlite_sync import SyncEngine
from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.log.vector_clock import parse_vector_clock, serialize_vector_clock
from sqlite_sync.import_apply.ordering import sort_operations_deterministically
from sqlite_sync.utils.uuid7 import generate_uuid_v7
from sqlite_sync.utils.msgpack_codec import pack_dict, pack_primary_key


def create_test_operation(
    op_id: bytes,
    device_id: bytes,
    vector_clock: dict[str, int],
    table_name: str = "test",
    op_type: str = "INSERT",
    row_pk: int = 1,
) -> SyncOperation:
    """Helper to create test operations."""
    return SyncOperation(
        op_id=op_id,
        device_id=device_id,
        parent_op_id=None,
        vector_clock=serialize_vector_clock(vector_clock),
        table_name=table_name,
        op_type=op_type,
        row_pk=pack_primary_key(row_pk),
        old_values=pack_dict({"id": row_pk}) if op_type != "INSERT" else None,
        new_values=pack_dict({"id": row_pk, "value": "test"}) if op_type != "DELETE" else None,
        schema_version=1,
        created_at=1000000,
        is_local=True,
        applied_at=1000000,
    )


class TestDeterministicOrdering:
    """Tests for deterministic operation ordering."""
    
    def test_same_operations_same_order(self):
        """Same operations always sort to same order."""
        device_a = generate_uuid_v7()
        device_b = generate_uuid_v7()
        
        op1 = create_test_operation(
            op_id=generate_uuid_v7(),
            device_id=device_a,
            vector_clock={device_a.hex(): 1},
            row_pk=1,
        )
        op2 = create_test_operation(
            op_id=generate_uuid_v7(),
            device_id=device_b,
            vector_clock={device_b.hex(): 1},
            row_pk=2,
        )
        op3 = create_test_operation(
            op_id=generate_uuid_v7(),
            device_id=device_a,
            vector_clock={device_a.hex(): 2, device_b.hex(): 1},
            row_pk=3,
        )
        
        # Sort in different input orders
        order_a = sort_operations_deterministically([op1, op2, op3])
        order_b = sort_operations_deterministically([op3, op1, op2])
        order_c = sort_operations_deterministically([op2, op3, op1])
        
        # Must produce identical output order
        assert [o.op_id for o in order_a] == [o.op_id for o in order_b]
        assert [o.op_id for o in order_b] == [o.op_id for o in order_c]
    
    def test_vector_clock_ordering_respected(self):
        """Operations with higher vector clocks come later."""
        device_a = generate_uuid_v7()
        
        early_op = create_test_operation(
            op_id=generate_uuid_v7(),
            device_id=device_a,
            vector_clock={device_a.hex(): 1},
            row_pk=1,
        )
        late_op = create_test_operation(
            op_id=generate_uuid_v7(),
            device_id=device_a,
            vector_clock={device_a.hex(): 5},
            row_pk=2,
        )
        
        sorted_ops = sort_operations_deterministically([late_op, early_op])
        
        # Early op should come first
        assert sorted_ops[0].op_id == early_op.op_id
        assert sorted_ops[1].op_id == late_op.op_id
    
    def test_op_id_breaks_ties(self):
        """When vector clocks are equal, op_id determines order."""
        device_a = generate_uuid_v7()
        
        # Same vector clock
        vc = {device_a.hex(): 5}
        
        op1 = create_test_operation(
            op_id=generate_uuid_v7(),
            device_id=device_a,
            vector_clock=vc,
            row_pk=1,
        )
        op2 = create_test_operation(
            op_id=generate_uuid_v7(),
            device_id=device_a,
            vector_clock=vc,
            row_pk=2,
        )
        
        # Sort multiple times - must be consistent
        for _ in range(10):
            sorted_ops = sort_operations_deterministically([op2, op1])
            first_id = sorted_ops[0].op_id
            
            sorted_again = sort_operations_deterministically([op1, op2])
            assert sorted_again[0].op_id == first_id


class TestDeterministicReplay:
    """Tests for deterministic replay guarantee."""
    
    def test_same_operations_same_final_state(self):
        """Applying same operations produces same final state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create two databases
            db1_path = os.path.join(tmpdir, "db1.db")
            db2_path = os.path.join(tmpdir, "db2.db")
            
            engine1 = SyncEngine(db1_path)
            engine1.initialize()
            
            engine2 = SyncEngine(db2_path)
            engine2.initialize()
            
            for engine in [engine1, engine2]:
                conn = engine.connection
                conn.execute("""
                    CREATE TABLE items (
                        id INTEGER PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                engine.enable_sync_for_table("items")
            
            # Perform same operations on both
            for engine in [engine1, engine2]:
                conn = engine.connection
                conn.execute("INSERT INTO items (id, value) VALUES (1, 'first')")
                conn.execute("INSERT INTO items (id, value) VALUES (2, 'second')")
                conn.execute("UPDATE items SET value = 'updated' WHERE id = 1")
            
            # Verify same final state
            cursor1 = engine1.connection.execute(
                "SELECT id, value FROM items ORDER BY id"
            )
            rows1 = cursor1.fetchall()
            
            cursor2 = engine2.connection.execute(
                "SELECT id, value FROM items ORDER BY id"
            )
            rows2 = cursor2.fetchall()
            
            assert rows1 == rows2
            
            engine1.close()
            engine2.close()


class TestCanonicalSerialization:
    """Tests for canonical (deterministic) serialization."""
    
    def test_dict_serialization_is_sorted(self):
        """Dictionary serialization produces sorted keys."""
        from sqlite_sync.utils.msgpack_codec import pack_dict
        
        # Different input orders
        dict1 = {"z": 1, "a": 2, "m": 3}
        dict2 = {"a": 2, "m": 3, "z": 1}
        dict3 = {"m": 3, "z": 1, "a": 2}
        
        # Must produce identical bytes
        packed1 = pack_dict(dict1)
        packed2 = pack_dict(dict2)
        packed3 = pack_dict(dict3)
        
        assert packed1 == packed2 == packed3
    
    def test_vector_clock_serialization_is_sorted(self):
        """Vector clock JSON has sorted keys."""
        vc = {"z" * 32: 1, "a" * 32: 2, "m" * 32: 3}
        serialized = serialize_vector_clock(vc)
        
        # Parse it back
        parsed = parse_vector_clock(serialized)
        reserialized = serialize_vector_clock(parsed)
        
        # Must be identical
        assert serialized == reserialized
        
        # Keys should be in sorted order in the JSON
        import json
        obj = json.loads(serialized)
        keys = list(obj.keys())
        assert keys == sorted(keys)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
