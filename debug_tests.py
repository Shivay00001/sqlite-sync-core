
import sys
import os
import traceback

sys.path.insert(0, os.path.join(os.getcwd(), "src"))

print(f"Python path: {sys.path}")

try:
    import tests.test_conflicts as tc
    print(f"Successfully imported tests.test_conflicts")
    print(f"TestConflictDetection is: {tc.TestConflictDetection}")
    print(f"Type: {type(tc.TestConflictDetection)}")
    
    # Try instantiation safely
    try:
        inst = tc.TestConflictDetection()
        print(f"Successfully instantiated TestConflictDetection: {inst}")
    except TypeError as e:
        print(f"FAILED to instantiate: {e}")
        # Inspect constructor
        import inspect
        print(f"Signature: {inspect.signature(tc.TestConflictDetection)}")
        
except ImportError as e:
    print(f"ImportError: {e}")
    traceback.print_exc()

except Exception as e:
    print(f"Other Exception: {e}")
    traceback.print_exc()
