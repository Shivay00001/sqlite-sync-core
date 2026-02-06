
import sys
import os
import unittest
import traceback

sys.path.insert(0, os.path.join(os.getcwd(), "src"))

print("Importing TestConflictDetection...")
from tests.test_conflicts import TestConflictDetection, TestNoAutoMerge, TestVectorClockConcurrency, TestConflictPreservation
print("Importing TestInvariants...")
from tests.test_invariants import TestAppendOnlyInvariant, TestVectorClockInvariant, TestTransactionInvariant, TestAtomicityGuarantee

def run_class_tests(instance):
    with open("verification_results.log", "a") as f:
        f.write(f"Testing {instance.__class__.__name__}...\n")
        print(f"Testing {instance.__class__.__name__}...")
        
        methods = [m for m in dir(instance) if m.startswith("test_")]
        for method_name in methods:
            method = getattr(instance, method_name)
            try:
                print(f"  Running {method_name}...", end='', flush=True)
                method()
                print(" [PASS]")
                f.write(f"  [PASS] {method_name}\n")
            except Exception as e:
                print(f" [FAIL]")
                f.write(f"  [FAIL] {method_name}: {e}\n")
                traceback.print_exc(file=f)
                traceback.print_exc()

if __name__ == "__main__":
    try:
        run_class_tests(TestConflictDetection())
        run_class_tests(TestNoAutoMerge())
        run_class_tests(TestVectorClockConcurrency())
        run_class_tests(TestConflictPreservation())
        
        run_class_tests(TestAppendOnlyInvariant())
        run_class_tests(TestVectorClockInvariant())
        run_class_tests(TestTransactionInvariant())
        run_class_tests(TestAtomicityGuarantee())
        
        # Determinism tests
        from tests.test_determinism import TestDeterministicOrdering, TestDeterministicReplay, TestCanonicalSerialization
        run_class_tests(TestDeterministicOrdering())
        run_class_tests(TestDeterministicReplay())
        run_class_tests(TestCanonicalSerialization())
        
        # Idempotency tests (need per-test setup/teardown)
        from tests.test_idempotency import TestIdempotentBundleImport, TestIdempotentOperationApplication
        
        with open("verification_results.log", "a") as f:
            f.write("Testing TestIdempotentBundleImport...\n")
        print("Testing TestIdempotentBundleImport...")
        
        for method_name in ['test_double_import_same_result', 'test_triple_import_same_result', 'test_partial_overlap_handled']:
            test_bundle = TestIdempotentBundleImport()
            test_bundle.setup_method()
            try:
                method = getattr(test_bundle, method_name)
                with open("verification_results.log", "a") as f:
                    try:
                        print(f"  Running {method_name}...", end='', flush=True)
                        method()
                        print(" [PASS]")
                        f.write(f"  [PASS] {method_name}\n")
                    except Exception as e:
                        print(f" [FAIL]")
                        f.write(f"  [FAIL] {method_name}: {e}\n")
                        traceback.print_exc()
            finally:
                test_bundle.teardown_method()
        
        run_class_tests(TestIdempotentOperationApplication())
    except Exception as e:
        print(f"Fatal error: {e}")
        traceback.print_exc()
