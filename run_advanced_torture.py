"""
Vedas Base - Advanced Torture Testing Suite Runner.
"Kill It and Prove It" - Production Hardening & Chaos Verification.

Usage:
    python run_advanced_torture.py
"""

import sys
import unittest
from tests.test_advanced_torture import TestAdvancedTorture

def main():
    print("=" * 72)
    print("  VEDAS BASE - ADVANCED DATABASE TORTURE TESTING SUITE")
    print("  'Kill It and Prove It' - Extreme Invariant Verification")
    print("=" * 72)
    
    suite = unittest.TestLoader().loadTestsFromTestCase(TestAdvancedTorture)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 72)
    print("  FINAL TORTURE TESTING SUMMARY")
    print("=" * 72)
    print(f"  Tests Executed: {result.testsRun}")
    print(f"  Passed:         {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"  Failed:         {len(result.failures)}")
    print(f"  Errors:         {len(result.errors)}")
    print("=" * 72)
    
    if not result.wasSuccessful():
        sys.exit(1)

if __name__ == "__main__":
    main()

