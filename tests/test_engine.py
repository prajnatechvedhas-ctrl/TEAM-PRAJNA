"""
Unit tests for Vedas Base storage engine core.
"""

import os
import sys
import unittest
import struct
import zlib

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from vedasbase import (
    open as vedas_open,
    VedasDB,
    VedasError,
    InvalidInputError,
    DiskFullError,
    CorruptionError,
    LockError,
    ClosedError,
)
from vedasbase.format import FILE_MAGIC, REC_MAGIC, TYPE_PUT, TYPE_COMMIT

class TestVedasEngine(unittest.TestCase):

    def setUp(self):
        self.db_path = "test_engine.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists(self.db_path + ".lock"):
            os.remove(self.db_path + ".lock")

    def tearDown(self):
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except OSError:
                pass
        if os.path.exists(self.db_path + ".lock"):
            try:
                os.remove(self.db_path + ".lock")
            except OSError:
                pass

    def test_basic_crud(self):
        db = vedas_open(self.db_path, max_bytes=1024 * 1024)
        db.put(b"k1", b"v1")
        self.assertEqual(db.get(b"k1"), b"v1")
        self.assertIsNone(db.get(b"k2"))

        # Overwrite
        db.put(b"k1", b"v2")
        self.assertEqual(db.get(b"k1"), b"v2")

        # Delete
        db.delete(b"k1")
        self.assertIsNone(db.get(b"k1"))
        db.close()

    def test_reopen_persistence(self):
        db = vedas_open(self.db_path)
        db.put(b"persist_key", b"persist_val")
        db.close()

        # Reopen and check
        db2 = vedas_open(self.db_path)
        self.assertEqual(db2.get(b"persist_key"), b"persist_val")
        db2.close()

    def test_batch_semantics_b5(self):
        db = vedas_open(self.db_path)
        # Put X then delete X in same batch -> key should be deleted
        db.batch([
            ("put", b"x", b"val_x"),
            ("del", b"x"),
            ("put", b"y", b"val_y"),
        ])
        self.assertIsNone(db.get(b"x"))
        self.assertEqual(db.get(b"y"), b"val_y")

        # Delete Y then put Y in same batch -> key should have new value
        db.batch([
            ("del", b"y"),
            ("put", b"y", b"new_y"),
        ])
        self.assertEqual(db.get(b"y"), b"new_y")
        db.close()

    def test_input_validation(self):
        db = vedas_open(self.db_path)
        # Empty key
        with self.assertRaises(InvalidInputError):
            db.put(b"", b"val")

        # Key too large (> 1024 bytes)
        with self.assertRaises(InvalidInputError):
            db.put(b"k" * 1025, b"val")

        # Value too large (> 1 MB)
        with self.assertRaises(InvalidInputError):
            db.put(b"k", b"v" * (1024 * 1024 + 1))

        # Non-bytes key
        with self.assertRaises(InvalidInputError):
            db.put("string_key", b"val")

        # Non-bytes val
        with self.assertRaises(InvalidInputError):
            db.put(b"key", "string_val")

        db.close()

    def test_disk_full_enforcement(self):
        # Header is 16 bytes. Small limit.
        db = vedas_open(self.db_path, max_bytes=100)
        # 16 + (20 + 2 + 2) + 24 = 64 bytes -> fits
        db.put(b"k1", b"v1")
        # Next put needs 44 bytes -> 64 + 44 = 108 > 100 -> should raise DiskFullError
        with self.assertRaises(DiskFullError):
            db.put(b"k2", b"v2")

        # File size must not have grown
        self.assertEqual(os.path.getsize(self.db_path), 64)
        self.assertEqual(db.get(b"k1"), b"v1")
        self.assertIsNone(db.get(b"k2"))
        db.close()

    def test_lock_exclusion(self):
        db1 = vedas_open(self.db_path)
        with self.assertRaises(LockError):
            db2 = vedas_open(self.db_path)
        db1.close()

        # After closing db1, db2 can open
        db2 = vedas_open(self.db_path)
        db2.close()

    def test_compaction_shrinks_file(self):
        db = vedas_open(self.db_path, max_bytes=1024 * 1024)
        for i in range(100):
            db.put(f"key_{i}".encode(), b"initial_val_1234567890")
        size_before_deletes = os.path.getsize(self.db_path)

        # Delete 90 keys
        for i in range(90):
            db.delete(f"key_{i}".encode())
        size_after_deletes = os.path.getsize(self.db_path)
        self.assertGreater(size_after_deletes, size_before_deletes)

        # Compact
        new_size = db.compact()
        self.assertLess(new_size, size_before_deletes)
        self.assertEqual(os.path.getsize(self.db_path), new_size)

        # Check remaining 10 keys
        for i in range(90, 100):
            self.assertEqual(db.get(f"key_{i}".encode()), b"initial_val_1234567890")
        for i in range(90):
            self.assertIsNone(db.get(f"key_{i}".encode()))

        db.close()

    def test_dump_sorted(self):
        db = vedas_open(self.db_path)
        db.put(b"z", b"1")
        db.put(b"a", b"2")
        db.put(b"m", b"3")
        dumped = db.dump()
        self.assertEqual(dumped, [(b"a", b"2"), (b"m", b"3"), (b"z", b"1")])
        db.close()

    def test_process_lock_released_on_sigkill(self):
        import subprocess, sys, time
        # Spawn child process that opens the DB and holds the lock
        code = f"""
import sys, time, os
sys.path.insert(0, os.path.abspath('src'))
from vedasbase import open as vopen
db = vopen(r"{self.db_path}")
sys.stdout.write("LOCKED\\n")
sys.stdout.flush()
time.sleep(30)
"""
        proc = subprocess.Popen([sys.executable, "-u", "-c", code], stdout=subprocess.PIPE, text=True)
        try:
            line = proc.stdout.readline().strip()
            self.assertEqual(line, "LOCKED")

            # Parent cannot open while child holds lock
            with self.assertRaises(LockError):
                vedas_open(self.db_path)

            # Hard kill child process (TerminateProcess / SIGKILL)
            proc.kill()
            proc.wait(timeout=2)
            if proc.stdout:
                proc.stdout.close()
        finally:
            if proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    pass

        # Windows NT kernel handle cleanup is asynchronous after TerminateProcess.
        # Retry with brief sleep (up to 1s) until kernel releases the handle lock.
        db = None
        for _ in range(50):
            try:
                db = vedas_open(self.db_path)
                break
            except LockError:
                time.sleep(0.02)

        self.assertIsNotNone(db, "Lock was not automatically released after child process killed")
        db.put(b"after_kill", b"alive")
        self.assertEqual(db.get(b"after_kill"), b"alive")
        db.close()

    def test_compaction_txid_continuity(self):
        db = vedas_open(self.db_path)
        for i in range(15):
            db.put(f"k_{i}".encode(), f"v_{i}".encode())
        txid_before_compact = db._next_txid
        self.assertGreaterEqual(txid_before_compact, 15)

        db.compact()
        txid_after_compact = db._next_txid
        # Compaction must NOT reset txid to 1; must be monotonically increasing
        self.assertGreater(txid_after_compact, txid_before_compact)
        db.close()

    def test_zero_length_values_and_key_limits(self):
        db = vedas_open(self.db_path, max_bytes=10 * 1024 * 1024)
        # 1-byte key
        db.put(b"k", b"val_1b")
        self.assertEqual(db.get(b"k"), b"val_1b")

        # 1024-byte key (exact maximum)
        max_key = b"K" * 1024
        db.put(max_key, b"max_key_val")
        self.assertEqual(db.get(max_key), b"max_key_val")

        # 0-byte value
        db.put(b"zero_val_key", b"")
        self.assertEqual(db.get(b"zero_val_key"), b"")

        # Delete non-existent key succeeds without error
        db.delete(b"non_existent_key_xyz")
        self.assertIsNone(db.get(b"non_existent_key_xyz"))

        # Reject key of 1025 bytes
        with self.assertRaises(InvalidInputError):
            db.put(b"K" * 1025, b"val")

        # Reject key of 0 bytes
        with self.assertRaises(InvalidInputError):
            db.put(b"", b"val")

        db.close()

    def test_duplicate_keys_within_batch(self):
        db = vedas_open(self.db_path)
        # B5 rule: last op wins. Put then Del -> key deleted.
        db.batch([
            ("put", b"dup_key_1", b"v1"),
            ("del", b"dup_key_1"),
        ])
        self.assertIsNone(db.get(b"dup_key_1"))

        # Del then Put -> key has new value.
        db.batch([
            ("del", b"dup_key_2"),
            ("put", b"dup_key_2", b"final_value"),
        ])
        self.assertEqual(db.get(b"dup_key_2"), b"final_value")
        db.close()

if __name__ == "__main__":
    unittest.main()

