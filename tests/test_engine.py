"""
Unit tests for Vedas Base storage engine core.
"""

import os
import unittest
import struct
import zlib
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

if __name__ == "__main__":
    unittest.main()

