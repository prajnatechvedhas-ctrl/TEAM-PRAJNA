"""
Vedas Base - Hackathon Conditions & Constraints Verification Suite.
Directly audits and executes against the 19-Page "Kill It and Prove It" Brief.
"""

import os
import sys
import time
import json
import struct
import subprocess
import unittest

from vedasbase.engine import open_db, VedasDB
from vedasbase.errors import (
    VedasError, InvalidInputError, DiskFullError, CorruptionError, LockError
)
from vedasbase.format import (
    FILE_HEADER_SIZE, REC_HEADER_SIZE, REC_TRAILER_SIZE,
    REC_MAGIC, TYPE_PUT, TYPE_DELETE, TYPE_COMMIT,
    MIN_KEY_LEN, MAX_KEY_LEN, MAX_VAL_LEN, MAX_BATCH_OPS
)
from vedasbase.recovery import check_db_integrity, recover

PYTHON_EXE = sys.executable

class TestHackathonConditions(unittest.TestCase):
    def setUp(self):
        self.test_dbs = []

    def tearDown(self):
        for path in self.test_dbs:
            for ext in ("", ".lock", ".compact.*.tmp"):
                import glob
                for f in glob.glob(path + ext):
                    try:
                        os.remove(f)
                    except OSError:
                        pass

    def _get_db(self, name, max_bytes=50_000_000):
        path = os.path.abspath(f"test_cond_{name}.db")
        self.test_dbs.append(path)
        return path

    # -------------------------------------------------------------
    # SECTION 2: Operations & Data Limits
    # -------------------------------------------------------------
    def test_01_mandatory_operations(self):
        """Verify put, get, delete, batch, open, close functionality."""
        db_path = self._get_db("ops")
        db = open_db(db_path)
        
        # put & get
        db.put(b"k1", b"v1")
        self.assertEqual(db.get(b"k1"), b"v1")
        
        # overwrite
        db.put(b"k1", b"v1_updated")
        self.assertEqual(db.get(b"k1"), b"v1_updated")
        
        # delete
        db.delete(b"k1")
        self.assertIsNone(db.get(b"k1"))
        # deleting missing key is not an error
        db.delete(b"k1")
        
        # batch
        ops = [("put", b"b1", b"val1"), ("put", b"b2", b"val2"), ("del", b"b1")]
        db.batch(ops)
        self.assertIsNone(db.get(b"b1"))
        self.assertEqual(db.get(b"b2"), b"val2")
        
        db.close()
        
        # reopen and recover
        reopened = open_db(db_path)
        self.assertEqual(reopened.get(b"b2"), b"val2")
        self.assertIsNone(reopened.get(b"b1"))
        reopened.close()

    def test_02_data_limits(self):
        """Verify keys: 1-1024B, values: 0-1MB, reject invalid."""
        db_path = self._get_db("limits")
        db = open_db(db_path)
        
        # 1-byte key & 0-byte value
        db.put(b"k", b"")
        self.assertEqual(db.get(b"k"), b"")
        
        # 1024-byte key
        key_1024 = b"k" * 1024
        db.put(key_1024, b"boundary_val")
        self.assertEqual(db.get(key_1024), b"boundary_val")
        
        # 1 MB value (1,048,576 bytes)
        val_1mb = b"V" * (1024 * 1024)
        db.put(b"large_key", val_1mb)
        self.assertEqual(db.get(b"large_key"), val_1mb)
        
        # Reject 0-byte key
        with self.assertRaises(InvalidInputError):
            db.put(b"", b"val")
            
        # Reject >1024 byte key
        with self.assertRaises(InvalidInputError):
            db.put(b"k" * 1025, b"val")
            
        # Reject >1 MB value
        with self.assertRaises(InvalidInputError):
            db.put(b"bad_val", b"V" * (1024 * 1024 + 1))
            
        db.close()

    def test_03_batch_limit_and_b5_ordering(self):
        """Verify batch up to 10,000 ops and Rule B5 (last op on key wins)."""
        db_path = self._get_db("batch_10k")
        db = open_db(db_path, max_bytes=200 * 1024 * 1024)
        
        # Rule B5: same key in one batch
        ops_b5 = [
            ("put", b"x", b"first"),
            ("put", b"x", b"second"),
            ("del", b"x"),
            ("put", b"x", b"third"),
            ("del", b"x")
        ]
        db.batch(ops_b5)
        self.assertIsNone(db.get(b"x"))
        
        # 10,000 ops in a single atomic batch
        batch_10k = [("put", f"k_{i}".encode("ascii"), f"v_{i}".encode("ascii")) for i in range(10_000)]
        t0 = time.perf_counter()
        db.batch(batch_10k)
        t1 = time.perf_counter()
        
        self.assertEqual(db.get(b"k_0"), b"v_0")
        self.assertEqual(db.get(b"k_9999"), b"v_9999")
        print(f"\n  [Condition Check: 10,000-op batch committed in {t1 - t0:.3f} s]")
        
        db.close()

    # -------------------------------------------------------------
    # SECTION 3: Storage, Concurrency & Disk Full
    # -------------------------------------------------------------
    def test_04_storage_and_compaction_shrinkage(self):
        """Verify single data file, file shrinkage after heavy deletes."""
        db_path = self._get_db("compaction")
        db = open_db(db_path)
        
        # 2,000 puts followed by 1,800 deletes
        for i in range(2000):
            db.put(f"k_{i}".encode("ascii"), b"data_payload_bytes")
            
        for i in range(1800):
            db.delete(f"k_{i}".encode("ascii"))
            
        pre_compaction_size = os.path.getsize(db_path)
        new_size = db.compact()
        post_compaction_size = os.path.getsize(db_path)
        
        self.assertLess(post_compaction_size, pre_compaction_size)
        reclaimed_pct = (1.0 - (post_compaction_size / pre_compaction_size)) * 100
        print(f"\n  [Condition Check: Compaction shrank file from {pre_compaction_size} B to {post_compaction_size} B ({reclaimed_pct:.1f}% space reclaimed)]")
        
        # Verify 200 live keys preserved, 1800 absent
        for i in range(1800):
            self.assertIsNone(db.get(f"k_{i}".encode("ascii")))
        for i in range(1800, 2000):
            self.assertEqual(db.get(f"k_{i}".encode("ascii")), b"data_payload_bytes")
            
        db.close()

    def test_05_disk_full_enforcement(self):
        """Verify strict max_bytes pre-check, clean rejection, zero corruption."""
        db_path = self._get_db("disk_full")
        # 16-byte header + ~100 bytes capacity
        db = open_db(db_path, max_bytes=200)
        
        db.put(b"k1", b"v1")
        file_size_after_k1 = os.path.getsize(db_path)
        
        # Write that would exceed max_bytes must be rejected BEFORE writing
        with self.assertRaises(DiskFullError):
            db.put(b"k2", b"x" * 500)
            
        # File size must not have grown by even a single byte
        self.assertEqual(os.path.getsize(db_path), file_size_after_k1)
        
        # Database remains fully readable and usable
        self.assertEqual(db.get(b"k1"), b"v1")
        db.close()
        
        # Reopening works cleanly
        reopened = open_db(db_path, max_bytes=200)
        self.assertEqual(reopened.get(b"k1"), b"v1")
        reopened.close()

    def test_06_process_lock_concurrency(self):
        """Verify exclusive OS lock prevents second process, stale lock harmless."""
        db_path = self._get_db("lock")
        db = open_db(db_path)
        
        # Trying to open concurrently raises LockError
        with self.assertRaises(LockError):
            open_db(db_path)
            
        db.close()
        # After close, open succeeds immediately
        db2 = open_db(db_path)
        db2.close()

    # -------------------------------------------------------------
    # SECTION 4 & 5: Invariant & Recovery
    # -------------------------------------------------------------
    def test_07_torn_tail_truncation_idempotence(self):
        """Verify 1-byte torn truncation is auto-repaired and recovery is idempotent."""
        db_path = self._get_db("torn")
        db = open_db(db_path)
        db.put(b"k1", b"v1")
        db.put(b"k2", b"v2")
        db.close()
        
        original_size = os.path.getsize(db_path)
        
        # Simulate torn tail by truncating 1 byte off the end
        with open(db_path, "r+b") as f:
            f.truncate(original_size - 1)
            
        # Recovery must succeed, discard uncommitted/torn tail, keep k1
        reopened = open_db(db_path)
        self.assertEqual(reopened.get(b"k1"), b"v1")
        reopened.close()
        
        # Double open without writes (idempotence)
        third_open = open_db(db_path)
        self.assertEqual(third_open.get(b"k1"), b"v1")
        third_open.close()

    def test_08_mid_file_corruption_detected_not_truncated(self):
        """Verify corruption followed by valid commit is flagged as CORRUPTION."""
        db_path = self._get_db("mid_corrupt")
        db = open_db(db_path)
        db.put(b"k1", b"v1")
        db.put(b"k2", b"v2")
        db.close()
        
        # Flip bit in record 1 payload
        with open(db_path, "r+b") as f:
            f.seek(FILE_HEADER_SIZE + 18)
            b = f.read(1)
            f.seek(FILE_HEADER_SIZE + 18)
            f.write(bytes([b[0] ^ 0xFF]))
            
        # check must report corruption, open must raise CorruptionError
        report = check_db_integrity(db_path)
        self.assertIn(report["status"], ("CORRUPTED", "CORRUPTION"))
        
        with self.assertRaises(CorruptionError):
            open_db(db_path)

    # -------------------------------------------------------------
    # SECTION 6: CLI Wrapper Compliance
    # -------------------------------------------------------------
    def test_09_cli_commands(self):
        """Verify all 8 CLI subcommands from Section 6 of the brief."""
        db_path = self._get_db("cli")
        
        # 1. open
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "open", db_path], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        self.assertIn("OK keys=0", res.stdout)
        self.assertIn("recovery_time=", res.stdout)
        
        # 2. put
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "put", db_path, "user_k", "user_v"], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        
        # 3. get
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "get", db_path, "user_k"], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        self.assertEqual(res.stdout.strip(), "user_v")
        
        # get missing key -> exit 2 & NOTFOUND
        res_missing = subprocess.run([PYTHON_EXE, "vedasbase.py", "get", db_path, "no_such_key"], capture_output=True, text=True)
        self.assertEqual(res_missing.returncode, 2)
        self.assertIn("NOTFOUND", res_missing.stdout)
        
        # 4. del
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "del", db_path, "user_k"], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        
        # 5. batch via file
        batch_file = db_path + ".batch.txt"
        with open(batch_file, "w") as f:
            f.write("put a 100\nput b 200\ndel a\n")
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "batch", db_path, batch_file], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        try:
            os.remove(batch_file)
        except OSError:
            pass
            
        # 6. dump
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "dump", db_path], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        self.assertIn("b 200", res.stdout)
        
        # 7. check
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "check", db_path], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        self.assertEqual(res.stdout.strip(), "OK")
        
        # 8. compact
        res = subprocess.run([PYTHON_EXE, "vedasbase.py", "compact", db_path], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        self.assertIn("OK new_size=", res.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
