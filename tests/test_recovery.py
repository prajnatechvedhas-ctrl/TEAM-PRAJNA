"""
Unit tests for crash recovery, torn writes, and CRC corruption.
"""

import os
import unittest
import struct
import zlib
from vedasbase import open as vedas_open, CorruptionError
from vedasbase.format import (
    FILE_MAGIC,
    FILE_HEADER_SIZE,
    FILE_HEADER_FMT,
    REC_MAGIC,
    TYPE_PUT,
    TYPE_COMMIT,
    encode_put_record,
    encode_commit_record,
)

class TestRecoveryScenarios(unittest.TestCase):

    def setUp(self):
        self.db_path = "test_scenarios.db"
        for p in (self.db_path, self.db_path + ".lock"):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def tearDown(self):
        for p in (self.db_path, self.db_path + ".lock"):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def test_missing_commit_marker_is_ignored_and_truncated(self):
        # Write valid record directly to file WITHOUT commit marker
        with open(self.db_path, "wb") as f:
            f.write(struct.pack(FILE_HEADER_FMT, FILE_MAGIC, 1, 0))
            rec = encode_put_record(b"uncommitted_key", b"uncommitted_val", 1)
            f.write(rec)
            f.flush()

        # Database should open cleanly, ignore uncommitted key, and truncate file to 16 bytes
        db = vedas_open(self.db_path)
        self.assertIsNone(db.get(b"uncommitted_key"))
        self.assertEqual(len(db.dump()), 0)
        db.close()

        self.assertEqual(os.path.getsize(self.db_path), FILE_HEADER_SIZE)

    def test_torn_write_in_record_payload(self):
        # Write valid record 1 + commit, then half-written record 2
        with open(self.db_path, "wb") as f:
            f.write(struct.pack(FILE_HEADER_FMT, FILE_MAGIC, 1, 0))
            rec1 = encode_put_record(b"k1", b"v1", 1)
            f.write(rec1)
            commit1 = encode_commit_record(1, 1)
            f.write(commit1)
            good_size = f.tell()

            # Append half of record 2 (torn write)
            rec2 = encode_put_record(b"k2", b"v2", 2)
            f.write(rec2[: len(rec2) // 2])
            f.flush()

        db = vedas_open(self.db_path)
        self.assertEqual(db.get(b"k1"), b"v1")
        self.assertIsNone(db.get(b"k2"))
        db.close()

        # Tail must be truncated to good_size
        self.assertEqual(os.path.getsize(self.db_path), good_size)

    def test_corrupted_crc_in_op(self):
        with open(self.db_path, "wb") as f:
            f.write(struct.pack(FILE_HEADER_FMT, FILE_MAGIC, 1, 0))
            rec1 = encode_put_record(b"k1", b"v1", 1)
            f.write(rec1)
            commit1 = encode_commit_record(1, 1)
            f.write(commit1)
            good_size = f.tell()

            # Write record with bad CRC
            rec2 = bytearray(encode_put_record(b"k2", b"v2", 2))
            rec2[-1] ^= 0xFF  # Corrupt CRC
            f.write(rec2)
            commit2 = encode_commit_record(1, 2)
            f.write(commit2)
            f.flush()

        db = vedas_open(self.db_path)
        self.assertEqual(db.get(b"k1"), b"v1")
        self.assertIsNone(db.get(b"k2"))
        db.close()
        self.assertEqual(os.path.getsize(self.db_path), good_size)

    def test_commit_op_count_mismatch(self):
        with open(self.db_path, "wb") as f:
            f.write(struct.pack(FILE_HEADER_FMT, FILE_MAGIC, 1, 0))
            # Write 2 ops
            f.write(encode_put_record(b"k1", b"v1", 1))
            f.write(encode_put_record(b"k2", b"v2", 1))
            # But commit claims op_count = 1
            f.write(encode_commit_record(1, 1))
            f.flush()

        # Because op_count mismatch (2 != 1), batch must be rejected and truncated to 16 bytes
        db = vedas_open(self.db_path)
        self.assertIsNone(db.get(b"k1"))
        self.assertIsNone(db.get(b"k2"))
        db.close()
        self.assertEqual(os.path.getsize(self.db_path), FILE_HEADER_SIZE)

    def test_corrupted_header_raises_corruption_error(self):
        with open(self.db_path, "wb") as f:
            f.write(b"BADMAGIC" + b"\x00" * 8)

        with self.assertRaises(CorruptionError):
            vedas_open(self.db_path)

    def test_truncate_one_byte_off_valid_file(self):
        db = vedas_open(self.db_path)
        db.put(b"hello", b"world")
        db.close()

        orig_size = os.path.getsize(self.db_path)
        # Truncate exactly 1 byte off the end (tears the COMMIT record's CRC32)
        with open(self.db_path, "r+b") as f:
            f.truncate(orig_size - 1)

        self.assertEqual(os.path.getsize(self.db_path), orig_size - 1)

        # Open should succeed, detect torn write, and roll back to 16 bytes (file header)
        db2 = vedas_open(self.db_path)
        self.assertIsNone(db2.get(b"hello"))
        self.assertEqual(len(db2.dump()), 0)
        db2.close()

        self.assertEqual(os.path.getsize(self.db_path), FILE_HEADER_SIZE)

if __name__ == "__main__":
    unittest.main()

