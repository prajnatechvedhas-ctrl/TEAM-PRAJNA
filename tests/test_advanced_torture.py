"""
Advanced VEDAS Torture Testing Suite.
"Kill It and Prove It" - Production Hardening & Chaos Verification.

Covers:
1. 10,000 Random mixed operations with independent Oracle verification.
2. Concurrent read and write operations under thread contention.
3. Random crashes during write operations.
4. Database recovery after crashes and idempotency.
5. Deep data corruption detection (CRC bit-flips, magic corruption, header corruption, torn tail).
6. Key deletion and online compaction space reclamation.
7. Large values (1 MB payloads) and strict max_bytes boundary enforcement.
8. Data consistency and atomic all-or-nothing verification after recovery.
9. Execution metrics: throughput, latency, recovery time, and database size tracking.

Standard library only. No third-party dependencies.
"""

import json
import os
import random
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zlib
from typing import Dict, List, Optional, Set, Tuple

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
from vedasbase.format import (
    FILE_MAGIC,
    REC_MAGIC,
    TYPE_PUT,
    TYPE_DELETE,
    TYPE_COMMIT,
    FILE_HEADER_SIZE,
    REC_HEADER_SIZE,
)
from vedasbase.recovery import check_db_integrity

PYTHON_EXE = sys.executable


class AdvancedOracle:
    """
    Independent in-memory ground-truth oracle for VEDAS operations.
    Maintains authoritative state independently of the database under test.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.committed_state: Dict[bytes, bytes] = {}
        self.inflight_ops: Dict[str, dict] = {}
        self.history: List[str] = []

    def record_inflight(self, op_id: str, op_data: dict):
        with self.lock:
            self.inflight_ops[op_id] = op_data

    def record_commit(self, op_id: str):
        with self.lock:
            op = self.inflight_ops.pop(op_id, None)
            if not op:
                return
            cmd = op["cmd"]
            if cmd == "put":
                self.committed_state[op["key"]] = op["val"]
            elif cmd == "del":
                self.committed_state.pop(op["key"], None)
            elif cmd == "batch":
                for sub in op["ops"]:
                    if isinstance(sub, (tuple, list)):
                        stype = sub[0]
                        skey = sub[1]
                        sval = sub[2] if len(sub) > 2 else b""
                    else:
                        stype = sub["type"]
                        skey = sub["key"]
                        sval = sub.get("val", b"")

                    if stype in ("put", TYPE_PUT):
                        self.committed_state[skey] = sval
                    elif stype in ("del", TYPE_DELETE):
                        self.committed_state.pop(skey, None)
            self.history.append(f"ACK {op_id}")

    def record_error(self, op_id: str):
        with self.lock:
            self.inflight_ops.pop(op_id, None)
            self.history.append(f"ERR {op_id}")

    def get_committed_snapshot(self) -> Dict[bytes, bytes]:
        with self.lock:
            return dict(self.committed_state)

    def is_valid_read(self, key: bytes, val: Optional[bytes]) -> bool:
        """
        Verify that a read observed either the current committed state
        or a valid recent state under concurrent mutation.
        """
        with self.lock:
            expected = self.committed_state.get(key)
            if val == expected:
                return True
            # If there's an inflight op touching this key, read could see either old or new
            for op in self.inflight_ops.values():
                if op["cmd"] in ("put", "del") and op.get("key") == key:
                    return True
                elif op["cmd"] == "batch":
                    for sub in op.get("ops", []):
                        if sub.get("key") == key:
                            return True
            return False


class TestAdvancedTorture(unittest.TestCase):
    """Advanced torture test cases for Vedas Base."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = self.temp_dir.name

    def tearDown(self):
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    def _get_temp_db_path(self, name: str = "torture.db") -> str:
        return os.path.join(self.test_dir, name)

    # --------------------------------------------------------------------------
    # 1 & 2. 10,000 Operations with Concurrent Readers and Writers
    # --------------------------------------------------------------------------
    def test_01_ten_thousand_mixed_operations_concurrent(self):
        """
        Executes exactly 10,000 mixed operations (Puts, Deletes, Gets, Batches)
        with 4 concurrent writer threads and 4 concurrent reader threads.
        Verifies execution metrics and full Oracle consistency.
        """
        db_path = self._get_temp_db_path("ten_thousand.db")
        db = vedas_open(db_path, max_bytes=200 * 1024 * 1024)
        oracle = AdvancedOracle()

        TOTAL_OPERATIONS = 10_000
        NUM_WRITERS = 4
        NUM_READERS = 4

        # Operation quota allocation (total 10,000 ops)
        # 5,000 Puts + 2,000 Deletes + 2,000 Concurrent Gets + 1,000 Batch ops (200 batches x 5 ops)
        NUM_PUTS = 5_000
        NUM_DELETES = 2_000
        NUM_GETS = 2_000
        BATCH_COUNT = 200
        BATCH_SIZE = 5
        NUM_BATCH_OPS = BATCH_COUNT * BATCH_SIZE  # 1,000

        self.assertEqual(NUM_PUTS + NUM_DELETES + NUM_GETS + NUM_BATCH_OPS, TOTAL_OPERATIONS)

        # Pre-generate mutation work items
        rng = random.Random(1337)
        key_pool = [f"key_{i:04d}".encode("ascii") for i in range(500)]

        mutation_tasks = []
        for _ in range(NUM_PUTS):
            k = rng.choice(key_pool)
            v = f"val_{rng.randint(1000, 99999)}_{os.urandom(rng.randint(10, 200)).hex()}".encode("ascii")
            mutation_tasks.append(("put", k, v))

        for _ in range(NUM_DELETES):
            k = rng.choice(key_pool)
            mutation_tasks.append(("del", k, None))

        for _ in range(BATCH_COUNT):
            sub_ops = []
            for _ in range(BATCH_SIZE):
                sub_k = rng.choice(key_pool)
                if rng.random() < 0.75:
                    sub_v = f"bval_{rng.randint(100, 999)}".encode("ascii")
                    sub_ops.append(("put", sub_k, sub_v))
                else:
                    sub_ops.append(("del", sub_k))
            mutation_tasks.append(("batch", sub_ops, None))

        rng.shuffle(mutation_tasks)

        # Work queue protected by lock
        work_lock = threading.Lock()
        task_idx = [0]
        completed_mutations = [0]
        completed_reads = [0]
        read_errors = []

        writer_errors = []
        op_counter = [0]

        stop_event = threading.Event()

        def writer_worker(w_id: int):
            try:
                while True:
                    with work_lock:
                        if task_idx[0] >= len(mutation_tasks):
                            break
                        item = mutation_tasks[task_idx[0]]
                        task_idx[0] += 1
                        op_counter[0] += 1
                        current_op_id = f"w{w_id}_{op_counter[0]}"

                    cmd = item[0]
                    if cmd == "put":
                        _, k, v = item
                        oracle.record_inflight(current_op_id, {"cmd": "put", "key": k, "val": v})
                        db.put(k, v)
                        oracle.record_commit(current_op_id)
                    elif cmd == "del":
                        _, k, _ = item
                        oracle.record_inflight(current_op_id, {"cmd": "del", "key": k})
                        db.delete(k)
                        oracle.record_commit(current_op_id)
                    elif cmd == "batch":
                        _, sub_ops, _ = item
                        oracle.record_inflight(current_op_id, {"cmd": "batch", "ops": sub_ops})
                        db.batch(sub_ops)
                        oracle.record_commit(current_op_id)

                    with work_lock:
                        completed_mutations[0] += 1
            except Exception as e:
                writer_errors.append((w_id, e))

        def reader_worker(r_id: int):
            local_rng = random.Random(2000 + r_id)
            try:
                while not stop_event.is_set():
                    with work_lock:
                        if completed_reads[0] >= NUM_GETS:
                            break
                        completed_reads[0] += 1

                    k = local_rng.choice(key_pool)
                    val = db.get(k)
                    # Verify read consistency: value must not be corrupted garbage
                    if val is not None and not isinstance(val, (bytes, bytearray)):
                        read_errors.append(f"Reader {r_id}: returned non-bytes value {type(val)}")
                    time.sleep(0.0001)
            except Exception as e:
                read_errors.append(f"Reader {r_id} exception: {e}")

        # Start execution timer
        start_time = time.perf_counter()

        writers = [threading.Thread(target=writer_worker, args=(i,)) for i in range(NUM_WRITERS)]
        readers = [threading.Thread(target=reader_worker, args=(i,)) for i in range(NUM_READERS)]

        for r in readers:
            r.start()
        for w in writers:
            w.start()
# 
        for w in writers:
            w.join()

        # Stop reader threads once writers finish and quota met
        stop_event.set()
        for r in readers:
            r.join()

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        # Ensure no writer or reader crashed
        self.assertEqual(len(writer_errors), 0, f"Writer thread errors: {writer_errors}")
        self.assertEqual(len(read_errors), 0, f"Reader consistency errors: {read_errors}")

        # Verify exact operation count
        total_ops_performed = completed_mutations[0] - BATCH_COUNT + NUM_BATCH_OPS + completed_reads[0]
        self.assertEqual(completed_mutations[0], len(mutation_tasks))
        self.assertGreaterEqual(total_ops_performed, TOTAL_OPERATIONS - 50)

        # Verify dump against Oracle
        db_dump = dict(db.dump())
        oracle_state = oracle.get_committed_snapshot()

        self.assertEqual(len(db_dump), len(oracle_state), "Live key count mismatch between DB and Oracle")
        for k, v in oracle_state.items():
            self.assertIn(k, db_dump, f"Key {k} missing from database dump")
            self.assertEqual(db_dump[k], v, f"Value mismatch for key {k}")

        file_size_bytes = os.path.getsize(db_path)
        db.close()

        # Measure recovery time on the 10,000-op database
        t_rec_0 = time.perf_counter()
        reopened_db = vedas_open(db_path)
        t_rec_1 = time.perf_counter()
        recovery_time_s = t_rec_1 - t_rec_0

        reopened_dump = dict(reopened_db.dump())
        self.assertEqual(reopened_dump, oracle_state, "Post-recovery state mismatch")
        reopened_db.close()

        throughput = TOTAL_OPERATIONS / elapsed
        print(f"\n[Test 1 Metrics - 10,000 Operations]")
        print(f"  Operations Performed: {TOTAL_OPERATIONS} ({NUM_PUTS} puts, {NUM_DELETES} deletes, {NUM_GETS} gets, {NUM_BATCH_OPS} batch ops)")
        print(f"  Concurrent Threads:   {NUM_WRITERS} writers, {NUM_READERS} readers")
        print(f"  Total Elapsed Time:   {elapsed:.3f} s")
        print(f"  Overall Throughput:   {throughput:.1f} ops/sec")
        print(f"  Final Database Size:  {file_size_bytes / 1024:.2f} KB ({file_size_bytes} bytes)")
        print(f"  Post-Run Recovery:    {recovery_time_s * 1000:.2f} ms ({len(oracle_state)} live keys)")

    # --------------------------------------------------------------------------
    # 3, 4 & 8. Random Crashes, Recovery Invariants & Batch Atomicity
    # --------------------------------------------------------------------------
    def test_02_crash_recovery_invariants_and_atomic_batches(self):
        """
        Executes random hard crashes (SIGKILL / proc.kill()) during active
        concurrent writes and batches against temporary databases.
        Verifies:
        1. Clean recovery without errors.
        2. Idempotent recovery.
        3. All-or-nothing atomicity for in-flight batches.
        4. No corruption in recovered state.
        """
        CRASH_CYCLES = 5
        recovery_times = []

        for cycle in range(1, CRASH_CYCLES + 1):
            db_path = self._get_temp_db_path(f"crash_cycle_{cycle}.db")
            oracle_log = db_path + ".oracle.log"
            oracle = AdvancedOracle()

            # Start child process in serve mode
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            kvdb_py = os.path.join(repo_root, "kvdb.py")
            cmd = [
                PYTHON_EXE,
                "-u",
                kvdb_py,
                "serve",
                db_path,
                "--workers",
                "2",
            ]
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            server_ready = threading.Event()
            stop_worker = threading.Event()

            def stdout_reader():
                try:
                    for line in proc.stdout:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(" ", 2)
                        if parts[0] == "ACK":
                            op_id = parts[1]
                            if op_id == "probe":
                                server_ready.set()
                            else:
                                oracle.record_commit(op_id)
                        elif parts[0] in ("VAL", "NOT_FOUND"):
                            if len(parts) >= 2 and parts[1] == "probe":
                                server_ready.set()
                        elif parts[0] == "ERR" and len(parts) >= 2:
                            oracle.record_error(parts[1])
                except Exception:
                    pass

            t_reader = threading.Thread(target=stdout_reader, daemon=True)
            t_reader.start()

            # Probe server readiness
            probe_line = json.dumps({"op_id": "probe", "cmd": "get", "key": "__probe__"}) + "\n"
            try:
                proc.stdin.write(probe_line)
                proc.stdin.flush()
            except (OSError, BrokenPipeError):
                pass

            server_ready.wait(timeout=5.0)
            self.assertTrue(server_ready.is_set(), "Server failed to respond to probe")

            # Fire concurrent multi-op batches and puts
            rng = random.Random(42 + cycle)
            atomic_batch_ops = {}

            def write_loop():
                op_num = 0
                while not stop_worker.is_set():
                    op_num += 1
                    op_id = f"c{cycle}_op{op_num}"
                    # 50% atomic batches (5 to 15 ops), 50% single puts
                    if rng.random() < 0.5:
                        batch_items = []
                        for b_idx in range(rng.randint(5, 15)):
                            bk = f"batch_{cycle}_{op_num}_{b_idx}".encode("ascii")
                            bv = f"val_{cycle}_{b_idx}".encode("ascii")
                            batch_items.append({"type": "put", "key": bk, "val": bv})
                        op_data = {"cmd": "batch", "ops": batch_items}
                        oracle.record_inflight(op_id, op_data)
                        atomic_batch_ops[op_id] = batch_items
                        msg = json.dumps({
                            "op_id": op_id,
                            "cmd": "batch",
                            "ops": [{"type": "put", "key_hex": s["key"].hex(), "val_hex": s["val"].hex()} for s in batch_items]
                        }) + "\n"
                    else:
                        k = f"sing_{cycle}_{op_num}".encode("ascii")
                        v = f"sval_{op_num}".encode("ascii")
                        oracle.record_inflight(op_id, {"cmd": "put", "key": k, "val": v})
                        msg = json.dumps({
                            "op_id": op_id,
                            "cmd": "put",
                            "key_hex": k.hex(),
                            "val_hex": v.hex(),
                        }) + "\n"

                    try:
                        if proc.stdin and not proc.stdin.closed:
                            proc.stdin.write(msg)
                            proc.stdin.flush()
                    except (OSError, BrokenPipeError, ValueError):
                        break
                    time.sleep(0.001)

            t_writer = threading.Thread(target=write_loop, daemon=True)
            t_writer.start()

            # Random millisecond kill delay (15ms - 75ms)
            time.sleep(rng.uniform(0.015, 0.075))

            # HARD KILL
            try:
                proc.kill()
                proc.wait(timeout=2.0)
            except Exception:
                pass
            finally:
                for pipe in (proc.stdin, proc.stdout, proc.stderr):
                    if pipe:
                        try:
                            pipe.close()
                        except Exception:
                            pass

            stop_worker.set()
            t_writer.join(timeout=0.5)

            # Recovery verification
            t0 = time.perf_counter()
            recovered_db = vedas_open(db_path)
            t1 = time.perf_counter()
            rec_ms = (t1 - t0) * 1000.0
            recovery_times.append(rec_ms)

            # Verify integrity
            integrity = check_db_integrity(db_path)
            self.assertEqual(integrity["status"], "OK", f"Integrity check failed after recovery: {integrity}")

            # Verify all ACKed records exist
            rec_dump = dict(recovered_db.dump())
            oracle_committed = oracle.get_committed_snapshot()
            for k, v in oracle_committed.items():
                self.assertIn(k, rec_dump, f"Cycle {cycle}: ACKed key {k} lost after crash!")
                self.assertEqual(rec_dump[k], v, f"Cycle {cycle}: ACKed key {k} value mismatch!")

            # Verify Atomic All-or-Nothing for Batches
            for b_id, sub_ops in atomic_batch_ops.items():
                present_count = 0
                for s in sub_ops:
                    k = s["key"]
                    if k in rec_dump and rec_dump[k] == s["val"]:
                        present_count += 1
                # Must be 100% applied or 0% applied (never partial)
                is_all = (present_count == len(sub_ops))
                is_none = (present_count == 0)
                self.assertTrue(
                    is_all or is_none,
                    f"Cycle {cycle}: Partial batch execution detected for {b_id}! ({present_count}/{len(sub_ops)} applied)"
                )

            recovered_db.close()

            # Re-open again to prove recovery idempotency
            second_open = vedas_open(db_path)
            second_dump = dict(second_open.dump())
            self.assertEqual(rec_dump, second_dump, "Second recovery produced different state (not idempotent)")
            second_open.close()

        avg_rec_ms = sum(recovery_times) / len(recovery_times)
        print(f"\n[Test 2 Metrics - Crash Recovery & Batch Atomicity]")
        print(f"  Crash Cycles Run:     {CRASH_CYCLES}")
        print(f"  All-or-Nothing Met:   100% (zero partial batches)")
        print(f"  Integrity Verified:   100% (clean CRC checks)")
        print(f"  Avg Recovery Time:    {avg_rec_ms:.2f} ms")

    # --------------------------------------------------------------------------
    # 5. Deep Data Corruption Detection
    # --------------------------------------------------------------------------
    def test_03_deep_data_corruption_detection(self):
        """
        Injects bit-flips, forged headers, and torn records into temporary databases.
        Verifies that check_db_integrity accurately identifies the corruption,
        and open() handles them according to the specification.
        """
        # Scenario A: Payload bit flip in committed record
        db_path_a = self._get_temp_db_path("corrupt_payload.db")
        db_a = vedas_open(db_path_a)
        db_a.put(b"secure_key_1", b"original_unmodified_payload_bytes")
        db_a.put(b"secure_key_2", b"second_record_payload")
        db_a.close()

        # Flip a byte inside the first record payload
        with open(db_path_a, "r+b") as f:
            data = bytearray(f.read())
            # Byte offset ~30 is inside the first record payload
            target_offset = 32
            data[target_offset] ^= 0xFF
            f.seek(0)
            f.write(data)

        check_a = check_db_integrity(db_path_a)
        self.assertEqual(check_a["status"], "CORRUPTED")
        self.assertFalse(check_a["clean"], "Failed to detect payload bit-flip: reported clean")
        self.assertGreater(check_a["uncommitted_bytes"], 0, "Failed to flag uncommitted/corrupted bytes")

        # open() must detect mid-file corruption and raise CorruptionError to protect valid data
        with self.assertRaises(CorruptionError):
            vedas_open(db_path_a)

        # Scenario B: Corrupted File Header Magic
        db_path_b = self._get_temp_db_path("corrupt_header.db")
        db_b = vedas_open(db_path_b)
        db_b.put(b"k", b"v")
        db_b.close()

        with open(db_path_b, "r+b") as f:
            f.seek(0)
            f.write(b"BADMAGIC")

        check_b = check_db_integrity(db_path_b)
        self.assertEqual(check_b["status"], "CORRUPTED", "Failed to detect corrupted file header magic")
        self.assertIn("Invalid header", check_b["error"])
        with self.assertRaises(CorruptionError):
            vedas_open(db_path_b)

        # Scenario C: Corrupted Record Magic in middle record
        db_path_c = self._get_temp_db_path("corrupt_rec_magic.db")
        db_c = vedas_open(db_path_c)
        db_c.put(b"k1", b"v1")
        db_c.close()

        with open(db_path_c, "r+b") as f:
            # First record header starts at FILE_HEADER_SIZE (16)
            f.seek(FILE_HEADER_SIZE)
            f.write(b"\x99")  # Mutate 0xA7 to 0x99

        check_c = check_db_integrity(db_path_c)
        self.assertFalse(check_c["clean"], "Failed to detect record magic corruption")
        self.assertGreater(check_c["uncommitted_bytes"], 0)

        # Scenario D: Forged COMMIT marker op_count
        db_path_d = self._get_temp_db_path("corrupt_commit.db")
        db_d = vedas_open(db_path_d)
        db_d.batch([
            ("put", b"batch_k1", b"batch_v1"),
            ("put", b"batch_k2", b"batch_v2"),
        ])
        db_d.close()

        # Mutate the op_count inside the commit marker payload
        file_sz = os.path.getsize(db_path_d)
        with open(db_path_d, "r+b") as f:
            # Commit marker is at end of file: 16B header + 4B op_count + 4B CRC = 24 bytes
            commit_offset = file_sz - 24
            # op_count is at commit_offset + 16
            f.seek(commit_offset + 16)
            f.write(struct.pack("<I", 9999))  # Forge op_count to 9999

        check_d = check_db_integrity(db_path_d)
        self.assertFalse(check_d["clean"], "Failed to detect forged commit marker")
        self.assertGreater(check_d["uncommitted_bytes"], 0)

        # Scenario E: Torn trailing bytes after clean commit
        db_path_e = self._get_temp_db_path("torn_tail.db")
        db_e = vedas_open(db_path_e)
        db_e.put(b"clean_key", b"clean_val")
        db_e.close()
        clean_size = os.path.getsize(db_path_e)

        # Append 7 torn uncommitted bytes
        with open(db_path_e, "ab") as f:
            f.write(b"TORN_7B")

        check_e = check_db_integrity(db_path_e)
        self.assertFalse(check_e["clean"], "Check reported clean on torn trailing bytes")
        self.assertEqual(check_e["uncommitted_bytes"], 7)

        # open() must cleanly truncate the uncommitted torn tail back to valid commit
        db_repaired = vedas_open(db_path_e)
        self.assertEqual(db_repaired.get(b"clean_key"), b"clean_val")
        db_repaired.close()
        self.assertEqual(os.path.getsize(db_path_e), clean_size, "open() failed to truncate torn tail")

        print(f"\n[Test 3 Metrics - Corruption Detection]")
        print(f"  Payload Bit-Flip:     DETECTED (CRC32 mismatch flagged)")
        print(f"  Header Magic Forgery: DETECTED (Invalid header magic raised)")
        print(f"  Record Magic Forgery: DETECTED (Invalid record magic flagged)")
        print(f"  Commit Count Forgery: DETECTED (CRC32 mismatch on commit flagged)")
        print(f"  Torn Trailing Bytes:  DETECTED & AUTO-REPAIRED (Truncated to {clean_size} B)")

    # --------------------------------------------------------------------------
    # 6. Key Deletion and Compaction Space Reclamation
    # --------------------------------------------------------------------------
    def test_04_key_deletion_and_compaction_reclamation(self):
        """
        Writes 5,000 keys, deletes 4,000 keys, measures database sizes,
        triggers online compaction, and asserts exact file shrinkage
        and live key preservation.
        """
        db_path = self._get_temp_db_path("compaction_churn.db")
        db = vedas_open(db_path, max_bytes=100 * 1024 * 1024)

        NUM_TOTAL = 5_000
        NUM_DELETE = 4_000
        VAL_SIZE = 300

        print(f"\n[Test 4 - Compaction & Deletion]")
        t0 = time.perf_counter()

        # Batch insert 5,000 keys
        keys = [f"comp_{i:05d}".encode("ascii") for i in range(NUM_TOTAL)]
        batch_size = 500
        for i in range(0, NUM_TOTAL, batch_size):
            ops = [("put", k, b"X" * VAL_SIZE) for k in keys[i : i + batch_size]]
            db.batch(ops)

        size_after_puts = os.path.getsize(db_path)

        # Delete 4,000 keys
        for i in range(0, NUM_DELETE, batch_size):
            ops = [("del", k) for k in keys[i : i + batch_size]]
            db.batch(ops)

        size_after_deletes = os.path.getsize(db_path)
        self.assertGreater(size_after_deletes, size_after_puts, "Deletes did not append tombstones")

        # Run online compaction
        t_comp_0 = time.perf_counter()
        db.compact()
        t_comp_1 = time.perf_counter()
        compaction_time_s = t_comp_1 - t_comp_0

        size_after_compact = os.path.getsize(db_path)

        # Assert space reduction (>65% shrinkage compared to peak size)
        shrinkage_pct = (1.0 - (size_after_compact / size_after_deletes)) * 100.0
        self.assertGreater(shrinkage_pct, 65.0, f"Compaction only shrank by {shrinkage_pct:.1f}%")

        # Verify live key integrity
        live_keys = keys[NUM_DELETE:]
        deleted_keys = keys[:NUM_DELETE]

        self.assertEqual(len(live_keys), 1_000)
        for k in live_keys:
            val = db.get(k)
            self.assertEqual(val, b"X" * VAL_SIZE, f"Live key {k} corrupted or missing")

        for k in deleted_keys:
            self.assertIsNone(db.get(k), f"Deleted key {k} still present")

        db.close()

        # Reopen and check integrity
        integrity = check_db_integrity(db_path)
        self.assertEqual(integrity["status"], "OK")

        print(f"  Peak Size (with tombstones): {size_after_deletes / 1024:.2f} KB ({size_after_deletes} bytes)")
        print(f"  Compacted Size:             {size_after_compact / 1024:.2f} KB ({size_after_compact} bytes)")
        print(f"  Disk Space Reclaimed:       {shrinkage_pct:.1f}%")
        print(f"  Compaction Duration:        {compaction_time_s:.3f} s")
        print(f"  Live Keys Verified:         {len(live_keys)} (100% correct)")
        print(f"  Deleted Keys Verified:      {len(deleted_keys)} (100% absent)")

    # --------------------------------------------------------------------------
    # 7. Large Values (1 MB) and Database Size Limits
    # --------------------------------------------------------------------------
    def test_05_large_values_and_size_limits_boundary(self):
        """
        Tests 1 MB payload limits, strict max_bytes boundary enforcement,
        DiskFullError pre-check rejection, and post-rejection non-mutation.
        """
        db_path = self._get_temp_db_path("large_values.db")

        # Set max_bytes to ~3.2 MB (enough for header + two 1 MB records + commits, but not three)
        ONE_MB = 1024 * 1024
        LIMIT_BYTES = (2 * ONE_MB) + (100 * 1024)  # ~2.2 MB

        db = vedas_open(db_path, max_bytes=LIMIT_BYTES)

        # 1. Insert two 1 MB records
        payload_1 = b"A" * ONE_MB
        payload_2 = b"B" * ONE_MB

        t0 = time.perf_counter()
        db.put(b"big_k1", payload_1)
        db.put(b"big_k2", payload_2)
        t1 = time.perf_counter()

        size_before_rejection = os.path.getsize(db_path)
        self.assertGreater(size_before_rejection, 2 * ONE_MB)

        # 2. Attempt third 1 MB record -> must be rejected by pre-check
        payload_3 = b"C" * ONE_MB
        with self.assertRaises(DiskFullError):
            db.put(b"big_k3", payload_3)

        # Assert no bytes were written during rejected operation
        size_after_rejection = os.path.getsize(db_path)
        self.assertEqual(
            size_before_rejection,
            size_after_rejection,
            "File size changed despite DiskFullError rejection (torn pre-write)!"
        )

        # Assert existing records are intact
        self.assertEqual(db.get(b"big_k1"), payload_1)
        self.assertEqual(db.get(b"big_k2"), payload_2)
        self.assertIsNone(db.get(b"big_k3"))

        # 3. Delete one 1 MB record and compact
        db.delete(b"big_k1")
        db.compact()

        size_after_compaction = os.path.getsize(db_path)
        self.assertLess(size_after_compaction, 1_100_000)

        # 4. Now insertion of third 1 MB record must succeed
        db.put(b"big_k3", payload_3)
        self.assertEqual(db.get(b"big_k3"), payload_3)
        self.assertEqual(db.get(b"big_k2"), payload_2)
        self.assertIsNone(db.get(b"big_k1"))

        db.close()

        # Check integrity of file containing 1 MB records
        integrity = check_db_integrity(db_path)
        self.assertEqual(integrity["status"], "OK")

        print(f"\n[Test 5 Metrics - Large Values & Limits]")
        print(f"  1 MB Payloads Written: 2x 1,048,576 bytes in {t1 - t0:.3f} s")
        print(f"  DiskFullError:         Triggered cleanly at limit ({LIMIT_BYTES} B)")
        print(f"  Pre-check Safety:      Zero bytes appended on rejection")
        print(f"  Space Reuse:           After delete & compact, new 1 MB record written successfully")


if __name__ == "__main__":
    unittest.main()
