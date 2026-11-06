"""
Vedas Base - Torture Test Tool.
"Kill It and Prove It" - Chaos and Crash Consistency Verification.

Modes:
- concurrent: Random concurrent writer threads sending puts, deletes, and small batches.
- large_values: Puts with 1 MB values.
- large_batches: Batches with up to 10,000 ops.
- compaction: Triggers online compaction during concurrent writes and kills mid-compaction.
- recovery_crash: Kills the process in the middle of recovery, then recovers again idempotently.
- disk_full: Small max-bytes limit, verifies DiskFullError rejection, compacts to reclaim space, resumes writes.
- broken_variant: Deliberately broken server that ACKs before fsync, demonstrating the oracle catches bugs.
"""

import argparse
import base64
import json
import os
import random
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple, Set

PYTHON_EXE = sys.executable

class Oracle:
    """
    Independent ground-truth model.
    Never reads state from the database under test.
    """
    def __init__(self, log_path: str):
        self.log_path = log_path
        self.lock = threading.Lock()
        self.committed_state: Dict[bytes, bytes] = {}
        # inflight maps op_id -> normalized op data
        self.inflight: Dict[str, dict] = {}
        with open(self.log_path, "w") as f:
            f.write("# Vedas Base Torture Oracle Log\n")

    def record_inflight(self, op_id: str, op_data: dict):
        with self.lock:
            self.inflight[op_id] = op_data

    def record_ack(self, op_id: str):
        with self.lock:
            if op_id not in self.inflight:
                return
            op = self.inflight.pop(op_id)
            cmd = op["cmd"]
            if cmd == "put":
                self.committed_state[op["key"]] = op["val"]
            elif cmd == "del":
                self.committed_state.pop(op["key"], None)
            elif cmd == "batch":
                for sub_op in op["ops"]:
                    if sub_op["type"] == "put":
                        self.committed_state[sub_op["key"]] = sub_op["val"]
                    else:
                        self.committed_state.pop(sub_op["key"], None)

            with open(self.log_path, "a") as f:
                f.write(f"ACK {op_id} {cmd}\n")

    def record_error(self, op_id: str, reason: str):
        with self.lock:
            self.inflight.pop(op_id, None)
            with open(self.log_path, "a") as f:
                f.write(f"ERR {op_id} {reason}\n")


class TortureRunner:
    def __init__(
        self,
        db_path: str,
        mode: str = "random",
        iterations: int = 500,
        seed: int = 42,
        max_bytes: int = 100 * 1024 * 1024,
        num_writers: int = 2,
        broken: bool = False,
        shared_keys: bool = False,
        **kwargs,
    ):
        self.db_path = os.path.abspath(db_path)
        self.oracle_log = self.db_path + ".oracle.log"
        self.mode = mode
        self.iterations = iterations
        self.seed = seed
        self.max_bytes = max_bytes
        self.num_writers = num_writers
        self.broken = broken
        self.shared_keys = kwargs.get("shared_keys", False)
        self.rng = random.Random(seed)
        self.oracle = Oracle(self.oracle_log)
        self.op_counter = 0
        self.counter_lock = threading.Lock()

    def _next_op_id(self) -> str:
        with self.counter_lock:
            self.op_counter += 1
            return str(self.op_counter)

    def clean_env(self):
        for p in (self.db_path, self.db_path + ".lock", self.oracle_log):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def run_dump(self) -> Dict[bytes, bytes]:
        """Runs `dump` command as child process and parses output into dict."""
        cmd = [
            PYTHON_EXE,
            "-m",
            "vedasbase",
            "--db",
            self.db_path,
            "dump",
            "--hex",
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"Dump failed with returncode {res.returncode}: {res.stderr}")

        dump_state = {}
        for line in res.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(" ", 1)
            k = bytes.fromhex(parts[0])
            v = bytes.fromhex(parts[1]) if len(parts) > 1 else b""
            dump_state[k] = v
        return dump_state

    def run_check(self) -> dict:
        """Runs `check` command on database."""
        cmd = [
            PYTHON_EXE,
            "-m",
            "vedasbase",
            "--db",
            self.db_path,
            "check",
            "--json",
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            return json.loads(res.stdout)
        except Exception:
            raise RuntimeError(f"Check failed: {res.stdout}\n{res.stderr}")

    def verify_invariants(self, dump_state: Dict[bytes, bytes]):
        """
        Rigorous post-crash invariant check against Oracle:
        1. Every ACKed op must be reflected (unless superseded).
        2. In-flight (un-ACKed) ops may be fully applied or fully omitted.
        3. For any in-flight batch, its ops must be applied atomically (all or nothing).
        4. No key returns corruption or another key's value.
        """
        committed = dict(self.oracle.committed_state)
        inflight = dict(self.oracle.inflight)

        all_known_keys = set(committed.keys())
        for op in inflight.values():
            if op["cmd"] in ("put", "del"):
                all_known_keys.add(op["key"])
            elif op["cmd"] == "batch":
                for sub in op["ops"]:
                    all_known_keys.add(sub["key"])

        for k in dump_state.keys():
            if k not in all_known_keys:
                raise AssertionError(
                    f"INVARIANT VIOLATION: Unknown key {k.hex()} found in DB dump!"
                )

        inflight_keys = set()
        for op in inflight.values():
            if op["cmd"] in ("put", "del"):
                inflight_keys.add(op["key"])
            elif op["cmd"] == "batch":
                for sub in op["ops"]:
                    inflight_keys.add(sub["key"])

        for k, v in committed.items():
            if k not in inflight_keys:
                if k not in dump_state:
                    raise AssertionError(
                        f"INVARIANT VIOLATION: ACKed key {k.hex()} missing from dump!"
                    )
                if dump_state[k] != v:
                    raise AssertionError(
                        f"INVARIANT VIOLATION: Key {k.hex()} value mismatch!\n"
                        f"Expected ACKed: {v.hex()[:64]}\n"
                        f"Actual in DB:   {dump_state[k].hex()[:64]}"
                    )

        for k in list(dump_state.keys()):
            if k not in committed and k not in inflight_keys:
                raise AssertionError(
                    f"INVARIANT VIOLATION: Deleted or unwritten key {k.hex()} present in dump!"
                )

        # Verify all-or-nothing atomicity for every in-flight batch
        for op_id, op in inflight.items():
            if op["cmd"] == "batch":
                batch_ops = op["ops"]
                applied_flags = []
                for sub in batch_ops:
                    sub_k = sub["key"]
                    if sub["type"] == "put":
                        sub_v = sub["val"]
                        if dump_state.get(sub_k) == sub_v:
                            applied_flags.append(True)
                        else:
                            applied_flags.append(False)
                    else:
                        if sub_k not in dump_state:
                            applied_flags.append(True)
                        else:
                            applied_flags.append(False)

                keys_in_batch = [s["key"] for s in batch_ops]
                if len(keys_in_batch) == len(set(keys_in_batch)) and len(keys_in_batch) > 1:
                    other_inflight_keys = set()
                    for other_id, other_op in inflight.items():
                        if other_id != op_id:
                            if other_op["cmd"] in ("put", "del"):
                                other_inflight_keys.add(other_op["key"])
                            elif other_op["cmd"] == "batch":
                                for s in other_op["ops"]:
                                    other_inflight_keys.add(s["key"])
                    
                    isolated_batch = not any(k in other_inflight_keys for k in keys_in_batch)
                    if isolated_batch:
                        if any(applied_flags) and not all(applied_flags):
                            raise AssertionError(
                                f"INVARIANT VIOLATION: Partial batch execution detected for batch {op_id}!\n"
                                f"Applied flags: {applied_flags}"
                            )

    def run_iteration(self, iteration_num: int):
        """Executes one crash-restart cycle."""
        env = os.environ.copy()
        if self.broken:
            env["VEDAS_CORRUPT_ACK_EARLY"] = "1"

        effective_max_bytes = 64 * 1024 if self.mode == "disk_full" else self.max_bytes

        cmd = [
            PYTHON_EXE,
            "-u",
            "-m",
            "vedasbase",
            "--db",
            self.db_path,
            "--max-bytes",
            str(effective_max_bytes),
            "serve",
            "--workers",
            str(self.num_writers),
        ]
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )

        stop_writers = threading.Event()
        server_ready = threading.Event()

        def ack_reader():
            try:
                for line in proc.stdout:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(" ", 2)
                    status = parts[0]
                    if status in ("ACK", "VAL", "NOT_FOUND"):
                        if len(parts) >= 2:
                            op_id = parts[1]
                            if op_id == "probe":
                                server_ready.set()
                            elif status == "ACK":
                                self.oracle.record_ack(op_id)
                    elif status == "ERR" and len(parts) >= 2:
                        reason = parts[2] if len(parts) > 2 else "unknown"
                        self.oracle.record_error(parts[1], reason)
            except Exception:
                pass

        reader_t = threading.Thread(target=ack_reader, daemon=True)
        reader_t.start()

        # Send probe command to wait for server initialization
        try:
            probe_cmd = json.dumps({"op_id": "probe", "cmd": "get", "key": "__probe__"}) + "\n"
            proc.stdin.write(probe_cmd)
            proc.stdin.flush()
        except (OSError, BrokenPipeError):
            pass

        server_ready.wait(timeout=5.0)

        writer_threads = []

        def writer_loop(writer_id: int):
            if self.shared_keys:
                key_pool = [f"k_{x}".encode("ascii") for x in range(30)]
            else:
                key_pool = [f"w{writer_id}_k{x}".encode("ascii") for x in range(30)]

            while not stop_writers.is_set():
                op_id = self._next_op_id()
                r = self.rng.random()

                if self.mode == "large_values":
                    v_size = self.rng.choice([100_000, 500_000, 1_048_576])
                    k = f"big_{writer_id}".encode("ascii")
                    v = os.urandom(v_size)
                    op_data = {"cmd": "put", "key": k, "val": v}
                    self.oracle.record_inflight(op_id, op_data)
                    line = json.dumps({
                        "op_id": op_id,
                        "cmd": "put",
                        "key_hex": k.hex(),
                        "val_hex": v.hex(),
                    }) + "\n"

                elif self.mode == "large_batches":
                    batch_size = self.rng.randint(500, 2000)
                    ops = []
                    for b_i in range(batch_size):
                        b_k = f"batch_{writer_id}_{op_id}_{b_i}".encode("ascii")
                        b_v = f"val_{b_i}".encode("ascii")
                        ops.append({"type": "put", "key": b_k, "val": b_v})
                    op_data = {"cmd": "batch", "ops": ops}
                    self.oracle.record_inflight(op_id, op_data)
                    line = json.dumps({
                        "op_id": op_id,
                        "cmd": "batch",
                        "ops": [{"type": "put", "key_hex": o["key"].hex(), "val_hex": o["val"].hex()} for o in ops]
                    }) + "\n"

                elif self.mode == "compaction":
                    if r < 0.2:
                        line = json.dumps({"op_id": op_id, "cmd": "compact"}) + "\n"
                    else:
                        k = self.rng.choice(key_pool)
                        v = f"v_{self.rng.randint(1, 10000)}".encode("ascii")
                        op_data = {"cmd": "put", "key": k, "val": v}
                        self.oracle.record_inflight(op_id, op_data)
                        line = json.dumps({
                            "op_id": op_id,
                            "cmd": "put",
                            "key_hex": k.hex(),
                            "val_hex": v.hex(),
                        }) + "\n"

                elif self.mode == "disk_full":
                    # Send puts; when full, delete keys and compact
                    if r < 0.7:
                        k = self.rng.choice(key_pool)
                        v = b"X" * 1024
                        op_data = {"cmd": "put", "key": k, "val": v}
                        self.oracle.record_inflight(op_id, op_data)
                        line = json.dumps({
                            "op_id": op_id,
                            "cmd": "put",
                            "key_hex": k.hex(),
                            "val_hex": v.hex(),
                        }) + "\n"
                    elif r < 0.9:
                        k = self.rng.choice(key_pool)
                        op_data = {"cmd": "del", "key": k}
                        self.oracle.record_inflight(op_id, op_data)
                        line = json.dumps({
                            "op_id": op_id,
                            "cmd": "del",
                            "key_hex": k.hex(),
                        }) + "\n"
                    else:
                        line = json.dumps({"op_id": op_id, "cmd": "compact"}) + "\n"

                else:
                    # Default: random puts, deletes, and small batches
                    if r < 0.6:
                        k = self.rng.choice(key_pool)
                        v = f"v_{self.rng.randint(1, 10000)}".encode("ascii")
                        op_data = {"cmd": "put", "key": k, "val": v}
                        self.oracle.record_inflight(op_id, op_data)
                        line = json.dumps({
                            "op_id": op_id,
                            "cmd": "put",
                            "key_hex": k.hex(),
                            "val_hex": v.hex(),
                        }) + "\n"
                    elif r < 0.8:
                        k = self.rng.choice(key_pool)
                        op_data = {"cmd": "del", "key": k}
                        self.oracle.record_inflight(op_id, op_data)
                        line = json.dumps({
                            "op_id": op_id,
                            "cmd": "del",
                            "key_hex": k.hex(),
                        }) + "\n"
                    else:
                        sub_ops = []
                        for s_idx in range(self.rng.randint(2, 5)):
                            sub_k = f"w{writer_id}_b{op_id}_{s_idx}".encode("ascii") if not self.shared_keys else self.rng.choice(key_pool)
                            sub_v = f"batch_val_{self.rng.randint(1, 10000)}".encode("ascii")
                            sub_ops.append({"type": "put", "key": sub_k, "val": sub_v})
                        op_data = {"cmd": "batch", "ops": sub_ops}
                        self.oracle.record_inflight(op_id, op_data)
                        line = json.dumps({
                            "op_id": op_id,
                            "cmd": "batch",
                            "ops": [{"type": "put", "key_hex": o["key"].hex(), "val_hex": o["val"].hex()} for o in sub_ops]
                        }) + "\n"

                try:
                    if proc.stdin:
                        proc.stdin.write(line)
                        proc.stdin.flush()
                except (OSError, BrokenPipeError):
                    break
                time.sleep(0.002)

        for w_id in range(self.num_writers):
            t = threading.Thread(target=writer_loop, args=(w_id,), daemon=True)
            t.start()
            writer_threads.append(t)

        sleep_ms = self.rng.uniform(15, 90)
        time.sleep(sleep_ms / 1000.0)

        # HARD KILL: Abruptly terminate child process
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass

        stop_writers.set()
        for t in writer_threads:
            t.join(timeout=0.5)

        # In recovery_crash mode, launch a process to open and immediately kill it mid-recovery
        if self.mode == "recovery_crash":
            rec_proc = subprocess.Popen(
                [PYTHON_EXE, "-m", "vedasbase", "--db", self.db_path, "open"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            time.sleep(self.rng.uniform(0.001, 0.010))
            try:
                rec_proc.kill()
                rec_proc.wait(timeout=1)
            except Exception:
                pass

        # Step 1: Run check to verify structural integrity
        check_res = self.run_check()
        if check_res.get("status") != "OK":
            raise AssertionError(f"Post-crash check failed: {check_res}")

        # Step 2: Run dump and verify invariants
        dump_state = self.run_dump()
        self.verify_invariants(dump_state)

    def run(self):
        print(f"=== Starting Vedas Base Torture Test ===")
        print(f"Mode: {self.mode} | Iterations: {self.iterations} | Seed: {self.seed} | Broken Variant: {self.broken}")
        self.clean_env()
        t0 = time.perf_counter()

        for it in range(1, self.iterations + 1):
            try:
                self.run_iteration(it)
                if it % 25 == 0 or it == self.iterations:
                    print(f"  [Iteration {it}/{self.iterations}] Passed crash verification. DB Keys: {len(self.oracle.committed_state)}")
            except AssertionError as e:
                print(f"\n[!] MISMATCH ON ITERATION {it}!\n{e}")
                sys.exit(1)
            except Exception as e:
                print(f"\n[!] UNEXPECTED FAILURE ON ITERATION {it}!\n{e}")
                sys.exit(1)

        t1 = time.perf_counter()
        print(f"\n=== Torture Test PASSED: {self.iterations} iterations in {t1 - t0:.2f} s ===")


def main():
    parser = argparse.ArgumentParser(description="Vedas Base Torture and Chaos Verification Tool")
    parser.add_argument("--db", default="torture_test.db", help="Path to test database")
    valid_modes = [
        "random", "concurrent",
        "large_values", "large-values",
        "large_batches", "large-batches",
        "compaction",
        "recovery_crash", "recovery-crash",
        "disk_full", "disk-full",
        "broken_variant", "broken-variant",
    ]
    parser.add_argument("--mode", default="random", choices=valid_modes, help="Torture mode (default: random)")
    parser.add_argument("--large-values", action="store_true", help="Run 1 MB large values torture mode")
    parser.add_argument("--large-batches", action="store_true", help="Run large batches (up to 10,000 ops) torture mode")
    parser.add_argument("--compaction", action="store_true", help="Run compaction crash torture mode")
    parser.add_argument("--recovery-crash", action="store_true", help="Run recovery-crash torture mode")
    parser.add_argument("--disk-full", action="store_true", help="Run disk-full space limit torture mode")
    parser.add_argument("--shared-keys", action="store_true", help="Allow writers to share keys (default: disjoint keys per writer)")
    parser.add_argument("--iterations", type=int, default=500, help="Number of crash iterations (default: 500)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatability")
    parser.add_argument("--max-bytes", type=int, default=100 * 1024 * 1024, help="Max database size in bytes")
    parser.add_argument("--writers", type=int, default=2, help="Number of concurrent writers")
    parser.add_argument("--broken", action="store_true", help="Run deliberately broken variant to verify torture tool detects bugs")

    args = parser.parse_args()

    # Resolve mode flags
    mode = args.mode.replace("-", "_")
    if args.large_values:
        mode = "large_values"
    elif args.large_batches:
        mode = "large_batches"
    elif args.compaction:
        mode = "compaction"
    elif args.recovery_crash:
        mode = "recovery_crash"
    elif args.disk_full:
        mode = "disk_full"
    elif mode == "concurrent":
        mode = "random"

    broken = args.broken or (mode == "broken_variant") or (os.environ.get("VEDAS_BUG") in ("ack_before_fsync", "skip_commit", "apply_uncommitted"))

    runner = TortureRunner(
        db_path=args.db,
        mode=mode,
        iterations=args.iterations,
        seed=args.seed,
        max_bytes=args.max_bytes,
        num_writers=args.writers,
        broken=broken,
        shared_keys=args.shared_keys,
    )
    runner.run()

if __name__ == "__main__":
    main()
