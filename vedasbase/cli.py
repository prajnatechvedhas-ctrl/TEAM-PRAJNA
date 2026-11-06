"""
Vedas Base - CLI and Server Interface.

Provides subcommands:
  open, put, get, del, batch, dump, check, compact, serve
"""

import argparse
import base64
import json
import os
import queue
import sys
import threading
from typing import List

from .engine import open_db, VedasDB
from .errors import VedasError
from .recovery import check_db_integrity

def decode_payload(data: dict, key_field: str = "key", b64_field: str = "key_b64", hex_field: str = "key_hex") -> bytes:
    """Extracts bytes from json dict supporting raw string, base64, or hex."""
    if b64_field in data and data[b64_field] is not None:
        return base64.b64decode(data[b64_field])
    if hex_field in data and data[hex_field] is not None:
        return bytes.fromhex(data[hex_field])
    if key_field in data and data[key_field] is not None:
        val = data[key_field]
        if isinstance(val, str):
            return val.encode("utf-8")
        elif isinstance(val, bytes):
            return val
        else:
            return str(val).encode("utf-8")
    raise ValueError(f"Missing payload field: {key_field}, {b64_field}, or {hex_field}")

def serve_loop(db: VedasDB, num_workers: int = 4):
    """
    Runs multi-threaded serve mode.
    Reads commands from stdin, distributes across worker threads sharing
    the engine's global lock, prints ACK <op-id> only after durability,
    and flushes stdout.
    """
    work_queue: queue.Queue = queue.Queue(maxsize=1024)
    stdout_lock = threading.Lock()
    stop_event = threading.Event()

    def worker_thread():
        while not stop_event.is_set():
            try:
                item = work_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if item is None:
                work_queue.task_done()
                break

            op_id, cmd, payload = item
            corrupt_early = os.environ.get("VEDAS_CORRUPT_ACK_EARLY") == "1"

            try:
                if corrupt_early and cmd in ("put", "del", "delete", "batch", "compact"):
                    # Deliberately broken variant: ACK before writing and fsyncing to disk!
                    with stdout_lock:
                        sys.stdout.write(f"ACK {op_id}\n")
                        sys.stdout.flush()

                if cmd == "put":
                    k = decode_payload(payload, "key", "key_b64", "key_hex")
                    v = decode_payload(payload, "val", "val_b64", "val_hex")
                    db.put(k, v)
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.write(f"ACK {op_id}\n")
                            sys.stdout.flush()

                elif cmd in ("del", "delete"):
                    k = decode_payload(payload, "key", "key_b64", "key_hex")
                    db.delete(k)
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.write(f"ACK {op_id}\n")
                            sys.stdout.flush()

                elif cmd == "get":
                    k = decode_payload(payload, "key", "key_b64", "key_hex")
                    val = db.get(k)
                    with stdout_lock:
                        if val is None:
                            sys.stdout.write(f"NOT_FOUND {op_id}\n")
                        else:
                            sys.stdout.write(f"VAL {op_id} {val.hex()}\n")
                        sys.stdout.flush()

                elif cmd == "batch":
                    ops_in = payload.get("ops", [])
                    norm_ops = []
                    for op_data in ops_in:
                        op_t = op_data.get("type", "put").lower()
                        k = decode_payload(op_data, "key", "key_b64", "key_hex")
                        if op_t == "put":
                            v = decode_payload(op_data, "val", "val_b64", "val_hex")
                            norm_ops.append(("put", k, v))
                        else:
                            norm_ops.append(("del", k))
                    db.batch(norm_ops)
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.write(f"ACK {op_id}\n")
                            sys.stdout.flush()

                elif cmd == "compact":
                    db.compact()
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.write(f"ACK {op_id}\n")
                            sys.stdout.flush()

                else:
                    with stdout_lock:
                        sys.stdout.write(f"ERR {op_id} Unknown command '{cmd}'\n")
                        sys.stdout.flush()

            except Exception as e:
                with stdout_lock:
                    err_msg = str(e).replace("\n", " ")
                    sys.stdout.write(f"ERR {op_id} {err_msg}\n")
                    sys.stdout.flush()
            finally:
                work_queue.task_done()

    threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker_thread, daemon=True)
        t.start()
        threads.append(t)

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except Exception as e:
                with stdout_lock:
                    sys.stdout.write(f"ERR unknown Invalid JSON: {e}\n")
                    sys.stdout.flush()
                continue

            op_id = data.get("op_id", "0")
            cmd = data.get("cmd", "").lower()
            if cmd in ("exit", "quit"):
                break

            work_queue.put((op_id, cmd, data))

        work_queue.join()
    finally:
        stop_event.set()
        for _ in threads:
            work_queue.put(None)
        for t in threads:
            t.join()

def main(args: List[str] = None):
    parser = argparse.ArgumentParser(description="Vedas Base Embedded Key-Value Database CLI")
    parser.add_argument("--db", required=True, help="Path to database data file")
    parser.add_argument("--max-bytes", type=int, default=10 * 1024 * 1024 * 1024, help="Max database size in bytes")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: open
    subparsers.add_parser("open", help="Open/recover database file and check status")

    # Subcommand: put
    p_put = subparsers.add_parser("put", help="Insert or update a key-value pair")
    p_put.add_argument("key", help="Key string (or hex if --hex)")
    p_put.add_argument("value", help="Value string (or hex if --hex)")
    p_put.add_argument("--hex", action="store_true", help="Treat key and value as hex strings")

    # Subcommand: get
    p_get = subparsers.add_parser("get", help="Retrieve value for a key")
    p_get.add_argument("key", help="Key string (or hex if --hex)")
    p_get.add_argument("--hex", action="store_true", help="Treat key as hex string")

    # Subcommand: del
    p_del = subparsers.add_parser("del", help="Delete a key")
    p_del.add_argument("key", help="Key string (or hex if --hex)")
    p_del.add_argument("--hex", action="store_true", help="Treat key as hex string")

    # Subcommand: batch
    p_batch = subparsers.add_parser("batch", help="Execute a JSON batch of operations")
    p_batch.add_argument("json_ops", nargs="?", help="JSON array of operations")

    # Subcommand: dump
    subparsers.add_parser("dump", help="Dump all live keys and values sorted by key")

    # Subcommand: check
    subparsers.add_parser("check", help="Validate file integrity and check for corruption")

    # Subcommand: compact
    subparsers.add_parser("compact", help="Reclaim disk space by compacting live keys")

    # Subcommand: serve
    p_serve = subparsers.add_parser("serve", help="Run database server on stdin/stdout")
    p_serve.add_argument("--workers", type=int, default=4, help="Number of concurrent writer threads")

    parsed = parser.parse_args(args)
    cmd = parsed.command

    if cmd == "check":
        res = check_db_integrity(parsed.db)
        print(json.dumps(res, indent=2))
        if res.get("status") != "OK":
            sys.exit(1)
        return

    if cmd == "dump" and not os.path.exists(parsed.db):
        return

    # For other commands, open the database engine
    try:
        db = open_db(parsed.db, max_bytes=parsed.max_bytes)
    except Exception as e:
        sys.stderr.write(f"Error opening database: {e}\n")
        sys.exit(1)

    try:
        if cmd == "open":
            print(f"OK keys={len(db._index)} size={db._last_committed_offset}")

        elif cmd == "put":
            k = bytes.fromhex(parsed.key) if parsed.hex else parsed.key.encode("utf-8")
            v = bytes.fromhex(parsed.value) if parsed.hex else parsed.value.encode("utf-8")
            db.put(k, v)
            print("OK")

        elif cmd == "get":
            k = bytes.fromhex(parsed.key) if parsed.hex else parsed.key.encode("utf-8")
            v = db.get(k)
            if v is None:
                print("NOT_FOUND")
                sys.exit(2)
            else:
                if parsed.hex:
                    print(v.hex())
                else:
                    try:
                        print(v.decode("utf-8"))
                    except UnicodeDecodeError:
                        print(v.hex())

        elif cmd == "del":
            k = bytes.fromhex(parsed.key) if parsed.hex else parsed.key.encode("utf-8")
            db.delete(k)
            print("OK")

        elif cmd == "batch":
            raw_json = parsed.json_ops
            if not raw_json:
                raw_json = sys.stdin.read()
            ops_data = json.loads(raw_json)
            norm_ops = []
            for op in ops_data:
                op_t = op.get("type", "put").lower()
                k = decode_payload(op, "key", "key_b64", "key_hex")
                if op_t == "put":
                    v = decode_payload(op, "val", "val_b64", "val_hex")
                    norm_ops.append(("put", k, v))
                else:
                    norm_ops.append(("del", k))
            db.batch(norm_ops)
            print("OK")

        elif cmd == "dump":
            pairs = db.dump()
            for k, v in pairs:
                # Output format: <hex_key> <hex_value>
                print(f"{k.hex()} {v.hex()}")

        elif cmd == "compact":
            new_size = db.compact()
            print(f"OK new_size={new_size}")

        elif cmd == "serve":
            serve_loop(db, num_workers=parsed.workers)

    finally:
        db.close()

if __name__ == "__main__":
    main()
