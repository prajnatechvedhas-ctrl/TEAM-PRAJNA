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
import time
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
            vedas_bug = os.environ.get("VEDAS_BUG", "").lower()
            corrupt_early = os.environ.get("VEDAS_CORRUPT_ACK_EARLY") == "1" or vedas_bug == "ack_before_fsync"

            try:
                if corrupt_early and cmd in ("put", "del", "delete", "batch", "compact"):
                    # Deliberately broken variant: ACK before writing and fsyncing to disk!
                    with stdout_lock:
                        sys.stdout.buffer.write(f"ACK {op_id}\n".encode("ascii"))
                        sys.stdout.buffer.flush()

                if cmd == "put":
                    k = decode_payload(payload, "key", "key_b64", "key_hex")
                    v = decode_payload(payload, "val", "val_b64", "val_hex")
                    db.put(k, v)
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.buffer.write(f"ACK {op_id}\n".encode("ascii"))
                            sys.stdout.buffer.flush()

                elif cmd in ("del", "delete"):
                    k = decode_payload(payload, "key", "key_b64", "key_hex")
                    db.delete(k)
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.buffer.write(f"ACK {op_id}\n".encode("ascii"))
                            sys.stdout.buffer.flush()

                elif cmd == "get":
                    k = decode_payload(payload, "key", "key_b64", "key_hex")
                    val = db.get(k)
                    with stdout_lock:
                        if val is None:
                            sys.stdout.buffer.write(f"NOT_FOUND {op_id}\n".encode("ascii"))
                        else:
                            sys.stdout.buffer.write(f"VAL {op_id} {val.hex()}\n".encode("ascii"))
                        sys.stdout.buffer.flush()

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
                            sys.stdout.buffer.write(f"ACK {op_id}\n".encode("ascii"))
                            sys.stdout.buffer.flush()

                elif cmd == "compact":
                    db.compact()
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.buffer.write(f"ACK {op_id}\n".encode("ascii"))
                            sys.stdout.buffer.flush()

                elif cmd == "close":
                    if not corrupt_early:
                        with stdout_lock:
                            sys.stdout.buffer.write(f"ACK {op_id}\n".encode("ascii"))
                            sys.stdout.buffer.flush()
                    stop_event.set()

                else:
                    with stdout_lock:
                        sys.stdout.buffer.write(f"ERR {op_id} Unknown command '{cmd}'\n".encode("utf-8"))
                        sys.stdout.buffer.flush()

            except Exception as e:
                with stdout_lock:
                    err_msg = str(e).replace("\n", " ")
                    sys.stdout.buffer.write(f"ERR {op_id} {err_msg}\n".encode("utf-8"))
                    sys.stdout.buffer.flush()
            finally:
                work_queue.task_done()

    threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker_thread, daemon=True)
        t.start()
        threads.append(t)

    try:
        # Use binary input buffer on Windows to avoid newline translation
        manual_counter = 0
        batch_mode = False
        batch_id = ""
        batch_ops = []

        for raw_line in sys.stdin.buffer:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            line_str = raw_line.decode("utf-8", errors="replace").strip()
            if not line_str or line_str.startswith("#"):
                continue

            # Check if collecting a multi-line batch block
            if batch_mode:
                if line_str.lower() in ("end", "commit"):
                    batch_mode = False
                    work_queue.put((batch_id, "batch", {"op_id": batch_id, "cmd": "batch", "ops": batch_ops}))
                    batch_ops = []
                    continue

                b_parts = line_str.split(" ", 2)
                sub_cmd = b_parts[0].lower()
                if sub_cmd == "put":
                    k = b_parts[1]
                    v = b_parts[2] if len(b_parts) > 2 else ""
                    batch_ops.append({"type": "put", "key": k, "val": v})
                elif sub_cmd in ("del", "delete"):
                    k = b_parts[1]
                    batch_ops.append({"type": "del", "key": k})
                else:
                    with stdout_lock:
                        sys.stdout.buffer.write(f"ERR {batch_id} Unknown batch op: {line_str}\n".encode("utf-8"))
                        sys.stdout.buffer.flush()
                continue

            if line_str.startswith("{"):
                try:
                    data = json.loads(line_str)
                except Exception as e:
                    with stdout_lock:
                        sys.stdout.buffer.write(f"ERR unknown Invalid JSON: {e}\n".encode("utf-8"))
                        sys.stdout.buffer.flush()
                    continue
                op_id = data.get("op_id", "0")
                cmd = data.get("cmd", "").lower()
                if cmd in ("exit", "quit"):
                    break
                work_queue.put((op_id, cmd, data))
                continue

            # Human-friendly line syntax:
            # Check for batch start: "batch <op-id>" or "begin <op-id>"
            parts = line_str.split(" ", 1)
            first_tok = parts[0].lower()
            if first_tok in ("batch", "begin"):
                batch_mode = True
                manual_counter += 1
                batch_id = parts[1].strip() if len(parts) > 1 else f"b{manual_counter}"
                batch_ops = []
                continue

            # Check if line starts with an op_id: "<op-id> <cmd> [args...]"
            KNOWN_CMDS = {"put", "get", "del", "delete", "compact", "close", "exit", "quit"}
            tokens = line_str.split(" ", 3)
            if len(tokens) >= 2 and tokens[1].lower() in KNOWN_CMDS:
                op_id = tokens[0]
                t_cmd = tokens[1].lower()
                rest = tokens[2:]
            elif tokens[0].lower() in KNOWN_CMDS:
                manual_counter += 1
                op_id = f"m{manual_counter}"
                t_cmd = tokens[0].lower()
                rest = tokens[1:]
            else:
                with stdout_lock:
                    sys.stdout.buffer.write(f"ERR unknown Unknown command line: {line_str}\n".encode("utf-8"))
                    sys.stdout.buffer.flush()
                continue

            if t_cmd in ("exit", "quit"):
                break

            if t_cmd == "put":
                if len(rest) < 1:
                    with stdout_lock:
                        sys.stdout.buffer.write(f"ERR {op_id} put requires key and value\n".encode("utf-8"))
                        sys.stdout.buffer.flush()
                    continue
                k = rest[0]
                v = rest[1] if len(rest) > 1 else ""
                work_queue.put((op_id, "put", {"op_id": op_id, "cmd": "put", "key": k, "val": v}))
            elif t_cmd == "get":
                k = rest[0] if len(rest) > 0 else ""
                work_queue.put((op_id, "get", {"op_id": op_id, "cmd": "get", "key": k}))
            elif t_cmd in ("del", "delete"):
                k = rest[0] if len(rest) > 0 else ""
                work_queue.put((op_id, "del", {"op_id": op_id, "cmd": "del", "key": k}))
            elif t_cmd == "compact":
                work_queue.put((op_id, "compact", {"op_id": op_id, "cmd": "compact"}))
            elif t_cmd == "close":
                work_queue.put((op_id, "close", {"op_id": op_id, "cmd": "close"}))

        work_queue.join()
    finally:
        stop_event.set()
        for _ in threads:
            work_queue.put(None)
        for t in threads:
            t.join()

def normalize_cli_args(args: List[str]) -> List[str]:
    """
    Normalizes CLI arguments so both forms work interchangeably:
      ./kvdb <cmd> <path> [args...]
      ./kvdb --db <path> <cmd> [args...]
    """
    if not args:
        return args

    valid_cmds = {"open", "put", "get", "del", "delete", "batch", "dump", "check", "compact", "serve"}

    # If --db is already in args, leave as is
    if "--db" in args:
        return list(args)

    # Find the first argument that is a known subcommand
    for idx, arg in enumerate(args):
        if arg in valid_cmds:
            # Check if the subsequent argument is a database path (doesn't start with '-')
            if idx + 1 < len(args) and not args[idx + 1].startswith("-"):
                db_path = args[idx + 1]
                new_args = list(args)
                new_args.pop(idx + 1)
                return ["--db", db_path] + new_args
            break

    return list(args)

def main(args: List[str] = None):
    if args is None:
        args = sys.argv[1:]

    args = normalize_cli_args(args)

    # Common options parent parser so --db can appear before OR after subcommand
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--db", default=argparse.SUPPRESS, help="Path to database data file")
    parent_parser.add_argument("--max-bytes", type=int, default=10 * 1024 * 1024 * 1024, help="Max database size in bytes")

    parser = argparse.ArgumentParser(
        description="Vedas Base Embedded Key-Value Database CLI",
        parents=[parent_parser],
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: open
    subparsers.add_parser("open", parents=[parent_parser], help="Open/recover database file and check status")

    # Subcommand: put
    p_put = subparsers.add_parser("put", parents=[parent_parser], help="Insert or update a key-value pair")
    p_put.add_argument("key", help="Key string (or hex if --hex)")
    p_put.add_argument("value", help="Value string (or hex if --hex)")
    p_put.add_argument("--hex", action="store_true", help="Treat key and value as hex strings")

    # Subcommand: get
    p_get = subparsers.add_parser("get", parents=[parent_parser], help="Retrieve value for a key")
    p_get.add_argument("key", help="Key string (or hex if --hex)")
    p_get.add_argument("--hex", action="store_true", help="Treat key as hex string")

    # Subcommand: del / delete
    p_del = subparsers.add_parser("del", parents=[parent_parser], help="Delete a key")
    p_del.add_argument("key", help="Key string (or hex if --hex)")
    p_del.add_argument("--hex", action="store_true", help="Treat key as hex string")

    # Subcommand: batch
    p_batch = subparsers.add_parser("batch", parents=[parent_parser], help="Execute a batch of operations")
    p_batch.add_argument("batch_input", nargs="?", default=None, help="File path with 'put k v'/'del k', JSON array, or '-' for stdin")

    # Subcommand: dump
    p_dump = subparsers.add_parser("dump", parents=[parent_parser], help="Dump all live keys and values sorted by key")
    p_dump.add_argument("--hex", action="store_true", help="Format keys and values as hex")

    # Subcommand: check
    p_check = subparsers.add_parser("check", parents=[parent_parser], help="Validate file integrity and check for corruption")
    p_check.add_argument("--json", action="store_true", help="Output JSON details")

    # Subcommand: compact
    subparsers.add_parser("compact", parents=[parent_parser], help="Reclaim disk space by compacting live keys")

    # Subcommand: serve
    p_serve = subparsers.add_parser("serve", parents=[parent_parser], help="Run database server on stdin/stdout")
    p_serve.add_argument("--workers", type=int, default=4, help="Number of concurrent writer threads")

    parsed = parser.parse_args(args)
    db_path = getattr(parsed, "db", None)
    if not db_path:
        parser.error("The database <path> or --db <path> argument is required.")
    parsed.db = db_path
    cmd = parsed.command

    if cmd == "check":
        res = check_db_integrity(parsed.db)
        if getattr(parsed, "json", False):
            print(json.dumps(res, indent=2))
        else:
            if res.get("status") == "OK":
                print("OK")
            else:
                sys.stderr.write(f"CORRUPTION: {res.get('reason', 'Integrity check failed')}\n")
        if res.get("status") != "OK":
            sys.exit(1)
        return

    if cmd == "dump" and not os.path.exists(parsed.db):
        return

    # For other commands, open the database engine (measuring recovery time)
    t0 = time.perf_counter()
    try:
        db = open_db(parsed.db, max_bytes=parsed.max_bytes)
    except Exception as e:
        sys.stderr.write(f"Error opening database: {e}\n")
        sys.exit(1)
    recovery_sec = time.perf_counter() - t0

    try:
        if cmd == "open":
            print(f"OK keys={len(db._index)} size={db._last_committed_offset} recovery_time={recovery_sec:.4f}s")

        elif cmd == "put":
            k = bytes.fromhex(parsed.key) if parsed.hex else parsed.key.encode("utf-8")
            v = bytes.fromhex(parsed.value) if parsed.hex else parsed.value.encode("utf-8")
            db.put(k, v)
            print("OK")

        elif cmd == "get":
            k = bytes.fromhex(parsed.key) if parsed.hex else parsed.key.encode("utf-8")
            v = db.get(k)
            if v is None:
                print("NOTFOUND")
                sys.exit(2)
            else:
                if parsed.hex:
                    print(v.hex())
                else:
                    try:
                        print(v.decode("utf-8"))
                    except UnicodeDecodeError:
                        print(v.hex())

        elif cmd in ("del", "delete"):
            k = bytes.fromhex(parsed.key) if parsed.hex else parsed.key.encode("utf-8")
            db.delete(k)
            print("OK")

        elif cmd == "batch":
            raw_input = parsed.batch_input
            if not raw_input or raw_input == "-":
                raw_input = sys.stdin.read()
            elif os.path.isfile(raw_input):
                with open(raw_input, "r", encoding="utf-8") as f:
                    raw_input = f.read()

            raw_stripped = raw_input.strip()
            norm_ops = []
            if raw_stripped.startswith("[") or raw_stripped.startswith("{"):
                ops_data = json.loads(raw_stripped)
                if isinstance(ops_data, dict) and "ops" in ops_data:
                    ops_data = ops_data["ops"]
                for op in ops_data:
                    op_t = op.get("type", op.get("cmd", "put")).lower()
                    k = decode_payload(op, "key", "key_b64", "key_hex")
                    if op_t == "put":
                        v = decode_payload(op, "val", "val_b64", "val_hex")
                        norm_ops.append(("put", k, v))
                    else:
                        norm_ops.append(("del", k))
            else:
                # File format with one op per line: "put k v" or "del k"
                for line in raw_stripped.splitlines():
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split(" ", 2)
                    op_type = parts[0].lower()
                    if op_type == "put":
                        k = parts[1].encode("utf-8")
                        v = parts[2].encode("utf-8") if len(parts) > 2 else b""
                        norm_ops.append(("put", k, v))
                    elif op_type in ("del", "delete"):
                        k = parts[1].encode("utf-8")
                        norm_ops.append(("del", k))
                    else:
                        raise ValueError(f"Unknown batch op in input: '{line}'")

            db.batch(norm_ops)
            print("OK")

        elif cmd == "dump":
            pairs = db.dump()
            for k, v in pairs:
                if getattr(parsed, "hex", False):
                    print(f"{k.hex()} {v.hex()}")
                else:
                    try:
                        k_str = k.decode("utf-8")
                        v_str = v.decode("utf-8")
                        if " " not in k_str and "\n" not in k_str and "\r" not in k_str and "\n" not in v_str and "\r" not in v_str:
                            print(f"{k_str} {v_str}")
                        else:
                            print(f"{k.hex()} {v.hex()}")
                    except UnicodeDecodeError:
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
