"""
Vedas Base - Storage Engine Core.

Implements the append-only log with in-memory hash index, strictly adhering
to the 7-step write ordering under a single global lock.
"""

import os
import threading
from typing import Dict, List, Optional, Tuple, Union

from .errors import (
    ClosedError,
    DiskFullError,
    InvalidInputError,
    VedasError,
)
from .format import (
    FILE_HEADER_SIZE,
    MAX_BATCH_OPS,
    MIN_KEY_LEN,
    MAX_KEY_LEN,
    MIN_VAL_LEN,
    MAX_VAL_LEN,
    TYPE_PUT,
    TYPE_DELETE,
    encode_put_record,
    encode_delete_record,
    encode_commit_record,
    validate_key,
    validate_value,
)
from .lock import FileLock
from .recovery import recover
from .compaction import run_compaction

class VedasDB:
    """
    Vedas Base single-process embedded key-value database engine.
    """

    def __init__(self, path: str, max_bytes: int = 10 * 1024 * 1024 * 1024):
        if max_bytes < FILE_HEADER_SIZE + 44:  # At least header + minimal op + commit
            raise InvalidInputError(f"max_bytes {max_bytes} is too small to hold a database.")
        
        self._path = os.path.abspath(path)
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._file_lock = FileLock(self._path)
        self._closed = True
        self._fd = None
        self._index: Dict[bytes, Tuple[int, int]] = {}
        self._next_txid = 1
        self._last_committed_offset = FILE_HEADER_SIZE

        # Acquire exclusive OS process lock
        self._file_lock.acquire()
        try:
            # Open or create database file with binary mode
            flags = os.O_CREAT | os.O_RDWR
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            self._fd = os.open(self._path, flags, 0o644)

            # Run crash recovery and index rebuild
            self._index, self._next_txid, self._last_committed_offset = recover(
                self._fd, self._path
            )
            self._closed = False
        except Exception:
            if self._fd is not None:
                try:
                    os.close(self._fd)
                except OSError:
                    pass
                self._fd = None
            self._file_lock.release()
            raise

    @property
    def is_closed(self) -> bool:
        return self._closed

    def _ensure_open(self):
        if self._closed or self._fd is None:
            raise ClosedError("Database is closed.")

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieves the value for the given key, or None if not found.
        Takes the global lock so it never reads a half-applied batch.
        """
        k = validate_key(key)
        with self._lock:
            self._ensure_open()
            if k not in self._index:
                return None
            val_off, val_len = self._index[k]
            os.lseek(self._fd, val_off, os.SEEK_SET)
            val = os.read(self._fd, val_len)
            if len(val) != val_len:
                raise VedasError(f"Failed to read complete value at offset {val_off}")
            return val

    def put(self, key: bytes, value: bytes):
        """
        Writes a single key-value pair as an atomic batch of 1 op.
        """
        self.batch([("put", key, value)])

    def delete(self, key: bytes):
        """
        Deletes a single key as an atomic batch of 1 op.
        """
        self.batch([("del", key)])

    def batch(self, ops: List[Union[Tuple, List]]):
        """
        Applies a batch of put and delete operations atomically.
        Batch semantics ("B5"): Operations are applied in order,
        so the last op on a key wins.
        """
        if not ops:
            return

        if len(ops) > MAX_BATCH_OPS:
            raise InvalidInputError(
                f"Batch exceeds maximum limit of {MAX_BATCH_OPS} operations: got {len(ops)}"
            )

        # Pre-validate ALL inputs before acquiring lock or writing anything
        norm_ops = []
        needed_bytes = 0
        for item in ops:
            if not isinstance(item, (tuple, list)) or len(item) < 2:
                raise InvalidInputError(f"Invalid operation format: {item}")
            op_type = item[0]
            if op_type in (TYPE_PUT, "put", "PUT"):
                if len(item) != 3:
                    raise InvalidInputError(f"PUT operation requires key and value: {item}")
                k = validate_key(item[1])
                v = validate_value(item[2])
                norm_ops.append((TYPE_PUT, k, v))
                needed_bytes += 20 + len(k) + len(v)
            elif op_type in (TYPE_DELETE, "del", "DEL", "delete", "DELETE"):
                if len(item) not in (2, 3):
                    raise InvalidInputError(f"DELETE operation requires key: {item}")
                k = validate_key(item[1])
                norm_ops.append((TYPE_DELETE, k, b""))
                needed_bytes += 20 + len(k)
            else:
                raise InvalidInputError(f"Unknown operation type: {op_type}")

        # Add COMMIT record size (24 bytes)
        needed_bytes += 24

        with self._lock:
            self._ensure_open()

            # Fault injection hook: ACK before fsync/write
            corrupt_ack_early = os.environ.get("VEDAS_CORRUPT_ACK_EARLY") == "1"
            skip_commit_marker = os.environ.get("VEDAS_SKIP_COMMIT_MARKER") == "1"
            skip_commit_fsync = os.environ.get("VEDAS_SKIP_COMMIT_FSYNC") == "1"

            # -------------------------------------------------------------
            # EXACT 7-STEP WRITE ORDERING UNDER GLOBAL LOCK
            # -------------------------------------------------------------

            # STEP 1: Check max_bytes BEFORE writing.
            # Disk state: Exactly unchanged at self._last_committed_offset.
            current_file_size = self._last_committed_offset
            if current_file_size + needed_bytes > self._max_bytes:
                raise DiskFullError(
                    f"Write would exceed max_bytes ({current_file_size + needed_bytes} > {self._max_bytes})"
                )

            txid = self._next_txid
            rollback_offset = self._last_committed_offset

            try:
                # Seek to current committed tail
                os.lseek(self._fd, current_file_size, os.SEEK_SET)

                # STEP 2: Write the op record(s).
                # Disk state if killed here: Uncommitted ops without commit marker.
                # Recovery action: Ignored and truncated back to rollback_offset.
                op_offsets = []  # Track for index update
                write_pos = current_file_size
                for op_type, k, v in norm_ops:
                    if op_type == TYPE_PUT:
                        rec = encode_put_record(k, v, txid)
                        val_off = write_pos + 16 + len(k)
                        op_offsets.append((TYPE_PUT, k, val_off, len(v)))
                    else:
                        rec = encode_delete_record(k, txid)
                        op_offsets.append((TYPE_DELETE, k, 0, 0))
                    
                    nwritten = os.write(self._fd, rec)
                    if nwritten != len(rec):
                        raise VedasError("Short write during op record output")
                    write_pos += len(rec)

                # STEP 3: os.fsync(fd).
                # Disk state if killed here: Op records durable on disk, but uncommitted.
                # Recovery action: Op count mismatch or missing commit marker; ops ignored and truncated.
                os.fsync(self._fd)

                # STEP 4: Write the COMMIT marker.
                # Disk state if killed here: Commit marker partially or fully written to kernel buffers.
                # If not durable, CRC check or EOF truncates cleanly.
                if not skip_commit_marker:
                    commit_rec = encode_commit_record(len(norm_ops), txid)
                    nwritten = os.write(self._fd, commit_rec)
                    if nwritten != len(commit_rec):
                        raise VedasError("Short write during commit marker output")
                    write_pos += len(commit_rec)

                # STEP 5: os.fsync(fd).
                # Disk state if killed here: Both op records and commit marker are durable.
                # Recovery action: Replay reconstructs complete committed batch.
                if not skip_commit_fsync:
                    os.fsync(self._fd)

                # STEP 6: Update the in-memory index in one step, still under the lock.
                for op_type, k, v_off, v_len in op_offsets:
                    if op_type == TYPE_PUT:
                        self._index[k] = (v_off, v_len)
                    else:
                        self._index.pop(k, None)

                self._last_committed_offset = write_pos
                self._next_txid = txid + 1

                # STEP 7: Return success. Never return before step 5 completes.
                return

            except Exception:
                # If write raises mid-way (e.g. ENOSPC), truncate back to last
                # committed offset and keep the database readable.
                try:
                    os.ftruncate(self._fd, rollback_offset)
                    os.fsync(self._fd)
                    os.lseek(self._fd, rollback_offset, os.SEEK_SET)
                except OSError:
                    pass
                raise

    def compact(self) -> int:
        """
        Reclaims disk space by compacting live keys into a new file.
        Takes the global lock.
        """
        with self._lock:
            self._ensure_open()
            return run_compaction(self)

    def dump(self) -> List[Tuple[bytes, bytes]]:
        """
        Returns all live (key, value) pairs sorted lexicographically by key.
        """
        with self._lock:
            self._ensure_open()
            result = []
            for k in sorted(self._index.keys()):
                val_off, val_len = self._index[k]
                os.lseek(self._fd, val_off, os.SEEK_SET)
                val = os.read(self._fd, val_len)
                if len(val) != val_len:
                    raise VedasError(f"Failed to read complete value for dump at {val_off}")
                result.append((k, val))
            return result

    def close(self):
        """
        Flushes and closes the database and releases the file lock.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._fd is not None:
                try:
                    os.fsync(self._fd)
                except OSError:
                    pass
                try:
                    os.close(self._fd)
                except OSError:
                    pass
                self._fd = None
            self._file_lock.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def open_db(path: str, max_bytes: int = 10 * 1024 * 1024 * 1024) -> VedasDB:
    """Convenience factory function matching open(path, max_bytes)."""
    return VedasDB(path, max_bytes)

