"""
Vedas Base - Atomic Online Compaction.

Reclaims disk space by writing all live keys and values to a temporary file,
committing with a single COMMIT record, fsyncing, atomically replacing the old
file with os.replace, and fsyncing the directory (on POSIX).
"""

import os
import struct
from typing import Dict, Tuple

from .errors import DiskFullError, VedasError
from .format import (
    FILE_HEADER_SIZE,
    encode_file_header,
    encode_put_record,
    encode_commit_record,
)

def fsync_directory_if_supported(dir_path: str):
    """Fsyncs directory on POSIX systems; safely skips on Windows."""
    if os.name == "nt":
        # Windows does not support directory fsync; documented in specification
        return
    try:
        dir_fd = os.open(dir_path or ".", os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass

def run_compaction(engine) -> int:
    """
    Executes compaction under the engine's global lock.
    Returns the new file size in bytes.
    """
    db_path = engine._path
    dir_name = os.path.dirname(os.path.abspath(db_path)) or "."
    temp_path = f"{db_path}.compact.{os.getpid()}_{engine._next_txid}.tmp"

    # Step 1: Pre-calculate required size for live keys
    live_count = len(engine._index)
    total_needed = FILE_HEADER_SIZE
    for k, (_, val_len) in engine._index.items():
        total_needed += 20 + len(k) + val_len
    total_needed += 24  # Commit marker

    if total_needed > engine._max_bytes:
        raise DiskFullError(
            f"Compaction requires {total_needed} bytes, which exceeds max_bytes limit {engine._max_bytes}"
        )

    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY

    temp_fd = None
    try:
        temp_fd = os.open(temp_path, flags, 0o644)

        # Step 2: Write file header
        os.write(temp_fd, encode_file_header())

        # Step 3: Stream live key/value pairs
        new_index: Dict[bytes, Tuple[int, int]] = {}
        cur_offset = FILE_HEADER_SIZE
        txid = 1

        for k, (val_off, val_len) in engine._index.items():
            # Read value from source database file
            os.lseek(engine._fd, val_off, os.SEEK_SET)
            val_bytes = os.read(engine._fd, val_len)
            if len(val_bytes) != val_len:
                raise VedasError(f"Failed to read live value for key during compaction")

            rec_bytes = encode_put_record(k, val_bytes, txid)
            os.write(temp_fd, rec_bytes)

            new_val_offset = cur_offset + 16 + len(k)
            new_index[k] = (new_val_offset, val_len)
            cur_offset += len(rec_bytes)

        # Step 4: Write COMMIT marker
        commit_bytes = encode_commit_record(live_count, txid)
        os.write(temp_fd, commit_bytes)
        cur_offset += len(commit_bytes)

        # Step 5: Fsync temp file and close
        os.fsync(temp_fd)
        os.close(temp_fd)
        temp_fd = None

        # Step 6: Close original file descriptor before atomic replace (critical on Windows)
        os.close(engine._fd)
        engine._fd = None

        # Step 7: Atomic rename
        os.replace(temp_path, db_path)

        # Step 8: Fsync directory
        fsync_directory_if_supported(dir_name)

        # Step 9: Re-open db_path in read/write mode
        rw_flags = os.O_RDWR
        if hasattr(os, "O_BINARY"):
            rw_flags |= os.O_BINARY
        engine._fd = os.open(db_path, rw_flags, 0o644)

        # Step 10: Commit state update
        engine._index = new_index
        engine._last_committed_offset = cur_offset
        engine._next_txid = txid + 1
        os.lseek(engine._fd, cur_offset, os.SEEK_SET)

        return cur_offset

    except Exception:
        # If compaction failed before replace, clean up temp file and ensure source fd is valid
        if temp_fd is not None:
            try:
                os.close(temp_fd)
            except OSError:
                pass
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        if engine._fd is None:
            rw_flags = os.O_RDWR
            if hasattr(os, "O_BINARY"):
                rw_flags |= os.O_BINARY
            engine._fd = os.open(db_path, rw_flags, 0o644)
            os.lseek(engine._fd, engine._last_committed_offset, os.SEEK_SET)
        raise
