"""
Vedas Base - High-Performance Recovery and Validation.

Scans the append-only log sequentially with large buffered memoryviews,
validates CRC32 on every record, enforces atomic commit markers,
and truncates torn or uncommitted tails idempotently.
"""

import io
import os
import glob
import struct
import zlib
from typing import Dict, Tuple, Optional

from .errors import CorruptionError
from .format import (
    FILE_MAGIC,
    FILE_VERSION,
    FILE_HEADER_SIZE,
    FILE_HEADER_FMT,
    REC_MAGIC,
    REC_HEADER_SIZE,
    REC_TRAILER_SIZE,
    REC_HEADER_FMT,
    TYPE_PUT,
    TYPE_DELETE,
    TYPE_COMMIT,
    MAX_KEY_LEN,
    MAX_VAL_LEN,
    encode_file_header,
)

RECOVERY_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB sequential read buffer

def _find_valid_commit_after(fd: int, start_offset: int, file_size: int, min_txid: int = 0) -> Optional[int]:
    """
    Scans from start_offset to EOF searching for any valid COMMIT marker with txid > min_txid.
    Returns the offset of a valid subsequent commit marker if found, or None.
    Distinguishes a torn uncommitted tail at EOF from mid-file corruption.
    """
    if start_offset >= file_size:
        return None

    scan_buf_size = 1024 * 1024
    cur = start_offset
    unpack_from = struct.unpack_from
    crc32 = zlib.crc32

    while cur < file_size:
        to_read = min(scan_buf_size, file_size - cur)
        os.lseek(fd, cur, os.SEEK_SET)
        chunk = os.read(fd, to_read)
        if not chunk:
            break

        idx = 0
        while True:
            idx = chunk.find(b"\xa7", idx)
            if idx == -1:
                break
            cand_offset = cur + idx
            if cand_offset + 24 <= file_size:
                os.lseek(fd, cand_offset, os.SEEK_SET)
                cand_bytes = os.read(fd, 24)
                if len(cand_bytes) == 24:
                    rmagic, rtype, k_len, v_len, rtxid = unpack_from(REC_HEADER_FMT, cand_bytes, 0)
                    if rmagic == REC_MAGIC and rtype == TYPE_COMMIT and k_len == 0 and v_len == 4:
                        expected_crc, = unpack_from("<I", cand_bytes, 20)
                        calc_crc = crc32(cand_bytes[:20]) & 0xFFFFFFFF
                        if expected_crc == calc_crc and rtxid > min_txid:
                            return cand_offset
            idx += 1

        cur += to_read

    return None

def cleanup_temp_files(db_path: str):
    """Deletes any leftover compaction or temporary files from previous crashed sessions."""
    dir_name = os.path.dirname(os.path.abspath(db_path)) or "."
    base_name = os.path.basename(db_path)
    pattern = os.path.join(dir_name, f"{base_name}.compact.*.tmp")
    for tmp_file in glob.glob(pattern):
        try:
            os.unlink(tmp_file)
        except OSError:
            pass
    # Also check generic .tmp
    single_tmp = os.path.join(dir_name, f"{base_name}.tmp")
    if os.path.exists(single_tmp):
        try:
            os.unlink(single_tmp)
        except OSError:
            pass

def recover(fd: int, db_path: str) -> Tuple[Dict[bytes, Tuple[int, int]], int, int]:
    """
    Recovers the database state from the open file descriptor.

    Returns:
        (index, next_txid, last_valid_commit_offset)
        where index maps key (bytes) -> (value_offset, value_len)
    """
    cleanup_temp_files(db_path)

    stat = os.fstat(fd)
    file_size = stat.st_size

    # Handle empty or fresh file
    if file_size == 0:
        hdr = encode_file_header()
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, hdr)
        os.fsync(fd)
        return {}, 1, FILE_HEADER_SIZE

    # Validate header
    if file_size < FILE_HEADER_SIZE:
        # Partial write during very first creation before any commit.
        # Idempotently re-initialize header.
        hdr = encode_file_header()
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, hdr)
        os.fsync(fd)
        return {}, 1, FILE_HEADER_SIZE

    os.lseek(fd, 0, os.SEEK_SET)
    header_bytes = os.read(fd, FILE_HEADER_SIZE)
    if len(header_bytes) != FILE_HEADER_SIZE:
        raise CorruptionError("Failed to read file header.")

    magic, ver, _ = struct.unpack_from(FILE_HEADER_FMT, header_bytes, 0)
    if magic != FILE_MAGIC or ver != FILE_VERSION:
        raise CorruptionError(
            f"Invalid file header: magic={magic!r}, version={ver}. Expected {FILE_MAGIC!r}, {FILE_VERSION}."
        )

    # Fast sequential recovery using buffered reads and memoryviews
    index: Dict[bytes, Tuple[int, int]] = {}
    pending_ops = []  # List of (type, key, val_off, val_len, rtxid)
    last_valid_commit_offset = FILE_HEADER_SIZE
    max_txid = 0
    torn_detected = False
    torn_offset = FILE_HEADER_SIZE
    corrupt_txid = 0
    start_search = FILE_HEADER_SIZE

    unpack_from = struct.unpack_from
    crc32 = zlib.crc32

    buffer = bytearray(RECOVERY_CHUNK_SIZE)
    view = memoryview(buffer)
    buf_len = 0
    file_offset = FILE_HEADER_SIZE

    # Ensure file pointer is positioned after header
    os.lseek(fd, FILE_HEADER_SIZE, os.SEEK_SET)
    file_reader = io.FileIO(fd, mode="rb", closefd=False)

    while True:
        needed = RECOVERY_CHUNK_SIZE - buf_len
        nread = file_reader.readinto(view[buf_len : buf_len + needed])
        buf_len += nread
        if buf_len == 0:
            break

        pos = 0

        while pos + 20 <= buf_len:
            # Check record magic byte 0xA7
            if view[pos] != REC_MAGIC:
                torn_detected = True
                torn_offset = file_offset + pos
                start_search = torn_offset + 1
                break

            rmagic, rtype, k_len, v_len, rtxid = unpack_from(REC_HEADER_FMT, view, pos)
            corrupt_txid = max(corrupt_txid, rtxid)

            # Sanity check fields
            if rtype not in (TYPE_PUT, TYPE_DELETE, TYPE_COMMIT):
                torn_detected = True
                torn_offset = file_offset + pos
                start_search = torn_offset + 20
                break
            if k_len > MAX_KEY_LEN or v_len > MAX_VAL_LEN:
                torn_detected = True
                torn_offset = file_offset + pos
                start_search = torn_offset + 20
                break
            if rtype == TYPE_COMMIT and (k_len != 0 or v_len != 4):
                torn_detected = True
                torn_offset = file_offset + pos
                start_search = torn_offset + 24
                break

            rec_size = 20 + k_len + v_len

            # Check if full record is within current buffer
            if pos + rec_size > buf_len:
                if nread > 0 and (needed > 0):
                    # We can still read more bytes from disk
                    break
                else:
                    # EOF reached without full record -> torn write
                    torn_detected = True
                    torn_offset = file_offset + pos
                    start_search = file_size
                    break

            # Verify CRC32
            payload_len = 16 + k_len + v_len
            expected_crc, = unpack_from("<I", view, pos + payload_len)
            calc_crc = crc32(view[pos : pos + payload_len]) & 0xFFFFFFFF
            if expected_crc != calc_crc:
                torn_detected = True
                torn_offset = file_offset + pos
                start_search = torn_offset + rec_size
                break

            rec_file_offset = file_offset + pos

            if rtype == TYPE_PUT:
                k = bytes(view[pos + 16 : pos + 16 + k_len])
                v_off = rec_file_offset + 16 + k_len
                pending_ops.append((TYPE_PUT, k, v_off, v_len, rtxid))
            elif rtype == TYPE_DELETE:
                k = bytes(view[pos + 16 : pos + 16 + k_len])
                pending_ops.append((TYPE_DELETE, k, 0, 0, rtxid))
            elif rtype == TYPE_COMMIT:
                op_count, = unpack_from("<I", view, pos + 16)
                txids_match = all(op[4] == rtxid for op in pending_ops)
                if len(pending_ops) == op_count and txids_match:
                    # Apply all batch ops atomically to index
                    for op_type, k, v_off, v_l, _ in pending_ops:
                        if op_type == TYPE_PUT:
                            index[k] = (v_off, v_l)
                        elif op_type == TYPE_DELETE:
                            index.pop(k, None)
                    last_valid_commit_offset = rec_file_offset + rec_size
                    if rtxid > max_txid:
                        max_txid = rtxid
                    pending_ops.clear()
                else:
                    # Op count or txid mismatch: torn or uncommitted batch
                    torn_detected = True
                    torn_offset = rec_file_offset
                    start_search = rec_file_offset + rec_size
                    break

            pos += rec_size

        if torn_detected:
            if pending_ops:
                corrupt_txid = max(corrupt_txid, max(op[4] for op in pending_ops))
            # Stop scan immediately; uncommitted ops in pending_ops are ignored
            pending_ops.clear()
            break

        # Advance file_offset by consumed bytes
        file_offset += pos
        unconsumed = buf_len - pos
        if unconsumed > 0:
            buffer[:unconsumed] = buffer[pos : buf_len]
        buf_len = unconsumed

        if nread == 0:
            # Reached true EOF
            break

    stat = os.fstat(fd)
    file_size = stat.st_size

    # Check for mid-file corruption vs torn tail
    if torn_detected:
        min_search_txid = max(max_txid, corrupt_txid)
        later_commit = _find_valid_commit_after(fd, start_search, file_size, min_txid=min_search_txid)
        if later_commit is not None:
            raise CorruptionError(
                f"Mid-file corruption detected at offset {torn_offset}: valid commit marker found later at offset {later_commit}. Truncation aborted to protect committed data."
            )

    # Truncate torn or uncommitted tail if file extends past last valid commit
    if file_size > last_valid_commit_offset:
        os.ftruncate(fd, last_valid_commit_offset)
        os.fsync(fd)

    os.lseek(fd, last_valid_commit_offset, os.SEEK_SET)
    next_txid = max_txid + 1
    return index, next_txid, last_valid_commit_offset


def check_db_integrity(db_path: str) -> dict:
    """
    Performs a non-mutating validation check of the database file.
    Returns a dictionary of integrity metrics or raises CorruptionError.
    """
    if not os.path.exists(db_path):
        return {
            "status": "OK",
            "file_size": 0,
            "valid_commits": 0,
            "valid_ops": 0,
            "live_keys": 0,
            "uncommitted_bytes": 0,
            "clean": True,
        }

    file_size = os.path.getsize(db_path)
    if file_size < FILE_HEADER_SIZE:
        return {
            "status": "CORRUPTED",
            "error": f"File size ({file_size} bytes) is smaller than header size ({FILE_HEADER_SIZE})",
            "file_size": file_size,
            "clean": False,
        }

    with open(db_path, "rb") as f:
        header_bytes = f.read(FILE_HEADER_SIZE)
        magic, ver, _ = struct.unpack_from(FILE_HEADER_FMT, header_bytes, 0)
        if magic != FILE_MAGIC or ver != FILE_VERSION:
            return {
                "status": "CORRUPTED",
                "error": f"Invalid header: magic={magic!r}, version={ver}",
                "file_size": file_size,
                "clean": False,
            }

        valid_commits = 0
        valid_ops = 0
        max_txid = 0
        live_keys = {}
        pending_ops = []
        last_valid_offset = FILE_HEADER_SIZE
        corrupt_txid = 0
        start_search = FILE_HEADER_SIZE

        buffer = bytearray(RECOVERY_CHUNK_SIZE)
        view = memoryview(buffer)
        buf_len = 0
        file_offset = FILE_HEADER_SIZE

        unpack_from = struct.unpack_from
        crc32 = zlib.crc32

        while True:
            needed = RECOVERY_CHUNK_SIZE - buf_len
            nread = f.readinto(view[buf_len : buf_len + needed])
            buf_len += nread
            if buf_len == 0:
                break

            pos = 0
            torn = False
            while pos + 20 <= buf_len:
                if view[pos] != REC_MAGIC:
                    torn = True
                    torn_offset = file_offset + pos
                    start_search = torn_offset + 1
                    break

                rmagic, rtype, k_len, v_len, rtxid = unpack_from(REC_HEADER_FMT, view, pos)
                corrupt_txid = max(corrupt_txid, rtxid)

                if rtype not in (TYPE_PUT, TYPE_DELETE, TYPE_COMMIT):
                    torn = True
                    torn_offset = file_offset + pos
                    start_search = torn_offset + 20
                    break
                if k_len > MAX_KEY_LEN or v_len > MAX_VAL_LEN:
                    torn = True
                    torn_offset = file_offset + pos
                    start_search = torn_offset + 20
                    break
                if rtype == TYPE_COMMIT and (k_len != 0 or v_len != 4):
                    torn = True
                    torn_offset = file_offset + pos
                    start_search = torn_offset + 24
                    break

                rec_size = 20 + k_len + v_len
                if pos + rec_size > buf_len:
                    if nread > 0 and (needed > 0):
                        break
                    else:
                        torn = True
                        torn_offset = file_offset + pos
                        start_search = file_size
                        break

                payload_len = 16 + k_len + v_len
                expected_crc, = unpack_from("<I", view, pos + payload_len)
                calc_crc = crc32(view[pos : pos + payload_len]) & 0xFFFFFFFF
                if expected_crc != calc_crc:
                    torn = True
                    torn_offset = file_offset + pos
                    start_search = torn_offset + rec_size
                    break

                rec_file_offset = file_offset + pos

                if rtype == TYPE_PUT:
                    k = bytes(view[pos + 16 : pos + 16 + k_len])
                    pending_ops.append((TYPE_PUT, k, rtxid))
                elif rtype == TYPE_DELETE:
                    k = bytes(view[pos + 16 : pos + 16 + k_len])
                    pending_ops.append((TYPE_DELETE, k, rtxid))
                elif rtype == TYPE_COMMIT:
                    op_count, = unpack_from("<I", view, pos + 16)
                    txids_match = all(op[2] == rtxid for op in pending_ops)
                    if len(pending_ops) == op_count and txids_match:
                        for op_t, k, _ in pending_ops:
                            if op_t == TYPE_PUT:
                                live_keys[k] = True
                            elif op_t == TYPE_DELETE:
                                live_keys.pop(k, None)
                        valid_commits += 1
                        valid_ops += op_count
                        if rtxid > max_txid:
                            max_txid = rtxid
                        last_valid_offset = rec_file_offset + rec_size
                        pending_ops.clear()
                    else:
                        torn = True
                        torn_offset = rec_file_offset
                        start_search = rec_file_offset + rec_size
                        break

                pos += rec_size

            if torn:
                if 'torn_offset' not in locals():
                    torn_offset = file_offset + pos
                    start_search = torn_offset + 20
                if pending_ops:
                    corrupt_txid = max(corrupt_txid, max(op[2] for op in pending_ops))
                # Check for mid-file corruption vs torn tail
                min_search_txid = max(max_txid, corrupt_txid)
                later_commit = _find_valid_commit_after(f.fileno(), start_search, file_size, min_txid=min_search_txid)
                if later_commit is not None:
                    return {
                        "status": "CORRUPTED",
                        "error": f"Mid-file corruption at offset {torn_offset} followed by valid commit marker at {later_commit}",
                        "file_size": file_size,
                        "valid_commits": valid_commits,
                        "valid_ops": valid_ops,
                        "uncommitted_bytes": file_size - last_valid_offset,
                        "clean": False,
                    }
                break

            file_offset += pos
            unconsumed = buf_len - pos
            if unconsumed > 0:
                buffer[:unconsumed] = buffer[pos : buf_len]
            buf_len = unconsumed

            if nread == 0:
                break

        uncommitted_bytes = file_size - last_valid_offset
        return {
            "status": "OK",
            "file_size": file_size,
            "valid_commits": valid_commits,
            "valid_ops": valid_ops,
            "live_keys": len(live_keys),
            "uncommitted_bytes": uncommitted_bytes,
            "clean": uncommitted_bytes == 0,
        }

