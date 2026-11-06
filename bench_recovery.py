"""
Benchmark script for Vedas Base 1,000,000 key recovery path.
Tests recovery scanning with buffered reads, memoryview, struct.unpack_from, and zlib.crc32.
"""
import os
import sys
import time
import struct
import zlib

MAGIC_HEADER = b"VEDASDB1"
FILE_HEADER_FMT = "<8sII"  # magic (8), version (4), reserved (4) = 16 bytes
REC_HEADER_FMT = "<BBHIQ"  # magic (1), type (1), key_len (2), val_len (4), txid (8) = 16 bytes
REC_MAGIC = 0xA7
TYPE_PUT = 1
TYPE_DELETE = 2
TYPE_COMMIT = 3

def create_sample_db(filename, num_keys=1_000_000, batch_size=1000):
    print(f"Creating sample DB file '{filename}' with {num_keys} keys (batch size {batch_size})...")
    t0 = time.perf_counter()
    with open(filename, "wb") as f:
        # File header
        header = struct.pack(FILE_HEADER_FMT, MAGIC_HEADER, 1, 0)
        f.write(header)
        
        txid = 0
        buf = bytearray()
        val = b"v" * 16  # 16-byte value
        
        for i in range(0, num_keys, batch_size):
            txid += 1
            cur_batch = min(batch_size, num_keys - i)
            for j in range(cur_batch):
                k_int = i + j
                key = f"key_{k_int:08d}".encode('ascii') # 12 bytes
                k_len = len(key)
                v_len = len(val)
                rec_hdr = struct.pack(REC_HEADER_FMT, REC_MAGIC, TYPE_PUT, k_len, v_len, txid)
                payload = rec_hdr + key + val
                crc = zlib.crc32(payload) & 0xFFFFFFFF
                buf.extend(payload)
                buf.extend(struct.pack("<I", crc))
            
            # Commit marker: type=3, key_len=0, val_len=4, txid=txid, payload = cur_batch (u32)
            commit_hdr = struct.pack(REC_HEADER_FMT, REC_MAGIC, TYPE_COMMIT, 0, 4, txid)
            commit_payload = commit_hdr + struct.pack("<I", cur_batch)
            commit_crc = zlib.crc32(commit_payload) & 0xFFFFFFFF
            buf.extend(commit_payload)
            buf.extend(struct.pack("<I", commit_crc))
            
            if len(buf) >= 4 * 1024 * 1024:
                f.write(buf)
                buf.clear()
        
        if buf:
            f.write(buf)
            buf.clear()
            
    t1 = time.perf_counter()
    size_mb = os.path.getsize(filename) / (1024 * 1024)
    print(f"Sample DB created: {size_mb:.2f} MB in {t1 - t0:.2f} s")


def run_recovery_benchmark(filename):
    print(f"\n--- Running Recovery Benchmark on '{filename}' ---")
    file_size = os.path.getsize(filename)
    
    # Strategy: Read in large chunks (e.g. 8MB or 16MB) or mmap?
    # Spec: "Do not use mmap or any helper in a grey area."
    # Use large sequential buffered reads with memoryview
    
    chunk_sizes = [4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024]
    
    for CHUNK_SIZE in chunk_sizes:
        t0 = time.perf_counter()
        
        index = {}
        pending_ops = [] # list of (type, key, val_off, val_len)
        last_valid_commit_offset = 16
        
        unpack_from = struct.unpack_from
        crc32 = zlib.crc32
        
        with open(filename, "rb") as f:
            # 1. Read and validate file header
            header_bytes = f.read(16)
            if len(header_bytes) < 16:
                raise ValueError("Truncated file header")
            magic, ver, res = unpack_from(FILE_HEADER_FMT, header_bytes, 0)
            if magic != MAGIC_HEADER or ver != 1:
                raise ValueError("Invalid file header")
            
            buffer = bytearray(CHUNK_SIZE)
            view = memoryview(buffer)
            buf_len = 0
            file_offset = 16  # position in file of view[0]
            
            while True:
                # Read more into remaining space in buffer
                needed = CHUNK_SIZE - buf_len
                nread = f.readinto(view[buf_len : buf_len + needed])
                buf_len += nread
                if buf_len == 0:
                    break
                
                pos = 0
                while pos + 20 <= buf_len:
                    # Quick check magic byte
                    if view[pos] != REC_MAGIC:
                        # Corrupted or uncommitted record
                        break
                    
                    # Unpack record header
                    rmagic, rtype, k_len, v_len, rtxid = unpack_from(REC_HEADER_FMT, view, pos)
                    rec_size = 20 + k_len + v_len
                    
                    if pos + rec_size > buf_len:
                        # Record is split across chunks
                        break
                    
                    # CRC check
                    expected_crc, = unpack_from("<I", view, pos + 16 + k_len + v_len)
                    # Payload for CRC is header (16) + key + val
                    calc_crc = crc32(view[pos : pos + 16 + k_len + v_len]) & 0xFFFFFFFF
                    if expected_crc != calc_crc:
                        # Torn write or corruption!
                        break
                    
                    rec_file_offset = file_offset + pos
                    
                    if rtype == TYPE_PUT:
                        key = bytes(view[pos + 16 : pos + 16 + k_len])
                        val_offset = rec_file_offset + 16 + k_len
                        pending_ops.append((TYPE_PUT, key, val_offset, v_len))
                    elif rtype == TYPE_DELETE:
                        key = bytes(view[pos + 16 : pos + 16 + k_len])
                        pending_ops.append((TYPE_DELETE, key, 0, 0))
                    elif rtype == TYPE_COMMIT:
                        op_count, = unpack_from("<I", view, pos + 16)
                        if len(pending_ops) == op_count:
                            for op_type, k, v_off, v_l in pending_ops:
                                if op_type == TYPE_PUT:
                                    index[k] = (v_off, v_l)
                                elif op_type == TYPE_DELETE:
                                    index.pop(k, None)
                            last_valid_commit_offset = rec_file_offset + rec_size
                        pending_ops.clear()
                    
                    pos += rec_size
                
                # Slide unconsumed bytes to front
                file_offset += pos
                unconsumed = buf_len - pos
                if unconsumed > 0:
                    buffer[:unconsumed] = buffer[pos : buf_len]
                buf_len = unconsumed
                
                if nread == 0:
                    # Reached EOF
                    break
        
        t1 = time.perf_counter()
        elapsed = t1 - t0
        print(f"Chunk size: {CHUNK_SIZE // (1024*1024)} MB | Recovered {len(index)} keys in {elapsed:.3f} s ({len(index)/elapsed:,.0f} keys/s)")

if __name__ == "__main__":
    fname = "bench_test_1m.db"
    try:
        create_sample_db(fname, num_keys=1_000_000, batch_size=1000)
        run_recovery_benchmark(fname)
    finally:
        if os.path.exists(fname):
            os.remove(fname)

