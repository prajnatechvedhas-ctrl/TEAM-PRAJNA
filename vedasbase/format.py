"""
Vedas Base - On-Disk Record and File Format Definitions.

Byte Layout:
File Header (16 bytes):
+--------------------+---------------+------------------+
| Magic 'VEDASDB1'   | Version (u32) | Reserved (u32)   |
| (8 bytes)          | (4 bytes)     | (4 bytes)        |
+--------------------+---------------+------------------+

Record Layout (Variable Length):
+-------+------+---------+---------+--------+-----------+-------------+---------+
| Magic | Type | Key Len | Val Len |  TxID  | Key Bytes | Value Bytes |  CRC32  |
| 0xA7  |  u8  |   u16   |   u32   |  u64   | (Key Len) |  (Val Len)  |   u32   |
| (1 B) | (1B) |  (2 B)  |  (4 B)  | (8 B)  |           |             |  (4 B)  |
+-------+------+---------+---------+--------+-----------+-------------+---------+
|<------------- Header (16 Bytes) --------->|
|<------------------------ CRC32 Covers ------------------------->|

COMMIT Marker Layout (24 bytes):
+-------+------+---------+---------+--------+------------------+---------+
| Magic | Type | Key Len | Val Len |  TxID  |  Op Count (u32)  |  CRC32  |
| 0xA7  |  3   |    0    |    4    |  u64   |     (4 bytes)    |   u32   |
| (1 B) | (1B) |  (2 B)  |  (4 B)  | (8 B)  |                  |  (4 B)  |
+-------+------+---------+---------+--------+------------------+---------+
"""

import struct
import zlib
from .errors import InvalidInputError

FILE_MAGIC = b"VEDASDB1"
FILE_VERSION = 1
FILE_HEADER_SIZE = 16
FILE_HEADER_FMT = "<8sII"

REC_MAGIC = 0xA7
REC_HEADER_SIZE = 16
REC_TRAILER_SIZE = 4
REC_HEADER_FMT = "<BBHIQ"  # magic(1), type(1), key_len(2), val_len(4), txid(8) = 16 bytes

TYPE_PUT = 1
TYPE_DELETE = 2
TYPE_COMMIT = 3

MIN_KEY_LEN = 1
MAX_KEY_LEN = 1024
MIN_VAL_LEN = 0
MAX_VAL_LEN = 1024 * 1024  # 1 MB = 1,048,576 bytes
MAX_BATCH_OPS = 10000

def validate_key(key: bytes) -> bytes:
    """Validates that key is bytes and within [1, 1024] bytes."""
    if not isinstance(key, (bytes, bytearray, memoryview)):
        raise InvalidInputError(f"Key must be bytes, got {type(key).__name__}")
    key_bytes = bytes(key)
    if not (MIN_KEY_LEN <= len(key_bytes) <= MAX_KEY_LEN):
        raise InvalidInputError(
            f"Key length must be between {MIN_KEY_LEN} and {MAX_KEY_LEN} bytes, got {len(key_bytes)}"
        )
    return key_bytes

def validate_value(value: bytes) -> bytes:
    """Validates that value is bytes and within [0, 1048576] bytes."""
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise InvalidInputError(f"Value must be bytes, got {type(value).__name__}")
    val_bytes = bytes(value)
    if not (MIN_VAL_LEN <= len(val_bytes) <= MAX_VAL_LEN):
        raise InvalidInputError(
            f"Value length must be between {MIN_VAL_LEN} and {MAX_VAL_LEN} bytes, got {len(val_bytes)}"
        )
    return val_bytes

def encode_file_header() -> bytes:
    """Encodes the 16-byte file header."""
    return struct.pack(FILE_HEADER_FMT, FILE_MAGIC, FILE_VERSION, 0)

def encode_put_record(key: bytes, value: bytes, txid: int) -> bytes:
    """Encodes a PUT record with its trailing CRC32."""
    k_len = len(key)
    v_len = len(value)
    hdr = struct.pack(REC_HEADER_FMT, REC_MAGIC, TYPE_PUT, k_len, v_len, txid)
    payload = hdr + key + value
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return payload + struct.pack("<I", crc)

def encode_delete_record(key: bytes, txid: int) -> bytes:
    """Encodes a DELETE record with its trailing CRC32."""
    k_len = len(key)
    hdr = struct.pack(REC_HEADER_FMT, REC_MAGIC, TYPE_DELETE, k_len, 0, txid)
    payload = hdr + key
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return payload + struct.pack("<I", crc)

def encode_commit_record(op_count: int, txid: int) -> bytes:
    """Encodes a COMMIT record with its trailing CRC32."""
    hdr = struct.pack(REC_HEADER_FMT, REC_MAGIC, TYPE_COMMIT, 0, 4, txid)
    payload = hdr + struct.pack("<I", op_count)
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return payload + struct.pack("<I", crc)

