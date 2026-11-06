# Vedas Base: Embedded Key-Value Storage Engine

> Built for the 24-hour national engineering hackathon **"Kill It and Prove It"**.  
> Pure Python 3.11+ using **ONLY** the standard library. Zero external dependencies.

---

## 1. Quick Start & Execution

### Run / Build Command (Pure Python Standard Library)
No compilation or external packages needed. Uses Python 3.11+ standard library only:
```bash
# Verify environment
python --version

# On Windows (PowerShell or Command Prompt):
.\kvdb open my.db
.\torture --iterations 200 --seed 42

# On Linux / macOS / Git Bash:
./kvdb open my.db
./torture --iterations 200 --seed 42

# Or directly via Python:
python vedasbase.py open my.db
python torture.py --iterations 200 --seed 42
```

### Run All Unit & Crash Tests
```bash
python -m unittest discover -s tests -v
```

### Run Chaos Torture Test (Judge Demo Commands)
```bash
# Standard 200/500-iteration random kill run
./torture --iterations 200 --seed 42

# Targeted judge torture modes
./torture --iterations 100 --seed 999 --mode large-values
./torture --iterations 100 --seed 7 --mode compaction
./torture --iterations 100 --seed 42 --mode recovery-crash
./torture --iterations 100 --seed 123 --mode disk-full
```

---

## 2. CLI Commands Reference

Supports both `./kvdb <cmd> <path> ...` (positional database path) and `--db <path> <cmd> ...`.

| Command | Usage | Description |
|---|---|---|
| `open` | `./kvdb open <path> [--max-bytes N]` | Opens or creates database, runs crash recovery, and prints key count and recovery time. |
| `put` | `./kvdb put <path> <key> <value>` | Inserts or updates key-value pair as an atomic durable transaction. Supports `--hex`. |
| `get` | `./kvdb get <path> <key>` | Reads value for key. Prints value, or `NOTFOUND` with exit code 2. Supports `--hex`. |
| `del` | `./kvdb del <path> <key>` | Deletes key as an atomic durable transaction. Deleting a missing key is not an error. Supports `--hex`. |
| `batch` | `./kvdb batch <path> <file>` | Applies batch of ops from file (one op per line: `put k v` or `del k`), JSON string, or `-` (stdin). |
| `dump` | `./kvdb dump <path> [--hex]` | Outputs all live key-value pairs sorted lexicographically by key (`<key> <val>` or hex if binary/`--hex`). |
| `check` | `./kvdb check <path> [--json]` | Verifies file integrity and CRCs. Prints `OK` (exit 0) or corruption reason to stderr (exit 1). |
| `compact` | `./kvdb compact <path>` | Reclaims disk space by compacting live keys into a new file using atomic replacement. |
| `serve` | `./kvdb serve <path> [--workers N]` | Long-running server on stdin/stdout. Processes JSON or text lines; prints `ACK <op-id>` only after durability. |

---

## 3. On-Disk Binary Format

Authoritative state is confined to **exactly one file**. All multi-byte integers are serialized as **fixed-width little-endian** via Python's standard `struct`.

### 3.1 File Header (16 Bytes)
```
+------------------------------------+------------------+------------------+
| Magic "VEDASDB1"                   | Version (u32)    | Reserved (u32)   |
| 8 bytes                            | 4 bytes (=1)     | 4 bytes (=0)     |
+------------------------------------+------------------+------------------+
```

### 3.2 Record Layout (Variable Length)
```
+-------+------+---------+---------+--------+-----------+-------------+---------+
| Magic | Type | Key Len | Val Len |  TxID  | Key Bytes | Value Bytes |  CRC32  |
| 0xA7  |  u8  |   u16   |   u32   |  u64   | (Key Len) |  (Val Len)  |   u32   |
| (1 B) | (1B) |  (2 B)  |  (4 B)  | (8 B)  |           |             |  (4 B)  |
+-------+------+---------+---------+--------+-----------+-------------+---------+
|<------------- Header (16 Bytes) --------->|
|<------------------------ CRC32 Covers ------------------------->|
```

- **Record Header (16 bytes)**:
  - `Magic` (1B): `0xA7`
  - `Type` (1B): `1 = PUT`, `2 = DELETE`, `3 = COMMIT`
  - `Key Len` (2B): `uint16` (1 to 1,024 bytes)
  - `Val Len` (4B): `uint32` (0 to 1,048,576 bytes)
  - `TxID` (8B): `uint64` monotonically increasing transaction counter
- **Payload**:
  - `Key Bytes`: Variable length raw bytes
  - `Value Bytes`: Variable length raw bytes
- **Trailer (4 bytes)**:
  - `CRC32` (4B): IEEE 802.3 CRC32 calculated over `[Header + Key + Value]`

### 3.3 COMMIT Marker Layout (24 Bytes)
```
+-------+------+---------+---------+--------+------------------+---------+
| Magic | Type | Key Len | Val Len |  TxID  |  Op Count (u32)  |  CRC32  |
| 0xA7  |  3   |    0    |    4    |  u64   |     (4 bytes)    |   u32   |
| (1 B) | (1B) |  (2 B)  |  (4 B)  | (8 B)  |                  |  (4 B)  |
+-------+------+---------+---------+--------+------------------+---------+
```

---

## 4. Recovery in Plain English

When a database is opened (`open()` in `engine.py` / `recovery.py`):
1. **Remove Old Compaction Clutter**: Any `.compact.*.tmp` files from aborted compaction crashes are immediately unlinked (`recovery.cleanup_temp_files`).
2. **Read File Header**: The first 16 bytes are read. If the file is 0 bytes (crashed on creation), the header is written and synced. Otherwise, it verifies `magic == b"VEDASDB1"` and `version == 1`.
3. **Scan Records in 8 MB Chunks**: The file is scanned sequentially starting at byte offset 16 using memoryviews and `struct.unpack_from`.
4. **Validate Record Magic and CRC32**: Every record must start with `0xA7`. The CRC32 checksum over the record header and payload is verified. If a CRC mismatch or EOF is encountered mid-record, the scan halts immediately.
5. **Enforce Atomic Commit Markers**: Operations (`PUT` or `DELETE`) are queued into memory as `pending_ops`. They are **only** applied to the active index when a valid `COMMIT` marker is encountered whose `op_count` matches `len(pending_ops)`. If an uncommitted batch is truncated or torn, it is discarded.
6. **Idempotently Truncate Torn Tails**: Any bytes trailing the last valid commit marker are cleanly truncated using `os.ftruncate(fd, last_commit_offset)` followed by `os.fsync(fd)`.

---

## 5. Write Ordering & Crash Safety Invariant

Every write operation strictly executes 7 steps under a single global lock:
```
1. Check max_bytes (write NOTHING if quota exceeded)
2. os.write(fd, op_records)
3. os.fsync(fd)
4. os.write(fd, commit_record)
5. os.fsync(fd)
6. Update in-memory hash index
7. Return success / ACK
```
An ACK is **never** emitted before Step 5 is durable on disk. Readers execute under the same lock, so no client can observe a half-applied transaction.

### Windows PowerShell Execution Examples

```powershell
# 1. Open database and print status
.\.venv\Scripts\python.exe vedasbase.py --db test.db open

# 2. Insert key-value pairs
.\.venv\Scripts\python.exe vedasbase.py --db test.db put alpha "Hello Vedas"
.\.venv\Scripts\python.exe vedasbase.py --db test.db put --hex 01020304 deadbeef

# 3. Retrieve key
.\.venv\Scripts\python.exe vedasbase.py --db test.db get alpha

# 4. Delete key
.\.venv\Scripts\python.exe vedasbase.py --db test.db del alpha

# 5. Execute atomic batch of operations
.\.venv\Scripts\python.exe vedasbase.py --db test.db batch '[{"type": "put", "key": "k1", "val": "v1"}, {"type": "del", "key": "k1"}, {"type": "put", "key": "k2", "val": "v2"}]'

# 6. Dump all live keys sorted lexicographically
.\.venv\Scripts\python.exe vedasbase.py --db test.db dump

# 7. Check database integrity
.\.venv\Scripts\python.exe vedasbase.py --db test.db check

# 8. Reclaim disk space via online compaction
.\.venv\Scripts\python.exe vedasbase.py --db test.db compact

# 9. Run server mode on stdin/stdout with 4 worker threads
.\.venv\Scripts\python.exe vedasbase.py --db test.db serve --workers 4

# 10. Run 200 crash iterations with torture tool
.\.venv\Scripts\python.exe torture.py --iterations 200 --seed 42
```

