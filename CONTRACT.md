# Vedas Base - CLI & Server Interface Contract

This document formally specifies the command-line interface, parsing rules, encoding standards, and server protocols for Vedas Base.

---

## 1. CLI Commands Specification

The executable wrapper (`kvdb`, `kvdb.cmd`, or `python kvdb.py`) accepts both positional syntax and `--db <path>` syntax.

```bash
# Positional syntax (Primary judge format):
kvdb <command> <path> [arguments...]

# Flag syntax:
kvdb --db <path> <command> [arguments...]
```

### 1.1 `open <path> [--max-bytes N]`
- **Purpose**: Opens or creates the database file, executes sequential crash recovery, verifies file headers and record checksums, truncates uncommitted tails idempotently, and reports readiness.
- **Output**: `OK keys=<count> size=<offset> recovery_time=<seconds>s`
- **Exit Code**: `0` on success, non-zero on failure.

### 1.2 `put <path> <key> <value>`
- **Purpose**: Appends an atomic, durable key-value pair under the global mutex, forcing physical `os.fsync` before returning.
- **Arguments**:
  - `key`: Key string (or hex if `--hex`).
  - `value`: Value string (or hex if `--hex`).
- **Output**: `OK`
- **Exit Code**: `0` on success.

### 1.3 `get <path> <key>`
- **Purpose**: Reads the current value for a given key.
- **Output**:
  - If found: prints value to stdout.
  - If not found: prints exactly `NOTFOUND`.
- **Exit Code**: `0` if found, `2` if not found (`NOTFOUND`).

### 1.4 `del <path> <key>`
- **Purpose**: Removes the specified key by writing a durable tombstone record. Deleting a non-existent key is not an error.
- **Output**: `OK`
- **Exit Code**: `0` on success.

### 1.5 `batch <path> <file>`
- **Purpose**: Executes an atomic transaction consisting of up to 10,000 operations from `<file>` (or `-` for standard input).
- **Format**:
  - **Text line format**: One operation per line:
    - `put <key> <value>`: `<key>` is the first space-delimited token; `<value>` is the entire remainder of the line (including all spaces).
    - `del <key>`: Deletes the specified `<key>`.
  - **JSON format**: Array of objects: `[{"cmd": "put", "key": "...", "val": "..."}, {"cmd": "del", "key": "..."}]`.
- **Semantics**: The entire file forms **ONE single atomic batch**. Rule B5 applies: operations execute in sequential order; the last operation on a key wins.
- **Output**: `OK`
- **Exit Code**: `0` on success.

### 1.6 `dump <path> [--hex]`
- **Purpose**: Outputs all live keys and values to standard output, one pair per line, sorted bytewise (lexicographically by key bytes).
- **Encoding Standard**:
  - If `--hex` is specified: prints `<hex_key> <hex_value>`.
  - If `--hex` is omitted:
    - If key and value are valid UTF-8 and contain no whitespace (` `) or newline characters (`\n`, `\r`): prints `<key> <val>`.
    - If either key or value contains spaces, newlines, or non-UTF-8 binary bytes: encoded as `<hex_key> <hex_value>`.
- **Exit Code**: `0` on success.

### 1.7 `check <path> [--json]`
- **Purpose**: Read-only integrity validation. Sequentially reads the log, verifies every record's IEEE 802.3 CRC32, checks commit marker operation counts and transaction IDs, and detects torn tails versus mid-file corruption.
- **Output**:
  - Clean database: prints exactly `OK` to stdout (Exit code `0`).
  - Corrupted database: prints a one-line explanation to stderr (Exit code `1`).
  - If `--json` is specified: outputs detailed JSON health report.
- **Invariant**: `check` **never** modifies the database file.

### 1.8 `compact <path>`
- **Purpose**: Reclaims storage space by copying all active live keys to a temporary file, fsyncing, and atomically swapping via `os.replace`.
- **Output**: `OK new_size=<bytes>`
- **Exit Code**: `0` on success.

### 1.9 `serve <path> [--max-bytes N] [--workers N]`
- **Purpose**: Long-running multi-threaded storage server communicating over `stdin` and `stdout`.
- **Concurrency**: Spawns worker threads sharing the database engine's single global lock.
- **Durability Invariant**: Every ACK is printed **only after** physical durability (`os.fsync` of the commit marker) is complete. Output is explicitly flushed immediately.

---

## 2. Server Input Protocols (`serve`)

`kvdb serve` accepts both JSON-lines and interactive plain-text commands.

### 2.1 JSON-Lines Protocol (Used by automated test harnesses)
```json
{"op_id": "1", "cmd": "put", "key": "k1", "val": "v1"}
{"op_id": "2", "cmd": "get", "key": "k1"}
{"op_id": "3", "cmd": "del", "key": "k1"}
{"op_id": "4", "cmd": "batch", "ops": [{"cmd": "put", "key": "a", "val": "1"}]}
{"op_id": "5", "cmd": "compact"}
{"op_id": "6", "cmd": "close"}
```

### 2.2 Human-Friendly Line Protocol (For interactive judge testing)
Judges can type commands directly into `stdin`:
```text
# Single operations with auto-generated op-ids:
put user_1 alice
get user_1
del user_1
compact
close

# Explicit op-id prefix:
101 put session_101 active
102 get session_101
103 del session_101

# Multi-line atomic batch block:
batch b_42
put account_a 500
put account_b 1000
del account_c
end
```
When `end` or `commit` is entered, the multi-line batch is committed atomically and prints `ACK b_42`.

### 2.3 Server Responses
- Success: `ACK <op-id>`
- Value retrieval: `VAL <op-id> <val>`
- Missing key: `NOTFOUND <op-id>`
- Error / Disk Full: `ERR <op-id> <error_message>`

