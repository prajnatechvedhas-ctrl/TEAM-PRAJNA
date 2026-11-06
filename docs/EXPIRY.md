# Key Expiry Design and Subtle Failure Modes in Vedas Base

## 1. Architectural Strategy: Storing Expiration in the Record Format

To add Time-To-Live (TTL) or expiration timestamps to Vedas Base without altering the existing v1 format, a version bump to **`VEDASDB2`** (file version 2) is required.

### Proposed Record Layout (v2)
In version 2, the 16-byte record header is expanded to 24 bytes by introducing an 8-byte little-endian unsigned integer `expire_at_ms`:
```
+-------+------+---------+---------+--------+------------------+
| Magic | Type | Key Len | Val Len |  TxID  |  Expire At (u64) |
| 0xA7  |  u8  |   u16   |   u32   |  u64   |    Unix ms (LE)  |
| (1 B) | (1B) |  (2 B)  |  (4 B)  | (8 B)  |     (8 Bytes)    |
+-------+------+---------+---------+--------+------------------+
|<---------------- Header (24 Bytes) ------------------------->|
```
- A value of `0` denotes **no expiration** (infinite TTL).
- A non-zero value denotes the absolute millisecond timestamp (UTC) at which the key becomes invalid.
- In-memory index maps: `key -> (val_offset, val_len, expire_at_ms)`.

---

## 2. Subtle Issues and Failure Modes

### 2.1 Expiry Evaluation: Read-Time vs. Recovery-Time
- **Read-Time Evaluation (Correct)**:
  When `get(key)` is invoked:
  ```python
  val_off, val_len, expire_at_ms = self._index[key]
  if expire_at_ms != 0 and current_time_ms() >= expire_at_ms:
      return None
  ```
  This is $O(1)$ and imposes zero background overhead.
- **Recovery-Time Evaluation (DANGEROUS / FLAWED)**:
  Evaluating expiration against the wall clock during `open()` recovery violates **replay determinism**. If recovery drops expired records based on `time.time()`, the exact recovered state of the database depends on *when* the computer rebooted. Replaying the log must be a pure, deterministic function of the on-disk byte stream. Recovery must strictly record `expire_at_ms` in the index without dropping keys.

### 2.2 System Clock Changes and Monotonic Skew
- Wall clocks (`time.time()`) are subject to NTP steps, leap seconds, daylight saving adjustments, and manual user clock resets.
- If an operator rolls back their system clock by 2 hours, previously expired keys could suddenly become live again.
- **Mitigation**: Store monotonic offsets tied to transaction generation or combine wall-clock timestamps with logical commit sequence numbers.

### 2.3 Expired Keys Surviving Compaction
- In an append-only engine without active purge threads, expired keys occupy disk space indefinitely unless compaction removes them.
- During online `compact()`:
  - If a key has `expire_at_ms != 0 and current_time_ms() >= expire_at_ms`, the compactor omits it from the compacted file.
  - **Subtle Race with Active Transactions**: The compactor must evaluate expiration under the global lock at the start of compaction to ensure no writer updates the key concurrently.

### 2.4 Interaction with Atomic Batches
- A batch might contain both immortal keys and expiring keys.
- If `batch([("put", "k1", "v1", ttl=100ms), ("put", "k2", "v2", ttl=0)])` commits, atomicity dictates both are committed durably.
- If the process crashes 200 ms later, upon recovery both keys are present in the log. `k2` is readable; `k1` is lazily filtered out upon subsequent reads.

### 2.5 Interaction with the Independent Oracle
- Testing TTL with chaos crashes is notoriously difficult because kills happen asynchronously.
- The oracle model must maintain `(val, expire_at_ms)`. When verifying `get(k)` or `dump()`, the oracle must query using the exact timestamp of the request, accounting for millisecond jitter between child process execution and oracle verification.
