# Key Expiry Design and Subtle Failure Modes in Vedas Base

## 1. Architectural Strategy: Storing Expiration in the Record Format

To add Time-To-Live (TTL) or expiration timestamps to Vedas Base without breaking backwards compatibility with the existing v1 format, a version bump to **`VEDASDB2`** (file version 2) is required.

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

## 2. Explicit Judge Questions & Concrete Answers

### Question 1: What happens to an expired key that is overwritten in a batch?
**Answer**:
1. **At Log Append Time**:
   - The engine appends the batch's op record(s) sequentially. If key `k` previously expired, the new op record for `k` (with a fresh value and either a new `expire_at_ms` or `0` for no expiration) is appended to the log.
   - The engine writes the batch `COMMIT` marker and calls `os.fsync(fd)`.
2. **At In-Memory Index Update Time (B5 Rule)**:
   - The in-memory index updates `self._index[k] = (new_offset, new_len, new_expire_at_ms)`.
   - The previously expired entry is completely superseded in memory.
3. **At Read Time (`get`)**:
   - Subsequent `get(k)` calls inspect the newly written index entry. Since the entry reflects the new transaction, `get(k)` returns the new value until the *new* expiration timestamp is crossed.
   - If the batch op was a `delete(k)`, the key is removed from `self._index`, returning `NOTFOUND` regardless of its prior expiration state.
4. **During Compaction (`compact`)**:
   - The compactor streams only the live entry referenced in `self._index`. The earlier obsolete, expired record in the log is dropped from the compacted file.

### Question 2: What happens when the clock goes backwards (NTP skew, VM migration)?
**Answer**:
- **Why Wall Clock (`time.time()`) vs. Monotonic Clock (`time.monotonic()`)?**
  - **Wall Clock (UTC Unix Timestamp)**: Necessary for persistent storage across process restarts and reboots. `time.monotonic()` resets to zero on system reboot and cannot be interpreted across different processes or reboots. Therefore, `expire_at_ms` on disk must store UTC wall-clock time.
  - **Monotonic Clock**: Used internally for timeout loops and kill-delay intervals in torture testing, but cannot be serialized to disk.
- **What Happens When the System Clock Goes Backwards?**
  1. *Scenario A (Clock Skewed Backward)*: Suppose key `k` was set to expire at $T = 1000$. The current time is $T = 1050$ (key is expired). An NTP step or VM hypervisor sync shifts the system clock backward to $T = 900$.
     - *Result*: Because read-time evaluation checks `current_time_ms >= expire_at_ms`, key `k` will temporarily appear **live again** until the clock re-advances past $T = 1000$.
  2. *Scenario B (Clock Skewed Forward)*: A clock jump forward causes keys to expire earlier than intended in real wall-clock elapsed time.
- **Edge Case Protection & System-Level Mitigations**:
  - **Compaction Immobility**: Compaction must never evaluate expiration using a clock that has stepped backward past the transaction commit timestamp.
  - **Monotonic Logical Timestamps (Hybrid Clock)**: In production distributed engines, the solution is a Hybrid Logical Clock (HLC): `max(wall_clock, last_recorded_tx_time)`. This ensures logical time never steps backward on disk, preventing "resurrected" keys even during severe NTP step-backs.

---

## 3. Subtle Issues and Failure Modes

### 3.1 Expiry Evaluation: Read-Time vs. Recovery-Time
- **Read-Time Evaluation (Correct)**:
  When `get(key)` is invoked:
  ```python
  val_off, val_len, expire_at_ms = self._index[key]
  if expire_at_ms != 0 and current_time_ms() >= expire_at_ms:
      return None
  ```
  This is $O(1)$ and imposes zero background overhead.
- **Recovery-Time Evaluation (DANGEROUS / FLAWED)**:
  Evaluating expiration against the wall clock during `open()` recovery violates **replay determinism**. If recovery drops expired records based on `time.time()`, the recovered state of the database depends on *when* the computer rebooted. Replaying the log must be a pure, deterministic function of the on-disk byte stream. Recovery must strictly load `expire_at_ms` into the index without dropping keys.

### 3.2 Interaction with Atomic Batches
- A single batch may contain both immortal keys (`expire_at_ms = 0`) and short-lived expiring keys (`expire_at_ms = T`).
- Durability and atomicity guarantee that either **all** keys in the batch commit or **none** commit.
- If the process crashes and restarts after $T$, both records exist durably in the log; on read, the immortal key returns its value, while the expired key returns `NOTFOUND`.

### 3.3 Expired Keys and Disk Reclamation
- In an append-only engine without active background purge threads, expired keys occupy disk space indefinitely unless `compact()` runs.
- During online `compact()`:
  - If a key in `self._index` satisfies `expire_at_ms != 0 and current_time_ms() >= expire_at_ms`, the compactor omits it from the compacted file.
  - Compaction evaluates expiration under the global lock, preventing any race condition with concurrent writers.
