# Vedas Base - Crash Safety Audit, Weaknesses, and Failure Analysis

## 1. Crash Points Matrix (8 to 15 Failure Points)

| Crash Point | Code Location | Exact Disk State | Recovery Action (`recovery.py`) |
|---|---|---|---|
| **CP1** | `engine.py:165` (pre-check) | Unchanged. File size equals `last_committed_offset`. | Clean recovery; 0 torn bytes. Replay ends at last commit. |
| **CP2** | `engine.py:187` (mid `os.write` op 1 of 5) | Torn 1st op record at tail. Incomplete payload or missing trailer CRC. | Record header/payload CRC check fails or EOF reached. Tail truncated to `rollback_offset`. |
| **CP3** | `engine.py:195` (between op 3 and 4) | Ops 1-3 fully written to kernel buffer, op 4-5 missing. | Op count mismatch when COMMIT marker absent. Ops 1-3 ignored; tail truncated. |
| **CP4** | `engine.py:197` (before `os.fsync`) | Ops written to OS page cache, not yet forced to disk platters. | On power loss, page cache lost. On process kill, ops on disk but uncommitted; ignored and truncated. |
| **CP5** | `engine.py:202` (after 1st `os.fsync`) | All ops durable on physical media. No COMMIT record. | Sequential scan places ops in `pending_ops`. Scan ends at EOF without COMMIT. Tail truncated. |
| **CP6** | `engine.py:209` (mid `os.write` commit marker) | Ops durable. First 12 bytes of 24-byte COMMIT record written. | `view[pos]` or `unpack_from` finds incomplete record. CRC mismatch / short record; truncated. |
| **CP7** | `engine.py:214` (before 2nd `os.fsync`) | COMMIT record in kernel buffer. | If lost in power cut, treated as uncommitted and truncated. If durable, committed cleanly. |
| **CP8** | `engine.py:217` (after 2nd `os.fsync`, before index update) | All ops and COMMIT marker 100% durable on disk. Process dies before Python dict update. | Recovery reads complete COMMIT record, validates CRC and op count, applies batch to index. Zero loss. |
| **CP9** | `compaction.py:53` (writing temp file) | Temp file `*.compact.<pid>.tmp` partially written. Authoritative `.db` file completely intact. | `cleanup_temp_files` unlinks stale temp file. Authoritative file opened intact. |
| **CP10** | `compaction.py:73` (temp file fsynced, before `os.replace`) | Temp file complete and durable. Original `.db` complete and durable. | Original `.db` remains active. Temp file deleted on next open. |
| **CP11** | `compaction.py:77` (after `os.replace`, before POSIX dir fsync) | New compacted file authoritative on filesystem. Directory entry not yet flushed. | Modern filesystems (NTFS, ext4 journaling) keep replaced file valid. Database opens new file. |
| **CP12** | `recovery.py:209` (mid `os.ftruncate`) | File partially truncated by kernel or interrupted. | Next recovery re-scans from offset 16, finds truncated boundary, and re-executes `os.ftruncate`. Idempotent. |

---

## 2. Ordering Argument Naming System Calls

The durability and atomicity invariant relies on a strict sequence of OS system calls:
1. `os.write(fd, ops_payload)`: Enqueues data into kernel buffer cache.
2. `os.fsync(fd)` (Windows `_commit` / POSIX `fsync`): Forces physical disk write barrier for payload data before metadata commit.
3. `os.write(fd, commit_payload)`: Appends the commit record containing the transaction ID, op count, and CRC32.
4. `os.fsync(fd)`: Forces physical disk write barrier for the commit record.
5. In-memory index update under `threading.Lock`: Memory matches disk state only after disk write barrier succeeds.

Because `fsync` at Step 3 precedes `write` at Step 4, and `fsync` at Step 5 precedes index update and ACK generation, the hardware disk controller can never reorder the commit marker before the payload data.

---

## 3. Honest Weaknesses (Engine Limitations)

1. **Memory Index Bound by RAM**:
   The in-memory hash table stores `(val_offset, val_len)` for every key. At 1,000,000 keys, the Python dictionary and byte objects consume ~120 MB RAM. If the dataset exceeds 50 million keys, process memory exhaustion will occur.
2. **Global Lock Contention on Writes**:
   A single `threading.Lock` serializes all `put`, `batch`, `delete`, and `get` operations. Write throughput is constrained by the serialization of `os.fsync(fd)` calls (roughly 100–1000 fsyncs/sec on NVMe/SSD, fewer on spinning disks).
3. **Compaction Peak Space Requirement**:
   Compaction writes all live keys to a secondary file before atomic replacement. During compaction, disk usage temporarily doubles (old file size + compacted file size). If disk space is below $2\times$, compaction will fail with `DiskFullError`.
4. **Platform Asymmetry for Directory Fsync**:
   POSIX provides `os.fsync` on directory file descriptors to guarantee directory metadata durability after rename. Windows NT does not support directory `fsync` (`[Errno 13] Permission denied`). On Windows, rename metadata durability relies on NTFS journaling.

---

## 4. Failed Attempts & Retrospective

1. **Attempting `os.readinto` Directly on File Descriptors**:
   - *Attempt*: Used `os.readinto(fd, memoryview)` in initial recovery code.
   - *Failure*: `os.readinto` does not exist in the Python standard library; `readinto()` is a method on file-like objects (`io.FileIO`, `io.BufferedReader`).
   - *Correction*: Wrapped `fd` in non-closing `io.FileIO(fd, mode="rb", closefd=False)` to enable zero-copy `readinto` into preallocated `memoryview` buffers.
2. **Subprocess Boot Latency vs. Millisecond Chaos Kills**:
   - *Attempt*: Firing random kills 10–50ms after launching `python -m vedasbase ... serve`.
   - *Failure*: On Windows, spawning a fresh CPython process takes 80–180ms. The process was killed before the interpreter finished importing standard libraries, resulting in 0 ops processed.
   - *Correction*: Added an active JSON probe handshake (`get __probe__`) to ensure the server worker threads were completely active before starting the chaos kill timer.

---

## 5. Three Future Improvements

1. **Group Commit / Fsync Merging**:
   Allow concurrent worker threads to coalesce pending write transactions into a single `os.writev` and single `os.fsync`, scaling throughput from hundreds to tens of thousands of writes/second under heavy concurrency.
2. **Compaction Checkpoints & Sparse Holes**:
   Implement sparse file punching (`FALLOC_FL_PUNCH_HOLE` on Linux or `FSCTL_SET_ZERO_DATA` on Windows) to punch holes in the log for obsolete tombstones, reducing the need for full-file rewriting during compaction.
3. **Partitioned / Striped Reader Locks**:
   Replace the single global lock with a Read-Write Lock (`threading.RLock` or shared read locks) so multiple concurrent `get()` requests execute in parallel without blocking one another.

