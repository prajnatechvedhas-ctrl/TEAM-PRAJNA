# Vedas Base - Chronological AI Prompt Log

> Requirement (Deliverable 4, Section 9 of Hackathon Problem Statement):
> "A file PROMPTS.md containing every prompt you sent to any AI tool during the hackathon,
> in chronological order, unedited. Include the tool name and approximate time."

**AI Assistant Tool**: Google Antigravity / Gemini 2.0  
**Environment**: Windows 11, PowerShell, Python 3.12 (Standard Library Only)

---

## Prompt 1
- **Timestamp**: `2026-11-06T10:32:36Z`
- **Step Index**: `0`
- **Tool**: Google Antigravity / Gemini

```markdown
You are a senior storage-engine engineer. Build "Vedas Base": a single-process embedded key-value database in Python 3.11+, using ONLY the Python standard library, for a 24-hour national engineering hackathon called "Kill It and Prove It". Work carefully and rigorously. Quality and honesty matter more than speed or feature count.

## 0. HARD RULES
- No SQLite, dbm, shelve, LevelDB, RocksDB, LMDB, Redis, or any third-party package. No library that provides WAL, B-tree, or LSM for me. Standard library only (os, struct, zlib, threading, io, sys, argparse, subprocess, random, time, json, signal, etc.).
- Do not use mmap or any helper in a grey area.
- All authoritative state lives in exactly ONE data file. A lock file or a temp file during compaction is allowed.
- Design: append-only log + in-memory hash index + ONE global lock. Do not add complexity (no caches, no threads for background work, no checkpoint/hint files).
- If you are unsure about an API, say "I'm not sure". Never invent standard-library functions. State the Python version you target.
- You cannot be sure the code is correct. Do NOT claim it is crash-safe, durable, or bug-free. State exactly what you did to make each property hold, and which parts are unverified until run. Actually run the code and the tests in this environment, and report real results only.
- Do not change the design, file format, or write ordering below. If you think something is wrong, tell me and explain BEFORE changing it. When editing, show only changed parts and say what changed.
- Before writing code, ask me any questions about genuinely ambiguous requirements. Then wait for my answers. For everything else use my defaults below and list them in the assumptions list.

## 1. API AND LIMITS
- open(path, max_bytes), put(key, value), get(key), delete(key), batch(ops), close().
- Keys: 1 to 1,024 bytes. Values: 0 to 1 MB (1,048,576 bytes). At least 1,000,000 keys. Batches up to 10,000 ops.
- Validate every input before writing. Reject invalid input with a clear excepti
<truncated 6226 bytes>
torture.py, tests/, docs.

## 9. FINAL REPORT (after code and test runs)
1. Line-by-line crash audit: for every write, fsync, rename and truncate, quote the line and state what is on disk if the process is killed right after it, and what open() does.
2. Hostile review: find at least 5 ways the code could violate the invariant (lost ACKed write, partial batch, torn value, failed open) and point to the exact lines. Fix any real bug you find, but tell me before changing the design.
3. Spec check: go through each requirement above and say "met" (with the function name) or "not met" (with the reason). Mark nothing as met without pointing to code or a test result you actually ran.
4. Compile/runtime honesty: list platform differences (Windows vs Linux for fsync, rename, directory fsync, file locking) and anything likely to fail.
5. Explain recovery in plain English, naming the exact functions and system calls. Then give three manual test cases (e.g. truncate 1 byte, kill mid-batch, corrupt a CRC) with the expected result for each.
6. Assumptions list, each labeled "from my spec" or "my own guess".
7. Real measured results: 1M-key recovery time, torture runs (iterations, seeds, modes) and their outcomes.

## 10. ORDER OF WORK
(a) Ask clarifying questions. (b) Benchmark the 1M-key recovery path. (c) Minimal engine: open/put/get/delete/close. (d) Durability + batch commit markers. (e) Torture tool and run it. (f) Concurrency, large values, large batches. (g) Compaction, disk-full, recovery-crash. (h) Docs and final report. Fix every bug the torture tool finds before moving on. Make small, meaningful commits at each phase.
```

---

## Prompt 2
- **Timestamp**: `2026-11-06T13:54:41Z`
- **Step Index**: `1187`
- **Tool**: Google Antigravity / Gemini

```markdown
Analyze my VEDAS database engine and its existing torture.py script.

I want to perform advanced database torture testing.

Test:

1. 10,000 random database operations.
2. Concurrent read and write operations.
3. Random crashes during write operations.
4. Database recovery after crashes.
5. Data corruption detection.
6. Key deletion and compaction.
7. Large values and database size limits.
8. Data consistency after recovery.

Do not modify the existing database engine or tests without my approval. First inspect the current implementation, identify existing test coverage, and propose a safe testing plan.
```

---

## Prompt 3
- **Timestamp**: `2026-11-06T13:59:16Z`
- **Step Index**: `1203`
- **Tool**: Google Antigravity / Gemini

```markdown
APPROVED. Proceed with implementing the Advanced VEDAS Torture Testing Suite.

Important requirements:

1. Do NOT modify the existing database engine unless absolutely necessary and explicitly approved.
2. Do NOT break existing tests or the current torture.py implementation.
3. Create a separate advanced test suite.
4. Perform all destructive crash and corruption tests on temporary database copies.
5. Implement exactly 10,000 mixed operations with an independent verification oracle.
6. Include concurrent readers and writers.
7. Verify crash recovery, atomic batches, CRC integrity and compaction.
8. Measure execution time, throughput, recovery time and database size.
9. Run all existing unit tests after implementation.
10. Do not report simulated or assumed results as actual results.

First implement the test suite, then execute it and provide a detailed report showing PASSED and FAILED tests.

Preserve the existing working VEDAS implementation.
```

---

## Prompt 4
- **Timestamp**: `2026-11-06T14:42:06Z`
- **Step Index**: `1320`
- **Tool**: Google Antigravity / Gemini

```markdown
You are a hostile, senior storage-engine reviewer and engineer. The repository "Vedas Base" (Python 3.12, standard library only, Windows 11 + PowerShell, run via .\.venv\Scripts\python.exe) already exists: vedasbase/, vedasbase.py, torture.py, bench_recovery.py, tests/, docs/. This is a national hackathon, "Kill It and Prove It". Judges will run my commands on MY Windows laptop, offline. Your job: audit the existing code against the requirements below, fix real bugs, and produce honest evidence. Do not rewrite from scratch. Do not change the on-disk format or write ordering without telling me why first and waiting for my OK. Show only changed code parts and what changed. Report only results you actually ran. Never claim the code is crash-safe or bug-free; state what is verified and what is not. If unsure about an API, say "I'm not sure"; never invent stdlib functions.

## A. FIXED DESIGN (do not change)
Append-only log + in-memory hash index (key -> value offset/len) + ONE global lock, standard library only, exactly ONE data file (lock file and compaction temp file allowed). Record: magic u8 | type u8 (PUT/DELETE/COMMIT) | key_len u16 | val_len u32 | txid u64 | key | value | crc32 u32 (CRC covers header + payload), fixed-width little-endian. A batch = op records, fsync, COMMIT marker (op_count + own CRC), fsync, then index update under the lock, then return. A single put/delete is a batch of one. Batch rule B5: ops apply in order, last op on a key wins. Limits: keys 1-1024 B, values 0-1 MB, 1,000,000+ keys, batches up to 10,000 ops, configurable max_bytes enforced BEFORE writing (nothing written, or rolled back to the last commit offset, plus a clear DiskFullError; DB stays readable; compaction frees space).

## B. WINDOWS-SPECIFIC AUDIT (check each, fix if wrong)
1. Every os.open must include os.O_BINARY (use getattr(os, "O_BINARY", 0)). Verify no text-mode I/O, no newline translation, and that sys.stdin.buffer / sys.stdout.buffer are used in serve mode with a flush after every ACK.
2. Process loc
<truncated 11648 bytes>
, state exactly what you did to make each property hold, and which parts you could not verify without running it."
	5.	Line-by-line crash audit: "For every write, fsync, rename, and truncate in the code, state: if the process is killed immediately after this line, what is on disk, and what does open() do? Quote the line."
	6.	Self-review for bugs: "Now act as a hostile reviewer. Find at least 5 ways this code could violate the invariant (lost ACKed write, partial batch, torn value, failed open). Point to the exact lines. If you find none, explain why you checked."
	7.	Check against my spec: "Compare your code against my requirements one by one. For each, say 'met' with the function name, or 'not met' with the reason. Do not mark anything met unless you can point to the code."
	8.	Compile-level honesty: "You cannot run this code. List anything likely to fail at compile or run time: wrong imports, type mismatches, platform differences (Windows vs Linux fsync and rename behavior)."
	9.	No silent changes: "Do not change my design, file format, or write ordering. If you think something is wrong, tell me and explain before changing it. When you edit code, show only the changed parts and say what changed."
	10.	Evidence-based explanation: "Explain how recovery works in plain English, naming the exact functions and system calls. Then give three test cases I can run by hand (e.g. truncate 1 byte, kill mid-batch) with the expected result for each."
```

---

## Prompt 5
- **Timestamp**: `2026-11-06T16:18:14Z`
- **Step Index**: `1576`
- **Tool**: Google Antigravity / Gemini

```markdown
these are our hackathon conditions, analyze it properly
```

---

## Prompt 6
- **Timestamp**: `2026-11-06T16:49:08Z`
- **Step Index**: `1746`
- **Tool**: Google Antigravity / Gemini

```markdown
test whether the database passes all the conditons and constraints given in the pdf
```

---
