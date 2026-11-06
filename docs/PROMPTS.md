# Vedas Base - Engineering Prompts and Hackathon Log

## Project Origin
- **Hackathon**: "Kill It and Prove It" (24-Hour National Engineering Hackathon)
- **Role**: Senior Storage-Engine Engineer
- **Goal**: Build "Vedas Base", a single-process embedded key-value database in pure Python (standard library only) with provable durability, crash-safety, and corruption resistance.

## Core Directives Followed
1. **Zero External Dependencies**: Standard library only (`os`, `struct`, `zlib`, `threading`, `io`, `sys`, `argparse`, `subprocess`, `random`, `time`, `json`).
2. **Single Data File**: All authoritative state resides in exactly one file; sidecar lockfile and atomic compaction temp files.
3. **Strict 7-Step Write Ordering**: Checked space -> write op records -> fsync -> write COMMIT marker -> fsync -> update in-memory index -> return.
4. **Chaos Verification**: Independent torture harness with external oracle and hard kill testing across all failure modes.
