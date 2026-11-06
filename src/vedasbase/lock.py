"""
Vedas Base - Single-Process File Locking.

Provides exclusive process-level locking via a sidecar lockfile (<db_path>.lock)
using OS-level primitives (msvcrt on Windows, fcntl on Unix).
"""

import os
import sys
from .errors import LockError

if os.name == "nt":
    import msvcrt
else:
    import fcntl

class FileLock:
    """Manages an exclusive, non-blocking process lock on a database file."""

    def __init__(self, db_path: str):
        self.db_path = os.path.abspath(db_path)
        self.lock_path = self.db_path + ".lock"
        self._fd = None

    def acquire(self):
        """Acquires the lock or raises LockError."""
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0)
        try:
            self._fd = os.open(self.lock_path, flags, 0o644)
        except OSError as e:
            raise LockError(f"Failed to open lock file '{self.lock_path}': {e}") from e

        try:
            if os.name == "nt":
                # Lock first byte non-blocking
                msvcrt.locking(self._fd, msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as e:
            # Another process holds the lock
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
            raise LockError(f"Database '{self.db_path}' is already locked by another process.") from e

        # Write current PID into lock file
        try:
            os.ftruncate(self._fd, 0)
            os.lseek(self._fd, 0, os.SEEK_SET)
            pid_str = f"{os.getpid()}\n".encode("ascii")
            os.write(self._fd, pid_str)
        except OSError:
            # PID recording is advisory, lock is already held at kernel level
            pass

    def release(self):
        """Releases the lock and closes the lock file descriptor."""
        if self._fd is None:
            return
        
        try:
            if os.name == "nt":
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        except OSError:
            pass

        try:
            os.close(self._fd)
        except OSError:
            pass
        finally:
            self._fd = None

        # Clean up lock file if possible
        try:
            os.unlink(self.lock_path)
        except OSError:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

