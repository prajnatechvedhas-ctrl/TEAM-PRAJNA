#!/usr/bin/env python3
"""
Vedas Base - kvdb CLI launcher.
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from vedasbase.cli import main

if __name__ == "__main__":
    main()
