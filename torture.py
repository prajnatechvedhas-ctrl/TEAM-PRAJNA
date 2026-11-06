#!/usr/bin/env python3
"""
Vedas Base - Torture Tool launcher.
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(REPO_ROOT, "src")
TORTURE_DIR = os.path.join(REPO_ROOT, "torture")
for d in (SRC_DIR, TORTURE_DIR):
    if d not in sys.path:
        sys.path.insert(0, d)

from runner import main

if __name__ == "__main__":
    main()

