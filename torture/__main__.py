"""
Entry point for python -m torture
"""
import sys
import os

# Add src to sys.path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from .runner import main

if __name__ == "__main__":
    main()
