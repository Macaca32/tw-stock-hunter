"""Shared test fixtures for tw-stock-hunter tests."""

import sys
from pathlib import Path

# Ensure core/ is importable as bare modules (matches production convention)
CORE_DIR = Path(__file__).parent.parent / "core"
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))
