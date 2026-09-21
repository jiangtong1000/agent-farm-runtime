"""Make the replay helpers importable as `fakes` regardless of the pytest rootdir."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "replay"))
