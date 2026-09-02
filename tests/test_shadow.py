import tempfile
import unittest
from pathlib import Path

from agent_farm_runtime.shadow import inspect_workspace, parse_awaiting


class ShadowTests(unittest.TestCase):
    def test_parse_explicit_job(self):
        self.assertEqual(parse_awaiting("waiting on job:12345"), ("job", "12345"))

    def test_no_guess_without_explicit_fact(self):
        with tempfile.TemporaryDirectory() as td:
            ws = Path(td)
            record = inspect_workspace(ws, live_cwds=set())
            self.assertTrue(record.v2_would.startswith("UNKNOWN("))

    def test_artifact_condition(self):
        with tempfile.TemporaryDirectory() as td:
            ws = Path(td)
            (ws / ".awaiting").write_text("artifact:result.dat\n", encoding="utf-8")
            first = inspect_workspace(ws, live_cwds=set())
            self.assertTrue(first.v2_would.startswith("WOULD_WAIT"))
            (ws / "result.dat").write_text("ok", encoding="utf-8")
            second = inspect_workspace(ws, live_cwds=set())
            self.assertEqual(second.v2_would, "WOULD_WAKE")


if __name__ == "__main__":
    unittest.main()
