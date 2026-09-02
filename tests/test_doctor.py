import tempfile
import unittest
from pathlib import Path

from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.models import Task, TaskState
from agent_farm_runtime.store import FarmPaths, TaskStore


class DoctorTests(unittest.TestCase):
    def test_waiting_requires_condition(self):
        with tempfile.TemporaryDirectory() as td:
            paths = FarmPaths(Path(td) / ".farm")
            paths.ensure()
            TaskStore(paths).create(Task("T1", "x", "out", "check", state=TaskState.WAITING))
            checks = run_doctor(paths)
            self.assertTrue(any(c.level == "FAIL" and "waiting_on" in c.message for c in checks))

    def test_done_requires_acceptance_receipt(self):
        with tempfile.TemporaryDirectory() as td:
            paths = FarmPaths(Path(td) / ".farm")
            paths.ensure()
            TaskStore(paths).create(Task("T1", "x", "out", "check", state=TaskState.DONE))
            checks = run_doctor(paths)
            self.assertTrue(any(c.level == "FAIL" and "acceptance_receipt" in c.message for c in checks))


if __name__ == "__main__":
    unittest.main()
